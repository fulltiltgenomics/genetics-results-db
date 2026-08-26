"""Per-execution (`jti`) request-count and concurrency limits for sandbox callers.

Design of record: `docs/code-execution-security.md` section 4 in genetics-results-suite.

db-api already meters a sandbox execution by BigQuery BYTES (`api/main.py`,
`SANDBOX_AGGREGATE_BYTES_BUDGET`, keyed on the same `jti`). That bounds SPEND and bounds
nothing else. This module is the availability half, and it is the same rule results-api
enforces in `app/core/sandbox_budget.py`: one in-process map keyed on the token's `jti`,
checked **before** the work starts, answering 429 with the same payload shape and the same
`code` vocabulary, so an operator reading a 429 from either service reads one thing.

**Why db-api needs it at all**, given the byte budget: the byte budget only charges paths that
run a BigQuery job. `/health`, `/docs`, `/redoc`, `/openapi.json`, an unmatched path, `/schema`
on a categorical-value cache hit and `/stats`' `get_table` metadata loop all cost this pod real
work and charge the budget nothing, so a script can issue them in a loop at unbounded
concurrency for its whole 60-120 second wall clock. db-api is `replicas: 1` at `cpu: 500m` /
`memory: 512Mi` and is on the browser's chat path (chat-backend calls it, as does mcp-server),
so one execution saturating it is a user-visible outage of something the sandbox does not own.

**Why ASGI middleware rather than a dependency.** `require_auth` is an app-level
`Depends`, and a dependency — with its teardown — is solved only for a MATCHED route. An
unmatched path 404s out of the router with no dependency ever entered, so it would be neither
counted nor released; `_sweep_locked` cannot reclaim an entry with `in_flight > 0` either, so
the leak would be permanent. Middleware runs before routing and sees every request, which is
also why `SandboxBudgetMiddleware` resolves the sandbox principal off the raw ASGI headers
itself instead of reading what `require_auth` will later put on `request.state`.

**Requests that carry no sandbox token are not touched at all.** chat-backend and mcp-server
authenticate with `INTERNAL_API_SECRET` and the kubelet probes `/health` with nothing; none of
them creates an entry here, and none of these limits can ever reject one. That is the property
that keeps production chat working, and `tests/test_sandbox_budget.py` pins it.

**Reject, never queue.** A queued request burns wall clock the script cannot see and can
complete after the execution is already dead. A fast, labelled 429 lets it back off instead.

**In-process, so `replicas: 1` is load-bearing.** At N replicas these bound per replica, not
per execution — the same caveat the byte budget already carries.

Two deliberate deviations from the byte budget next door in `api/main.py`, both matching
results-api rather than `_jti_bytes`:

1. **Eviction on token expiry, not LRU.** `_jti_bytes` trims to 1024 entries LRU, which *can*
   drop a live execution and silently reset its budget — the fail-open direction. Here an entry
   is evictable only once its token can no longer authenticate a request **and** it has nothing
   in flight; when `MAX_TRACKED` is reached it is a *new* execution that is refused, never a
   running one that is evicted.
2. **Env-configurable**, so an operator can widen or tighten without a rebuild.

`_jti_bytes` is deliberately left exactly as it is: two per-`jti` maps with different eviction
policies is an oddity worth knowing about, not a thing to fix in the same change as this.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass

try:  # packaged as `api.` in the image, run as a bare module in some scripts
    from api import sandbox_auth
except ImportError:  # pragma: no cover
    import sandbox_auth

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """Read a positive integer limit, failing at import rather than at the first request.

    Every value here is a *ceiling* and `admit` compares with `>=`, so 0 or a negative turns the
    limit into "reject every sandbox request" — a silent, total outage of the sandbox data path
    that no health check would attribute to a typo in a manifest.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(
            f"{name} must be a positive integer, got {value}. Every limit here is a ceiling "
            "compared with >=, so 0 or a negative rejects every sandbox request."
        )
    return value


# Requests one execution may issue. The byte budget does not bound a loop of requests that run
# no BigQuery job at all, and every one of those still costs this pod a request. 1000 over 120
# seconds is ~8 rps, well above any legitimate loop of queries.
SANDBOX_MAX_REQUESTS_PER_EXECUTION = _env_int("SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1000)
# In-flight requests per execution. db-api holds a BigQuery result set in memory while it
# serialises it, on a pod limited to 512Mi; concurrency is the control here with a
# memory-and-CPU exhaustion failure mode rather than a spend one.
SANDBOX_MAX_CONCURRENT_REQUESTS = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS", 4)
# In-flight sandbox requests across the whole pod, i.e. across all executions. The sandbox runs
# at concurrency 1 with replicas 1, so today the per-execution limit binds first; this exists so
# that raising the sandbox's own concurrency cannot silently multiply this pod's peak load.
SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 8)
# Hard bound on the map itself, so a flood of distinct `jti`s cannot grow it without limit.
# Entries live at most one token lifetime (~305s), so at the measured peak of 23 chat turns/hour
# this is never approached; it is a backstop, not a working limit.
SANDBOX_MAX_TRACKED_EXECUTIONS = _env_int("SANDBOX_MAX_TRACKED_EXECUTIONS", 4096)

if SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL < SANDBOX_MAX_CONCURRENT_REQUESTS:
    # the pod-wide bound is meant to sit *above* the per-execution one; inverted, a single
    # execution can never reach its own allowance and the per-execution number in the manifest
    # is a lie an operator would have to read the code to catch
    raise ValueError(
        "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL "
        f"({SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL}) must be >= "
        f"SANDBOX_MAX_CONCURRENT_REQUESTS ({SANDBOX_MAX_CONCURRENT_REQUESTS}): the pod-wide "
        "bound is a ceiling over all executions and cannot be tighter than one execution's."
    )


@dataclass
class _Execution:
    """One execution's running totals. `expires_at` is the token's `exp`."""

    expires_at: int
    requests: int = 0
    in_flight: int = 0


@dataclass(frozen=True)
class Rejection:
    """A refused admission. `code` names *which* limit, so a 429 is actionable in a log."""

    code: str
    limit: int
    observed: int
    detail: str


_executions: dict[str, _Execution] = {}
_in_flight_total = 0
_lock = threading.Lock()


def _sweep_locked(now: float) -> None:
    """Drop entries that can never be charged again.

    Evictable means **both**: the token has passed the point where `verify_sandbox_token` would
    still accept it (`exp` plus that verifier's own leeway), so no further request can present
    that `jti`; and nothing is in flight under it. Anything else is a live execution, and
    evicting one would silently reset its counters — the fail-open direction, and the reason
    this is not `_jti_bytes`' LRU.
    """
    cutoff = now - sandbox_auth.LEEWAY_SECONDS
    for jti in [
        jti
        for jti, entry in _executions.items()
        if entry.in_flight == 0 and entry.expires_at < cutoff
    ]:
        del _executions[jti]


def admit(principal: sandbox_auth.SandboxPrincipal) -> Rejection | None:
    """Reserve a request slot for `principal`'s execution, or say why not.

    Returns `None` when admitted — the caller **must** then call `release` exactly once — or a
    `Rejection` to be turned into a 429. Checked before routing, so a spent execution costs no
    handler work and no BigQuery job.
    """
    global _in_flight_total
    jti = principal.execution_id
    with _lock:
        entry = _executions.get(jti)
        if entry is None:
            _sweep_locked(time.time())
            if len(_executions) >= SANDBOX_MAX_TRACKED_EXECUTIONS:
                # refuse the new execution rather than evict a live one: an evicted counter is a
                # reset budget, which is the failure that matters
                return Rejection(
                    code="sandbox_execution_tracker_full",
                    limit=SANDBOX_MAX_TRACKED_EXECUTIONS,
                    observed=len(_executions),
                    detail=(
                        "Too many sandbox executions are being tracked by this pod "
                        f"({len(_executions)} of {SANDBOX_MAX_TRACKED_EXECUTIONS}). Retry in a "
                        "few minutes."
                    ),
                )
            entry = _executions[jti] = _Execution(expires_at=principal.expires_at)

        if entry.requests >= SANDBOX_MAX_REQUESTS_PER_EXECUTION:
            return Rejection(
                code="sandbox_request_count",
                limit=SANDBOX_MAX_REQUESTS_PER_EXECUTION,
                observed=entry.requests,
                detail=(
                    "Request limit for this execution reached: "
                    f"{entry.requests} of {SANDBOX_MAX_REQUESTS_PER_EXECUTION} requests. "
                    "Ask for more in fewer queries and aggregate in the sandbox."
                ),
            )
        if entry.in_flight >= SANDBOX_MAX_CONCURRENT_REQUESTS:
            return Rejection(
                code="sandbox_concurrency",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS,
                observed=entry.in_flight,
                detail=(
                    "Too many concurrent requests for this execution: "
                    f"{entry.in_flight} of {SANDBOX_MAX_CONCURRENT_REQUESTS} in flight. "
                    "Await the requests already issued before starting more."
                ),
            )
        if _in_flight_total >= SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL:
            return Rejection(
                code="sandbox_concurrency_pod",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL,
                observed=_in_flight_total,
                detail=(
                    "Too many concurrent sandbox requests on this pod: "
                    f"{_in_flight_total} of {SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL} in flight. "
                    "Retry shortly."
                ),
            )

        entry.requests += 1
        entry.in_flight += 1
        _in_flight_total += 1
        return None


def release(jti: str) -> None:
    """Give back one in-flight slot for `jti`, pod-wide and per-execution.

    Never raises and never goes negative: both counters clamp at zero, and a `jti` whose
    entry has already been swept is a no-op. So releasing more often than admitting cannot
    corrupt the accounting — which is what makes it safe to call unconditionally from the
    caller's `finally`."""
    global _in_flight_total
    with _lock:
        _in_flight_total = max(0, _in_flight_total - 1)
        entry = _executions.get(jti)
        if entry is None:
            return
        entry.in_flight = max(0, entry.in_flight - 1)


def snapshot(jti: str) -> _Execution | None:
    """The running totals for `jti`, for tests and for diagnostics. Never mutated by callers."""
    with _lock:
        entry = _executions.get(jti)
        return None if entry is None else _Execution(**vars(entry))


def reset() -> None:
    """Drop all accounting. Test-only; nothing in the request path calls this."""
    global _in_flight_total
    with _lock:
        _executions.clear()
        _in_flight_total = 0


def log_rejection(rejection: Rejection, path: str | None, principal) -> None:
    logger.warning({
        "message": "sandbox per-execution limit exceeded",
        # deliberately not an `endpoint_access` row: those carry `service`/`log_source`/
        # `principal` from `api/main.py::_access_log_fields`, and a half-populated row in that
        # shared sink is worse than a plain warning beside it
        "endpoint_path": path,
        "code": rejection.code,
        "limit": rejection.limit,
        "observed": rejection.observed,
        "sid": getattr(principal, "session_id", None),
        "jti": getattr(principal, "execution_id", None),
    })


def _sandbox_principal(scope) -> "sandbox_auth.SandboxPrincipal | None":
    """Resolve the execution token straight from the ASGI scope, non-raising.

    The limits above have to be admitted *before* routing — that is the whole point of a
    request-count and a concurrency bound — whereas `request.state.principal` is set later, by
    `api/main.py::require_auth`, and for an unmatched path it is never set at all. So this
    resolves the bearer itself, with the same `is_sandbox_shaped` routing and the same HS256
    decode against the same key, which is why it cannot disagree with what `_caps_for` later
    reads off the request state.

    Never raises, and never rejects: a sandbox-shaped bearer that fails validation is left to
    `require_auth`, which answers the 401 it already answers today. A caller presenting
    `INTERNAL_API_SECRET` (chat-backend, mcp-server) or nothing at all (the `/health` probe)
    returns `None` here and is not counted, gated or tracked in any way.
    """
    # depends on the server lowercasing header names, which the ASGI spec requires and uvicorn
    # (the only server this image runs) does. Under a server that passed `Authorization`
    # through as sent, this returns None and the gate silently becomes a no-op — it fails open
    # for the GATE ONLY, never for auth: `require_auth` reads the header through Starlette's
    # case-insensitive mapping and still answers its 401, so no request becomes authorised that
    # was not before, and chat traffic (which is never gated) is unaffected either way.
    for key, value in scope.get("headers") or ():
        if key == b"authorization":
            header = value.decode("latin-1")
            break
    else:
        return None
    if not header.startswith("Bearer "):
        return None
    token = header[7:]
    if not sandbox_auth.is_sandbox_shaped(token):
        return None
    try:
        return sandbox_auth.verify_sandbox_token(token)
    except sandbox_auth.SandboxTokenError:
        return None


async def _send_429(send, payload: dict) -> None:
    body = json.dumps(payload).encode()
    await send({
        "type": "http.response.start",
        "status": 429,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body, "more_body": False})


class SandboxBudgetMiddleware:
    """Admit and release the per-execution request slot, for **every** request.

    Pure ASGI and a strict no-op for anything that is not a sandbox execution: chat-backend,
    mcp-server and the unauthenticated `/health` probe are never inspected beyond one header
    lookup and never accounted.

    Registered so that it runs before routing, which is the point: `/health`, `/docs`,
    `/redoc`, `/openapi.json`, an unmatched path, a `/schema` cache hit and `/stats`' metadata
    loop all reach this layer, and none of them charges the BigQuery byte budget next door in
    `api/main.py`. The release is in a `finally`, so an exception or a 404 out of the router
    cannot strand a slot — `_sweep_locked` refuses to evict an entry with `in_flight > 0`, so
    a stranded slot would be permanent in both directions.

    The 429 payload is `detail`/`code`/`limit`/`observed`, identical to results-api's, so the
    two services are diagnosable the same way.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        principal = _sandbox_principal(scope)
        if principal is None:
            await self.app(scope, receive, send)
            return

        rejection = admit(principal)
        if rejection is not None:
            log_rejection(rejection, scope.get("path"), principal)
            await _send_429(send, {
                "detail": rejection.detail,
                "code": rejection.code,
                "limit": rejection.limit,
                "observed": rejection.observed,
            })
            return

        try:
            await self.app(scope, receive, send)
        finally:
            release(principal.execution_id)
