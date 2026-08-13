"""
Genetics Results API - BigQuery query service for AI agents.
Provides SQL query interface to genetics fine-mapping and colocalization data.
"""

import hmac
import json
import os
import logging
import re
import sys
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, Forbidden

try:  # packaged as `api.` in the image, run as a bare module in some scripts
    from api import sandbox_auth
except ImportError:  # pragma: no cover
    import sandbox_auth


class _GCPJsonFormatter(logging.Formatter):
    """JSON formatter compatible with GCP Cloud Logging."""

    SEVERITY_MAP = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        msg = record.msg
        if isinstance(msg, dict):
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": self.SEVERITY_MAP.get(record.levelno, "DEFAULT"),
                "logger": record.name,
                **msg,
            }
        else:
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": self.SEVERITY_MAP.get(record.levelno, "DEFAULT"),
                "logger": record.name,
                "message": record.getMessage(),
            }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_GCPJsonFormatter())
logging.root.handlers = [_handler]
logging.root.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
for _name in ("uvicorn.access", "google", "urllib3", "asyncio"):
    logging.getLogger(_name).setLevel(logging.WARNING)


# the service discriminator in the shared endpoint_access sink, where db-api's rows sit beside
# results-api's. Constant and not env-derived on purpose: every previous candidate for this job
# was something that moves. `endpoint_path IS NULL` worked only while db-api emitted no path, and
# LOG_SOURCE below is derived from the environment, carries no service name and has already been
# renamed once in production (genetics-results-api-prod -> finngenie_prod). A discriminator that
# can be renamed by a deploy is not a discriminator; this one can only change by editing this line.
SERVICE = "db-api"

# which *environment* wrote the row — deliberately a separate axis from SERVICE above
LOG_SOURCE = os.environ.get("LOG_SOURCE", "genetics_db_api_prod")

# kubelet probes and the monitor CronJob poll /health with no credentials
_UNAUTHENTICATED_PATHS = {"/health"}

# marks a request verified by the shared secret; the sandbox path leaves a SandboxPrincipal
_INTERNAL_PRINCIPAL = "internal"


def require_auth(request: Request) -> None:
    """Authenticate the caller: a sandbox execution token, or the shared internal secret.

    The shared-secret path serves chat-backend and mcp-server, which already send
    `Authorization: Bearer $INTERNAL_API_SECRET` on every request. Before this the sole
    control was the cluster NetworkPolicy, and mcp-server sits on both sides of that boundary
    — anything able to reach mcp-server could reach BigQuery through it. That path
    deliberately fails open when the secret is unset, so a cluster that has not yet wired the
    env var keeps serving rather than hard-failing mid-rollout.

    The sandbox path does **not** inherit that. A sandbox-shaped bearer (JOSE `alg: HS256`,
    see sandbox_auth) is routed to the sandbox validator *before* the unset-secret early
    return can short-circuit it, and a failure there is a hard 401 — never a fallthrough to
    the shared-secret comparison, which would degrade a malformed token into "is this string
    equal to the secret". The sandbox is the one caller whose input is attacker-authored.

    The resolved principal is left on `request.state.principal` (None for the fail-open and
    /health cases) so handlers can key per-credential behaviour on it.
    """
    request.state.principal = None
    if request.url.path in _UNAUTHENTICATED_PATHS:
        return

    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else ""

    if token and sandbox_auth.is_sandbox_shaped(token):
        try:
            principal = sandbox_auth.verify_sandbox_token(token)
        except sandbox_auth.SandboxTokenError as exc:
            logger.warning({
                "message": "sandbox token rejected",
                "log_type": "endpoint_access",
                "service": SERVICE,
                "log_source": LOG_SOURCE,
                "endpoint_path": request.url.path,
                "http_method": request.method,
                "reason": str(exc),
            })
            raise HTTPException(status_code=401, detail="Unauthorized") from None
        request.state.principal = principal
        logger.info({
            "message": "sandbox request authorized",
            "log_type": "endpoint_access",
            "service": SERVICE,
            "log_source": LOG_SOURCE,
            "endpoint_path": request.url.path,
            "http_method": request.method,
            "principal": "sandbox",
            "sub": principal.user,
            "sid": principal.session_id,
            "jti": principal.execution_id,
        })
        return

    secret = _internal_api_secret()
    if secret is None:
        # nothing to compare against that this process is willing to trust — refuse rather than
        # fall through to the fail-open return below. See `_internal_api_secret`.
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not secret:
        return
    # compare as bytes: compare_digest on str raises TypeError for non-ASCII, which would
    # surface as a 500 instead of failing closed with a 401.
    #
    # The two codecs differ on purpose — do not "fix" the latin-1 one to utf-8. Starlette
    # decodes raw header bytes as latin-1, so re-encoding the presented token with latin-1
    # undoes that decode exactly; utf-8 would re-encode the mojibake instead (b"s\xc3\xa9cret"
    # comes back out as b"s\xc3\x83\xc2\xa9cret").
    #
    # It does NOT recover "the bytes the client sent" in general: measured off a real socket
    # the clients disagree with each other — node fetch/undici and python-requests put latin-1
    # on the wire, aiohttp puts utf-8, and httpx 0.28 refuses to send a non-ASCII header value
    # at all. No codec is right for all of them, so under a hypothetical non-ASCII secret this
    # pairing would favour the aiohttp-shaped caller and 401 the others. What makes the
    # comparison well defined is `_require_ascii_secret` below, which refuses a non-ASCII
    # secret at startup; every codec coincides on ASCII, which is what deployments have.
    #
    # No try/except on the re-encode, unlike results-api's: this takes a starlette Request, so
    # the only str it can see came from starlette's own latin-1 decode and re-encodes by
    # construction. Add the guard if a str-taking entry point is ever introduced here.
    if not auth_header.startswith("Bearer ") or not hmac.compare_digest(
        token.encode("latin-1"), secret.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    request.state.principal = _INTERNAL_PRINCIPAL


def _access_log_fields(
    request: Request | None, endpoint_path: str, http_method: str
) -> dict[str, Any]:
    """The fields every db-api `endpoint_access` line shares with results-api's.

    There is deliberately no `user_email`: db-api sits behind results-api and the internal
    secret rather than in front of users, so its caller is a *service*, not a person. The only
    principal that exists here is the credential that authorized the call, which is what
    `principal` records — inventing a user_email would misrepresent it.
    """
    principal = getattr(request.state, "principal", None) if request is not None else None
    if isinstance(principal, sandbox_auth.SandboxPrincipal):
        principal_name = "sandbox"
    elif principal == _INTERNAL_PRINCIPAL:
        principal_name = _INTERNAL_PRINCIPAL
    else:
        # the fail-open branch: INTERNAL_API_SECRET unset, so nothing was verified
        principal_name = "unauthenticated"
    return {
        "log_type": "endpoint_access",
        "service": SERVICE,
        "log_source": LOG_SOURCE,
        "endpoint_path": endpoint_path,
        "http_method": http_method,
        "principal": principal_name,
    }


def _require_ascii_secret(secret: str) -> None:
    """Refuse a non-ASCII INTERNAL_API_SECRET (`genetics-results-suite-ctq`).

    HTTP clients do not agree on how to put a non-ASCII header value on the wire — node
    fetch/undici and python-requests send latin-1, aiohttp sends utf-8, httpx refuses to send
    one at all — so no server-side codec can recover the same secret from every caller and
    byte-exactness is unachievable in general. The ASCII invariant is what makes `require_auth`
    well defined, so it is enforced here rather than merely written down.

    Failing at startup is the good failure mode: the pod never passes readiness, the rollout
    stalls with the old pods still serving, and the message names the variable — versus every
    internal call 401ing at request time with nothing local saying why.

    Silent when the secret is absent or empty: that is the dev/test configuration, and the
    fail-open branch of `require_auth` (warned about below) already covers it.
    """
    if secret and not secret.isascii():
        raise RuntimeError(
            "INTERNAL_API_SECRET contains non-ASCII characters. HTTP clients disagree on how "
            "to encode a non-ASCII header value (node/undici and python-requests send latin-1, "
            "aiohttp sends utf-8, httpx refuses to send one at all), so no server-side decoding "
            "recovers the same secret from every caller. Set INTERNAL_API_SECRET to an ASCII "
            "value — scripts/create-secrets.sh generates one with `openssl rand -base64 32`."
        )


# set once this process has observed a usable secret, and never cleared. See the asymmetry
# note in `_internal_api_secret`, which is the only thing that reads or writes it.
_authentication_was_configured = False


def _internal_api_secret() -> str | None:
    """The shared secret `require_auth` compares against, read fresh — or None to refuse.

    Returns a non-empty str to authenticate against; `""` when no secret is configured and none
    ever was in this process, which is the documented fail-open dev shape; and **None when the
    request must be refused** (401). It never raises: a RuntimeError out of here would leave a
    FastAPI dependency 500ing *every* call, including one presenting the correct credential, on
    a pod kubelet keeps Ready because `/health` returns before the read.

    Read per request rather than snapshotted at import (`genetics-results-suite-xi6`). The
    snapshot was global state fixed by whoever imported this module first: pytest imports every
    test module at COLLECTION time, before any fixture sets the variable, so which tests ran
    first decided whether authentication ran at all — at `--randomly-seed=2662673150` the
    secret froze to "", require_auth took its fail-open branch, and the nine auth tests that
    assert 401 got 200 and **failed** (a seed-dependent red suite, not a silent green one). The
    hazard is the order-dependence itself: a shuffled run going red for reasons unrelated to
    the change under test, and any future module-scope `import api.main` freezing the secret
    for an unrelated test file.

    Reading and validating in the SAME accessor is the point, not incidental: `_require_ascii_secret`
    used to validate the import-time snapshot only, so a request-time read past it could compare
    against a value that was never checked — and ASCII is exactly the invariant that makes the
    `latin-1` vs `utf-8` pairing in `require_auth` well defined. There is no way to obtain the
    secret except through here, so the value compared is the value validated.
    """
    global _authentication_was_configured

    secret = os.environ.get("INTERNAL_API_SECRET", "")

    if secret and not secret.isascii():
        # startup already refused this value, so getting here means the variable changed under
        # a live process — impossible for a pod, whose environ is immutable. Fail closed: see
        # the "never raises" paragraph above for why this is not `_require_ascii_secret(secret)`
        return None

    if secret:
        _authentication_was_configured = True
        return secret

    # THE ASYMMETRY IS THE POINT — do not "simplify" this to `return secret`.
    # A runtime change may ENABLE authentication (empty -> set, which is what makes the
    # collection-order hazard above unreachable) but must never DISABLE it. Under the old module
    # global no in-process mutation could turn a secret-set app into a fail-open one, because the
    # app held its own copy; a plain per-request read would hand that away, so that deleting or
    # emptying the variable admitted every caller with principal=None. Once this process has
    # observed a secret, an absent one is a fail-CLOSED condition. Pinned by
    # tests/test_secret_read_timing.py::test_a_configured_process_never_fails_open_afterwards.
    return None if _authentication_was_configured else ""


_startup_secret = os.environ.get("INTERNAL_API_SECRET", "")

# fail fast at import: the environment does not change under a running pod, so this is where a
# non-ASCII secret crashes the process (readiness never passes, the rollout stalls, the old pods
# keep serving) rather than at request time, where the accessor above fails closed instead
_require_ascii_secret(_startup_secret)

_authentication_was_configured = bool(_startup_secret)

if not _startup_secret:
    logger.warning(
        "INTERNAL_API_SECRET is not set: every endpoint is reachable without authentication"
    )

# refuses to start rather than warn when the sandbox is deployed and either secret is missing
sandbox_auth.require_sandbox_config(_startup_secret)

app = FastAPI(
    title="Genetics Results API",
    description="Query interface for genetics fine-mapping and colocalization data",
    version="1.0.0",
    dependencies=[Depends(require_auth)],
    # FastAPI mounts its docs with add_route, which bypasses app-level dependencies — the
    # schema would stay readable on an otherwise authenticated service. Re-declared below as
    # ordinary routes so they inherit require_auth (and still work locally, where it no-ops).
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_OPENAPI_URL = "/openapi.json"


@app.get(_OPENAPI_URL, include_in_schema=False)
async def openapi_schema():
    return JSONResponse(app.openapi())


@app.get("/docs", include_in_schema=False)
async def swagger_ui():
    return get_swagger_ui_html(openapi_url=_OPENAPI_URL, title=f"{app.title} - Swagger UI")


@app.get("/redoc", include_in_schema=False)
async def redoc_ui():
    return get_redoc_html(openapi_url=_OPENAPI_URL, title=f"{app.title} - ReDoc")

# browsers reject a wildcard Access-Control-Allow-Origin on credentialed requests,
# so allowed origins must be listed explicitly
CORS_ORIGINS = [
    o.strip()
    for o in os.environ.get(
        "CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000"
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = os.environ.get("PROJECT_ID", "google-project-id")
DATASET_ID = os.environ.get("DATASET_ID", "genetics_results")
MAX_ROWS = int(os.environ.get("MAX_ROWS", "100000"))
MAX_BYTES_BILLED = int(os.environ.get("MAX_BYTES_BILLED", str(100 * 1024**3)))  # 100 GB default

# Defaults for every request, per docs/code-execution-security.md section 4. The operator
# values above are the *relaxed* case, reached only by a request verified against
# INTERNAL_API_SECRET — so no caller can obtain looser limits by presenting a weaker
# credential, or none at all.
SANDBOX_MAX_ROWS = 25_000
SANDBOX_MAX_BYTES_BILLED = 50 * 1024**3
# aggregate across every BigQuery job of one execution (`jti`) — /query plus the service's own
# scans on /schema, /stats and /tables/{t}/sample, none of which is cached at the HTTP layer;
# the per-query cap alone bounds one query against a caller that has 120 seconds in which to loop
SANDBOX_AGGREGATE_BYTES_BUDGET = 200 * 1024**3
# bound on the counter itself, so a flood of distinct `jti`s cannot grow it without limit
_JTI_BUDGET_LRU_SIZE = 1024

bq_client = bigquery.Client(project=PROJECT_ID)


@dataclass(frozen=True)
class _Caps:
    """Resolved per-credential limits for one request."""

    max_rows: int
    max_bytes_billed: int
    # set only for a sandbox execution — the key of the aggregate budget below
    jti: str | None


_RELAXED_CAPS = _Caps(MAX_ROWS, MAX_BYTES_BILLED, None)


def _caps_for(request: Request | None) -> _Caps:
    """The limits this request runs under, keyed on the principal `require_auth` resolved.

    Tight by default. Relaxed only for `_INTERNAL_PRINCIPAL`, i.e. a successful
    `hmac.compare_digest` against `INTERNAL_API_SECRET` — db-api has no other caller and no
    other auth path. `None` (the fail-open branch when the secret is unset) stays tight: a
    deployment that has not wired the secret keeps serving, but at sandbox limits rather than
    at the operator's.

    That is a behaviour change for an unwired deployment. The three internal query paths —
    `/schema`'s distinct-value scans, `/stats`, and `/tables/{t}/sample` — used to run under
    the deleted `_internal_job_config()` at `MAX_BYTES_BILLED`; with the secret unset they now
    run at `SANDBOX_MAX_BYTES_BILLED` (50 GB). Production is unaffected, since the secret is a
    required `secretKeyRef` in `k8s/deployments/db-api.yaml`, but local dev and an unwired
    cluster get the tighter ceiling, and raising `MAX_BYTES_BILLED` there will not make
    `/schema` benefit.
    """
    principal = getattr(request.state, "principal", None) if request is not None else None
    if principal == _INTERNAL_PRINCIPAL:
        return _RELAXED_CAPS
    jti = principal.execution_id if isinstance(principal, sandbox_auth.SandboxPrincipal) else None
    return _Caps(SANDBOX_MAX_ROWS, SANDBOX_MAX_BYTES_BILLED, jti)


# in-process, bounded LRU: {jti -> bytes *processed* so far}. Processed, not billed: a dry run
# reports only `total_bytes_processed` (`total_bytes_billed` is 0 on a dry-run job), so it is the
# only figure available on both sides of the pre-flight charge and its reconcile — charging one
# unit and reconciling in the other would make the correction wrong by construction. The two
# differ only by BigQuery's 10 MB minimum and its round-up, immaterial against a 200 GB budget.
_jti_bytes: "OrderedDict[str, int]" = OrderedDict()
_jti_bytes_lock = threading.Lock()


def _charge_aggregate(jti: str, additional: int) -> tuple[bool, int]:
    """Charge `additional` bytes to `jti`'s running total.

    Returns `(allowed, total)`. When the charge would exceed the budget nothing is charged
    and `allowed` is False — the caller must turn that into a 429, never a truncated result.
    """
    with _jti_bytes_lock:
        current = _jti_bytes.get(jti, 0)
        allowed = current + additional <= SANDBOX_AGGREGATE_BYTES_BUDGET
        _jti_bytes[jti] = current + additional if allowed else current
        _jti_bytes.move_to_end(jti)
        # trimmed on both branches: the reject path also inserts (a 0-valued entry for a `jti`
        # that never spent anything), so trimming only when allowed leaves the bound unreal
        while len(_jti_bytes) > _JTI_BUDGET_LRU_SIZE:
            _jti_bytes.popitem(last=False)
        return allowed, _jti_bytes.get(jti, current)


def _charge_aggregate_spent(jti: str, spent: int) -> None:
    """Add bytes a job has *already* billed to `jti`'s running total.

    Unconditional, unlike `_charge_aggregate`: the bytes are gone either way, so refusing the
    charge would only lose the accounting. Used by the paths that have no dry run to price the
    statement with (see `_run_internal_query`).
    """
    if spent <= 0:
        return
    with _jti_bytes_lock:
        _jti_bytes[jti] = _jti_bytes.get(jti, 0) + spent
        _jti_bytes.move_to_end(jti)
        while len(_jti_bytes) > _JTI_BUDGET_LRU_SIZE:
            _jti_bytes.popitem(last=False)


def _aggregate_spent(jti: str) -> int:
    with _jti_bytes_lock:
        return _jti_bytes.get(jti, 0)


def _reconcile_aggregate(jti: str, delta: int) -> None:
    """Correct a charge after the fact: the pre-flight charge uses the dry run's estimate, and the
    executed job can process less (a cache hit) or more than that estimate. A negative `delta`
    equal to the whole estimate is the refund for a query that raised before it ran."""
    if delta == 0:
        return
    with _jti_bytes_lock:
        if jti in _jti_bytes:
            _jti_bytes[jti] = max(0, _jti_bytes[jti] + delta)


def _aggregate_budget_exceeded(jti: str, requested: int, spent: int) -> HTTPException:
    logger.warning({
        "message": "sandbox aggregate byte budget exceeded",
        "log_type": "endpoint_access",
        "jti": jti,
        "bytes_spent": spent,
        "bytes_requested": requested,
        "budget": SANDBOX_AGGREGATE_BYTES_BUDGET,
    })
    return HTTPException(
        status_code=429,
        detail=(
            f"Aggregate BigQuery byte budget for this execution exhausted: "
            f"{spent} of {SANDBOX_AGGREGATE_BYTES_BUDGET} bytes already processed, this query "
            f"needs a further {requested}. Narrow the query or aggregate in fewer scans."
        ),
    )


def _run_internal_query(sql: str, caps: _Caps) -> "bigquery.QueryJob":
    """Run one of the service's own queries (/schema, /stats, /tables/{t}/sample) under `caps`.

    Only /query used to carry `maximum_bytes_billed` and only /query charged the aggregate
    budget, so the endpoints that scan whole views on a cache miss were both uncapped and free
    — and none of them is cached at the HTTP layer, so a script could loop them for its whole
    wall clock outside the budget the constant claims to be an aggregate over every query of
    one execution.

    These paths have no dry run to price the statement with, so the budget is checked before
    the job starts and the bytes it processed are charged after it finishes. Post-hoc charging
    means the budget can be overshot by at most one query — and by at most that query's
    `maximum_bytes_billed`, which is exactly what the per-query cap bounds.

    `caps` is the *triggering* caller's, including on the shared `_get_categorical_values`
    cache: a per-credential ceiling there decides only who pays and how much this job may
    bill, never what a later caller finds cached. A job over the triggering caller's ceiling
    fails and leaves the cache unpopulated, so the next caller simply retries under its own.
    """
    if caps.jti is not None:
        spent = _aggregate_spent(caps.jti)
        if spent >= SANDBOX_AGGREGATE_BYTES_BUDGET:
            raise _aggregate_budget_exceeded(caps.jti, 0, spent)

    job = bq_client.query(
        sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=caps.max_bytes_billed)
    )
    job.result()
    if caps.jti is not None:
        _charge_aggregate_spent(caps.jti, job.total_bytes_processed or 0)
    return job

# expose views (not underlying tables) so AI agents use the enriched schemas
VIEWS = ["credible_sets_v", "colocalization_v", "coloc_credsets_v", "exome_variant_results_v", "gene_burden_results_v", "asm_qtl_v", "gene_annotations_v", "open_chromatin_v", "variant_effect_v", "mpra_v", "variant_annotation_v", "peak_to_gene_v", "hla_associations_v", "phenotypes_v", "datasets_v"]
# map base table names to views for backwards-compatible query auto-qualification
_BASE_TABLES = {name.removesuffix("_v"): name for name in VIEWS}
# every name /query accepts unqualified -> the view it resolves to (views map to themselves)
_QUALIFY_TARGETS = {**{name: name for name in VIEWS}, **_BASE_TABLES}


def _qualify_tables(sql: str) -> str:
    """Qualify unqualified view/base-table names with the project and dataset.

    Only genuine table positions - a name directly after FROM or JOIN - are rewritten.
    The previous implementation replaced every occurrence of " <name>", which also hit
    ordinary English inside string literals now that `datasets` and `phenotypes` are
    table names: `LIKE '%uk biobank datasets%'` became
    `LIKE '%uk biobank ``project.dataset.datasets_v``%'`, which is still valid SQL and
    silently matched nothing, and `COUNT(*) AS datasets ... ORDER BY datasets` had its
    alias rewritten. `\\s+` rather than a literal space because a multi-line
    `FROM\\n  credible_sets_v` is a normal shape.

    RESIDUAL: a string literal that itself contains the words "FROM <table>" or
    "JOIN <table>" is still rewritten - telling that apart from real SQL needs a parser,
    which is out of scope. The result is still checked by authorize_query.
    """
    for name, view in _QUALIFY_TARGETS.items():
        fq = f"`{PROJECT_ID}.{DATASET_ID}.{view}`"
        sql = re.sub(
            rf"\b(FROM|JOIN)(\s+){re.escape(name)}\b",
            lambda m, fq=fq: f"{m.group(1)}{m.group(2)}{fq}",
            sql,
            flags=re.IGNORECASE,
        )
    return sql

# load all metadata from shared datasets.yaml (single source of truth)
try:
    from api.yaml_loader import load_all as _load_yaml_config
except ImportError:
    from yaml_loader import load_all as _load_yaml_config

_yaml_config = _load_yaml_config()
if _yaml_config is None:
    raise RuntimeError(
        "datasets.yaml could not be loaded. Set DATASETS_CONFIG_PATH or "
        "run scripts/sync-datasets.sh to create configs/datasets.yaml"
    )

_RESOURCE_METADATA: dict[str, dict[str, Any]] = _yaml_config["resource_metadata"]
_COLLECTION_RESOURCE_PREFIXES: dict[str, dict[str, str]] = _yaml_config["collection_resource_prefixes"]
_TABLE_DESCRIPTIONS: dict[str, str] = _yaml_config["table_descriptions"]
_COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = _yaml_config["column_descriptions"]
_TABLE_EXAMPLES: dict[str, list[dict[str, str]]] = _yaml_config["table_examples"]
_CATEGORICAL_COLUMNS: dict[str, dict[str, str | None]] = _yaml_config["categorical_columns"]
logger.info("Loaded dataset config from YAML (%d resources, %d tables)",
            len(_RESOURCE_METADATA), len(_TABLE_DESCRIPTIONS))

_VALUES_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_VALUES_CACHE_TTL_SECONDS = 3600

# View-only derived columns aren't in any base table, and BigQuery reports all
# view columns as NULLABLE. These are deterministic non-null transforms of
# REQUIRED base columns, so declare their true mode here (anything not listed
# falls back to NULLABLE): `variant` = CONCAT of REQUIRED chr/pos/ref/alt;
# `resource*` = CASE over REQUIRED dataset with a non-null ELSE. `maf` is
# intentionally absent — LEAST(aaf, 1-aaf) is NULL when the nullable aaf is.
_DERIVED_COLUMN_MODES = {
    "variant": "REQUIRED",
    "resource": "REQUIRED",
    "resource1": "REQUIRED",
    "resource2": "REQUIRED",
}


def _get_categorical_values(view_name: str, caps: _Caps) -> dict[str, Any]:
    """Return distinct values for a view's categorical columns.

    Result keys are either the column name (flat list of allowed values) or
    `<col>_by_<dep>` (mapping from dependency value to allowed values). Cached
    in-process for `_VALUES_CACHE_TTL_SECONDS` to keep `/schema` cheap.

    On a cache miss the full-column scans below run under the *triggering* caller's `caps`
    and are charged to it — see `_run_internal_query` for why that does not let one caller's
    ceiling decide what a later caller finds cached.
    """
    config = _CATEGORICAL_COLUMNS.get(view_name)
    if not config:
        return {}

    cached = _VALUES_CACHE.get(view_name)
    now = time.time()
    if cached and now - cached[0] < _VALUES_CACHE_TTL_SECONDS:
        return cached[1]

    flat_cols = [col for col, dep in config.items() if dep is None]
    dep_cols = [(col, dep) for col, dep in config.items() if dep is not None]

    result: dict[str, Any] = {}
    fq = f"`{PROJECT_ID}.{DATASET_ID}.{view_name}`"

    if flat_cols:
        agg = ", ".join(f"ARRAY_AGG(DISTINCT {c} IGNORE NULLS) AS {c}" for c in flat_cols)
        sql = f"SELECT {agg} FROM {fq}"
        try:
            row = next(iter(_run_internal_query(sql, caps).result()))
            for c in flat_cols:
                result[c] = sorted(row[c] or [])
        except HTTPException:
            raise  # an exhausted aggregate budget is the caller's answer, not a warning
        except Exception as e:
            logger.warning(f"Distinct value fetch failed for {view_name}: {e}")

    deps_grouped: dict[str, list[str]] = {}
    for col, dep in dep_cols:
        deps_grouped.setdefault(dep, []).append(col)

    for dep, cols in deps_grouped.items():
        agg = ", ".join(f"ARRAY_AGG(DISTINCT {c} IGNORE NULLS) AS {c}" for c in cols)
        sql = f"SELECT {dep}, {agg} FROM {fq} WHERE {dep} IS NOT NULL GROUP BY {dep}"
        try:
            for row in _run_internal_query(sql, caps).result():
                key = row[dep]
                for c in cols:
                    result.setdefault(f"{c}_by_{dep}", {})[key] = sorted(row[c] or [])
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Grouped distinct value fetch failed for {view_name}.{dep}: {e}")

    if result:
        _VALUES_CACHE[view_name] = (now, result)
    return result


def _compact_categorical_values(cat_values: dict[str, Any]) -> dict[str, Any]:
    """Collapse collection resources (e.g. qtdNNNNNN) into summaries.

    Keeps named resources inline and replaces hundreds of collection IDs
    with a compact description, reducing schema size dramatically.
    """
    result: dict[str, Any] = {}
    for key, values in cat_values.items():
        if isinstance(values, list):
            # flat allowed_values list — separate named vs collection
            named = []
            collections: dict[str, int] = {}
            for v in values:
                matched = False
                for prefix in _COLLECTION_RESOURCE_PREFIXES:
                    if v.lower().startswith(prefix):
                        collections[prefix] = collections.get(prefix, 0) + 1
                        matched = True
                        break
                if not matched:
                    named.append(v)
            if collections:
                result[key] = named
                for prefix, count in collections.items():
                    meta = _COLLECTION_RESOURCE_PREFIXES[prefix]
                    result.setdefault("collection_resources", {})[meta["label"]] = {
                        "count": count,
                        "pattern": f"{prefix}NNNNNN (e.g. {prefix}000001)",
                        "description": meta["description"],
                        "data_types": meta.get("data_types", ""),
                    }
            else:
                result[key] = values
        elif isinstance(values, dict):
            # grouped allowed_values_by_X — keep only named resources
            compacted = {}
            collection_types: set[str] = set()
            for group_key, group_vals in values.items():
                is_collection = any(
                    group_key.lower().startswith(prefix)
                    for prefix in _COLLECTION_RESOURCE_PREFIXES
                )
                if is_collection:
                    for v in group_vals:
                        collection_types.add(v)
                else:
                    compacted[group_key] = group_vals
            if collection_types:
                compacted["_eqtl_catalogue_resources"] = (
                    f"All eQTL Catalogue (qtdNNNNNN) resources have data_types: {sorted(collection_types)}"
                )
            result[key] = compacted
        else:
            result[key] = values
    return result


class QueryRequest(BaseModel):
    """SQL query request."""

    sql: str = Field(..., description="SQL query to execute", min_length=1)
    max_rows: int = Field(default=1000, le=MAX_ROWS, description="Maximum rows to return")
    dry_run: bool = Field(default=False, description="Estimate query cost without executing")


class QueryResponse(BaseModel):
    """Query result response."""

    columns: list[str]
    rows: list[list[Any]]
    total_rows: int
    bytes_processed: int
    truncated: bool


class TableInfo(BaseModel):
    """Table metadata."""

    name: str
    description: str
    row_count: int
    columns: list[dict[str, Any]]
    examples: list[dict[str, str]] = []


class SchemaWarning(BaseModel):
    """Per-view error encountered while building the schema response."""

    view: str
    error: str


class SchemaResponse(BaseModel):
    """Database schema response."""

    resources: dict[str, Any] = {}
    tables: list[TableInfo]
    warnings: list[SchemaWarning] = []


def _estimate_bq_cost(bytes_processed: int) -> float:
    """Estimate BigQuery on-demand query cost (USD). $6.25 per TiB."""
    return round((bytes_processed / (1024**4)) * 6.25, 6)


# tables a caller may reference: the exposed views plus the base tables they wrap
# (BigQuery may report either in a dry run's referencedTables for a view query).
_ALLOWED_TABLE_IDS = {
    f"{PROJECT_ID}.{DATASET_ID}.{name}" for name in (*VIEWS, *_BASE_TABLES)
}


def authorize_query(sql: str, job_config: bigquery.QueryJobConfig) -> "bigquery.QueryJob":
    """Dry-run `sql` and reject it unless it is a plain read of the exposed tables.

    Replaces the previous keyword blocklist, which only inspected whitespace-delimited
    tokens and so let `EXECUTE IMMEDIATE`, `EXPORT DATA`, `CALL`, `LOAD` and `GRANT`
    through, and never constrained *which* tables a SELECT could read (project-level
    bigquery.dataViewer means that was every dataset in the project).

    BigQuery itself parses the statement here, so there is no pattern to evade: the
    dry run reports the real statement type and the real set of referenced tables.
    Returns the completed dry-run job so the caller can reuse its cost estimate.
    """
    dry_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=job_config.maximum_bytes_billed,
        dry_run=True,
        use_query_cache=False,
    )
    try:
        probe = bq_client.query(sql, job_config=dry_config)
    except BadRequest as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {e.message}")

    statement_type = probe.statement_type
    if statement_type != "SELECT":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Only single SELECT statements are allowed (got {statement_type or 'unknown'}). "
                "Scripts, DDL, DML and EXPORT are rejected."
            ),
        )

    referenced = {
        f"{t.project}.{t.dataset_id}.{t.table_id}" for t in (probe.referenced_tables or [])
    }
    disallowed = sorted(referenced - _ALLOWED_TABLE_IDS)
    if disallowed:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Query references tables outside the exposed set: {disallowed}. "
                f"Available: {sorted(VIEWS)}"
            ),
        )

    return probe


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/schema", response_model=SchemaResponse)
async def get_schema(http_request: Request, table: str | None = None):
    """Get database schema information. Optionally filter to a single table."""
    start_time = time.perf_counter()
    caps = _caps_for(http_request)
    if table and table not in VIEWS:
        resolved = _BASE_TABLES.get(table)
        if resolved:
            table = resolved
        else:
            raise HTTPException(status_code=404, detail=f"Unknown table: {table}. Available: {VIEWS}")

    views_to_fetch = [table] if table else VIEWS
    tables = []
    warnings: list[dict[str, str]] = []

    for table_name in views_to_fetch:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            table_meta = bq_client.get_table(table_ref)
            overrides = _COLUMN_DESCRIPTIONS.get(table_name, {})
            raw_cat_values = _get_categorical_values(table_name, caps)
            cat_values = _compact_categorical_values(raw_cat_values)

            # get row count and column modes from the base table: views report
            # 0 rows and always-NULLABLE modes, so read the underlying table for
            # both. View-only derived columns (resource, variant, maf) aren't in
            # the base table and fall back to the view's mode.
            row_count = 0
            base_modes: dict[str, str] = {}
            try:
                base_table = table_name.removesuffix("_v")
                base_ref = f"{PROJECT_ID}.{DATASET_ID}.{base_table}"
                base_meta = bq_client.get_table(base_ref)
                row_count = base_meta.num_rows or 0
                base_modes = {f.name: f.mode for f in base_meta.schema}
            except Exception:
                pass

            # build column list with collection_resources pulled out to table level
            collection_resources = cat_values.pop("collection_resources", None)
            columns: list[dict[str, Any]] = []
            for field in table_meta.schema:
                col: dict[str, Any] = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": base_modes.get(field.name) or _DERIVED_COLUMN_MODES.get(field.name, field.mode),
                    "description": overrides.get(field.name, field.description or ""),
                }
                if field.name in cat_values:
                    col["allowed_values"] = cat_values[field.name]
                grouped_key = next(
                    (k for k in cat_values if k.startswith(f"{field.name}_by_")), None
                )
                if grouped_key:
                    col[grouped_key.replace(field.name, "allowed_values", 1)] = cat_values[
                        grouped_key
                    ]
                columns.append(col)

            table_info = TableInfo(
                name=table_name,
                description=_TABLE_DESCRIPTIONS.get(table_name, table_meta.description or ""),
                row_count=row_count,
                columns=columns,
                examples=_TABLE_EXAMPLES.get(table_name, []),
            )
            # attach collection_resources as extra field on the dict
            table_dict = table_info.model_dump()
            if collection_resources:
                table_dict["collection_resources"] = collection_resources
            tables.append(table_dict)
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Could not get schema for {table_name}: {e}")
            warnings.append({"view": table_name, "error": f"{type(e).__name__}: {e}"})

    logger.info({
        "message": "schema",
        **_access_log_fields(http_request, "/schema", "GET"),
        "table": table or "all",
        "tables_returned": len(tables),
        "warnings": len(warnings),
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })

    # surface total upstream failure rather than silently returning an empty list
    if views_to_fetch and not tables:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "schema unavailable: no views could be loaded from BigQuery",
                "warnings": warnings,
            },
        )

    return {"resources": _RESOURCE_METADATA, "tables": tables, "warnings": warnings}


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest, http_request: Request):
    """Execute a SQL query against the genetics database."""
    start_time = time.perf_counter()
    sql = request.sql

    sql = _qualify_tables(sql)

    # `QueryRequest.max_rows` carries a class-level `le=MAX_ROWS`, evaluated once at model
    # definition time and therefore identical for every caller; the per-credential cap has to
    # be applied here, after the principal is known. Tightening MAX_ROWS itself would move
    # that class-level bound and so cap the relaxed callers too.
    caps = _caps_for(http_request)
    max_rows = min(request.max_rows, caps.max_rows)

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=caps.max_bytes_billed,
        dry_run=request.dry_run,
    )

    # BigQuery parses the statement and reports its type and referenced tables; anything
    # that is not a plain SELECT over the exposed views is rejected before it can run
    probe = authorize_query(sql, job_config)

    # the dry run already priced the statement, so the aggregate budget is checked *before*
    # the bytes are spent rather than after — over budget is a 429, never a truncated result
    estimated_bytes = probe.total_bytes_processed or 0
    charged = caps.jti is not None and not request.dry_run
    if charged:
        allowed, spent = _charge_aggregate(caps.jti, estimated_bytes)
        if not allowed:
            raise _aggregate_budget_exceeded(caps.jti, estimated_bytes, spent)

    settled = False
    try:
        if request.dry_run:
            bytes_processed = probe.total_bytes_processed
            logger.info({
                "message": "query",
                **_access_log_fields(http_request, "/query", "POST"),
                "sql": request.sql,
                "dry_run": True,
                "total_rows": 0,
                "bytes_processed": bytes_processed,
                "estimated_cost_usd": _estimate_bq_cost(bytes_processed),
                "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
            })
            return QueryResponse(
                columns=[],
                rows=[],
                total_rows=0,
                bytes_processed=bytes_processed,
                truncated=False,
            )

        query_job = bq_client.query(sql, job_config=job_config)
        results = query_job.result()
        rows = []
        columns = [field.name for field in results.schema]

        for i, row in enumerate(results):
            if i >= max_rows:
                break
            rows.append([_serialize_value(v) for v in row.values()])

        bytes_processed = query_job.total_bytes_processed
        if charged:
            _reconcile_aggregate(caps.jti, (bytes_processed or 0) - estimated_bytes)
            settled = True
        total_rows = results.total_rows
        logger.info({
            "message": "query",
            **_access_log_fields(http_request, "/query", "POST"),
            "sql": request.sql,
            "dry_run": False,
            "total_rows": total_rows,
            "rows_returned": len(rows),
            "bytes_processed": bytes_processed,
            "estimated_cost_usd": _estimate_bq_cost(bytes_processed),
            "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
        })
        return QueryResponse(
            columns=columns,
            rows=rows,
            total_rows=total_rows,
            bytes_processed=bytes_processed,
            truncated=total_rows > max_rows,
        )

    except BadRequest as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {e.message}")
    except Forbidden as e:
        raise HTTPException(status_code=403, detail=f"Query forbidden: {e.message}")
    except Exception as e:
        logger.exception("Query execution failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # a job that never ran bills nothing, so a query that raises between the pre-flight
        # charge and the reconcile must give the estimate back — otherwise a script's syntax
        # errors, each priced by the dry run, eat the execution's budget without spending a byte
        if charged and not settled:
            _reconcile_aggregate(caps.jti, -estimated_bytes)


def _serialize_value(value: Any) -> Any:
    """Serialize BigQuery values to JSON-compatible types."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


@app.get("/tables/{table_name}/sample")
async def get_sample(table_name: str, http_request: Request, limit: int = 10):
    """Get sample rows from a table."""
    start_time = time.perf_counter()
    # accept both view names and base table names
    resolved = _BASE_TABLES.get(table_name, table_name)
    if resolved not in VIEWS:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")

    limit = min(limit, 100)
    sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{resolved}` LIMIT {limit}"

    results = _run_internal_query(sql, _caps_for(http_request)).result()

    columns = [field.name for field in results.schema]
    rows = [[_serialize_value(v) for v in row.values()] for row in results]

    logger.info({
        "message": "sample",
        **_access_log_fields(http_request, "/tables/{table_name}/sample", "GET"),
        "table": resolved,
        "rows_returned": len(rows),
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })
    return {"columns": columns, "rows": rows}


@app.get("/stats")
async def get_stats(http_request: Request):
    """Get summary statistics for the database."""
    start_time = time.perf_counter()
    stats = {}

    # row counts
    for view in VIEWS:
        try:
            # get row count from underlying table (views don't have num_rows)
            base_table = view.removesuffix("_v")
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{base_table}"
            table_info = bq_client.get_table(table_ref)
            stats[f"{view}_rows"] = table_info.num_rows
        except Exception:
            stats[f"{view}_rows"] = None

    # credible sets breakdown
    try:
        sql = f"""
        SELECT
            dataset,
            data_type,
            COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.credible_sets_v`
        GROUP BY dataset, data_type
        ORDER BY count DESC
        """
        results = _run_internal_query(sql, _caps_for(http_request)).result()
        stats["credible_sets_by_source"] = [
            {"dataset": row.dataset, "data_type": row.data_type, "count": row.count}
            for row in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Could not get credible sets breakdown: {e}")

    logger.info({
        "message": "stats",
        **_access_log_fields(http_request, "/stats", "GET"),
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })
    return stats


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_config=None)
