"""Guards the tests against importing another checkout's source.

genetics-results-suite-6o3 in this repo's shape. There is no install here — the project
declares no build-system, so `uv sync` installs the dependencies only, and `api` is a
namespace package (no `api/__init__.py`) found on sys.path. Every test module already
does its own `sys.path.insert(0, <root>)`, which puts THIS tree first, so the mcp-server
failure (a foreign interpreter importing a foreign copy) cannot happen here.

What can happen is subtler: a namespace package MERGES every matching directory on
sys.path, so `PYTHONPATH=<other checkout>` adds that tree's `api/` to `api.__path__`.
Modules present there but not here then import silently from the other checkout, and a
deletion made here is invisible.

The insert below is the same one every test module performs; it is repeated once here so
the check can run at configure time, before a single test is collected.
"""

import os
import sys
from pathlib import Path

import pytest

_AUTH_ENV = ("INTERNAL_API_SECRET", "SANDBOX_TOKEN_SIGNING_KEY", "SANDBOX_ENABLED")


@pytest.fixture(autouse=True)
def restore_auth_env():
    """A net, not the fix. Keeps one test's auth environment out of the next one's app.

    The fix is in `_reload` (test_sandbox_token_auth), which is what mutated os.environ and put
    nothing back; it now goes through monkeypatch and restores what it touches, PROJECT_ID
    included. This stays as defence in depth, because the next author to write an
    `os.environ[...] = ...` in a test gets no warning that anything depends on it — but it is
    the second line, and a leak it catches is still a bug where it was written.

    Why it matters more since genetics-results-suite-xi6: only INTERNAL_API_SECRET is read at
    request time, so only that one can affect an already-imported app. api/sandbox_auth.py
    snapshots SANDBOX_TOKEN_SIGNING_KEY and SANDBOX_ENABLED at import, so popping either affects
    no live app object at all — and popping the signing key fails closed anyway. They are
    restored here because they configure the NEXT import, not because a live app rereads them.

    Function-scoped and autouse, so it wraps the test but not the module-scoped fixtures that
    set this env up: pytest instantiates higher-scoped fixtures first, so the snapshot already
    contains what a module fixture configured, and the restore preserves it for that module's
    remaining tests.
    """
    saved = {key: os.environ.get(key) for key in _AUTH_ENV}
    yield
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def pytest_configure(config):
    import api

    rootdir = Path(config.rootpath).resolve()
    outside = [p for p in api.__path__ if not Path(p).resolve().is_relative_to(rootdir)]
    if outside:
        raise pytest.UsageError(
            "the `api` namespace package spans directories OUTSIDE the tree pytest is "
            "testing, so this run can import another checkout's source and report green.\n"
            "    outside this tree : " + ", ".join(outside) + "\n"
            f"    pytest rootdir    : {rootdir}\n"
            f"    interpreter       : {sys.executable}\n"
            f"    PYTHONPATH        : {os.environ.get('PYTHONPATH', '(unset)')}\n"
            f"Almost always PYTHONPATH points at another checkout. Fix, from {rootdir}:\n"
            "    unset PYTHONPATH\n"
            "    uv sync --extra dev\n"
            '    uv run python -c "import api, sys; print(list(api.__path__)); '
            'print(sys.executable)"\n'
            f"Every printed path must be under {rootdir}."
        )
