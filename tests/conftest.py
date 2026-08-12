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
