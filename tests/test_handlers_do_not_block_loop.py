"""The BigQuery handlers must not run on the event loop.

Every handler calls the BigQuery client synchronously. Declared `async def`, one cold /schema
held the loop for 86 s on staging: the sandbox's 60 s cap timed out a point lookup that
BigQuery answers in 0.3 s, and the liveness probe then killed the pod. Starlette only moves a
handler to its threadpool when it is a plain function, so this pins that property.
"""

import inspect
import os
import threading
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("PROJECT_ID", "test-project")

from api import main  # noqa: E402

BIGQUERY_HANDLERS = ("get_schema", "execute_query", "get_sample", "get_stats")


@pytest.mark.parametrize("name", BIGQUERY_HANDLERS)
def test_bigquery_handler_runs_in_the_threadpool(name):
    handler = getattr(main, name)
    assert not inspect.iscoroutinefunction(handler), f"{name} would block the event loop"


def test_startup_warms_the_values_cache_off_the_serving_path():
    """The warm-up runs in its own thread: /health answers while it is still scanning."""
    scanned: list[str] = []
    gate = threading.Event()

    def fake_scan(view_name, caps):
        scanned.append(view_name)
        gate.wait(timeout=5)
        return {}

    main._VALUES_CACHE.clear()
    with patch.object(main, "_get_categorical_values", side_effect=fake_scan):
        with TestClient(main.app) as client:
            assert client.get("/health").status_code == 200
            assert scanned, "warm-up had not started"
            gate.set()
    assert set(scanned) <= set(main.VIEWS)


def test_concurrent_cold_callers_scan_a_view_once():
    main._VALUES_CACHE.clear()
    calls: list[str] = []
    release = threading.Event()

    def slow_scan(view_name, config, caps):
        calls.append(view_name)
        release.wait(timeout=5)
        main._VALUES_CACHE[view_name] = (main.time.time(), {"resource": ["x"]})
        return {"resource": ["x"]}

    view = next(iter(main._CATEGORICAL_COLUMNS))
    with patch.object(main, "_scan_categorical_values", side_effect=slow_scan):
        threads = [
            threading.Thread(target=main._get_categorical_values, args=(view, main._RELAXED_CAPS))
            for _ in range(3)
        ]
        for t in threads:
            t.start()
        release.set()
        for t in threads:
            t.join(timeout=5)
    assert calls == [view]
