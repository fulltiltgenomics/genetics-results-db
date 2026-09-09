"""STRUCT and ARRAY values survive /query as JSON objects and arrays.

The sandbox SDK builds a polars frame from `rows`, so a struct that arrives as its Python
repr string is a Utf8 column and `.struct.field()` raises on it. The value serializer has to
recurse into dicts and lists instead of stringifying them, while still stringifying the
scalars JSON cannot carry (Decimal, date).
"""

import datetime
import decimal
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_query_caps import GB, FakeBQ, _Job, _Ref  # noqa: E402
from test_sandbox_token_auth import SECRET, SIGNING_KEY, _mint, _reload  # noqa: E402

LEAD = {"variant": "8:52700000:A:G", "pip": 0.93, "nested": {"n": 2}}
TAGS = ["missense_variant", "splice_region_variant"]


class _Row:
    def values(self):
        return (LEAD, TAGS, decimal.Decimal("1.5"), datetime.date(2026, 9, 9))


class _Result:
    total_rows = 1
    schema = [type("F", (), {"name": n})() for n in ("lead", "tags", "dec", "day")]

    def __iter__(self):
        return iter([_Row()])


class _StructJob(_Job):
    def result(self):
        return _Result()


class _StructBQ(FakeBQ):
    def query(self, sql, job_config=None):
        self.configs.append(job_config)
        return _StructJob(job_config, 1, 1 * GB)


@pytest.fixture(scope="module")
def main():
    mp = pytest.MonkeyPatch()
    module = _reload(mp, INTERNAL_API_SECRET=SECRET, SANDBOX_TOKEN_SIGNING_KEY=SIGNING_KEY)
    yield module
    mp.undo()


def test_struct_and_array_columns_stay_nested(main, monkeypatch):
    monkeypatch.setattr(main, "bq_client", _StructBQ())
    resp = TestClient(main.app).post(
        "/query",
        json={"sql": "SELECT 1 FROM credible_sets_v"},
        headers={"Authorization": f"Bearer {_mint()}"},
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["lead", "tags", "dec", "day"]
    assert body["rows"] == [[LEAD, TAGS, "1.5", "2026-09-09"]]


def test_serialize_value_recurses_and_keeps_scalars(main):
    assert main._serialize_value({"a": [1, {"b": None}]}) == {"a": [1, {"b": None}]}
    assert main._serialize_value(("x", 2)) == ["x", 2]
    assert main._serialize_value(decimal.Decimal("0.25")) == "0.25"
    assert main._serialize_value(True) is True
