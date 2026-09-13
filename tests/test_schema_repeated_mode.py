"""A view's REPEATED column keeps its mode on /schema; the base table only answers for scalars.

rcnv_segments_v SPLITs six scalar STRING base columns into ARRAY<STRING>. The view's
`field_type` is still STRING, so REPEATED is the only thing /schema carries that says the
column is an array — taking the base table's REQUIRED/NULLABLE for it would serve the
column as a scalar while its description says "ARRAY of ...".
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_internal_query_caps import FakeInternalBQ  # noqa: E402
from test_query_caps import GB, client, main  # noqa: E402,F401  (fixtures)
from test_sandbox_token_auth import SECRET, _reload  # noqa: E402,F401

VIEW = "rcnv_segments_v"


class _Field:
    def __init__(self, name, field_type, mode):
        self.name, self.field_type, self.mode, self.description = name, field_type, mode, ""


class _ViewMeta:
    num_rows = 0
    description = ""
    schema = [
        _Field("region", "STRING", "NULLABLE"),
        _Field("genes", "STRING", "REPEATED"),
        _Field("resource", "STRING", "NULLABLE"),
    ]


class _BaseMeta:
    num_rows = 5
    description = ""
    schema = [_Field("region", "STRING", "REQUIRED"), _Field("genes", "STRING", "REQUIRED")]


class _SchemaBQ(FakeInternalBQ):
    def get_table(self, ref):
        return _ViewMeta() if ref.endswith("_v") else _BaseMeta()


def test_repeated_view_column_is_not_overridden_by_its_scalar_base_column(
    main, client, monkeypatch
):
    monkeypatch.setattr(main, "_CATEGORICAL_COLUMNS", {})
    monkeypatch.setattr(main, "bq_client", _SchemaBQ([]))

    resp = client.get(f"/schema?table={VIEW}", headers={"Authorization": f"Bearer {SECRET}"})

    assert resp.status_code == 200
    body = resp.json()
    assert not body.get("warnings")
    (table,) = body["tables"]
    modes = {c["name"]: c["mode"] for c in table["columns"]}
    assert modes == {"region": "REQUIRED", "genes": "REPEATED", "resource": "REQUIRED"}
    assert table["row_count"] == 5
