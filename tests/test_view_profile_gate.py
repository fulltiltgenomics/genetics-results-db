"""A table with `profiles: [...]` is exposed only on those profiles; master's real yaml gates
nothing, so the behaviour is pinned against a small fixture."""

import logging
import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import yaml_loader  # noqa: E402
from api.yaml_loader import load_views  # noqa: E402

FIXTURE = """
profiles:
  daly: {}
  finngen: {}
tables:
  plain_v: {exposed: true}
  daly_only_v: {exposed: true, profiles: [daly]}
  both_v: {exposed: true, profiles: [daly, finngen]}
  gated_unexposed_v: {profiles: [daly]}
"""


@pytest.fixture
def config():
    return yaml.safe_load(FIXTURE)


def test_each_profile_sees_only_its_own_gated_views(config):
    assert set(load_views(config, "daly")) == {"plain_v", "daly_only_v", "both_v"}
    assert set(load_views(config, "finngen")) == {"plain_v", "both_v"}


@pytest.mark.parametrize("profile", [None, "", "nonesuch"])
def test_missing_or_unknown_profile_hides_every_gated_view(config, profile):
    assert load_views(config, profile) == ["plain_v"]


def test_gate_never_widens_beyond_exposed(config):
    assert "gated_unexposed_v" not in load_views(config, "daly")


def test_unset_config_profile_hides_gated_views_and_logs_an_error(tmp_path, monkeypatch, caplog):
    path = tmp_path / "datasets.yaml"
    path.write_text(FIXTURE)
    monkeypatch.setattr(yaml_loader, "_DATASETS_CONFIG_PATH", str(path))
    with caplog.at_level(logging.ERROR, logger=yaml_loader.logger.name):
        assert yaml_loader.load_all(None)["views"] == ["plain_v"]
    assert "daly_only_v" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger=yaml_loader.logger.name):
        assert set(yaml_loader.load_all("daly")["views"]) == {"plain_v", "daly_only_v", "both_v"}
    assert caplog.text == ""


@pytest.mark.parametrize("value", ["daly_staging", "daly", {"daly": 1}, None])
def test_non_list_profiles_hides_the_view_and_logs(config, value, caplog):
    config["tables"]["daly_only_v"]["profiles"] = value
    assert "daly_only_v" not in load_views(config, "daly")
    assert any("not a list" in p and "daly_only_v" in p for p in yaml_loader.gate_errors(config, "daly"))


# endpoint level: the gate must reach the sets main.py derives from VIEWS
from types import SimpleNamespace  # noqa: E402

from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from test_sandbox_token_auth import _reload  # noqa: E402

SECRET = "test-secret"


def _main_for(profile, tmp_path, monkeypatch):
    path = tmp_path / "datasets.yaml"
    path.write_text(FIXTURE)
    sys.modules.pop("api.yaml_loader", None)
    monkeypatch.setenv("DATASETS_CONFIG_PATH", str(path))
    monkeypatch.delenv("CONFIG_PROFILE", raising=False)
    if profile:
        monkeypatch.setenv("CONFIG_PROFILE", profile)
    main = _reload(monkeypatch, INTERNAL_API_SECRET=SECRET)
    sys.modules.pop("api.yaml_loader", None)
    return main


def _authorize(main, monkeypatch, table):
    probe = SimpleNamespace(
        statement_type="SELECT",
        referenced_tables=[SimpleNamespace(
            project=main.PROJECT_ID, dataset_id=main.DATASET_ID, table_id=table)])
    monkeypatch.setattr(main.bq_client, "query", lambda *a, **k: probe, raising=False)
    return main.authorize_query("SELECT 1", bigquery.QueryJobConfig())


@pytest.mark.parametrize("table", ["daly_only_v", "daly_only"])
def test_finngen_refuses_the_gated_view_and_its_base_table(tmp_path, monkeypatch, table):
    main = _main_for("finngen", tmp_path, monkeypatch)
    with pytest.raises(HTTPException) as refused:
        _authorize(main, monkeypatch, table)
    assert refused.value.status_code == 403
    response = TestClient(main.app, raise_server_exceptions=False).get(
        "/schema", params={"table": table}, headers={"Authorization": f"Bearer {SECRET}"})
    assert response.status_code == 404


@pytest.mark.parametrize("table", ["daly_only_v", "daly_only"])
def test_daly_accepts_the_gated_view(tmp_path, monkeypatch, table):
    main = _main_for("daly", tmp_path, monkeypatch)
    assert _authorize(main, monkeypatch, table).statement_type == "SELECT"
