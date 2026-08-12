"""Tests for the shape of db-api's `endpoint_access` log payload.

These rows land in `phewas-development.genetics_api_logs.stdout` alongside results-api's.
Before this, db-api emitted neither `log_source` nor `endpoint_path`, so the only way to tell
the two services apart in BigQuery was `endpoint_path IS NULL` — an accident of a missing
field that would silently change meaning the moment db-api started emitting one. `log_source`
is no better as a replacement: it is env-derived, carries no service name and has already been
renamed once in production. The discriminator is `service`, a module constant, and the asserts
below pin it so that cannot regress.
"""

import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("PROJECT_ID", "test-project")


@pytest.fixture(scope="module")
def main():
    """Imported lazily: api.main reads INTERNAL_API_SECRET at module scope, so importing it at
    collection time would fix the secret before test_api_auth's fixture sets it."""
    from api import main as main_module

    return main_module


def _request(principal):
    return types.SimpleNamespace(state=types.SimpleNamespace(principal=principal))


def test_service_names_this_service(main):
    fields = main._access_log_fields(_request(main._INTERNAL_PRINCIPAL), "/query", "POST")
    assert fields["service"] == "db-api"


def test_service_does_not_come_from_the_environment(main, monkeypatch):
    """The discriminator must survive a rename of the environment.

    `log_source` is env-derived and has been renamed in production before
    (`genetics-results-api-prod` -> `finngenie_prod`), which makes a query keyed on it return
    an empty result and no error. `service` is a module constant precisely so that a deploy
    cannot move it; if this assert ever needs `monkeypatch.setenv`, the constant has regressed.
    """
    monkeypatch.setenv("LOG_SOURCE", "some-renamed-env")
    assert main._access_log_fields(_request(None), "/stats", "GET")["service"] == "db-api"


def test_log_source_is_still_emitted_as_the_environment_axis(main):
    fields = main._access_log_fields(_request(main._INTERNAL_PRINCIPAL), "/query", "POST")
    assert fields["log_source"] == main.LOG_SOURCE
    assert main.LOG_SOURCE, "an empty log_source puts db-api's rows back in the NULL bucket"


def test_endpoint_path_and_method_are_recorded(main):
    fields = main._access_log_fields(_request(None), "/tables/{table_name}/sample", "GET")
    assert fields["endpoint_path"] == "/tables/{table_name}/sample"
    assert fields["http_method"] == "GET"
    assert fields["log_type"] == "endpoint_access"


def test_no_user_email_is_claimed(main):
    """db-api's caller is a service, not a person — there is no user to attribute to."""
    assert "user_email" not in main._access_log_fields(_request(None), "/stats", "GET")


@pytest.mark.parametrize(
    "kind, expected",
    [("internal", "internal"), ("sandbox", "sandbox"), ("none", "unauthenticated")],
    ids=["internal-secret", "sandbox-token", "fail-open"],
)
def test_principal_names_the_credential_that_authorized_the_call(main, kind, expected):
    from api import sandbox_auth

    principal = {
        "internal": main._INTERNAL_PRINCIPAL,
        "sandbox": sandbox_auth.SandboxPrincipal(
            user="u@example.org",
            session_id="sid-1",
            execution_id="jti-1",
            scope="sandbox",
            expires_at=0,
        ),
        "none": None,
    }[kind]
    assert main._access_log_fields(_request(principal), "/query", "POST")["principal"] == expected


def test_missing_request_does_not_raise(main):
    assert main._access_log_fields(None, "/schema", "GET")["principal"] == "unauthenticated"
