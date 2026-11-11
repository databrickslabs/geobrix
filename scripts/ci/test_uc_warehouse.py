"""Host tests for the warehouse check of the UC commands: examples/tests/_uc_warehouse.py. No workspace call."""

import importlib
import sys
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT


@pytest.fixture(scope="module")
def uw():
    sys.path.insert(0, str(REPO_ROOT / "examples" / "tests"))
    try:
        return importlib.import_module("_uc_warehouse")
    finally:
        sys.path.remove(str(REPO_ROOT / "examples" / "tests"))


class ResourceDoesNotExist(Exception):
    pass


class PermissionDenied(Exception):
    pass


class Client:
    def __init__(self, get=None):
        self._get = get
        self.warehouses = SimpleNamespace(get=self._do_get)

    def _do_get(self, wid):
        if isinstance(self._get, Exception):
            raise self._get
        return self._get


def wh(wid, name, state="RUNNING"):
    return SimpleNamespace(id=wid, name=name, state=SimpleNamespace(value=state))


def test_existing_warehouse_passes(uw):
    assert uw.warehouse_problem(Client(get=wh("a1", "gate")), "a1") is None


def test_stopped_warehouse_passes(uw):
    assert uw.warehouse_problem(Client(get=wh("a1", "gate", "STOPPED")), "a1") is None


def test_deleted_warehouse_names_the_key_and_a_fix(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text("GBX_UC_WAREHOUSE_ID=shared\nGBX_UC_GATE_WAREHOUSE_ID=dead1\n")
    w = Client(get=ResourceDoesNotExist("SQL warehouse dead1 has been deleted."))
    msg = uw.warehouse_problem(w, "dead1", cfg, "p")
    assert "dead1" in msg and "GBX_UC_GATE_WAREHOUSE_ID" in msg and "deleted or does not exist" in msg
    assert "Fix:" in msg and "--warehouse-id" in msg
    assert "databricks warehouses list --profile p" in msg


def test_unknown_id_is_permission_denied_and_still_refused(uw):
    msg = uw.warehouse_problem(Client(get=PermissionDenied("not authorized")), "zzz", None, "p")
    assert "does not exist, or you may not use it" in msg and "--warehouse-id" in msg


def test_deleted_state_is_refused(uw):
    assert "is DELETED" in uw.warehouse_problem(Client(get=wh("a1", "x", "DELETED")), "a1")


def test_other_api_errors_are_reported_not_hidden(uw):
    msg = uw.warehouse_problem(Client(get=RuntimeError("boom")), "a1")
    assert "could not look up" in msg and "RuntimeError" in msg


def test_key_comes_from_the_config_only_when_the_value_matches(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text("GBX_UC_GATE_WAREHOUSE_ID=g\nGBX_UC_WAREHOUSE_ID='s'\n")
    assert uw.config_key_of("g", cfg) == "GBX_UC_GATE_WAREHOUSE_ID"
    assert uw.config_key_of("s", cfg) == "GBX_UC_WAREHOUSE_ID"
    assert uw.config_key_of("other", cfg) is None
    assert uw.config_key_of("g", tmp_path / "missing.env") is None
