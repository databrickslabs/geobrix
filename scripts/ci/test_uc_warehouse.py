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


# ---- the per-profile gate target: catalog, warehouse and Volume folder of each workspace ----------------------------

HUB_CFG = (
    "GBX_UC_WAREHOUSE_ID=shared\n"
    "GBX_UC_GATE_WAREHOUSE_ID=legacy-gate\n"
    "GBX_UC_GATE_WAREHOUSE_ID__hub_ws=hubwh\n"
    "GBX_GATE_CATALOG__hub_ws=hub_cat\n"
    "GBX_GATE_VOLROOT__hub_ws=/Volumes/hub_cat/s/v\n"
    "GBX_UC_GATE_WAREHOUSE_ID__e2_demo=e2wh\n"
    "GBX_GATE_CATALOG__e2_demo=e2_cat\n"
)


def args(**kw):
    return SimpleNamespace(**{"profile": "hub-ws", "catalog": None, "warehouse_id": None, "volroot": None, **kw})


def test_the_profile_name_becomes_a_key_suffix(uw):
    assert uw.profile_key("GBX_GATE_CATALOG", "fevm-geospatial") == "GBX_GATE_CATALOG__fevm_geospatial"
    assert uw.profile_key("GBX_GATE_CATALOG", "e2.demo x") == "GBX_GATE_CATALOG__e2_demo_x"


def test_each_profile_reads_its_own_keys(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    hub = args(profile="hub-ws")
    uw.apply_gate_target(hub, cfg, "dflt", "/Volumes/dflt")
    assert (hub.catalog, hub.warehouse_id, hub.volroot) == ("hub_cat", "hubwh", "/Volumes/hub_cat/s/v")
    e2 = args(profile="e2-demo")
    uw.apply_gate_target(e2, cfg, "dflt", "/Volumes/dflt")
    assert (e2.catalog, e2.warehouse_id, e2.volroot) == ("e2_cat", "e2wh", "/Volumes/dflt")


def test_options_beat_the_config(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    a = args(catalog="mine", warehouse_id="mywh", volroot="/Volumes/m")
    uw.apply_gate_target(a, cfg, "dflt", "/Volumes/dflt")
    assert (a.catalog, a.warehouse_id, a.volroot) == ("mine", "mywh", "/Volumes/m")
    assert set(a.target_source.values()) == {"option"}


def test_a_profile_without_keys_falls_back_to_the_global_warehouse_and_says_so(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    a = args(profile="other")
    uw.apply_gate_target(a, cfg, "dflt", "/Volumes/dflt")
    assert (a.catalog, a.warehouse_id, a.volroot) == ("dflt", "legacy-gate", "/Volumes/dflt")
    assert a.target_source == {"catalog": "default", "warehouse": "GBX_UC_GATE_WAREHOUSE_ID", "volroot": "default"}


def test_the_shared_warehouse_is_the_last_fallback(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text("GBX_UC_WAREHOUSE_ID=shared\n")
    a = args(profile="other")
    uw.apply_gate_target(a, cfg, "dflt", "/Volumes/dflt")
    assert a.warehouse_id == "shared"


def test_the_sources_name_the_per_profile_keys(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    a = args()
    uw.apply_gate_target(a, cfg, "dflt", "/Volumes/dflt")
    assert a.target_source == {
        "catalog": "GBX_GATE_CATALOG__hub_ws", "warehouse": "GBX_UC_GATE_WAREHOUSE_ID__hub_ws", "volroot": "GBX_GATE_VOLROOT__hub_ws"}
    assert "hub_cat" in uw.target_line(a, "hub-ws")


def test_no_profile_and_no_config_leave_the_defaults(uw, tmp_path):
    a = args(profile=None)
    uw.apply_gate_target(a, tmp_path / "missing.env", "dflt", "/Volumes/dflt")
    assert (a.catalog, a.warehouse_id, a.volroot) == ("dflt", None, "/Volumes/dflt")


def test_a_deleted_per_profile_warehouse_names_its_own_key(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    msg = uw.warehouse_problem(Client(get=ResourceDoesNotExist("gone")), "hubwh", cfg, "hub-ws")
    assert "GBX_UC_GATE_WAREHOUSE_ID__hub_ws" in msg and "Fix:" in msg and "--profile hub-ws" in msg


def test_a_warehouse_of_another_workspace_is_refused_with_a_fix(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    msg = uw.warehouse_problem(Client(get=ResourceDoesNotExist("no such warehouse")), "e2wh", cfg, "hub-ws")
    assert "e2wh" in msg and "deleted or does not exist" in msg and "Fix:" in msg and "--warehouse-id" in msg


class Cats:
    def __init__(self, exc=None):
        self.exc = exc
        self.catalogs = SimpleNamespace(get=self._get)

    def _get(self, name):
        if self.exc:
            raise self.exc
        return SimpleNamespace(name=name)


def test_an_existing_catalog_passes(uw):
    assert uw.catalog_problem(Cats(), "hub_cat", None, "p") is None


def test_a_catalog_that_the_workspace_lacks_is_refused_with_its_key_and_a_fix(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG)
    msg = uw.catalog_problem(Cats(ResourceDoesNotExist("Catalog 'e2_cat' does not exist.")), "e2_cat", cfg, "hub-ws")
    assert "e2_cat" in msg and "does not exist" in msg and "Fix:" in msg
    assert "GBX_GATE_CATALOG__hub_ws" in msg and "--catalog" in msg and "databricks catalogs list --profile hub-ws" in msg


def test_other_catalog_lookup_errors_are_reported_not_hidden(uw):
    msg = uw.catalog_problem(Cats(RuntimeError("boom")), "c", None, "p")
    assert "could not look up" in msg and "RuntimeError" in msg


# ---- the evidence of a finished run: a failed run never replaces a pass of the same sources ----------------------------


def ev(result, sources="s1", profile="hub-ws", **kw):
    return {"result": result, "sources_hash": sources, "profile": profile, **kw}


def read(path):
    import json

    return json.loads(path.read_text())


def test_a_finished_run_writes_the_shared_file_and_a_copy_for_its_profile(uw, tmp_path):
    kept = uw.publish_evidence(ev("pass"), tmp_path)
    assert kept is True
    assert read(tmp_path / "evidence.json")["result"] == "pass"
    assert read(tmp_path / "evidence.hub_ws.json")["result"] == "pass"


def test_a_failed_run_at_the_same_sources_keeps_the_pass_and_records_itself_per_profile(uw, tmp_path):
    uw.publish_evidence(ev("pass", profile="e2-demo"), tmp_path)
    kept = uw.publish_evidence(ev("fail", profile="hub-ws"), tmp_path)
    assert kept is False
    assert read(tmp_path / "evidence.json")["profile"] == "e2-demo" and read(tmp_path / "evidence.json")["result"] == "pass"
    assert read(tmp_path / "evidence.hub_ws.json")["result"] == "fail"


def test_a_failed_run_replaces_a_pass_of_other_sources_and_a_fail(uw, tmp_path):
    uw.publish_evidence(ev("pass", sources="old"), tmp_path)
    assert uw.publish_evidence(ev("fail", sources="new"), tmp_path) is True
    assert read(tmp_path / "evidence.json")["sources_hash"] == "new"
    assert uw.publish_evidence(ev("fail", sources="new", note="again"), tmp_path) is True
    assert read(tmp_path / "evidence.json")["note"] == "again"


def test_a_pass_always_replaces_the_shared_file(uw, tmp_path):
    uw.publish_evidence(ev("pass", profile="e2-demo", n=1), tmp_path)
    assert uw.publish_evidence(ev("pass", profile="hub-ws", n=2), tmp_path) is True
    assert read(tmp_path / "evidence.json")["n"] == 2


def test_an_unreadable_shared_file_is_replaced(uw, tmp_path):
    (tmp_path / "evidence.json").write_text("{not json")
    assert uw.publish_evidence(ev("fail"), tmp_path) is True
    assert read(tmp_path / "evidence.json")["result"] == "fail"


def test_the_evidence_name_and_a_missing_profile_are_handled(uw, tmp_path):
    uw.publish_evidence({"result": "pass", "sources_hash": "x"}, tmp_path, name="evidence-x")
    assert (tmp_path / "evidence-x.json").is_file() and not (tmp_path / "evidence.json").exists()


def test_the_deps_mode_comes_from_its_per_profile_key_and_the_option_beats_it(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text(HUB_CFG + "GBX_GATE_DEPS__hub_ws=volume\n")
    hub = args(profile="hub-ws", deps=None)
    uw.apply_gate_target(hub, cfg, "dflt", "/Volumes/dflt")
    assert hub.deps == "volume" and hub.target_source["deps"] == "GBX_GATE_DEPS__hub_ws"
    e2 = args(profile="e2-demo", deps=None)
    uw.apply_gate_target(e2, cfg, "dflt", "/Volumes/dflt")
    assert e2.deps == "pypi" and e2.target_source["deps"] == "default"
    opt = args(profile="hub-ws", deps="pypi")
    uw.apply_gate_target(opt, cfg, "dflt", "/Volumes/dflt")
    assert opt.deps == "pypi" and opt.target_source["deps"] == "option"
    assert "deps volume" in uw.target_line(hub, "hub-ws")


def test_a_bad_deps_key_value_is_refused_with_a_fix(uw, tmp_path):
    cfg = tmp_path / "c.env"
    cfg.write_text("GBX_GATE_DEPS__hub_ws=wheelhouse\n")
    with pytest.raises(ValueError, match="pypi.*volume"):
        uw.apply_gate_target(args(profile="hub-ws", deps=None), cfg, "dflt", "/Volumes/dflt")
