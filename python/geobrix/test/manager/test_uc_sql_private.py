"""An L4 SQL body may call private Python routines (``private``): the manifest checks the key, install creates them."""

import copy
import json
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest, uc

VOL = "/Volumes/cat/sch/vol/uc"
RAW = json.loads(Path(manifest.__file__).with_name("functions.json").read_text())
# A row with an L4 SQL body of its own (not the L0 one) and a private routine that already exist; the fixture below
# joins them.
FIXTURE_FN = "gbx_rst_memsize"
FIXTURE_PRIVATE = "gbx__rst_polygonize_features"


def _levels(**l4):
    return {"L4": {"handler": "m:f", **l4}}


def test_private_key_requires_sql_and_known_names():
    known = {"gbx__fromfile_plan"}
    with pytest.raises(ValueError, match="private"):  # an unknown name
        manifest._check_sql_levels(
            "gbx_rst_x",
            _levels(sql="select 1", private=["gbx__nope"]),
            "scalar",
            known,
        )
    with pytest.raises(ValueError, match="needs sql"):  # no body to call it from
        manifest._check_sql_levels(
            "gbx_rst_x", _levels(private=["gbx__fromfile_plan"]), "scalar", known
        )
    with pytest.raises(ValueError, match="non-empty list"):
        manifest._check_sql_levels(
            "gbx_rst_x", _levels(sql="select 1", private=[]), "scalar", known
        )
    # a known name with a body is accepted
    manifest._check_sql_levels(
        "gbx_rst_x",
        _levels(sql="select 1", private=["gbx__fromfile_plan"]),
        "scalar",
        known,
    )


def test_private_with_the_l0_body_is_refused():
    # the session's L0 function would call a private routine nothing creates there
    levels = {
        "L0": {"sql": "1"},
        "L4": {"sql": "L0", "private": ["gbx__fromfile_plan"]},
    }
    with pytest.raises(ValueError, match="L0"):
        manifest._check_sql_levels(
            "gbx_rst_x", levels, "scalar", {"gbx__fromfile_plan"}
        )


def test_private_listing_a_name_twice_is_refused():
    with pytest.raises(ValueError, match="twice"):
        manifest._check_sql_levels(
            "gbx_rst_x",
            _levels(sql="select 1", private=["gbx__fromfile_plan"] * 2),
            "scalar",
            {"gbx__fromfile_plan"},
        )


def test_private_beside_a_stub_is_refused():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if "stub" in r["levels"].get("L4", {}))
    row["levels"]["L4"]["private"] = [FIXTURE_PRIVATE]
    with pytest.raises(ValueError, match="stub"):
        manifest.parse(raw)


def _with_private(monkeypatch, **extra_l4):
    """TEST FIXTURE ONLY: an in-memory manifest whose FIXTURE_FN row also lists FIXTURE_PRIVATE as `private`."""
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == FIXTURE_FN)
    row["levels"]["L4"]["private"] = [FIXTURE_PRIVATE]
    row["levels"]["L4"].update(extra_l4)
    man = manifest.parse(raw)
    monkeypatch.setattr(manifest, "load", lambda: man)
    return man


def test_loader_rejects_a_private_name_that_is_not_a_uc_private_row():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == FIXTURE_FN)
    row["levels"]["L4"]["private"] = ["gbx__nope"]
    with pytest.raises(ValueError, match="gbx__nope"):
        manifest.parse(raw)


def test_install_creates_private_routines_before_the_function(monkeypatch):
    _with_private(monkeypatch)
    stmts = uc.install("cat", "sch", VOL, dry_run=True, only=[FIXTURE_FN])
    text = "\n".join(stmts)
    i_priv = text.index(f"`cat`.`sch`.{FIXTURE_PRIVATE}(")
    i_fn = text.index(f"`cat`.`sch`.{FIXTURE_FN}(")
    assert i_priv < i_fn
    assert text.count(f"FUNCTION `cat`.`sch`.{FIXTURE_PRIVATE}(") == 1


def test_a_private_shared_by_two_functions_is_created_once(monkeypatch):
    # a private gbx_rst_fromfile calls, listed on a second SQL row too: still one CREATE in the install
    raw = copy.deepcopy(RAW)
    shared = "gbx__fromfile_plan"
    row = next(r for r in raw["functions"] if r["sql_name"] == FIXTURE_FN)
    row["levels"]["L4"]["private"] = [shared]
    both = manifest.parse(raw)
    monkeypatch.setattr(manifest, "load", lambda: both)
    stmts = uc.install(
        "cat", "sch", VOL, dry_run=True, only=[FIXTURE_FN, "gbx_rst_fromfile"]
    )
    assert sum(f"FUNCTION `cat`.`sch`.{shared}(" in s for s in stmts) == 1


def test_private_honours_volume_deps_like_a_tvf_private(monkeypatch):
    _with_private(monkeypatch)
    stmts = uc.install(
        "cat",
        "sch",
        VOL,
        dry_run=True,
        only=[FIXTURE_FN],
        deps="volume",
        wheels_dir=VOL,
    )
    priv = next(s for s in stmts if f"`sch`.{FIXTURE_PRIVATE}(" in s)
    assert "--no-index" in priv and f"--find-links={VOL}/x86_64" in priv


def test_a_private_is_not_a_public_function(monkeypatch):
    _with_private(monkeypatch)
    assert FIXTURE_PRIVATE not in uc._l4_names(None)
    with pytest.raises(ValueError, match="no UC function"):
        uc._l4_names([FIXTURE_PRIVATE])


def test_install_dry_run_creates_private_before_function():
    stmts = uc.install("cat", "sch", VOL, dry_run=True, only=["gbx_rst_fromfile"])
    text = "\n".join(stmts)
    i_plan = text.index("FUNCTION `cat`.`sch`.gbx__fromfile_plan")
    i_read = text.index("FUNCTION `cat`.`sch`.gbx__fromfile_read")
    i_fn = text.index("FUNCTION `cat`.`sch`.gbx_rst_fromfile(")
    assert i_plan < i_fn and i_read < i_fn
    assert text.count("gbx__fromfile_plan(") >= 2  # created once, called in the body
