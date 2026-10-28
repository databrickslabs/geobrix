"""Contract checks generated from the subset manifest (rule 15 item 4). No per-function tests."""

import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest

MAN = manifest.load()
FNS = sorted(MAN.functions.values(), key=lambda f: f.sql_name)
REPO = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
def test_signature_shape(fn):
    assert fn.sql_name.startswith("gbx_") and re.fullmatch(r"[a-z0-9_]+", fn.sql_name)
    assert 1 <= fn.min_args <= fn.max_args == len(fn.params)
    names = [p.name for p in fn.params]
    assert len(set(names)) == len(names) and all(
        re.fullmatch(r"[a-z][a-z0-9_]*", n) for n in names
    )
    assert all(
        p.default is not None for p in fn.params[fn.min_args :]
    ), "only trailing params are optional"


def _refs(fn):
    l3 = fn.levels.get("L3", {})
    out = [("L3", r) for r in l3.get("variants", {}).values()]
    out += [("L3", l3[k]) for k in ("factory", "callable", "udtf") if k in l3]
    if "L4" in fn.levels:
        out.append(("L4", fn.levels["L4"]["handler"]))
    return out


# Task 5 creates databricks.labs.gbx.uc.handlers; remove this marker there.
_L4_PENDING = pytest.mark.xfail(strict=True, reason="uc.handlers lands in Task 5")
REFS = [
    pytest.param(
        f.sql_name,
        ref,
        id=f"{f.sql_name}-{lvl}-{ref.rpartition(':')[2]}",
        marks=[_L4_PENDING] if lvl == "L4" else [],
    )
    for f in FNS
    for lvl, ref in _refs(f)
]


@pytest.mark.parametrize("sql_name,ref", REFS)
def test_python_refs_resolve(sql_name, ref):
    assert manifest.resolve_ref(ref) is not None, ref


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
def test_oracle_rows_fit_signature(fn):
    from databricks.labs.gbx.bench import oracles

    rows = [c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name]
    assert rows, "every manifest function needs oracle rows"
    assert len({c.cid for c in rows}) == len(rows), "oracle row ids must be unique"
    for c in rows:
        assert fn.min_args <= len(c.args) <= fn.max_args, c.cid
        assert c.surfaces and set(c.surfaces) <= {"session", "uc"}, c.cid


@pytest.mark.parametrize(
    "fn", [f for f in FNS if "L1" in f.levels], ids=lambda f: f.sql_name
)
def test_l1_companion_exists_in_source(fn):
    fqn = fn.levels["L1"]["companion"]
    pkg, _, obj = fqn.rpartition(".")
    src = REPO / "src/main/scala" / pkg.replace(".", "/")
    assert any(f"object {obj} " in p.read_text() for p in src.glob("*.scala")), fqn


def test_tile_sql_matches_v2_schema():
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    assert manifest.TILE_SQL == V2_TILE_SCHEMA.simpleString()


def test_uc_pins_match_env6_lock():
    lock = (REPO / "python/geobrix/requirements-light-env6-ci.txt").read_text()
    for name, ver in MAN.uc_dependencies.items():
        assert re.search(rf"^{re.escape(name)}=={re.escape(ver)}\b", lock, re.M), (
            name,
            ver,
        )
