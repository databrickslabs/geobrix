"""Contract checks generated from the per-surface manifest. No per-function tests."""

import dataclasses
import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.core import crs as gcrs
from databricks.labs.gbx.manager import capabilities as caps_mod
from databricks.labs.gbx.manager import levels
from databricks.labs.gbx.uc import ddl

from .connect_stub import ConnectStub

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
    assert all(
        p.default is None for p in fn.params[: fn.min_args]
    ), "a required param carries no default"
    if "L4" in fn.levels:  # the UC DDL is emitted from these types
        assert fn.returns_uc and all(
            p.uc for p in fn.params
        ), "an L4 function types every UC param and return"


def _refs(fn):
    l3 = fn.levels.get("L3", {})
    out = [("L3", r) for r in l3.get("variants", {}).values()]
    out += [("L3", l3[k]) for k in ("factory", "callable", "udtf") if k in l3]
    if "handler" in fn.levels.get("L4", {}):
        out.append(("L4", fn.levels["L4"]["handler"]))
    return out


REFS = [
    pytest.param(f.sql_name, ref, id=f"{f.sql_name}-{lvl}-{ref.rpartition(':')[2]}")
    for f in FNS
    for lvl, ref in _refs(f)
]


@pytest.mark.parametrize("sql_name,ref", REFS)
def test_python_refs_resolve(sql_name, ref):
    assert manifest.resolve_ref(ref) is not None, ref


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
def test_oracle_rows_fit_signature(fn):
    from databricks.labs.gbx.bench import oracles

    rows = [c for c in oracles.TABLES[fn.oracle] if c.fn == fn.sql_name]
    assert rows, "every manifest function needs oracle rows"
    assert len({c.cid for c in rows}) == len(rows), "oracle row ids must be unique"
    # an aggregate's first argument is the group's rows: one row fills 1 parameter (a tile) or 2 (cellid, value)
    extra = oracles.group_width(fn.params) - 1 if fn.kind == "aggregate" else 0
    for c in rows:
        arg_rows = (
            c.args
            if c.kind in ("exact_rows", "windows_batch")
            else (c.args, *c.variants)
        )
        assert all(
            fn.min_args <= len(r) + extra <= fn.max_args for r in arg_rows
        ), c.cid
        assert c.kind != "exact_rows" or len(c.expect) == len(c.args), c.cid
        assert c.surfaces and set(c.surfaces) <= {"session", "uc"}, c.cid
        assert not c.variants or c.kind in ("exact", "null"), c.cid
        if (
            c.kind == "raises"
        ):  # an error row names the offending value, so a missing function cannot pass
            assert isinstance(c.expect, str) and c.expect, c.cid


@pytest.mark.parametrize(
    "fn",
    [f for f in FNS if "companion" in f.levels.get("L1", {})],
    ids=lambda f: f.sql_name,
)
def test_l1_companion_exists_in_source(fn):
    fqn = fn.levels["L1"]["companion"]
    pkg, _, obj = fqn.rpartition(".")
    src = REPO / "src/main/scala" / pkg.replace(".", "/")
    texts = [p.read_text() for p in src.glob("*.scala")]
    assert any(f"object {obj} " in t and f'"{fn.sql_name}"' in t for t in texts), fqn


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
def test_level_keys_are_in_the_closed_vocabulary(fn):
    assert set(fn.levels) <= set(manifest.LEVEL_KEYS), fn.levels.keys()
    for lvl, spec in fn.levels.items():
        assert set(spec) <= manifest.LEVEL_KEYS[lvl], (
            lvl,
            set(spec) - manifest.LEVEL_KEYS[lvl],
        )
        assert set(manifest.LEVEL_REQUIRED.get(lvl, ())) <= set(spec), lvl
        if lvl in manifest.LEVEL_REQUIRED_ANY:
            assert set(spec) & manifest.LEVEL_REQUIRED_ANY[lvl], lvl


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
def test_schema2_fields(fn):
    assert fn.status in manifest.STATUSES
    assert all(p.py in manifest.PY_MODES for p in fn.params)
    if (
        fn.status == "mvp"
    ):  # a probe: installed by name only, with no Python entry or docs
        assert fn.python is None, fn.sql_name
        return
    assert fn.doc.summary and fn.doc.page.startswith("api/"), fn.sql_name
    assert re.fullmatch(r"\d+\.\d+\.\d+", fn.since), fn.sql_name
    if fn.status == "stable":
        assert fn.python is not None and fn.python.module == manifest.package_of(
            fn.sql_name
        )
        assert (
            fn.example
        ), f"{fn.sql_name}: name the oracle row its generated example comes from"
    if fn.python is not None and fn.python.extras:
        assert fn.python.extras in manifest.PY_EXTRAS and fn.python.extra_impl
        assert manifest.resolve_ref(fn.python.extra_impl) is not None


@pytest.mark.parametrize("helper", MAN.helpers, ids=lambda h: h.name)
def test_helpers_import_and_have_oracle_rows(helper):
    import importlib

    from databricks.labs.gbx.bench import oracles

    assert callable(getattr(importlib.import_module(helper.module), helper.name))
    assert any(
        c.fn == helper.name for c in oracles.MVP_H
    ), "every helper needs oracle rows"


def test_manifest_is_hashable_and_immutable():
    fn = MAN.functions["gbx_st_makevalid"]
    assert hash(fn) is not None and fn in {fn}
    with pytest.raises(TypeError):
        fn.levels["L3"]["register"] = "x"
    with pytest.raises(TypeError):
        MAN.functions["gbx_other"] = fn
    with pytest.raises(TypeError):
        MAN.uc_dependencies["h3"] = "0"


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


def _caps(**kw):
    base = dict(
        connect=False,
        serverless=False,
        jar=None,
        jar_reason="no geobrix JAR on classpath",
        gdal_reason="",
        sql_functions=True,
    )
    base.update(kw)
    return caps_mod.Capabilities(**base)


OK_JAR = caps_mod.JarIdentity("0.6.0", "dev")


def _l1_shape_ok(fn):
    l1 = fn.levels.get("L1", {})
    return (
        "L1" in fn.levels
        and l1.get("session_returns", fn.returns_session) == fn.returns_session
    )


@pytest.mark.parametrize("fn", FNS, ids=lambda f: f.sql_name)
@pytest.mark.parametrize(
    "case",
    [
        ("serverless", dict(connect=True, serverless=True)),
        ("no-jar", dict()),
        (
            "stale-jar",
            dict(jar=None, jar_reason="JAR 0.5.3 on classpath != wheel 0.6.0"),
        ),
        ("dedicated", dict(jar=OK_JAR, jar_reason="", jvm_gdal=MAN.gdal.pin)),
        (
            "standard",
            dict(connect=True, jar=OK_JAR, jar_reason="", jvm_gdal=MAN.gdal.pin),
        ),
        (
            "no-gdal",
            dict(jar=OK_JAR, jar_reason="", gdal_reason="GDAL natives not loadable"),
        ),
        # A dedicated JVM on another GDAL than the pin: no engine at L1, the row's own L1 where it has one.
        ("other-jvm-gdal", dict(jar=OK_JAR, jar_reason="", jvm_gdal="3.11.4")),
        # A row with no per-name order takes its family's: a family that prefers light selects L3 with no
        # reasons, even on a dedicated cluster where L1 is available.
        ("family-l3", dict(jar=OK_JAR, jar_reason="", jvm_gdal=MAN.gdal.pin)),
    ],
    ids=lambda c: c[0],
)
def test_level_selection_contract(fn, case, monkeypatch):
    """Invariants of the level table, generated per manifest row (not a hand-written expectation list)."""
    name, kw = case
    if name == "family-l3":
        monkeypatch.delitem(levels.PREFERENCE, fn.sql_name, raising=False)
        monkeypatch.setitem(levels.FAMILY_PREFERENCE, fn.family, ("L3",))
    if (
        name == "dedicated"
    ):  # an env wheel: this process runs the light engine the L1 engine sends virtual tiles to
        from databricks.labs.gbx.engine import run

        monkeypatch.setattr(run, "unavailable_reason", lambda: None)
    choice = levels.select(fn, _caps(**kw))
    pref = [lv for lv in levels.order(fn) if lv in fn.levels]
    if name == "family-l3":
        assert (choice.level, choice.reasons) == ("L3", ())
    assert choice.level in pref
    assert len(choice.reasons) == pref.index(
        choice.level
    )  # one reason per skipped, preferred level
    if name in ("serverless", "no-jar", "stale-jar"):
        assert choice.level == "L3"
    if name == "standard":
        assert choice.level != "L1"  # Catalyst is never chosen on Connect
        l2 = fn.levels.get("L2", {})
        if (
            not l2
            or fn.kind != "scalar"
            or (fn.min_args != fn.max_args and "wrapper" not in l2)
        ):
            assert choice.level == "L3"
    if name == "dedicated" and pref[0] == "L1":
        assert (choice.level == "L1") == _l1_shape_ok(fn)
    if name == "no-gdal" and fn.levels.get("L1", {}).get("needs_gdal"):
        assert choice.level == "L3"
    if name == "other-jvm-gdal" and pref[0] == "L1":
        own_l1 = "companion" in fn.levels["L1"]
        assert (choice.level == "L1") == (_l1_shape_ok(fn) and own_l1)
        assert choice.variant != levels.ENGINE or choice.level != "L1"


# Functions whose preferred levels may return different types (none today).
RETURN_TYPE_ALLOWLIST: dict = {}


def test_preferred_levels_share_one_return_type():
    """A SQL name returns one type on every compute: each preferred level's session return type
    (its `session_returns`, else the row's `returns.session`) is the same."""
    mixed = {}
    for name, pref in levels.PREFERENCE.items():
        fn = MAN.functions.get(name)
        if fn is None or name in RETURN_TYPE_ALLOWLIST:
            continue
        types = {
            lv: fn.levels[lv].get("session_returns", fn.returns_session)
            for lv in pref
            if lv in fn.levels
        }
        if len(set(types.values())) > 1:
            mixed[name] = types
    assert not mixed, f"preferred levels return different types: {mixed}"


@pytest.mark.parametrize(
    "fn", [f for f in FNS if {"L1", "L2"} & set(f.levels)], ids=lambda f: f.sql_name
)
def test_override_is_honoured(fn):
    forced = "L1" if "L1" in fn.levels else "L2"
    assert levels.select(fn, _caps()).level != forced  # no JAR: the default is light
    assert levels.select(fn, _caps(), override=(forced, "-")).level == forced


class _Session:
    """A session that records what an install registers (``udf.register``) and runs (``sql``)."""

    def __init__(self):
        self.registered, self.statements, self.udf = [], [], self

    def register(self, name, f, returnType=None):
        self.registered.append(name)

    def sql(self, text):
        self.statements.append(text)


@pytest.mark.parametrize(
    "engine,connect,want",
    [
        (True, True, "engine-c0"),  # Connect: the row-wise UDF alone
        (
            True,
            False,
            "engine-c1",
        ),  # classic with SQL functions: the batch UDF behind the wrapper
        (False, True, "fallback"),  # no engine here: the light kernel, and why
    ],
    ids=["engine-connect", "engine-classic", "no-engine"],
)
def test_an_unforced_engine_row_installs_the_engine_or_says_why_not(
    monkeypatch, capsys, engine, connect, want
):
    """The default (not forced) light install of a row with a ``gdal`` block: the engine where it runs, as c0 on
    Connect and as c1 on a classic session; else the light kernel, with the reason said once.
    """
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.manager import jvm

    fn = MAN.functions["gbx_rst_slope"]
    monkeypatch.setattr(
        levels, "engine_unavailable", lambda f, lv, c: None if engine else "no payload"
    )
    monkeypatch.setattr(caps_mod, "is_connect", lambda s: connect)
    monkeypatch.setattr(caps_mod, "_sql_functions", lambda s: True)
    monkeypatch.setattr(file_gbx, "report_detected_cap", lambda s=None: 1 << 26)
    spark = _Session()
    lines = []
    choice = manager._install(
        spark, fn, levels.Choice("L3", "-", ()), _caps(), forced=False, lines=lines
    )
    said = "\n".join(line for _, line in lines)
    if want == "fallback":
        assert choice.variant not in levels.ENGINE_L3
        assert spark.registered == [
            "gbx__l3d_rst_slope"
        ]  # the light kernel, behind the wrapper that carries the defaults
        assert (
            f"{fn.sql_name} runs without the GDAL spec engine at light (no payload)"
            in said
        )
        return
    assert choice.variant == want and "runs without" not in said
    if want == "engine-c0":
        assert spark.registered == [fn.sql_name]
    else:
        assert spark.registered == [jvm._priv(fn, "L3"), jvm._priv(fn, "L3c1")]
        assert any(fn.sql_name in s for s in spark.statements), spark.statements


def test_l1_engine_needs_the_light_engine_for_its_virtual_tiles(monkeypatch):
    """L1's engine sends virtual tiles to the light engine (the c0 UDF beside it): where this process cannot run
    that (no payload osgeo), the L1 engine is unavailable, with the light engine's reason, not a runtime failure.
    """
    from databricks.labs.gbx.engine import run

    fn = MAN.functions["gbx_rst_transformcrs"]
    assert fn.accepts_virtual_tile
    dedicated = _caps(jar=OK_JAR, jar_reason="", jvm_gdal=MAN.gdal.pin)
    monkeypatch.setattr(run, "unavailable_reason", lambda: None)
    assert levels.engine_unavailable(fn, "L1", dedicated) is None
    monkeypatch.setattr(run, "unavailable_reason", lambda: "no osgeo here")
    why = levels.engine_unavailable(fn, "L1", dedicated)
    assert why and "no osgeo here" in why and "virtual" in why
    assert levels.select(fn, dedicated).variant != levels.ENGINE


def test_l2_is_refused_while_proj_grids_are_registered(tmp_path, monkeypatch):
    """A Java UDF has no channel for PROJ grid directories: a warp through it would ignore them. The JVM engine
    keeps L1: every GDALManager.init adds a configuration's grid directories to PROJ's search path, also after
    an earlier init without them (GDALManagerProjGridsSpec holds that).
    """
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.engine import run

    monkeypatch.setattr(
        run, "unavailable_reason", lambda: None
    )  # the L1 engine's virtual-tile side runs here
    fn = MAN.functions["gbx_rst_transformcrs"]
    # L2 for a raster function exists on classic only (standard has no Java handler for it): judge the rule there
    classic = _caps(jar=OK_JAR, jar_reason="")
    assert levels.available(fn, "L2", classic) is None
    saved = proj_grids.get_registered_dirs()
    proj_grids.set_registered_dirs(str(tmp_path), replace=True)
    try:
        why = levels.available(fn, "L2", classic)
    finally:
        proj_grids.set_registered_dirs(saved, replace=True)
    assert why and "PROJ grids" in why
    # The JVM engine reads them: with grids registered, L1 still runs the engine.
    dedicated = _caps(jar=OK_JAR, jar_reason="", jvm_gdal=MAN.gdal.pin)
    assert levels.select(fn, dedicated).variant == levels.ENGINE
    proj_grids.set_registered_dirs(str(tmp_path), replace=True)
    try:
        choice = levels.select(fn, dedicated)
        why = levels.engine_unavailable(fn, "L1", dedicated)
    finally:
        proj_grids.set_registered_dirs(saved, replace=True)
    assert (choice.level, choice.variant) == ("L1", levels.ENGINE) and why is None


def test_tile_reference_compares_a_grid_one_pixel_off_by_pixels():
    """A grid one pixel larger and shifted by one pixel is not accepted on its mean alone."""
    import numpy as np
    from rasterio.crs import CRS
    from rasterio.io import MemoryFile
    from rasterio.warp import calculate_default_transform

    from databricks.labs.gbx.bench import oracles

    case = next(c for c in oracles.MVP_S if c.cid == "tcrs_epsg")
    src_tile = oracles._build("tile_4326", "")
    with MemoryFile(oracles._raster_of(src_tile)) as mf, mf.open() as src:
        width, height, ref = oracles._reference_warp(src, "EPSG:3857")
        transform, _, _ = calculate_default_transform(
            src.crs, CRS.from_epsg(3857), src.width, src.height, *src.bounds
        )
        nodata = src.nodata

    def gtiff(pixels):
        profile = dict(driver="GTiff", count=1, dtype="float32", nodata=nodata)
        profile.update(crs=CRS.from_epsg(3857), transform=transform)
        with MemoryFile() as out:
            with out.open(
                height=pixels.shape[0], width=pixels.shape[1], **profile
            ) as ds:
                ds.write(pixels[None])
            return out.read()

    grid = ref.filled(nodata).astype("float32")
    shifted = np.full((height, width + 1), nodata, dtype="float32")
    shifted[:, 1:] = grid
    check = lambda raster: oracles._check_tile(  # noqa: E731
        case, {"raster": raster}, (src_tile, "EPSG:3857")
    )
    assert check(gtiff(grid)) is None
    assert "pixels match" in check(gtiff(shifted))


def test_pending_heavy_rows_name_real_oracle_rows():
    from databricks.labs.gbx.bench import oracles

    rows = {c.cid for table in oracles.TABLES.values() for c in table}
    assert set(oracles.HEAVY_PENDING) <= rows
    assert all(
        set(levels_) <= {"L1", "L2"} for levels_ in oracles.HEAVY_PENDING.values()
    )


def _declares(fqn, kind):
    pkg, _, name = fqn.rpartition(".")
    src = REPO / "src/main/scala" / pkg.replace(".", "/")
    return any(
        re.search(rf"^{kind} {name}\b", p.read_text(), re.M)
        for p in src.glob("*.scala")
    )


@pytest.mark.parametrize(
    "fqn",
    [f.levels["L2"]["class"] for f in FNS if "L2" in f.levels]
    + [caps_mod.BUILDINFO_UDF, caps_mod.GDALPROBE_UDF],
    ids=lambda fqn: fqn.rpartition(".")[2],
)
def test_java_udf_classes_exist_in_source(fqn):
    """A mistyped class name would read as "no geobrix JAR on classpath" on every standard cluster."""
    assert _declares(fqn, "class"), fqn


@pytest.mark.parametrize(
    "identity,jar_found",
    [
        ("unknown|unknown", False),
        ("unknown|abc", False),
        ("dev|dev", False),
        ("0.5.3|abc", False),
        ("0.6.0|abc", True),
    ],
)
def test_jar_identity_is_judged_against_the_wheel(monkeypatch, identity, jar_found):
    """The JVM reports `<version>|<sha256>`; an unknown or development JAR is never used."""
    from types import SimpleNamespace as NS

    # an env wheel's own bundled JAR would judge the sha as well; this row is about the version
    monkeypatch.setattr(caps_mod, "_expected_sha", lambda: None)

    kernels = NS(
        BuildInfo=NS(identity=lambda: identity), KernelGdal=NS(probe=lambda: "ok:3")
    )
    spark = NS(
        _jvm=NS(com=NS(databricks=NS(labs=NS(gbx=NS(kernels=kernels))))),
        conf=NS(get=lambda k, d=None: d, set=lambda k, v: None),
    )
    assert (caps_mod.detect(spark, True).jar is not None) is jar_found


def test_detect_points_the_jvm_at_the_wheel_natives_before_probing_gdal(monkeypatch):
    """On a stock dedicated runtime the GDAL probe is the JVM's first GDAL load: it must already see the wheel's
    native directory, or it finds no JNI and every raster name falls back to light (measured on DBR 19).
    """
    from types import SimpleNamespace as NS

    from databricks.labs.gbx.manager import jvm

    seen = []
    monkeypatch.setattr(caps_mod, "_identity", lambda spark, connect: ("0.6.0|abc", ""))
    monkeypatch.setattr(caps_mod, "_expected_sha", lambda: "abc")
    monkeypatch.setattr(
        jvm, "_point_jvm_at_payload", lambda spark: seen.append("point")
    )
    monkeypatch.setattr(
        caps_mod,
        "_gdal",
        lambda spark, connect: (seen.append("probe"), ("", "3.13.3"))[1],
    )
    monkeypatch.setattr(caps_mod, "_sql_functions", lambda spark: True)
    assert caps_mod.detect(NS(), True).jvm_gdal == "3.13.3"
    assert seen == ["point", "probe"]


@pytest.mark.parametrize(
    "runtime,reason",
    [
        ("client.6.0", "Spark Connect session (Serverless)"),
        (None, "Spark Connect session (standard access mode)"),
    ],
    ids=["serverless", "standard"],
)
def test_connect_session_never_touches_the_jvm(monkeypatch, capsys, runtime, reason):
    from databricks.labs.gbx import manager

    if (
        runtime is None
    ):  # standard access mode: the JAR is probed through Connect-safe calls
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    else:
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", runtime)
    from databricks.labs.gbx.manager import _legacy

    stub = ConnectStub()
    out = manager.register(stub)
    assert {c.level for c in out.values()} == {"L3"}
    # every name, the ones not yet in the manifest too, and the light data sources
    assert set(out) == (set(_legacy.names()) | set(MAN.functions)) - {
        f.sql_name for f in FNS if f.status == "mvp"
    }
    assert "raster_gbx" in stub.sources
    assert stub.touched == []  # _jvm/_jsc/sparkContext/conf.set never accessed
    printed = capsys.readouterr().out.splitlines()
    assert len(printed) <= 6, printed  # one line per reason, not one per name
    assert printed.count(manager.messages.REMINDER) == 1
    assert len(printed) == len(
        set(printed)
    )  # each reason line once per session, across all names
    assert any(f"{reason}; using light" in p for p in printed)
    assert not any(
        "Error" in p for p in printed
    )  # a reason is plain words, never an exception class
    manager.register(stub)  # a second call in the same session says nothing new
    assert capsys.readouterr().out == ""


def test_one_unavailable_function_does_not_stop_the_rest(monkeypatch, capsys):
    import importlib.util

    from databricks.labs.gbx import manager

    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda n, *a, **k: None if n == "rasterio" else real(n, *a, **k),
    )
    from databricks.labs.gbx.engine import run

    # an activated payload's osgeo would serve transformcrs through the spec engine without rasterio
    monkeypatch.setattr(run, "unavailable_reason", lambda: "no osgeo in this test")
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.6.0")
    stub = ConnectStub()
    out = manager.register(stub)
    assert "gbx_rst_transformcrs" not in out and "gbx_bng_eastnorthasbng" in out
    assert (
        "gbx_st_makevalid" in stub.registered
    )  # registered after the skipped one (it takes its own defaults: no wrapper)
    skipped = [
        p for p in capsys.readouterr().out.splitlines() if "gbx_rst_transformcrs" in p
    ]
    assert len(skipped) == 1 and "rasterio" in skipped[0]


def test_bad_target():
    from databricks.labs.gbx import manager

    with pytest.raises(ValueError, match="target"):
        manager.register(object(), target="both")


UC_FNS = [f for f in FNS if "L4" in f.levels]


@pytest.mark.parametrize("fn", UC_FNS, ids=lambda f: f.sql_name)
def test_uc_ddl_contract(fn):
    stmts = ddl.function_ddl(
        fn,
        "cat",
        "sch",
        "file:///Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl",
        MAN.uc_dependencies,
        "0.6.0",
    )
    impl = stmts[0]
    assert all("`cat`.`sch`." in s for s in stmts)  # 3-level names only
    if "tvf_over" in fn.levels["L4"]:
        # A generator is a SQL table function over the array function: no Python, no environment.
        (tvf,) = stmts
        over = fn.levels["L4"]["tvf_over"]
        if any(
            p.default for p in MAN.functions[over].params
        ):  # its public name is a SQL wrapper
            over = f"gbx__{over[len('gbx_'):]}"
        assert "COMMENT 'geobrix 0.6.0'" in tvf and "LANGUAGE PYTHON" not in tvf
        assert f"RETURNS TABLE ({', '.join(f'{n} {t}' for n, t in fn.columns)})" in tvf
        assert tvf.endswith(
            f"RETURN SELECT inline(`cat`.`sch`.{over}({', '.join(p.name for p in fn.params)}))"
        )
        assert (
            all(f"{p.name} {p.uc}" in tvf for p in fn.params) and "DEFAULT false" in tvf
        )
        return
    assert "environment_version = '6'" in impl and "geobrix @ file:///Volumes/" in impl
    assert "COMMENT 'geobrix 0.6.0'" in impl and "LANGUAGE PYTHON" in impl
    has_default = any(p.default for p in fn.params)
    assert len(stmts) == (2 if has_default else 1)
    if has_default:
        assert (
            f"gbx__{fn.sql_name[4:]}(" in impl
            and "LANGUAGE SQL" in stmts[1]
            and "DEFAULT" in stmts[1]
        )
    for p in fn.params:
        if "GEOMETRY" in p.session or p.uc == "GEOMETRY":
            assert f"{p.name} GEOMETRY" in impl


def test_uc_ddl_batch_style_names_a_pandas_handler_beside_the_product_function():
    (stmt,) = ddl.function_ddl(
        MAN.functions["gbx_bng_eastnorthasbng"],
        "cat",
        "sch",
        "file:///Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl",
        MAN.uc_dependencies,
        "0.6.0",
        style="batch",
        name="gbx_mvp_bng_eastnorthasbng_batch",
    )
    assert "LANGUAGE PYTHON\nPARAMETER STYLE PANDAS\n" in stmt
    assert "`cat`.`sch`.gbx_mvp_bng_eastnorthasbng_batch(" in stmt
    assert "bng_eastnorthasbng_batch as handler" in stmt


@pytest.mark.parametrize(
    "value,want",
    [
        (3857, ("EPSG", 3857)),
        ("3857", ("EPSG", 3857)),
        ("EPSG:4326", ("EPSG", 4326)),
        ("epsg:4326", ("EPSG", 4326)),
        ("ESRI:102003", ("ESRI", 102003)),
        (102003, ("ESRI", 102003)),
        ("EPSG:102003", None),  # an ESRI code under the EPSG name is not an EPSG code
        # a digit run beyond any code gets the same Fix, not an int() error
        pytest.param("EPSG:" + "9" * 5000, None, id="epsg-5000-digits"),
        pytest.param("9" * 5000, None, id="5000-digits"),
        pytest.param(10**30, None, id="int-1e30"),
        ("IAU_2015:30100", None),
        ("urn:ogc:def:crs:EPSG::4326", None),
        ('GEOGCS["WGS 84"]', None),
        ("+proj=longlat +datum=WGS84", None),
        ('{"type": "GeographicCRS"}', None),
        (0, None),
        (-1, None),
        (True, None),
    ],
)
def test_uc_crs_parser_contract(value, want):
    """From the product CRS probe: an int SRID or EPSG:/ESRI:<n> only, as an INT; the rest refused."""
    if want:
        got = gcrs.parse_uc_crs(value, fn="gbx_x")
        assert (got.authority, got.srid) == want and isinstance(got.srid, int)
    else:
        # Raised, never NULL: with ANSI off a bad string would reach the product st_* as a silent NULL.
        with pytest.raises(ValueError, match=r"Fix: .*session function gbx_x"):
            gcrs.parse_uc_crs(value, fn="gbx_x")


def test_uc_handlers_import_and_run_in_a_sandbox_without_pyspark():
    """A UC Python sandbox holds the declared deps only: no pyspark, and none of the light tier's own extras."""
    import os
    import subprocess
    import sys

    from databricks.labs.gbx.bench import oracles

    code = (
        "import sys, tempfile\n"
        "for blocked in ('pyspark', 'pandas', 'pyarrow', 'scipy'):\n"
        "    sys.modules[blocked] = None  # an import of it raises ImportError\n"
        "from databricks.labs.gbx.bench import oracles\n"
        "ran = oracles.run_handlers(oracles.MVP_S, tmpdir=tempfile.mkdtemp())\n"
        "ran += oracles.run_handlers_rows(oracles.MVP_G, tmpdir=tempfile.mkdtemp())\n"
        "bad = [m for _, m in ran if m]\n"
        "assert ran and not bad, bad\n"
        "print(len(ran))\n"
    )
    src = (
        Path(manifest.__file__).resolve().parents[4]
    )  # the root of the databricks package
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            filter(None, [str(src), os.environ.get("PYTHONPATH")])
        ),
    }
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert int(
        out.stdout
    ) == sum(  # every UC row ran: the scalars, and the table function's handler
        "uc" in c.surfaces for c in oracles.MVP_S
    ) + sum(
        "uc" in c.surfaces and c.fn == oracles.TE for c in oracles.MVP_G
    )


class _UcSpark:
    """A session stand-in: records statements and answers information_schema.routines.

    Like Unity Catalog it stores the schema (`stored_schema`) and the routine names lower-case, and
    it applies the WHERE on the schema the query sends.
    """

    def __init__(self, routines=None, stored_schema="sch"):
        self.stmts, self.routines, self.stored_schema = [], routines, stored_schema

    def sql(self, query):
        self.stmts.append(query)
        rows = []
        if "information_schema.routines" in query:
            asked = re.search(r"lower\(routine_schema\) = '([^']*)'", query)
            if asked and asked.group(1) == self.stored_schema:
                rows = [
                    {"routine_name": n, "comment": c}
                    for n, c in (self.routines or {}).items()
                ]
        return type("R", (), {"collect": lambda _s, r=rows: r})()


ENV6_WHEEL = "geobrix-0.6.0+env6-py3-none-any.whl"
ENV5_WHEEL = "geobrix-0.6.0+env5-py3-none-any.whl"
PLAIN_WHEEL = "geobrix-0.6.0-py3-none-any.whl"
UC_NAMES = {f.sql_name: "geobrix 0.6.0" for f in UC_FNS}
VOLUME_DIR = "/Volumes/c/s/v/dir"


@pytest.fixture
def volume(monkeypatch):
    """Stand-in for the Volume folder: set `.files` to the wheel names it holds."""
    from databricks.labs.gbx.ds import _listing

    box = type("Volume", (), {"files": []})()
    monkeypatch.setattr(
        _listing,
        "list_files",
        lambda path, filter_regex=".*", **_kw: [  # the real filter, on the names given
            f"{path}/{n}" for n in box.files if re.match(filter_regex, f"{path}/{n}")
        ],
    )
    return box


@pytest.mark.parametrize(
    "files,wheel,error",
    [
        ([PLAIN_WHEEL], PLAIN_WHEEL, None),
        ([ENV6_WHEEL], ENV6_WHEEL, None),
        ([ENV5_WHEEL, ENV6_WHEEL], None, "one folder per environment"),
        ([PLAIN_WHEEL, ENV5_WHEEL], None, "one folder per environment"),
        ([ENV5_WHEEL], None, re.escape(ENV6_WHEEL)),
        ([], None, "exactly one"),
        ([PLAIN_WHEEL, ENV6_WHEEL], None, "exactly one"),
        (["geobrix-0.5.3-py3-none-any.whl"], None, "exactly one"),
    ],
    ids=[
        "plain",
        "env6",
        "env6-beside-env5",
        "plain-beside-env5",
        "env5-only",
        "none",
        "two",
        "other-version",
    ],
)
def test_uc_install_deploys_one_env6_wheel_and_verifies(
    volume, monkeypatch, files, wheel, error
):
    from databricks.labs.gbx import uc

    volume.files = files
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "6")
    spark = _UcSpark(UC_NAMES)
    if error:
        with pytest.raises(RuntimeError, match=error):
            uc.install("cat", "sch", VOLUME_DIR, spark=spark)
        assert not spark.stmts  # nothing is created from the wrong wheel
        return
    stmts = uc.install("cat", "sch", VOLUME_DIR, spark=spark)
    assert spark.stmts[: len(stmts)] == stmts  # every statement ran, in order
    assert f"geobrix @ file://{VOLUME_DIR}/{wheel}" in stmts[0]
    assert (
        "information_schema.routines" in spark.stmts[-1]
    )  # then the schema was verified


def test_uc_install_refuses_a_runtime_that_is_not_env6(volume, monkeypatch):
    from databricks.labs.gbx import uc

    volume.files = [ENV6_WHEEL]
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "5")
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.5.0")
    with pytest.raises(RuntimeError, match="env 6"):
        uc.install("cat", "sch", VOLUME_DIR, spark=_UcSpark(UC_NAMES))


def test_uc_install_dry_run_emits_the_ddl_without_a_session_or_a_wheel(monkeypatch):
    from databricks.labs.gbx import uc

    monkeypatch.delenv("DATABRICKS_ENV_VERSION", raising=False)
    everything = uc.install("cat", "sch", VOLUME_DIR, dry_run=True)
    assert len(everything) == sum(
        (
            1
            if "tvf_over" in f.levels["L4"]
            else 2 if any(p.default for p in f.params) else 1
        )
        for f in UC_FNS
    )
    assert all(ENV6_WHEEL in s for s in everything if "LANGUAGE PYTHON" in s)
    only = uc.install(
        "cat", "sch", VOLUME_DIR, only=["gbx_bng_eastnorthasbng"], dry_run=True
    )
    assert len(only) == 1 and "gbx_bng_eastnorthasbng(" in only[0]
    # a table function is created after the function it wraps, which `only` brings along
    tvf = uc.install(
        "cat", "sch", VOLUME_DIR, only=["gbx_bng_tessellateexplode"], dry_run=True
    )
    assert ["LANGUAGE PYTHON" in s for s in tvf] == [True, False, False]
    assert "RETURNS TABLE" in tvf[2] and "gbx_bng_tessellateexplode(" in tvf[2]
    hyphen = uc.install(
        "my-cat", "sch", VOLUME_DIR, only=["bng_eastnorthasbng"], dry_run=True
    )
    assert (
        "`my-cat`.`sch`.gbx_bng_eastnorthasbng(" in hyphen[0]
    )  # a name is always quoted


@pytest.mark.parametrize(
    "catalog,schema,volume_dir",
    [
        ("cat", "x' OR '1'='1", VOLUME_DIR),
        ("ca`t", "sch", VOLUME_DIR),
        ("cat", "sch.other", VOLUME_DIR),
        ("cat", "", VOLUME_DIR),
        ("cat", "sch", "/Volumes/c/s/o'brien"),
        ("cat", "sch", "/Volumes/c/s/v/a\\b"),
        ("cat", "sch", "/Volumes/c/s/v/a\nb"),
        ("cat", "sch", "/Volumes/c/s/v/a\x00b"),
        ("cat", "sch", "/Volumes/c/s"),
        ("cat", "sch", "relative/dir"),
        ("cat", "sch", "dbfs:/Volumes/c/s/v/dir"),
    ],
)
def test_uc_names_and_paths_are_validated_before_any_sql_is_built(
    volume, monkeypatch, catalog, schema, volume_dir
):
    from databricks.labs.gbx import register, uc

    volume.files = [ENV6_WHEEL]
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "6")
    spark = _UcSpark(UC_NAMES)
    with pytest.raises(ValueError, match="Fix:"):
        uc.install(catalog, schema, volume_dir, spark=spark, dry_run=True)
    with pytest.raises(ValueError, match="Fix:"):
        uc.install(catalog, schema, volume_dir, spark=spark)
    if volume_dir == VOLUME_DIR:  # a hostile name must not reach verify's WHERE either
        with pytest.raises(ValueError, match="Fix:"):
            register(spark, target="uc", catalog=catalog, schema=schema)
    assert not spark.stmts


@pytest.mark.parametrize(
    "routines,schema,stored,error",
    [
        (UC_NAMES, "sch", "sch", None),
        (
            {**UC_NAMES, "gbx_st_makevalid": "geobrix 0.5.3"},
            "sch",
            "sch",
            "gbx_st_makevalid",
        ),
        (
            {n: c for n, c in UC_NAMES.items() if n != "gbx_st_makevalid"},
            "sch",
            "sch",
            "gbx_st_makevalid",
        ),
        # Unity Catalog stores names lower-case: the schema is asked for, and the routines compared, lower-cased
        (UC_NAMES, "GeoBrix_UC", "geobrix_uc", None),
        ({n.upper(): c for n, c in UC_NAMES.items()}, "sch", "sch", None),
    ],
    ids=["match", "old-version", "missing", "mixed-case-schema", "mixed-case-routines"],
)
def test_register_uc_checks_the_schema_against_this_wheel(
    routines, schema, stored, error
):
    from databricks.labs.gbx import register

    spark = _UcSpark(routines, stored_schema=stored)
    if error:
        with pytest.raises(RuntimeError, match=error):
            register(spark, target="uc", catalog="cat", schema=schema)
    else:
        assert register(spark, target="uc", catalog="cat", schema=schema) == {}
    with pytest.raises(ValueError, match="catalog= and schema="):
        register(_UcSpark(routines), target="uc")


# --- Java handlers on standard access mode -----------------------------------------------------------------
# Measured on a DBR 19 standard cluster: a JAR installed as a cluster library is invisible to the Java UDF
# sandbox (CLASS_NOT_FOUND, for a Java class too); a JAR added to the SESSION (`spark.addArtifact`) is seen, but
# only its Java classes are loaded (a Scala `class` still fails) and their parameters must be concrete types
# (an `Object` parameter fails in the input conversion). So the manifest names a Java `connect_class` per row.

BNG = MAN.functions["gbx_bng_eastnorthasbng"]
CONNECT_BNG = BNG.levels["L2"]["connect_class"]


def _standard_stub(monkeypatch, tmp_path, jar=True):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.setattr(
        caps_mod, "_expected_sha", lambda: None
    )  # not the payload's bundled JAR
    path = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    path.write_bytes(b"PK")
    if jar:
        monkeypatch.setenv("GBX_JAR_PATH", str(path))
    else:
        monkeypatch.delenv("GBX_JAR_PATH", raising=False)
        from databricks.labs.gbx import _native

        monkeypatch.setattr(_native, "jar_path", lambda root=None: None)
    stub = ConnectStub()
    stub.sandbox_classes = {
        CONNECT_BNG,
        caps_mod.BUILDINFO_CONNECT_UDF,
        caps_mod.GDALPROBE_CONNECT_UDF,
    }
    return stub, str(path)


def test_standard_l2_registers_the_java_handler_on_a_session_jar(monkeypatch, tmp_path):
    from databricks.labs.gbx.manager import jvm

    stub, jar = _standard_stub(monkeypatch, tmp_path)
    assert jvm.install_l2(stub, BNG, _caps(connect=True, jar=OK_JAR)) is None
    assert stub.artifacts == [jar]  # the JAR goes to the session first
    # the Java UDF under its private name; the SQL name is the wrapper that casts to its declared types
    assert stub.java_registered == [
        ("gbx__l2_bng_eastnorthasbng", CONNECT_BNG, "string")
    ]
    # one JAR, one attach per session, however many names register
    jvm.install_l2(stub, BNG, _caps(connect=True, jar=OK_JAR))
    assert stub.artifacts == [jar]


def test_cast_call_wraps_the_private_java_udf_with_declared_types():
    """The L2 SQL wrapper, as text (the JAR-gated test only sees it run): each parameter typed in order, each
    argument cast to the same type, the session return type, and a call to the private name.
    """
    from databricks.labs.gbx.manager import sqlwrap

    assert sqlwrap.cast_call(
        BNG, "gbx__l2_bng_eastnorthasbng", ["DOUBLE", "DOUBLE", "STRING"]
    ) == (
        "CREATE OR REPLACE TEMPORARY FUNCTION gbx_bng_eastnorthasbng("
        "easting DOUBLE, northing DOUBLE, resolution STRING) RETURNS STRING "
        "RETURN gbx__l2_bng_eastnorthasbng(CAST(easting AS DOUBLE), "
        "CAST(northing AS DOUBLE), CAST(resolution AS STRING))"
    )


@pytest.mark.parametrize("types", [["DOUBLE", "DOUBLE"], ["DOUBLE"] * 4, []])
def test_cast_call_refuses_a_type_list_that_does_not_match_the_parameters(types):
    from databricks.labs.gbx.manager import sqlwrap

    with pytest.raises(ValueError, match="arg_types"):
        sqlwrap.cast_call(BNG, "gbx__l2_bng_eastnorthasbng", types)


_ARG_TYPE_NAMES = {"DOUBLE", "STRING", "INT", "BIGINT", "BOOLEAN", "BINARY", "FLOAT"}


@pytest.mark.parametrize(
    "fn",
    [f for f in FNS if "arg_types" in f.levels.get("L2", {})],
    ids=lambda f: f.sql_name,
)
def test_l2_arg_types_name_every_parameter_with_a_sql_type(fn):
    types = fn.levels["L2"]["arg_types"]
    assert len(types) == len(fn.params), (fn.sql_name, types)
    assert set(types) <= _ARG_TYPE_NAMES, types
    assert fn.min_args == fn.max_args, "a cast wrapper takes every parameter"


def test_l2_with_arg_types_needs_sql_functions_and_falls_back_without_them():
    """Both polarities of the gate: with SQL functions L2 is offered; without, it is refused with a reason
    (the manager then falls back to the next level and says why)."""
    on = _caps(connect=True, jar=OK_JAR, sql_functions=True)
    off = _caps(connect=True, jar=OK_JAR, sql_functions=False)
    assert levels.available(BNG, "L2", on) is None
    why = levels.available(BNG, "L2", off)
    assert why and "needs a SQL function wrapper" in why
    assert why == levels.no_sql_functions(BNG)


@pytest.mark.parametrize(
    "fn, connect",
    [
        (BNG, True),  # an `arg_types` row (standard access mode)
        (next(f for f in FNS if "bytes_param" in f.levels.get("L2", {})), False),
    ],  # a `bytes_param` row has no Java handler for standard access mode: classic
    ids=lambda p: p.sql_name if hasattr(p, "sql_name") else str(p),
)
def test_forced_l2_without_sql_functions_refuses_before_touching_the_session(
    fn, connect, monkeypatch, tmp_path
):
    """A forced L2 skips `levels.available`. It must refuse before it registers the Java UDF or runs the
    wrapper's DROP, so the registration already under the SQL name (L3, say) stays."""
    from databricks.labs.gbx.manager import jvm

    stub, _ = _standard_stub(monkeypatch, tmp_path)
    caps = _caps(connect=connect, jar=OK_JAR, sql_functions=False)
    why = jvm.install_l2(stub, fn, caps)
    assert why == levels.no_sql_functions(fn)
    assert stub.artifacts == [] and stub.java_registered == []
    assert stub.sql_text == []  # no DROP, no CREATE


def test_l2_wrapper_replaces_the_sql_name_after_the_private_udf_is_registered(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx.manager import jvm, sqlwrap

    stub, _ = _standard_stub(monkeypatch, tmp_path)
    assert jvm.install_l2(stub, BNG, _caps(connect=True, jar=OK_JAR)) is None
    assert stub.sql_text == [
        "DROP TEMPORARY FUNCTION IF EXISTS gbx_bng_eastnorthasbng",
        sqlwrap.cast_call(
            BNG, "gbx__l2_bng_eastnorthasbng", ["DOUBLE", "DOUBLE", "STRING"]
        ),
    ]


def test_standard_l2_without_a_jar_says_where_to_get_one(monkeypatch, tmp_path):
    from databricks.labs.gbx.manager import jvm

    stub, _ = _standard_stub(monkeypatch, tmp_path, jar=False)
    why = jvm.install_l2(stub, BNG, _caps(connect=True, jar=OK_JAR))
    assert why and "GBX_JAR_PATH" in why and "standard access mode" in why
    assert stub.artifacts == [] and stub.java_registered == []


def test_detect_on_standard_reads_the_jar_through_the_session_handlers(
    monkeypatch, tmp_path
):
    stub, jar = _standard_stub(monkeypatch, tmp_path)
    caps = caps_mod.detect(stub, need_jvm=True)
    assert caps.connect and caps.jar is not None and caps.gdal_reason == ""
    assert stub.artifacts == [jar]
    assert {c for _, c, _ in stub.java_registered} == {
        caps_mod.BUILDINFO_CONNECT_UDF,
        caps_mod.GDALPROBE_CONNECT_UDF,
    }


@pytest.mark.parametrize(
    "fn", [f for f in FNS if "L2" in f.levels], ids=lambda f: f.sql_name
)
def test_standard_offers_l2_only_for_a_row_with_a_java_handler(fn):
    why = levels.available(fn, "L2", _caps(connect=True, jar=OK_JAR))
    if "connect_class" in fn.levels["L2"]:
        assert why is None
    else:
        assert why and "Java handler" in why and "standard access mode" in why
    # classic is unaffected: the Scala class is what it registers
    assert "Java handler" not in (levels.available(fn, "L2", _caps(jar=OK_JAR)) or "")


@pytest.mark.parametrize(
    "fn",
    [f for f in FNS if "L2" in f.levels and "connect_class" not in f.levels["L2"]],
    ids=lambda f: f.sql_name,
)
def test_forced_l2_on_standard_refuses_a_row_without_a_java_handler_cleanly(
    fn, monkeypatch, tmp_path
):
    """The test-only L2 override skips `levels.available`; install must still refuse, not raise a raw KeyError."""
    from databricks.labs.gbx.manager import jvm

    stub, _ = _standard_stub(monkeypatch, tmp_path)
    why = jvm.install_l2(stub, fn, _caps(connect=True, jar=OK_JAR))
    assert why and "has no Java handler for standard access mode" in why
    assert "KeyError" not in why
    assert stub.java_registered == []
    # the same words the default path gives
    assert levels.available(fn, "L2", _caps(connect=True, jar=OK_JAR)) == why


def _gates():
    import importlib.util
    import sys

    path = REPO / "scripts/manifest/check_surfaces.py"
    spec = importlib.util.spec_from_file_location("gbx_check_surfaces", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_replacement_gates_pass_on_the_committed_manifest():
    assert _gates().run() == []


def test_overlap_gate_refuses_a_uc_type_that_is_not_a_form_of_the_session_type(
    monkeypatch,
):
    gates = _gates()
    fn = MAN.functions["gbx_bng_eastnorthasbng"]
    bad = dataclasses.replace(
        fn, params=(dataclasses.replace(fn.params[0], uc="STRING"),) + fn.params[1:]
    )
    assert any("easting" in f for f in gates.overlap_findings([bad]))


# --- the replacement gates fire on what they guard (each case feeds a break and expects the finding) ----------


def _row(name, **changes):
    return dataclasses.replace(MAN.functions[name], **changes)


def test_oracle_gate_names_a_missing_table_a_missing_row_and_a_multi_row_example():
    from databricks.labs.gbx.bench import oracles

    gates = _gates()
    assert any(
        "does not exist" in f
        for f in gates.oracle_findings([_row("gbx_bng_eastnorthasbng", oracle="NOPE")])
    )
    fn = MAN.functions["gbx_bng_eastnorthasbng"]
    assert any(
        "no row in" in f
        for f in gates.oracle_findings([_row(fn.sql_name, sql_name="gbx_bng_zzz")])
    )
    assert any(
        "is not a single-row case" in f
        for f in gates.oracle_findings([_row(fn.sql_name, example="nonexistent")])
    )
    with_variants = next(
        c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name and c.variants
    )
    assert any(
        "is not a single-row case" in f
        for f in gates.oracle_findings([_row(fn.sql_name, example=with_variants.cid)])
    )


def test_completeness_gate_names_each_missing_surface(tmp_path, monkeypatch):
    gates = _gates()
    name = "gbx_rst_transformcrs"
    fn = MAN.functions[name]

    def found(fn_, **patch):
        for k, v in patch.items():
            monkeypatch.setattr(gates, k, v)
        return gates.completeness_findings(MAN, [fn_])

    # a function-info file and a docs page that lack the row
    info = tmp_path / "function-info.json"
    info.write_text('{"functions": {}}')
    assert any(
        "no entry in" in f and "function-info" in f
        for f in found(fn, FUNCTION_INFO=info)
    )
    monkeypatch.undo()
    docs = tmp_path / "docs"
    (docs / "api").mkdir(parents=True)
    (docs / f"{fn.doc.page}.mdx").write_text("### other\n")
    assert any("no section '### rst_transformcrs'" in f for f in found(fn, DOCS=docs))
    monkeypatch.undo()
    # a Python entry that is not in the generated region
    ghost = dataclasses.replace(fn.python, name="not_generated")
    assert any(
        "no generated Python entry not_generated" in f
        for f in found(_row(name, python=ghost))
    )
    # a UC handler that is not defined, and a table function over a row that does not exist
    l4 = {
        **fn.levels,
        "L4": {**fn.levels["L4"], "handler": "databricks.labs.gbx.uc.handlers:missing"},
    }
    assert any(
        "UC handler" in f and "not defined" in f for f in found(_row(name, levels=l4))
    )
    tvf = {**fn.levels, "L4": {"tvf_over": "gbx_nope"}}
    assert any("gbx_nope, which has no row" in f for f in found(_row(name, levels=tvf)))
    # a Scala companion that does not declare the name, and a surface with no wrapper in its own package
    bng = MAN.functions["gbx_bng_eastnorthasbng"]
    l1 = {
        **bng.levels,
        "L1": {
            **bng.levels["L1"],
            "companion": "com.databricks.labs.gbx.gridx.bng.BNG_Tessellate",
        },
    }
    assert any(
        "does not declare that name" in f for f in found(_row(bng.sql_name, levels=l1))
    )
    assert any(
        "no wrapper" in f
        for f in found(
            bng,
            _scala_wrappers=lambda: {
                "bng_eastnorthasbng": {"other/functions.scala": [[]]}
            },
        )
    )


def test_a_companion_must_declare_the_name_in_its_own_object():
    gates = _gates()
    text = 'object A extends X {\n  def name: String = "gbx_a"\n}\nobject B extends X {\n  override def name: String = "gbx_b"\n}\n'
    assert gates._declares_name(text, "A", "gbx_a")
    assert gates._declares_name(text, "B", "gbx_b")
    assert not gates._declares_name(
        text, "A", "gbx_b"
    )  # quoted in another object of the file
    assert not gates._declares_name(text, "C", "gbx_a")


def test_a_row_with_no_python_entry_still_needs_its_docs_section(tmp_path, monkeypatch):
    gates = _gates()
    docs = tmp_path / "docs"
    (docs / "api").mkdir(parents=True)
    fn = MAN.functions["gbx_bng_eastnorthasbng"]
    (docs / f"{fn.doc.page}.mdx").write_text("### other\n")
    monkeypatch.setattr(gates, "DOCS", docs)
    found = gates.completeness_findings(MAN, [dataclasses.replace(fn, python=None)])
    assert any("no section '### bng_eastnorthasbng'" in f for f in found)


def test_overlap_gate_compares_the_uc_return_type():
    gates = _gates()
    assert any(
        "UC returns INT" in f
        for f in gates.overlap_findings(
            [_row("gbx_bng_eastnorthasbng", returns_uc="INT")]
        )
    )
    # a BINARY session return may be GEOMETRY on UC
    makevalid = MAN.functions["gbx_st_makevalid"]
    assert makevalid.returns_session == "BINARY"
    assert (
        gates.overlap_findings([_row("gbx_st_makevalid", returns_uc="GEOMETRY")]) == []
    )


def test_collision_gate_fires_on_a_private_name_equal_to_a_public_one_and_a_duplicate_row(
    tmp_path, monkeypatch
):
    """The G2 seed: a manifest where `gbx__x` is also a public row (the private UC name of `gbx_x`, which has a
    defaulted parameter), a row that does not start gbx_, and the same row twice."""
    import json
    from types import SimpleNamespace

    gates = _gates()
    base = MAN.functions[
        "gbx_rst_slope"
    ]  # has defaulted parameters, so it has a private UC name
    assert any(p.default for p in base.params)
    private = dataclasses.replace(base, sql_name="gbx__rst_slope")
    unprefixed = dataclasses.replace(base, sql_name="rst_x")
    man = SimpleNamespace(
        functions={f.sql_name: f for f in (base, private, unprefixed)}
    )
    raw = tmp_path / "manifest"
    raw.mkdir()
    rows = [
        {"sql_name": n}
        for n in ("gbx_rst_slope", "gbx__rst_slope", "rst_x", "gbx_rst_slope")
    ]
    (raw / "functions.json").write_text(json.dumps({"functions": rows}))
    monkeypatch.setattr(gates, "PKG", tmp_path)
    found = gates.collision_findings(man)
    assert any("gbx_rst_slope: two manifest rows" in f for f in found)
    assert any("rst_x: a routine name must start gbx_" in f for f in found)
    assert any("gbx__rst_slope: a routine name must start gbx_" in f for f in found)
    assert any(
        "gbx_rst_slope: its UC private function name is a public name" in f
        for f in found
    )


def test_param_name_gate_covers_usage_arity_orphan_wrapper_and_snake_case(monkeypatch):
    gates = _gates()
    fn = MAN.functions["gbx_rst_transformcrs"]
    renamed = dataclasses.replace(
        fn, params=(fn.params[0], dataclasses.replace(fn.params[1], name="to_crs"))
    )
    found = gates.name_findings([renamed])
    assert any("function-info.json" in f and "to_crs" in f for f in found)
    for where in ("rasterx/functions.py", "functions.scala"):
        assert any("to_crs" in f and where in f for f in found), (where, found)
    camel = dataclasses.replace(
        fn, params=(fn.params[0], dataclasses.replace(fn.params[1], name="targetCrs"))
    )
    assert any("not snake_case" in f for f in gates.name_findings([camel]))
    # a Scala wrapper with more arguments than the signature, and a wrapper for a row with no Scala surface
    own = gates._rel(gates._scala_functions_file(fn))
    monkeypatch.setattr(
        gates,
        "_scala_wrappers",
        lambda: {"rst_transformcrs": {own: [["a", "b", "c", "d"]]}},
    )
    assert any(
        "Scala wrapper arities [4] above" in f for f in gates.name_findings([fn])
    )
    bare = _row(
        "gbx_rst_transformcrs",
        levels={k: v for k, v in fn.levels.items() if k != "L1"},
        gdal=None,
    )
    assert "scala" not in bare.surfaces
    assert any("delete the wrapper" in f for f in gates.name_findings([bare]))
    # a wrapper of the same name in ANOTHER package is not this row's
    monkeypatch.setattr(
        gates,
        "_scala_wrappers",
        lambda: {
            "rst_transformcrs": {"src/other/functions.scala": [["a", "b", "c", "d"]]}
        },
    )
    assert not any("Scala wrapper" in f for f in gates.name_findings([bare]))


def test_docs_tab_gate_rejects_a_bad_tab(tmp_path, monkeypatch):
    gates = _gates()
    fn = MAN.functions["gbx_rst_slope"]
    assert "scala" not in fn.surfaces
    docs = tmp_path / "docs"
    (docs / "api").mkdir(parents=True)
    src = tmp_path / "src.py"
    src.write_text("def rst_slope_python_example(spark):\n    return 1\n")
    (docs / f"{fn.doc.page}.mdx").write_text(
        '<FunctionExamples\n  name="rst_slope"\n'
        '  pythonHeavy={a}  pythonHeavySource="x.py"\n'
        '  python={b}  pythonSource="%s"\n'
        '  scala={c}  scalaSource="x.scala"\n/>\n' % src
    )
    monkeypatch.setattr(gates, "DOCS", docs)
    monkeypatch.setattr(gates, "REPO", tmp_path)
    found = gates.docs_findings([fn])
    assert any("'pythonHeavy' tab" in f for f in found)
    assert any("'scala' tab for a surface it does not have" in f for f in found)
    assert any("needs rst_slope_python_example_output" in f for f in found)
    assert not any("needs rst_slope_python_example in" in f for f in found)


def test_since_gate_names_a_stable_row_without_a_release():
    gates = _gates()
    for bad in ("", "0.4", "v0.4.0"):
        assert any(
            "`since`" in f
            for f in gates.since_findings([_row("gbx_rst_slope", since=bad)])
        ), bad
    probe = next((f for f in FNS if f.status != "stable"), None)
    if probe is not None:
        assert gates.since_findings([dataclasses.replace(probe, since="")]) == []


# register() serves every registered name: grouped fallback lines, names not yet in the manifest, the
# FILE-reference wrapper, and capabilities detected once per session.


def test_shared_fallback_reason_prints_once_with_a_count(capsys):
    from databricks.labs.gbx.manager import messages

    spark = object.__new__(type("S", (), {}))
    line = "geobrix: heavy unavailable: Spark Connect session (Serverless); using light"
    messages.say_grouped(spark, [(f"gbx_rst_f{i}", line) for i in range(40)])
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1 and "40 functions" in out[0], out


def test_file_ref_wrapper_leaves_the_file_expression_as_given_for_any_parameter_name():
    """The caller builds the FILE expression around the tile parameter's own name; the wrapper must not rewrite a
    `tile.` inside a longer name (`raster_tile.` stays `raster_tile.`)."""
    from types import SimpleNamespace as NS

    from databricks.labs.gbx.manager import sqlwrap

    fn = NS(
        sql_name="gbx_rst_x",
        returns_session="TILE",
        params=[
            NS(name="raster_tile", session=["TILE"], default=None),
            NS(name="target_crs", session=["STRING"], default=None),
        ],
    )
    ddl = sqlwrap.file_ref_wrapper(
        fn, "gbx__l3f_rst_x", "try_to_file(raster_tile.path)"
    )
    assert (
        "gbx__l3f_rst_x(raster_tile, try_to_file(raster_tile.path), target_crs)" in ddl
    ), ddl


def test_first_python_call_carries_the_remote_read_switch(spark, monkeypatch):
    """The first Python use of a driver-reading name registers it with the driver's remote-path switch carried,
    so a first Python call reads a remote path the way SQL does after register()."""
    from databricks.labs.gbx import _api, _gdal_policy

    seen = []
    real = _gdal_policy._CarryingRegistration.register

    def spy(self, name, f, *a, **k):
        seen.append((name, self._allowed))
        return real(self, name, f, *a, **k)

    monkeypatch.setattr(_gdal_policy._CarryingRegistration, "register", spy)
    monkeypatch.setenv(_gdal_policy.REMOTE_ENV, "true")
    _api._done(spark).discard("gbx_rst_fromfile")
    _api.ensure(spark, "gbx_rst_fromfile")
    assert ("gbx_rst_fromfile", True) in seen


def test_a_file_ref_row_registers_the_file_udf_behind_its_wrapper(monkeypatch):
    """An L3 `file_ref` row on a session with SQL functions: the light UDF that takes the FILE reference under its
    private name, and the wrapper that sends it under the SQL name."""
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.manager import l3

    fn = MAN.functions["gbx_rst_slope"]
    l3_spec = dict(fn.levels["L3"], file_ref=fn.levels["L3"]["variants"]["c0"])
    row = dataclasses.replace(fn, levels={**fn.levels, "L3": l3_spec})
    monkeypatch.setattr(caps_mod, "_sql_functions", lambda s: True)
    monkeypatch.setattr(file_gbx, "file_supported", lambda spark=None: False)
    spark = _Session()
    l3.register_l3(spark, row, "c0")
    assert spark.registered == ["gbx__l3f_rst_slope"]
    assert any(
        "gbx__l3f_rst_slope(tile, NULL, " in s and s.startswith("CREATE")
        for s in spark.statements
    ), spark.statements


def test_every_manifest_family_has_a_family_order():
    missing = sorted({f.family for f in FNS} - set(levels.FAMILY_PREFERENCE))
    assert not missing, missing
    assert all(o[-1] == "L3" for o in levels.FAMILY_PREFERENCE.values())


def test_l1_gives_the_jvm_the_light_read_cap_unless_the_session_names_one():
    """One number decides when a materialized tile is too large to read: the JVM's read cap is the light tier's
    decoded budget, and a value the session already has (a user's, a test's) is kept."""
    from databricks.labs.gbx.manager import jvm
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    class Conf(dict):
        def get(self, k, default=None):
            return super().get(k, default)

        def set(self, k, v):
            self[k] = v

    class S:
        conf = Conf()

    jvm._pass_read_cap(S)
    assert S.conf[jvm.READ_CAP_KEY] == str(decoded_budget_bytes("classic"))
    S.conf[jvm.READ_CAP_KEY] = "4096"
    jvm._pass_read_cap(S)
    assert S.conf[jvm.READ_CAP_KEY] == "4096"


@pytest.mark.parametrize(
    "names,want",
    [
        (
            ["gbx_rst_a"],
            "geobrix: gbx_rst_a runs without the GDAL spec engine at light (x)",
        ),
        (
            ["gbx_rst_a", "gbx_rst_b", "gbx_rst_c"],
            "geobrix: 3 functions (gbx_rst_a, gbx_rst_b, gbx_rst_c) run without the GDAL spec engine at light (x)",
        ),
    ],
    ids=["one", "three"],
)
def test_a_grouped_line_agrees_in_number(capsys, names, want):
    from databricks.labs.gbx.manager import messages

    spark = object.__new__(type("S3", (), {}))
    messages.say_grouped(
        spark,
        [
            (n, f"geobrix: {n} runs without the GDAL spec engine at light (x)")
            for n in names
        ],
    )
    assert capsys.readouterr().out.strip() == want


# A tile decoded over the read cap: the gate's own Fix points at gbx_rst_retile, and a header answer needs no pixel, so
# neither may be refused by the size gate (the pixel readers stay gated).
_HEADER_ANSWERS = {
    "rst_width": 64,
    "rst_height": 48,
    "rst_numbands": 1,
    "rst_srid": 4326,
    "rst_format": "GTiff",
}


def _answers(spark, tmp_path, names, register_it, fixture="tile_4326", cap=None):
    """The accessors' answers for a fixture tile, with the read cap lowered to `cap` (when given) while `register_it`
    runs (a light function carries the cap from its registration) and while the query runs (the JVM reads it per
    query)."""
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    tile = oracles._build(fixture, str(tmp_path))
    schema = StructType([StructField("tile", V2_TILE_SCHEMA)])
    spark.createDataFrame([(tile,)], schema).createOrReplaceTempView("_gbx_answers")
    if cap is not None:
        spark.conf.set(READ_CAP_KEY, str(cap))
    try:
        register_it()
        calls = ", ".join(f"gbx_{n}(tile) AS {n}" for n in names)
        return spark.sql(f"SELECT {calls} FROM _gbx_answers").collect()[0].asDict()
    finally:
        spark.conf.unset(READ_CAP_KEY)


def _light(spark, names):
    from databricks.labs.gbx import register

    return lambda: register(spark, only=[f"gbx_{n}" for n in names])


def _heavy(spark, names):
    """The Catalyst accessors under their own names (the manifest does not hold them yet)."""
    from databricks.labs.gbx.manager import jvm

    def install():
        jvm._point_jvm_at_payload(spark)
        inst = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
        base = "com.databricks.labs.gbx.rasterx.expressions.accessors.RST_"
        for n in names:
            cls = {"rst_numbands": "NumBands", "rst_srid": "SRID"}.get(
                n, n[len("rst_") :].capitalize()
            )
            r = inst.install(spark._jsparkSession, f"gbx_{n}", base + cls)
            assert str(r) == "ok", r

    return install


def test_light_header_accessors_answer_for_a_tile_over_the_read_cap(spark, tmp_path):
    names = [*_HEADER_ANSWERS, "rst_avg"]
    got = _answers(spark, tmp_path, names, _light(spark, names), cap=4096)
    assert {k: got[k] for k in _HEADER_ANSWERS} == _HEADER_ANSWERS
    assert got["rst_avg"] is None, "a pixel reader still refuses the tile"


@pytest.mark.integration
def test_heavy_header_accessors_answer_for_a_tile_over_the_read_cap(
    jvm_spark, tmp_path
):
    names = [*_HEADER_ANSWERS, "rst_avg"]
    got = _answers(jvm_spark, tmp_path, names, _heavy(jvm_spark, names), cap=4096)
    assert {k: got[k] for k in _HEADER_ANSWERS} == _HEADER_ANSWERS
    assert got["rst_avg"] is None, "a pixel reader still refuses the tile"


def test_light_format_of_a_netcdf_tile_is_its_own(spark, tmp_path):
    names = ["rst_format"]
    got = _answers(spark, tmp_path, names, _light(spark, names), "tile_4326_netcdf")
    assert got["rst_format"] == "netCDF"


@pytest.mark.integration
def test_heavy_format_of_a_netcdf_tile_is_its_own_not_the_geotiff_it_is_read_as(
    jvm_spark, tmp_path
):
    """Heavy reads netCDF bytes as their GeoTIFF translation; the format still names the tile's own driver, as
    light's does."""
    names = ["rst_format"]
    got = _answers(
        jvm_spark, tmp_path, names, _heavy(jvm_spark, names), "tile_4326_netcdf"
    )
    assert got["rst_format"] == "netCDF"


def test_a_failing_data_source_registration_is_a_reported_line_not_a_failure(
    monkeypatch, capsys
):
    """The light data sources register last; if that fails the functions are registered all the same, the failure is
    one of the grouped lines, and the session still counts the names as done."""
    from databricks.labs.gbx import _api, manager
    from databricks.labs.gbx.ds import register as sources

    def boom(spark):
        raise RuntimeError("no data source support here")

    monkeypatch.setattr(sources, "register", boom)
    stub = ConnectStub()
    out = manager.register(stub)
    printed = capsys.readouterr().out.splitlines()
    assert any(
        "light data sources not registered (RuntimeError: no data source support here)"
        in p
        for p in printed
    ), printed
    assert out and set(out) <= _api._done(stub), "the names are still marked registered"


from databricks.labs.gbx.pyrx.core import budget as _budget  # noqa: E402

# --- the read cap is decided on the driver and carried to the workers -----------------------------------------
# A Spark Connect session is Serverless OR standard access mode. A standard worker can read neither the classic
# memory env (budget.CLASSIC_MEMORY_ENV) nor a cgroup limit, so its own budget is Serverless's 64 MiB: the driver decides, whenever the compute is not Serverless.
_MIB = 1024 * 1024
_WORKER_MEM = _budget.CLASSIC_MEMORY_ENV[0]


@pytest.mark.parametrize(
    "env,want",
    [
        ({_WORKER_MEM: "4g"}, 1536 * _MIB),
        (
            {_WORKER_MEM: "4g", "DATABRICKS_RUNTIME_VERSION": "17.3"},
            1536 * _MIB,
        ),
        ({"DATABRICKS_RUNTIME_VERSION": "client.4.1"}, None),
        (
            {_WORKER_MEM: "4g", "DATABRICKS_RUNTIME_VERSION": "client.4.1"},
            None,
        ),
        ({_WORKER_MEM: "4g", "IS_SERVERLESS": "true"}, None),
        ({}, None),
    ],
    ids=[
        "standard",
        "standard-dbr",
        "serverless",
        "serverless-with-memory",
        "is-serverless",
        "no-driver-signal",
    ],
)
def test_a_connect_session_carries_the_classic_cap_unless_the_compute_is_serverless(
    monkeypatch, env, want
):
    from databricks.labs.gbx.ds.file_gbx import session_read_cap

    for k in (
        *_budget.CLASSIC_MEMORY_ENV,
        "IS_SERVERLESS",
        "DATABRICKS_RUNTIME_VERSION",
    ):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert session_read_cap(ConnectStub()) == want
    assert ConnectStub().touched == []  # nothing is read from the Connect session


def test_carrying_udf_carries_the_cap_beside_the_switch():
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    from databricks.labs.gbx import _gdal_policy

    probe = F.udf(lambda x: _gdal_policy.carried_read_cap(), LongType())
    assert _gdal_policy.carrying_udf(probe, True, cap=123).func(None) == 123


def test_every_pyrx_column_api_udf_carries_the_cap_at_the_call():
    """The structural invariant of the Column API: no module-level light UDF or UDTF is called bare (a bare call reaches
    a worker with no cap, and a standard-access worker then reads at Serverless's 64 MiB).
    """
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.pyrx import functions as prx

    bare = sorted(
        n
        for n, v in vars(prx).items()
        if _gdal_policy.is_light_udf(v)
        and not isinstance(v, _gdal_policy.CapCarryingCall)
    )
    assert bare == []


def test_the_column_api_reads_at_the_sessions_read_cap(spark, tmp_path):
    """A Column-API call carries the session's cap from the driver, as a registered function does: the same tile is
    refused under a lowered cap and read without it."""
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    tile = oracles._build("tile_4326", str(tmp_path))  # 12 KiB decoded
    df = spark.createDataFrame(
        [(tile,)], StructType([StructField("tile", V2_TILE_SCHEMA)])
    )
    assert df.select(prx.rst_avg("tile")).first()[0] is not None
    spark.conf.set(READ_CAP_KEY, "4096")
    try:
        assert df.select(prx.rst_avg("tile")).first()[0] is None
    finally:
        spark.conf.unset(READ_CAP_KEY)


def test_a_worker_with_no_memory_signal_reads_a_tile_at_the_classic_cap(spark):
    """End to end: the host's Python workers have no SPARK_*_MEMORY (a standard-access worker's position), so their own
    budget is 64 MiB. The session is classic, so the driver carries 1536 MiB, and a 67 MB tile is read through the
    Column API and through a registered function."""
    import numpy as np
    from pyspark.sql.types import StructField, StructType
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx import register
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    side = 4200  # float32: 70.56 MB decoded, over Serverless's 64 MiB, a few KB encoded
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype="float32",
        compress="zstd",
        transform=from_origin(0, side, 1, 1),
        crs="EPSG:3857",
        tiled=True,
        blockxsize=256,
        blockysize=256,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.full((1, side, side), 2.0, dtype="float32"))
        raw = mf.read()
    tile = {
        "cellid": 0,
        "raster": raw,
        "path": None,
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    df = spark.createDataFrame(
        [(tile,)], StructType([StructField("tile", V2_TILE_SCHEMA)])
    )
    assert df.select(prx.rst_avg("tile")).first()[0] == [2.0]
    register(spark, only=["gbx_rst_avg"])
    df.createOrReplaceTempView("_gbx_big")
    assert spark.sql("SELECT gbx_rst_avg(tile) FROM _gbx_big").first()[0] == [2.0]


def test_the_grouped_executor_reads_at_the_sessions_read_cap(spark, tmp_path):
    """`grouped_tile_map` (mapInPandas) carries the driver's cap with its closure."""
    from pyspark.sql.types import LongType, StructField, StructType

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    tile = oracles._build("tile_4326", str(tmp_path))  # 12 KiB decoded
    df = spark.createDataFrame(
        [(tile,)], StructType([StructField("tile", V2_TILE_SCHEMA)])
    )

    def width():
        out = grouped_tile_map(
            df,
            lambda ds, cellid: int(ds.width),
            return_field=StructField("w", LongType()),
        )
        return out.select("w").first()[0]

    assert width() == 64
    spark.conf.set(READ_CAP_KEY, "4096")
    try:
        assert width() is None
    finally:
        spark.conf.unset(READ_CAP_KEY)


# --- a member over the read cap is refused with its reason, never dropped ------------------------------------
# A validity probe asks "can this be opened"; the decode that follows applies the gate. A group of [normal tile,
# over-cap tile] is the error tile with the refusal, not a result built from the normal tile alone.
def _over_cap_group(spark, tmp_path, register_it, calls=None, columns=None):
    """The one-row answer of a group of [normal tile, over-cap tile] (columns `tile` and `idx`, 1 and 2), under a read cap
    of 4 KiB: `calls` are SQL select expressions over the view `_gbx_group`, or `columns()` are Column-API aggregate
    columns over the same DataFrame."""
    from pyspark.sql.types import IntegerType, StructField, StructType
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    # the same 64 x 48 grid in two types, as combine needs aligned tiles: 3 KiB decoded as bytes, 12 KiB as float32
    with MemoryFile(oracles._tile_bytes("EPSG:4326")) as mf, mf.open() as src:
        profile = {**src.profile, "dtype": "uint8", "nodata": 255}
        pixels = (src.read() % 200).astype("uint8")
    with MemoryFile() as out:
        with out.open(**profile) as dst:
            dst.write(pixels)
        small = oracles._tile(out.read())
    large = oracles._build("tile_4326", str(tmp_path))
    schema = StructType(
        [StructField("tile", V2_TILE_SCHEMA), StructField("idx", IntegerType())]
    )
    df = spark.createDataFrame([(small, 1), (large, 2)], schema)
    df.createOrReplaceTempView("_gbx_group")
    spark.conf.set(READ_CAP_KEY, "4096")
    try:
        register_it()
        if columns is not None:
            return df.agg(*columns()).collect()[0].asDict(True)
        return spark.sql(f"SELECT {calls} FROM _gbx_group").collect()[0].asDict(True)
    finally:
        spark.conf.unset(READ_CAP_KEY)


def _refused_tile(tile):
    md = tile["metadata"] if tile else {}
    return tile is not None and tile["raster"] is None and "read cap" in str(md)


def test_light_array_merge_and_combine_refuse_an_over_cap_member(spark, tmp_path):
    names = ["rst_merge", "rst_combineavg", "rst_combinemax"]
    got = _over_cap_group(
        spark,
        tmp_path,
        _light(spark, names),
        ", ".join(f"gbx_{n}(collect_list(tile)) AS {n}" for n in names),
    )
    assert {n: _refused_tile(got[n]) for n in names} == dict.fromkeys(names, True), got


_PYFUNC = "def f(in_ar, out_ar, *a, **kw): out_ar[:] = in_ar[0]"


def test_light_aggregates_refuse_an_over_cap_member_not_drop_it(spark, tmp_path):
    """The bytes aggregates have no place for a reason: the group is NULL, not the normal member's mosaic."""
    names = [
        "rst_merge_agg",
        "rst_combineavg_agg",
        "rst_frombands_agg",
        "rst_derivedband_agg",
    ]
    calls = (
        "gbx_rst_merge_agg(tile) AS m, gbx_rst_combineavg_agg(tile) AS c, "
        "gbx_rst_frombands_agg(tile, idx) AS b, "
        f"gbx_rst_derivedband_agg(tile, '{_PYFUNC}', 'f') AS d"
    )
    got = _over_cap_group(spark, tmp_path, _light(spark, names), calls)
    assert got == {"m": None, "c": None, "b": None, "d": None}


def test_light_frombands_refuses_an_over_cap_band(spark, tmp_path):
    got = _over_cap_group(
        spark,
        tmp_path,
        _light(spark, ["rst_frombands"]),
        "gbx_rst_frombands(collect_list(tile)) AS b",
    )
    assert _refused_tile(got["b"]), got


def _column_api_forms(tmp_path):
    """The Column API's forms of the array and aggregate functions, as thunks building their Column."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx

    out = str(tmp_path / "virtual")
    tiles = F.collect_list("tile")
    return {
        "merge-virtualize": lambda: prx.rst_merge(tiles, virtualize_dir=out),
        "merge-materialize": lambda: prx.rst_merge(tiles, materialize=True),
        "combineavg-virtualize": lambda: prx.rst_combineavg(tiles, virtualize_dir=out),
        "combinemax": lambda: prx.rst_combinemax(tiles),
        "combinemax-virtualize": lambda: prx.rst_combinemax(tiles, virtualize_dir=out),
        "frombands": lambda: prx.rst_frombands(tiles),
        "frombands-virtualize": lambda: prx.rst_frombands(tiles, virtualize_dir=out),
        "merge_agg": lambda: prx._light_rst_merge_agg("tile"),
        "merge_agg-virtualize": lambda: prx._light_rst_merge_agg(
            "tile", virtualize_dir=out
        ),
        "derivedband_agg": lambda: prx.rst_derivedband_agg("tile", _PYFUNC, "f"),
    }


@pytest.mark.parametrize(
    "form",
    [
        "merge-virtualize",
        "merge-materialize",
        "combineavg-virtualize",
        "combinemax",
        "combinemax-virtualize",
        "frombands",
        "frombands-virtualize",
        "merge_agg",
        "merge_agg-virtualize",
        "derivedband_agg",
    ],
)
def test_column_api_forms_refuse_an_over_cap_member_with_an_error_tile(
    spark, tmp_path, form
):
    """Every Column-API form of a group function (the force-output variants and the tile-returning aggregates too)
    answers an over-cap member as the group's error tile with the reason."""
    got = _over_cap_group(
        spark,
        tmp_path,
        lambda: None,
        columns=lambda: [_column_api_forms(tmp_path)[form]().alias("out")],
    )
    assert _refused_tile(got["out"]), got


@pytest.mark.parametrize("name", ["_uf_merge", "_uf_combineavg", "_uf_frombands"])
def test_file_ref_forms_refuse_an_over_cap_member_with_an_error_tile(tmp_path, name):
    """The FILE-reference bodies (the forms a session with FILE support runs) refuse as the others do."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.pyrx import functions as prx

    with MemoryFile(oracles._tile_bytes("EPSG:4326")) as mf, mf.open() as src:
        profile = {**src.profile, "dtype": "uint8", "nodata": 255}
        pixels = (src.read() % 200).astype("uint8")
    with MemoryFile() as out:
        with out.open(**profile) as dst:
            dst.write(pixels)
        small = oracles._tile(out.read())
    tiles = [small, oracles._build("tile_4326", str(tmp_path))]
    with _gdal_policy.carried(None, 4096):
        tile = getattr(prx, name)._f.func(tiles, [None, None])
    assert _refused_tile(tile), (name, tile)


@pytest.mark.integration
def test_heavy_aggregates_refuse_an_over_cap_member_not_drop_it(jvm_spark, tmp_path):
    from databricks.labs.gbx.manager import jvm

    def install():
        jvm._point_jvm_at_payload(jvm_spark)
        inst = jvm_spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
        base = "com.databricks.labs.gbx.rasterx.expressions.agg.RST_"
        for sql, cls in (
            ("gbx_rst_merge_agg", "MergeAgg"),
            ("gbx_rst_combineavg_agg", "CombineAvgAgg"),
        ):
            assert str(inst.install(jvm_spark._jsparkSession, sql, base + cls)) == "ok"

    got = _over_cap_group(
        jvm_spark,
        tmp_path,
        install,
        "gbx_rst_merge_agg(tile) AS m, gbx_rst_combineavg_agg(tile) AS c",
    )
    assert _refused_tile(got["m"]) and _refused_tile(got["c"]), got
    # the cell id of a group refusal is the light tier's (an aggregate has no group key to name)
    assert got["m"]["cellid"] == got["c"]["cellid"] == 0, got


# --- review of the read cap's carriage: the Column-API wrappers, the keyed helpers, the session value -------------


def test_a_cap_carrying_call_survives_a_copy():
    """A wrapper built without `__init__` (a copy, an unpickled object) has no `_f` yet: reading an attribute then is an
    AttributeError, not a recursion."""
    import copy

    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    from databricks.labs.gbx import _gdal_policy

    probe = _gdal_policy.CapCarryingCall(F.udf(lambda x: 1, LongType()))
    assert copy.copy(probe)._f is probe._f
    bare = _gdal_policy.CapCarryingCall.__new__(_gdal_policy.CapCarryingCall)
    with pytest.raises(AttributeError):
        bare.returnType  # noqa: B018


def test_with_cap_rebuilds_a_udtf_and_both_kinds_of_pandas_udf(spark):
    """`_with_cap` keeps a pandas UDF's kind (scalar and grouped aggregate) and its determinism, and a UDTF still
    runs with the cap carried into its handler."""
    import pandas as pd
    from pyspark.sql import functions as F

    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY

    @F.pandas_udf("long")
    def scalar(v: pd.Series) -> pd.Series:
        from databricks.labs.gbx import _gdal_policy as gp

        return pd.Series([gp.carried_read_cap()] * len(v))

    @F.pandas_udf("long")
    def grouped(v: pd.Series) -> int:
        from databricks.labs.gbx import _gdal_policy as gp

        return gp.carried_read_cap()

    @F.udtf(returnType="cap: long")
    class Probe:
        def eval(self, x):
            from databricks.labs.gbx import _gdal_policy as gp

            yield (gp.carried_read_cap(),)

    nondeterministic = scalar.asNondeterministic()
    import warnings

    for original in (scalar, grouped, nondeterministic):
        with warnings.catch_warnings():
            # the rebuild must not take PySpark's explicit-eval-type path (SPARK-28264): the docs tests make it an error
            warnings.simplefilter("error")
            rebuilt = _gdal_policy._with_cap(original, 777)
        assert rebuilt.evalType == original.evalType
        assert rebuilt.deterministic == original.deterministic
    spark.conf.set(READ_CAP_KEY, "777")
    try:
        df = spark.range(3).withColumn("g", F.lit(1))
        assert (
            df.select(_gdal_policy.CapCarryingCall(scalar)("id"))
            .distinct()
            .collect()[0][0]
            == 777
        )
        assert (
            df.groupBy("g")
            .agg(_gdal_policy.CapCarryingCall(grouped)("id"))
            .collect()[0][1]
            == 777
        )
        assert _gdal_policy.CapCarryingCall(Probe)(F.lit(1)).collect()[0][0] == 777
    finally:
        spark.conf.unset(READ_CAP_KEY)


def test_with_cap_rebuilds_through_the_connect_class_on_a_remote_session(monkeypatch):
    """Under Spark Connect the rebuilt UDF is the Connect class (the classic one resolves through SparkContext._jvm when
    applied), with the original's eval type and return type. Connect cannot run here (no grpcio), so the module is a
    recording stand-in."""
    import sys
    import types

    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    from databricks.labs.gbx import _gdal_policy

    class ConnectUDF:
        def __init__(
            self, func, returnType, name=None, evalType=None, deterministic=True
        ):
            self.func, self.returnType, self.evalType = func, returnType, evalType

        def _wrapped(self):
            return self

    stand_in = types.ModuleType("pyspark.sql.connect.udf")
    stand_in.UserDefinedFunction = ConnectUDF
    original = F.udf(lambda x: 1, LongType())
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.udf", stand_in)
    monkeypatch.setattr("pyspark.sql.utils.is_remote", lambda: True)
    rebuilt = _gdal_policy._with_cap(original, 5)
    assert type(rebuilt) is ConnectUDF
    assert rebuilt.evalType == original.evalType
    assert rebuilt.returnType == original.returnType


def _held_udfs(obj, where, seen, found):
    """Every UDF / UDTF object held by `obj` (containers, a function's defaults and closure cells), as (where, object)."""
    import types

    from databricks.labs.gbx import _gdal_policy

    if id(obj) in seen:
        return
    seen.add(id(obj))
    if _gdal_policy.is_light_udf(obj) or isinstance(obj, _gdal_policy.CapCarryingCall):
        found.append((where, obj))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _held_udfs(v, f"{where}[{k!r}]", seen, found)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for i, v in enumerate(obj):
            _held_udfs(v, f"{where}[{i}]", seen, found)
    elif isinstance(obj, types.FunctionType) and obj.__module__ == (
        "databricks.labs.gbx.pyrx.functions"
    ):
        for i, v in enumerate(obj.__defaults__ or ()):
            _held_udfs(v, f"{where}.defaults[{i}]", seen, found)
        for name, cell in zip(obj.__code__.co_freevars, obj.__closure__ or ()):
            try:
                _held_udfs(cell.cell_contents, f"{where}.closure[{name}]", seen, found)
            except ValueError:  # an empty cell
                pass


def test_no_light_udf_is_held_bare_below_the_pyrx_namespace():
    """The Column API's cap is carried at the call of a module-level UDF; a UDF held in a container, a default or a
    closure would be called bare. The registries hold the wrappers (a registration unwraps them), and no function
    keeps a bare UDF of its own."""
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.pyrx import functions as prx

    found, seen = [], set()
    for name, value in vars(prx).items():
        _held_udfs(value, name, seen, found)
    bare = sorted(
        w for w, o in found if not isinstance(o, _gdal_policy.CapCarryingCall)
    )
    assert bare == []


def test_session_read_cap_falls_back_to_the_classic_budget_for_a_bad_value():
    import types

    from databricks.labs.gbx.ds.file_gbx import session_read_cap
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    classic = decoded_budget_bytes("classic")
    for bad in ("abc", "0", "-5", ""):
        spark = types.SimpleNamespace(
            conf=types.SimpleNamespace(get=lambda k, d=None, v=bad: v)
        )
        assert session_read_cap(spark) == classic, bad
    named = types.SimpleNamespace(
        conf=types.SimpleNamespace(get=lambda k, d=None: " 4096 ")
    )
    assert session_read_cap(named) == 4096


def test_every_level_a_row_declares_is_in_its_order():
    """`select` only tries the levels in `order(fn)`: a row that declares L2 and whose family order omits it would
    never reach its Java UDF. A row with a deliberate omission names it in PREFERENCE, so this lists none.
    """
    omitted = {
        f.sql_name: sorted(set(f.levels) & {"L1", "L2", "L3"} - set(levels.order(f)))
        for f in FNS
        if "L2" in f.levels and "L2" not in levels.order(f)
    }
    assert omitted == {}


def test_a_full_registration_probes_the_session_again(monkeypatch, spark):
    from databricks.labs.gbx import register
    from databricks.labs.gbx.manager import capabilities

    calls = []
    real = capabilities._detect_uncached
    monkeypatch.setattr(
        capabilities,
        "_detect_uncached",
        lambda s, need_jvm: calls.append(need_jvm) or real(s, need_jvm),
    )
    capabilities._CACHE.clear()
    register(spark, only=["gbx_bng_eastnorthasbng"])
    register(spark, only=["gbx_st_makevalid"])
    assert len(calls) == 1  # the per-name first-use registrations share the probes
    register(spark)  # a JAR may have been staged since: probe again
    assert len(calls) == 2
    capabilities.forget(spark)
    register(spark, only=["gbx_st_makevalid"])
    assert len(calls) == 3


def test_the_keyed_helpers_carry_the_sessions_read_cap(spark, tmp_path):
    """`merge_by_key` / `combine_by_key` fold each member through the same read gate as the aggregates, so their
    closures carry the driver's cap, as `grouped_tile_map`'s does."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
    from databricks.labs.gbx.pyrx import helpers
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    tile = oracles._build("tile_4326", str(tmp_path))  # 12 KiB decoded
    df = spark.createDataFrame(
        [(1, tile), (1, tile)],  # two members: a lone member is returned as it is
        StructType(
            [StructField("k", IntegerType()), StructField("tile", V2_TILE_SCHEMA)]
        ),
    )
    for fold in (helpers.merge_by_key, helpers.combine_by_key):
        assert fold(df, ["k"]).collect()[0]["raster"] is not None
        spark.conf.set(READ_CAP_KEY, "4096")
        try:
            with pytest.raises(Exception, match="read cap"):
                fold(df, ["k"]).collect()
        finally:
            spark.conf.unset(READ_CAP_KEY)


def _fake_wheel(
    tmp_path, jars, record_hash=None, name="geobrix-0.6.0+env6-py3-none-any.whl"
):
    """A wheel with the given JAR names (bytes b"jar-bytes") and a RECORD listing each one's sha256
    (``record_hash`` overrides the listed hash)."""
    import base64
    import hashlib
    import zipfile

    good = base64.urlsafe_b64encode(hashlib.sha256(b"jar-bytes").digest()).rstrip(b"=")
    whl = tmp_path / name
    with zipfile.ZipFile(whl, "w") as z:
        rows = []
        for jar in jars:
            member = f"databricks/labs/gbx/_jar/{jar}"
            z.writestr(member, b"jar-bytes")
            rows.append(f"{member},sha256={record_hash or good.decode()},9")
        z.writestr("databricks/labs/gbx/__init__.py", b"")
        z.writestr("geobrix-0.6.0.dist-info/RECORD", "\n".join(rows) + "\n")
    return str(whl)


def test_jar_from_wheel_takes_the_one_jar_and_refuses_otherwise(tmp_path):
    from databricks.labs.gbx import stage

    assert (
        stage.jar_from_wheel(
            _fake_wheel(tmp_path, ["geobrix-0.6.0-jar-with-dependencies.jar"])
        )
        == b"jar-bytes"
    )
    with pytest.raises(ValueError, match="Fix:"):
        stage.jar_from_wheel(_fake_wheel(tmp_path, []))


def test_jar_from_wheel_refuses_a_jar_of_another_version(tmp_path):
    from databricks.labs.gbx import stage

    whl = _fake_wheel(tmp_path, ["geobrix-0.5.3-jar-with-dependencies.jar"])
    with pytest.raises(ValueError, match=r"0\.5\.3.*0\.6\.0.*Fix:.*rebuild"):
        stage.jar_from_wheel(whl)


def test_jar_from_wheel_refuses_bytes_that_differ_from_the_record(tmp_path):
    from databricks.labs.gbx import stage

    whl = _fake_wheel(
        tmp_path,
        ["geobrix-0.6.0-jar-with-dependencies.jar"],
        record_hash="A" * 43,
    )
    with pytest.raises(ValueError, match=r"RECORD.*Fix:.*rebuild"):
        stage.jar_from_wheel(whl)


def test_stage_jar_writes_the_jar_and_its_sha_and_refuses_a_second_jar(
    tmp_path, monkeypatch, capsys
):
    import hashlib

    from databricks.labs.gbx import _native, stage

    src = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    src.write_bytes(b"jar-bytes")
    vol = tmp_path / "vol"
    monkeypatch.setattr(_native, "jar_path", lambda root=None: str(src))
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)
    dst = stage.stage_jar(str(vol))
    assert open(dst, "rb").read() == b"jar-bytes"
    assert (
        open(dst + ".sha256").read().split()[0]
        == hashlib.sha256(b"jar-bytes").hexdigest()
    )
    assert "cluster library" in capsys.readouterr().out
    (vol / "geobrix-0.5.3-jar-with-dependencies.jar").write_bytes(b"old")
    with pytest.raises(RuntimeError, match="one geobrix JAR"):
        stage.stage_jar(str(vol))


def test_stage_jar_recopies_a_jar_that_is_gone_or_truncated_behind_its_sidecar(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx import _native, stage

    src = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    src.write_bytes(b"jar-bytes")
    vol = tmp_path / "vol"
    monkeypatch.setattr(_native, "jar_path", lambda root=None: str(src))
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)
    dst = stage.stage_jar(str(vol))
    import os

    os.remove(dst)  # the sidecar still matches
    stage.stage_jar(str(vol))
    assert open(dst, "rb").read() == b"jar-bytes"
    with open(dst, "wb") as fh:
        fh.write(b"jar-by")
    stage.stage_jar(str(vol))
    assert open(dst, "rb").read() == b"jar-bytes"


def test_stage_jar_without_a_bundled_jar_or_off_a_volume_says_how_to_fix_it(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx import _native, stage

    monkeypatch.setattr(_native, "jar_path", lambda root=None: None)
    with pytest.raises(RuntimeError, match="Fix:.*env wheel"):
        stage.stage_jar("/Volumes/c/s/v/jars")
    src = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    src.write_bytes(b"jar-bytes")
    monkeypatch.setattr(_native, "jar_path", lambda root=None: str(src))
    with pytest.raises(ValueError, match="Fix:"):
        stage.stage_jar(str(tmp_path / "not-a-volume"))


def test_stage_jar_is_a_lazy_package_export():
    import databricks.labs.gbx as gbx
    from databricks.labs.gbx import stage

    assert gbx.stage_jar is stage.stage_jar and "stage_jar" in gbx.__all__
