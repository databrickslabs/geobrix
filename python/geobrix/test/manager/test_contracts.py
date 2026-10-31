"""Contract checks generated from the per-surface manifest. No per-function tests."""

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

    rows = [c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name]
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
    ],
    ids=lambda c: c[0],
)
def test_level_selection_contract(fn, case, monkeypatch):
    """Invariants of the level table, generated per manifest row (not a hand-written expectation list)."""
    name, kw = case
    if (
        name == "dedicated"
    ):  # an env wheel: this process runs the light engine the L1 engine sends virtual tiles to
        from databricks.labs.gbx.engine import run

        monkeypatch.setattr(run, "unavailable_reason", lambda: None)
    choice = levels.select(fn, _caps(**kw))
    pref = [lv for lv in levels.PREFERENCE[fn.sql_name] if lv in fn.levels]
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
    choice = manager._install(
        spark, fn, levels.Choice("L3", "-", ()), _caps(), forced=False
    )
    said = capsys.readouterr().out
    if want == "fallback":
        assert choice.variant not in levels.ENGINE_L3
        assert spark.registered == [fn.sql_name]
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

    assert set(oracles.HEAVY_PENDING) <= {c.cid for c in oracles.MVP_S}
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
    stub = ConnectStub()
    out = manager.register(stub)
    assert {c.level for c in out.values()} == {"L3"}
    assert stub.touched == []  # _jvm/_jsc/sparkContext/conf.set never accessed
    printed = capsys.readouterr().out.splitlines()
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
    assert "gbx_st_makevalid" in stub.registered  # registered after the skipped one
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
    assert stub.java_registered == [("gbx_bng_eastnorthasbng", CONNECT_BNG, "string")]
    # one JAR, one attach per session, however many names register
    jvm.install_l2(stub, BNG, _caps(connect=True, jar=OK_JAR))
    assert stub.artifacts == [jar]


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
