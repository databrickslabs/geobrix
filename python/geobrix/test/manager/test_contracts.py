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
    for c in rows:
        arg_rows = c.args if c.kind == "exact_rows" else (c.args,)
        assert all(fn.min_args <= len(r) <= fn.max_args for r in arg_rows), c.cid
        assert c.kind != "exact_rows" or len(c.expect) == len(c.args), c.cid
        assert c.surfaces and set(c.surfaces) <= {"session", "uc"}, c.cid
        if (
            c.kind == "raises"
        ):  # an error row names the offending value, so a missing function cannot pass
            assert isinstance(c.expect, str) and c.expect, c.cid


@pytest.mark.parametrize(
    "fn", [f for f in FNS if "L1" in f.levels], ids=lambda f: f.sql_name
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
        ("dedicated", dict(jar=OK_JAR, jar_reason="")),
        ("standard", dict(connect=True, jar=OK_JAR, jar_reason="")),
        (
            "no-gdal",
            dict(jar=OK_JAR, jar_reason="", gdal_reason="GDAL natives not loadable"),
        ),
    ],
    ids=lambda c: c[0],
)
def test_level_selection_contract(fn, case):
    """Invariants of the level table, generated per manifest row (not a hand-written expectation list)."""
    name, kw = case
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


@pytest.mark.parametrize(
    "fn", [f for f in FNS if {"L1", "L2"} & set(f.levels)], ids=lambda f: f.sql_name
)
def test_override_is_honoured(fn):
    forced = "L1" if "L1" in fn.levels else "L2"
    assert levels.select(fn, _caps()).level != forced  # no JAR: the default is light
    assert levels.select(fn, _caps(), override=(forced, "-")).level == forced


def test_l2_is_refused_while_proj_grids_are_registered(tmp_path):
    """A Java UDF has no channel for PROJ grid directories: a warp through it would ignore them."""
    from databricks.labs.gbx.core import proj_grids

    fn = MAN.functions["gbx_rst_transformcrs"]
    standard = _caps(
        connect=True, jar=OK_JAR, jar_reason=""
    )  # L2 is the heavy option here
    assert levels.select(fn, standard).level == "L2"
    saved = proj_grids.get_registered_dirs()
    proj_grids.set_registered_dirs(str(tmp_path), replace=True)
    try:
        choice = levels.select(fn, standard)
    finally:
        proj_grids.set_registered_dirs(saved, replace=True)
    assert choice.level == "L3"
    assert any("PROJ grids" in r for r in choice.reasons)


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
def test_jar_identity_is_judged_against_the_wheel(identity, jar_found):
    """The JVM reports `<version>|<sha256>`; an unknown or development JAR is never used."""
    from types import SimpleNamespace as NS

    kernels = NS(
        BuildInfo=NS(identity=lambda: identity), KernelGdal=NS(probe=lambda: "ok:3")
    )
    spark = NS(_jvm=NS(com=NS(databricks=NS(labs=NS(gbx=NS(kernels=kernels))))))
    assert (caps_mod.detect(spark, True).jar is not None) is jar_found


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
    assert "environment_version = '6'" in impl and "geobrix @ file:///Volumes/" in impl
    assert "COMMENT 'geobrix 0.6.0'" in impl and "LANGUAGE PYTHON" in impl
    assert all("`cat`.`sch`." in s for s in stmts)  # 3-level names only (§2.3)
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
    """User decision (product CRS probe): int SRID or EPSG:/ESRI:<n> only, as an INT; the rest refused."""
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
    assert int(out.stdout) == sum(
        "uc" in c.surfaces for c in oracles.MVP_S
    )  # every UC row ran


class _UcSpark:
    """A session stand-in: records statements, answers the information_schema read with `routines`."""

    def __init__(self, routines=None):
        self.stmts, self.routines = [], routines

    def sql(self, query):
        self.stmts.append(query)
        rows = []
        if "information_schema.routines" in query:
            rows = [
                {"routine_name": n, "comment": c}
                for n, c in (self.routines or {}).items()
            ]
        return type("R", (), {"collect": lambda _s, r=rows: r})()


ENV6_WHEEL = "geobrix-0.6.0+env6-py3-none-any.whl"
UC_NAMES = {f.sql_name: "geobrix 0.6.0" for f in UC_FNS}


@pytest.mark.parametrize(
    "files,wheel,error",
    [
        (["geobrix-0.6.0-py3-none-any.whl"], "geobrix-0.6.0-py3-none-any.whl", None),
        ([ENV6_WHEEL], ENV6_WHEEL, None),
        (["geobrix-0.6.0+env5-py3-none-any.whl", ENV6_WHEEL], ENV6_WHEEL, None),
        (["geobrix-0.6.0+env5-py3-none-any.whl"], None, re.escape(ENV6_WHEEL)),
        ([], None, "exactly one"),
        (["geobrix-0.6.0-py3-none-any.whl", ENV6_WHEEL], None, "exactly one"),
        (["geobrix-0.5.3-py3-none-any.whl"], None, "exactly one"),
    ],
    ids=[
        "plain",
        "env6",
        "env6-beside-env5",
        "env5-only",
        "none",
        "two",
        "other-version",
    ],
)
def test_uc_install_deploys_one_env6_wheel_and_verifies(
    tmp_path, monkeypatch, files, wheel, error
):
    from databricks.labs.gbx import uc

    for name in files:
        (tmp_path / name).write_bytes(b"")
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "6")
    spark = _UcSpark(UC_NAMES)
    if error:
        with pytest.raises(RuntimeError, match=error):
            uc.install("cat", "sch", str(tmp_path), spark=spark)
        assert not spark.stmts  # nothing is created from the wrong wheel
        return
    stmts = uc.install("cat", "sch", str(tmp_path), spark=spark)
    assert spark.stmts[: len(stmts)] == stmts  # every statement ran, in order
    assert all(f"geobrix @ file://{tmp_path}/{wheel}" in s for s in stmts[:1])
    assert (
        "information_schema.routines" in spark.stmts[-1]
    )  # then the schema was verified


def test_uc_install_refuses_a_runtime_that_is_not_env6(tmp_path, monkeypatch):
    from databricks.labs.gbx import uc

    (tmp_path / ENV6_WHEEL).write_bytes(b"")
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "5")
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.5.0")
    with pytest.raises(RuntimeError, match="env 6"):
        uc.install("cat", "sch", str(tmp_path), spark=_UcSpark(UC_NAMES))


def test_uc_install_dry_run_emits_the_ddl_without_a_session_or_a_wheel(monkeypatch):
    from databricks.labs.gbx import uc

    monkeypatch.delenv("DATABRICKS_ENV_VERSION", raising=False)
    everything = uc.install("cat", "sch", "/Volumes/c/s/v/dir", dry_run=True)
    assert len(everything) == sum(
        2 if any(p.default for p in f.params) else 1 for f in UC_FNS
    )
    assert all(ENV6_WHEEL in s for s in everything if "LANGUAGE PYTHON" in s)
    only = uc.install(
        "cat",
        "sch",
        "/Volumes/c/s/v/dir",
        only=["gbx_bng_eastnorthasbng"],
        dry_run=True,
    )
    assert len(only) == 1 and "gbx_bng_eastnorthasbng(" in only[0]


@pytest.mark.parametrize(
    "routines,error",
    [
        (UC_NAMES, None),
        ({**UC_NAMES, "gbx_st_makevalid": "geobrix 0.5.3"}, "gbx_st_makevalid"),
        (
            {n: c for n, c in UC_NAMES.items() if n != "gbx_st_makevalid"},
            "gbx_st_makevalid",
        ),
    ],
    ids=["match", "old-version", "missing"],
)
def test_register_uc_checks_the_schema_against_this_wheel(routines, error):
    from databricks.labs.gbx import register

    call = lambda: register(  # noqa: E731
        _UcSpark(routines), target="uc", catalog="cat", schema="sch"
    )
    if error:
        with pytest.raises(RuntimeError, match=error):
            call()
    else:
        assert call() == {}
    with pytest.raises(ValueError, match="catalog= and schema="):
        register(_UcSpark(routines), target="uc")
