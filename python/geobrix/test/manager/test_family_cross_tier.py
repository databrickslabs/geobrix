"""Every family oracle table at every implementation level the manifest declares and this environment
offers: L3 forms on the host (through SQL and the Python API), the JVM levels and the GDAL spec engine's
forms in the container with the staged JAR and the native payload. One harness for every manifest row.

Select tables with GBX_ORACLE_TABLES=<TABLE>[,<TABLE2>] (the container commands forward it), or names with
-k "gbx_bng_" (the ids are <sql_name>-<level><variant>-<api>)."""

import os

import pytest

from databricks.labs.gbx import _rs, manifest, register
from databricks.labs.gbx.bench import oracles, product_stand_ins
from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY
from databricks.labs.gbx.engine import run as engine_run
from databricks.labs.gbx.manager import capabilities
from databricks.labs.gbx.manifest import TILE_SQL

_ONLY = {t for t in os.environ.get("GBX_ORACLE_TABLES", "").split(",") if t}
_IMPL_KEYS = ("variants", "factory", "callable", "udtf")


def _tables_of(fn) -> list:
    """The oracle tables whose rows run for `fn` under the table selection: its own, and (a row with a `gdal` block
    runs on the GDAL spec engine) SPEC_SE's, where the engine's input, bytes-rule and read-cap rows live.
    """
    tables = [fn.oracle] + (["SPEC_SE"] if fn.gdal is not None else [])
    return [t for t in dict.fromkeys(tables) if not _ONLY or t in _ONLY]


def _l3_forms(l3) -> list:
    forms = list(l3.get("variants") or ())
    if not forms and any(k in l3 for k in _IMPL_KEYS):
        forms = ["-"]
    if l3.get("engine"):
        forms += ["engine-c0", "engine-c1"]
    return forms


def _rows():
    for fn in sorted(manifest.load().functions.values(), key=lambda f: f.sql_name):
        if fn.status != "stable" or not _tables_of(fn):
            continue
        for variant in _l3_forms(fn.levels.get("L3", {})):
            engine = variant.startswith("engine")
            # the Python API leg needs a Python entry: a Column function, or for a generator the DataFrame-first
            # function (`_api.table`), run over the same frame as the SQL leg
            apis = ("sql",) if fn.python is None else ("sql", "python")
            for api in apis:
                marks = [pytest.mark.integration] if engine else []
                yield pytest.param(
                    fn.sql_name,
                    "L3",
                    variant,
                    api,
                    marks=marks,
                    id=f"{fn.sql_name}-L3{variant}-{api}",
                )
        if (
            "L0" in fn.levels
        ):  # the session's SQL kernel, a temporary SQL function: no JVM, no import
            yield pytest.param(
                fn.sql_name, "L0", "-", "sql", id=f"{fn.sql_name}-L0-sql"
            )
        l1 = fn.levels.get("L1", {})
        for variant in (["-"] if "companion" in l1 else []) + (
            ["engine"] if l1.get("engine") else []
        ):
            yield pytest.param(
                fn.sql_name,
                "L1",
                variant,
                "sql",
                marks=pytest.mark.integration,
                id=f"{fn.sql_name}-L1{variant}-sql",
            )
        if (
            "companion" in l1
            and fn.accepts_virtual_tile
            and fn.kind == "scalar"
            and any(
                c.fn == fn.sql_name and c.kind == "error_tile"
                for t in _tables_of(fn)
                for c in oracles.TABLES[t]
            )
        ):
            # The Catalyst function itself, without the dispatch wrapper in front of it: the error-tile rows must
            # hold for a RasterX function that receives an error tile directly.
            yield pytest.param(
                fn.sql_name,
                "L1",
                "raw",
                "sql",
                marks=pytest.mark.integration,
                id=f"{fn.sql_name}-L1-raw-sql",
            )


def _run(spark, fn, cases, tmpdir, column_of=None):
    if oracles.has_groups(
        fn
    ):  # an aggregate, or a table function over TABLE(...) PARTITION BY
        return oracles.run_groups(spark, cases, tmpdir=tmpdir, column_of=column_of)
    if fn.kind == "generator":
        return oracles.run_rows(spark, cases, tmpdir=tmpdir, table_of=column_of)
    empty = [c for c in cases if c.kind == "no_rows"]
    if fn.kind == "scalar" and empty:
        # a scalar that returns an array of chips: the rows that expect no chip are judged on the exploded array
        # (SQL only: the Column API has no exploded form)
        ran = oracles.run_session(
            spark,
            [c for c in cases if c not in empty],
            tmpdir=tmpdir,
            column_of=column_of,
        )
        if column_of is None:
            ran += oracles.run_rows(
                spark,
                empty,
                shape_sql="SELECT inline({fn}({args})) FROM _gbx_oracle",
                tmpdir=tmpdir,
            )
        return ran
    return oracles.run_session(spark, cases, tmpdir=tmpdir, column_of=column_of)


ROWS = list(_rows())


@pytest.mark.parametrize("sql_name,level,variant,api", ROWS)
def test_family_oracle(request, tmp_path, sql_name, level, variant, api):
    if (level, variant) == ("L3", "d") and _rs.unavailable_reason():
        pytest.skip(_rs.unavailable_reason())
    if variant.startswith("engine"):
        from databricks.labs.gbx.engine import run

        if level == "L3" and run.unavailable_reason():
            pytest.skip(run.unavailable_reason())
    fn = manifest.load().functions[sql_name]
    jvm = level == "L1" or variant.startswith("engine")
    spark = request.getfixturevalue("jvm_spark" if jvm else "spark")
    if (level, variant) == ("L1", "engine"):
        _require_pinned_jvm_gdal(spark)
    if level == "L0" and sql_name in product_stand_ins.WORKSPACE_ONLY:
        pytest.skip(
            f"{sql_name} {product_stand_ins.WORKSPACE_ONLY[sql_name]}: judged on the workspace"
        )
    if (
        level == "L0"
    ):  # a body that calls a product built-in the local engine lacks runs against its stand-in
        with product_stand_ins.l0_stand_in(fn):
            _test_family_oracle(
                spark, request, tmp_path, fn, sql_name, level, variant, api
            )
        return
    _test_family_oracle(spark, request, tmp_path, fn, sql_name, level, variant, api)


def _require_pinned_jvm_gdal(spark):
    """The L1 engine needs the JVM on the manifest's GDAL pin; elsewhere its rows skip with that reason."""
    pin = manifest.load().gdal.pin
    caps = capabilities.detect(spark, need_jvm=True)
    if caps.jvm_gdal != pin:
        pytest.skip(
            f"PRECONDITION MISSING: the JVM runs GDAL {caps.jvm_gdal or caps.gdal_reason}, not {pin}; it needs "
            "GBX_NATIVE_ROOT set to a payload whose bundled _jar is this session's JAR (gbx:test:parity refreshes "
            "the payload's _jar from the staged JAR; by hand, copy the JAR into its _jar/)"
        )


def _test_family_oracle(spark, request, tmp_path, fn, sql_name, level, variant, api):
    raw = variant == "raw"
    register(
        spark, only=[sql_name], _levels={sql_name: (level, "-" if raw else variant)}
    )
    cases = [
        c
        for table in _tables_of(fn)
        for c in oracles.TABLES[table]
        if c.fn == sql_name
        and ("session" in c.surfaces or "python" in c.surfaces)
        and (not raw or c.kind == "error_tile")
    ]
    if (
        raw
    ):  # the Catalyst function by its private name: only the error-tile rows apply to it
        ran = oracles.run_session(
            spark,
            cases,
            name_of=lambda f: f"gbx__l1_{f[len('gbx_'):]}",
            tmpdir=str(tmp_path),
        )
        assert [m for _, m in ran if m] == []
        return
    column_of = (
        (lambda f: manifest.resolve_ref(manifest.load().functions[f].python.ref))
        if api == "python"
        else None
    )
    capped = [c for c in cases if c.cid in oracles.READ_CAP_ROWS]
    ran = _run(
        spark, fn, [c for c in cases if c not in capped], str(tmp_path), column_of
    )
    # the decoded-size gate, with the session's read cap lowered for the row: the JVM reads the key per query, a
    # light function carries it from its registration
    for c in capped:
        spark.conf.set(READ_CAP_KEY, str(oracles.READ_CAP_ROWS[c.cid]))
        try:
            register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
            ran += _run(spark, fn, [c], str(tmp_path), column_of)
        finally:
            spark.conf.unset(READ_CAP_KEY)
            register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
    red = {c.cid: m for c, m in ran if m}
    pending = {
        cid
        for cid, lv in oracles.HEAVY_PENDING.items()
        if level in lv and cid in {c.cid for c in cases}
    }
    bad = [m for cid, m in red.items() if cid not in pending]
    assert not bad, "\n".join(
        m[-500:] for m in bad
    )  # every red row, not pytest's cut-down list
    assert pending <= set(red), f"{level} now passes {sorted(pending - set(red))}"


# A table module opts its scalars in with `L3_CALLABLE = True` (see oracles.l3_opted_in), once its rows pass there.
def _callable_scalars():
    return [
        pytest.param(fn.oracle, fn.sql_name, id=f"{fn.sql_name}-L3-callable")
        for fn in sorted(manifest.load().functions.values(), key=lambda f: f.sql_name)
        if oracles.l3_opted_in(fn)
        and fn.kind == "scalar"
        and fn.status == "stable"
        and (not _ONLY or fn.oracle in _ONLY)
        and oracles.l3_callable(fn) is not None
    ]


@pytest.mark.parametrize("table,sql_name", _callable_scalars())
def test_family_oracle_l3_callables(tmp_path, table, sql_name):
    """The session rows of a scalar, on the Python function its registration wraps: no Spark, no worker. The same
    rows run through SQL and the Column API in test_family_oracle; this leg is the fast host loop and the one a
    mutation run can use."""
    cases = [c for c in oracles.TABLES[table] if c.fn == sql_name]
    ran = oracles.run_l3_callables(cases, tmpdir=str(tmp_path))
    assert ran
    assert [m for _, m in ran if m] == []


@pytest.mark.integration
def test_l1_arity_contract_for_every_companion(jvm_spark):
    installer = jvm_spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    for fn in manifest.load().functions.values():
        if "companion" in fn.levels.get("L1", {}):
            want = ",".join(str(n) for n in range(fn.min_args, fn.max_args + 1))
            got = installer.acceptedArities(
                fn.levels["L1"]["companion"], fn.min_args, fn.max_args
            )
            assert str(got) == want, fn.sql_name


@pytest.mark.integration
def test_detect_sees_the_jar_and_gdal(jvm_spark):
    caps = capabilities.detect(jvm_spark, need_jvm=True)
    assert caps.jar is not None, caps
    assert caps.jar_reason == "" and caps.gdal_reason == "", caps


# --- The GDAL spec engine's level-specific rows (folded in from the spike's own file) -----------------------------
# The rows of SPEC_SE (and the engine rows of SPEC_DEM and MVP_S) run above, at every level, in `test_family_oracle`.
# These are what a table row cannot say: L1 against L3 on one input, a grid registered after the JVM's GDAL began, the
# analysis-time refusal of a literal, the read cap, and the bytes rule. The L3 engine needs the env wheel payload's
# osgeo (the container with GBX_NATIVE_ROOT); the L1 engine, the staged JAR on a JVM running the payload's GDAL.
MAN = manifest.load()
LEVELS = [("L1", "engine"), ("L3", "engine-c0"), ("L3", "engine-c1")]


def _engine_session(request, level):
    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(why)
    if level == "L3":
        return request.getfixturevalue("spark")
    spark = request.getfixturevalue("jvm_spark")
    _require_pinned_jvm_gdal(spark)
    return spark


@pytest.mark.integration
def test_mapalgebra_on_its_heavy_companion_behind_the_tiles_wrapper(request, tmp_path):
    """L1 without the engine (as where the JVM runs another GDAL): mapalgebra runs on its RST_MapAlgebra companion
    behind the tiles dispatch wrapper. An array of materialized tiles goes to the companion, an array of virtual
    tiles to the light kernel the wrapper names, and each gives the tile."""
    from rasterio.io import MemoryFile

    spark = _engine_session(request, "L1")
    fn = MAN.functions["gbx_rst_mapalgebra"]
    register(spark, only=[fn.sql_name], _levels={fn.sql_name: ("L1", "-")})
    pair = oracles._se_build("se_pair")
    virtual = []
    for i, t in enumerate(pair):
        f = tmp_path / f"t{i}.tif"
        f.write_bytes(t["raster"])
        virtual.append(oracles._tile(path=str(f)))
    spec = '{"calc": "(A - B) / (A + B)", "A_index": 0, "B_index": 1}'
    schema = f"tiles {oracles._ddl_type(pair, fn.params[0])}, spec string"
    df = spark.createDataFrame([(pair, spec), (virtual, spec)], schema)
    outs = [
        r["out"] for r in df.selectExpr(f"{fn.sql_name}(tiles, spec) AS out").collect()
    ]
    for out in outs:
        assert out is not None and out["raster"] is not None, out
        with MemoryFile(bytes(out["raster"])) as mf, mf.open() as ds:
            assert (ds.width, ds.height) == (64, 48)


# One success row per function: the same input through every engine level.
SAME_PIXELS = [
    "se_slope_deg",
    "se_slope_scales",
    "tcrs_epsg",
    "tcrs_esri102003",
    "tcrs_proj4",
    "tcrs_wkt",
    "se_ma_nd_two_tiles",
    "se_slope_png",
    "se_tcrs_projjson",
]


@pytest.mark.integration
def test_l1_and_l3_give_the_same_pixels(request, tmp_path):
    """L1 equals L3 (c0 and c1) pixel for pixel, NoData included; the bytes are equal too (same argv, same GDAL)."""
    import numpy as np

    _engine_session(request, "L1")
    spark = request.getfixturevalue("jvm_spark")
    cases = [c for c in oracles.SPEC_SE + oracles.MVP_S if c.cid in SAME_PIXELS]
    outs = {}
    for level, variant in LEVELS:
        for case in cases:
            register(spark, only=[case.fn], _levels={case.fn: (level, variant)})
            rows = oracles._argument_rows(case, str(tmp_path))
            fn = MAN.functions[case.fn]
            schema = ", ".join(
                f"c{i} {oracles._ddl_type(v, fn.params[i])}"
                for i, v in enumerate(rows[0])
            )
            df = spark.createDataFrame([tuple(rows[0])], schema)
            cols = ", ".join(f"c{i}" for i in range(len(rows[0])))
            df.createOrReplaceTempView("_gbx_same")
            (r,) = spark.sql(
                f"SELECT {case.fn}({cols}) AS out FROM _gbx_same"
            ).collect()
            outs[(case.cid, level, variant)] = r["out"]
    differ = []
    for case in cases:
        base = outs[(case.cid, "L1", "engine")]
        for level, variant in LEVELS[1:]:
            other = outs[(case.cid, level, variant)]
            a = oracles._masked_band(base["raster"])
            b = oracles._masked_band(other["raster"])
            same_px = (
                a.shape == b.shape
                and np.array_equal(np.ma.getmaskarray(a), np.ma.getmaskarray(b))
                and np.array_equal(a.filled(0), b.filled(0))
            )
            if not same_px:
                differ.append(f"{case.cid}: pixels differ at {level} {variant}")
            elif bytes(base["raster"]) != bytes(other["raster"]):
                differ.append(
                    f"{case.cid}: same pixels, different bytes at {level} {variant}"
                )
            if dict(base["metadata"]) != dict(other["metadata"]):
                differ.append(
                    f"{case.cid}: metadata {dict(base['metadata'])} vs {dict(other['metadata'])}"
                )
    assert differ == []


@pytest.mark.integration
def test_a_registered_grid_reaches_l1_after_an_earlier_gdal_init(request, tmp_path):
    """A target CRS whose grid is only in a registered directory: L1 gives L3's tile, byte for byte, though the JVM's
    GDAL was initialized (by the capability probe) before the grid was registered."""
    import shutil
    import uuid
    from pathlib import Path

    from databricks.labs.gbx import crs_grids
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.manager import levels

    grid = (
        Path(__file__).resolve().parents[4]
        / "sample-data/Volumes/main/geobrix_samples/geobrix-examples/proj-grids/synthetic.gsb"
    )
    if not grid.is_file():
        pytest.skip("PRECONDITION MISSING: the synthetic.gsb fixture; this row not run")
    # Every precondition skips with its reason: the L3 engine (the payload's osgeo), the staged JAR in this session,
    # then (in _session) the JVM on the pinned GDAL and (below) the L1 engine itself.
    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(f"PRECONDITION MISSING: the L3 engine: {why}; this row not run")
    staged = sorted(
        (Path(__file__).resolve().parents[2] / "lib").glob(
            "geobrix-*-jar-with-dependencies.jar"
        )
    )
    if not staged or str(staged[-1]) not in request.getfixturevalue("spark").conf.get(
        "spark.jars", ""
    ):
        pytest.skip(
            "PRECONDITION MISSING: the staged JAR in this session (gbx-test-parity.sh); this row not run"
        )
    # the probe in _session initializes the JVM's GDAL, with no grid registered yet
    _engine_session(request, "L1")
    spark = request.getfixturevalue("jvm_spark")
    fn = MAN.functions["gbx_rst_transformcrs"]
    why = levels.engine_unavailable(fn, "L1", capabilities.detect(spark, need_jvm=True))
    if why:
        pytest.skip(f"PRECONDITION MISSING: the L1 engine: {why}; this row not run")
    name = f"gbx_se_{uuid.uuid4().hex}.gsb"  # a name PROJ has never looked up in either process
    (tmp_path / "grids").mkdir()
    shutil.copy(grid, tmp_path / "grids" / name)
    # tile_4326 lies inside the grid's box (50..53 N, 1 W..1 E)
    case = oracles.Case(
        "se_tcrs_registered_grid",
        fn.sql_name,
        (
            oracles.Build("tile_4326"),
            f"+proj=longlat +ellps=GRS80 +nadgrids={name} +no_defs +type=crs",
        ),
        "tile_props",
        None,
        ("session",),
    )
    saved = proj_grids.get_registered_dirs()
    crs_grids.register_proj_grids(spark, str(tmp_path / "grids"))
    outs = {}
    try:
        rows = oracles._argument_rows(case, str(tmp_path))
        schema = ", ".join(
            f"c{i} {oracles._ddl_type(v, fn.params[i])}" for i, v in enumerate(rows[0])
        )
        spark.createDataFrame([tuple(rows[0])], schema).createOrReplaceTempView(
            "_gbx_grid"
        )
        for level, variant in (("L1", "engine"), ("L3", "engine-c0")):
            register(spark, only=[fn.sql_name], _levels={fn.sql_name: (level, variant)})
            (r,) = spark.sql(
                f"SELECT {fn.sql_name}(c0, c1) AS out FROM _gbx_grid"
            ).collect()
            outs[level] = r["out"]
    finally:
        crs_grids.register_proj_grids(spark, list(saved), replace=True)
    for level in outs:
        assert (
            outs[level]["raster"] is not None
        ), f"{level}: {dict(outs[level]['metadata'])}"
    assert bytes(outs["L1"]["raster"]) == bytes(outs["L3"]["raster"])


@pytest.mark.integration
def test_l1_refuses_a_literal_it_cannot_bind_when_the_query_is_analyzed(request):
    """The JVM engine binds literal arguments once, at analysis: an unknown unit fails the query before it runs."""
    spark = _engine_session(request, "L1")
    register(spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": ("L1", "engine")})
    spark.createDataFrame(
        [(None,)], f"tile {manifest.TILE_SQL}"
    ).createOrReplaceTempView("_gbx_lit")
    with pytest.raises(Exception, match="radians"):
        spark.sql(
            "SELECT gbx__l1_rst_slope(tile, 'radians') FROM _gbx_lit"
        )  # analyzed, never run


def test_a_virtual_tile_over_the_read_cap_is_refused_from_its_header(
    request, tmp_path, monkeypatch
):
    """The read cap (the driver's, captured when the engine registers) is applied to a virtual tile's decoded window
    from its header, before its pixels are read: over it, an error tile with a Fix, not a failed query.
    """
    spark = _engine_session(request, "L3")
    monkeypatch.setenv(
        "GBX_STREAM_MAX_BYTES", "1024"
    )  # the 64 x 64 Float32 DEM is 16 KiB decoded
    register(
        spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": ("L3", "engine-c0")}
    )
    tile = oracles._build("dem_vrt", str(tmp_path))
    df = spark.createDataFrame([(tile,)], f"tile {manifest.TILE_SQL}")
    (r,) = df.selectExpr("gbx_rst_slope(tile) AS s").collect()
    reason = r["s"]["metadata"]["last_error"]
    assert r["s"]["raster"] is None and "read cap" in reason and "Fix:" in reason


def test_a_tile_in_another_driver_is_translated_under_the_decoded_budget(monkeypatch):
    """A materialized PNG runs on its GeoTIFF translation and leaves /vsimem/gbx-spec empty; over the decoded
    budget (judged from its header) it is refused before any translation, with a Fix."""
    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(why)
    fn = MAN.functions["gbx_rst_slope"]
    png = oracles._build("dem_png_bytes", "")["raster"]
    vals = {"unit": "degrees", "xscale": None, "yscale": None}
    res = engine_run.run(fn, [png], vals)
    assert res.error is None and res.raster[:4] in engine_run._TIFF_MAGIC
    translations = []
    real = engine_run._to_gtiff
    monkeypatch.setattr(
        engine_run, "_to_gtiff", lambda *a: translations.append(a) or real(*a)
    )
    monkeypatch.setattr(
        engine_run, "decoded_budget", lambda: 1024
    )  # the 64 x 64 Byte PNG is 4 KiB decoded
    over = engine_run.run(fn, [png], vals)
    assert over.raster is None and "Fix: retile" in over.error and translations == []
    assert not engine_run._gdal().ReadDirRecursive(engine_run.PREFIX)


def _opened(gdal, call):
    """The files GDAL opened (its CPL_DEBUG "GDALOpen(<name>" lines) while `call()` ran, and its result."""
    lines = []
    gdal.PushErrorHandler(lambda _cls, _no, msg: lines.append(msg))
    try:
        with gdal.config_options({"CPL_DEBUG": "ON"}, thread_local=True):
            res = call()
    finally:
        gdal.PopErrorHandler()
    return [
        m.split("GDALOpen(", 1)[1].split(",", 1)[0] for m in lines if "GDALOpen(" in m
    ], res


# what the refusal says per source: the header check names the VRT driver; the tile index driver is deregistered,
# so no driver identifies its bytes
REFUSAL = {
    "local": "is a VRT raster",
    "vsicurl": "is a VRT raster",
    "gti": "is not a raster GeoBrix reads from tile bytes",
}


@pytest.mark.parametrize("source", ["local", "vsicurl", "gti"])
def test_tile_bytes_that_name_other_files_are_refused_before_any_open(tmp_path, source):
    """The bytes of a VRT (its source a local file, or a URL) and of a tile index are an error tile with a Fix: a
    driver outside the manifest's self-contained bytes class is named from the header, never opened, so nothing they
    name is read, and the call's /vsimem prefix is left empty."""
    from databricks.labs.gbx.bench.oracles import _vrt

    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(why)
    gdal = engine_run._gdal()
    sentinel = tmp_path / "gbx_sentinel.tif"
    sentinel.write_bytes(oracles._se_build("dem_3857")["raster"])
    if source == "gti":
        data = f"<GDALTileIndexDataset><IndexDataset>{sentinel}</IndexDataset></GDALTileIndexDataset>"
    else:
        src = (
            str(sentinel)
            if source == "local"
            else "/vsicurl/https://example.invalid/gbx_sentinel.tif"
        )
        data = _vrt(src)
    if (
        source == "local"
    ):  # the detector sees the sentinel when GDAL opens the VRT itself
        name = f"/vsimem/gbx-probe-{tmp_path.name}.vrt"
        gdal.FileFromMemBuffer(name, data.encode())

        def read_it():
            ds = gdal.Open(name)
            ds.GetRasterBand(1).Checksum()

        try:
            assert str(sentinel) in _opened(gdal, read_it)[0]
        finally:
            gdal.Unlink(name)
    fn = MAN.functions["gbx_rst_slope"]
    vals = {"unit": "degrees", "xscale": None, "yscale": None}
    opened, res = _opened(gdal, lambda: engine_run.run(fn, [data.encode()], vals))
    assert (
        res.raster is None and "Fix:" in res.error and "tile bytes" in res.error
    ), res.error
    # A failed open logs no GDALOpen line, so for vsicurl (the host never resolves) and gti (the driver is
    # deregistered) `opened` proves nothing: the refusal message is the proof that the header check, not an
    # open attempt, ended the call.
    assert REFUSAL[source] in res.error, res.error
    assert not [o for o in opened if not o.startswith(engine_run.PREFIX)], opened
    assert not gdal.ReadDirRecursive(engine_run.PREFIX)


def test_unreadable_tile_bytes_are_an_error_tile_and_leave_nothing():
    """Bytes no driver reads, and a PNG whose pixels are cut short (the translation fails), are error tiles."""
    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(why)
    fn = MAN.functions["gbx_rst_slope"]
    vals = {"unit": "degrees", "xscale": None, "yscale": None}
    png = oracles._build("dem_png_bytes", "")["raster"]
    for data in (b"\x01\x02\x03\x04\x05", png[: len(png) // 2]):
        res = engine_run.run(fn, [data], vals)
        assert res.raster is None and res.error, res
    assert not engine_run._gdal().ReadDirRecursive(engine_run.PREFIX)


# --- Unity Catalog: the handler behind every UC row of the slice tables, on Python values (no Spark) ---------------
# The warehouse statements of the same rows are run by bench.probes.run_uc_slice; a function that has a Scala handler
# is answered by the same rows in KernelFacadesSmokeTest (the JAR, in the container).
_UC_TABLES = (
    "GRID_SCALAR",
    "GRID_ARRAY",
    "GRID_GENERATOR",
    "VECTOR_SCALAR",
    "RASTER_ACCESSOR",
    "RASTER_STAT",
    "SPEC_ANALYSIS",
    "RASTER_TILE_OP",
    "SPEC_WARP",
    "SPEC_CALC",
    "SPEC_DEM",
    "RASTER_ARRAY_OP",
    "SPEC_SE",
    "RASTER_RASTERIZE",
    "RASTER_VECTORIZE",
    "RASTER_CONSTRUCTOR",
    "WEB_TILES",
    "RASTER_AGGREGATE",
    "VECTOR_GENERATOR",
    "CELL_AGGREGATE",
    "RASTER_TO_GRID",
    "RASTER_GENERATOR",
    "VECTOR_AGGREGATE",
    "RASTER_FILE_OP",
)


def _uc_fns(table):
    man = manifest.load()
    return sorted(
        {
            c.fn
            for c in oracles.TABLES[table]
            if "uc" in c.surfaces
            and "L4" in man.functions[c.fn].levels
            and "sql"
            not in man.functions[c.fn].levels[
                "L4"
            ]  # a SQL kernel is judged by run_sql_kernels
        }
    )


UC_ROWS = [
    pytest.param(t, fn, id=f"{fn}-L4")
    for t in _UC_TABLES
    if not _ONLY or t in _ONLY
    for fn in _uc_fns(t)
]


@pytest.mark.parametrize("table,sql_name", UC_ROWS)
def test_family_oracle_uc_handlers(tmp_path, table, sql_name):
    cases = [c for c in oracles.TABLES[table] if c.fn == sql_name]
    fn = manifest.load().functions[sql_name]
    run = (
        oracles.run_handlers_rows
        if "tvf_over" in fn.levels["L4"]
        else oracles.run_handlers
    )
    ran = run(cases, tmpdir=str(tmp_path))
    # a recipe row is a statement for the workspace (and the session runs it): no handler runs it
    assert ran and sum("uc" in c.surfaces and c.sql is None for c in cases) == len(ran)
    assert [m for _, m in ran if m] == []


SQL_KERNEL_ROWS = [
    pytest.param(t, fn, id=f"{fn}-L4-sql")
    for t in _UC_TABLES
    if not _ONLY or t in _ONLY
    for fn in sorted(
        {
            c.fn
            for c in oracles.TABLES[t]
            if "uc" in c.surfaces
            and "sql" in manifest.load().functions[c.fn].levels.get("L4", {})
        }
    )
]


@pytest.mark.parametrize("table,sql_name", SQL_KERNEL_ROWS)
def test_family_oracle_uc_sql_kernels(spark, tmp_path, table, sql_name):
    """The UC rows of a function whose UC form is a SQL kernel, on the local Spark: the kernel is created as a temporary
    function with the UC signature (the warehouse runs the same text in bench.probes.run_uc_slice).
    """
    if sql_name in product_stand_ins.WORKSPACE_ONLY:
        pytest.skip(
            f"{sql_name} {product_stand_ins.WORKSPACE_ONLY[sql_name]}: judged on the workspace"
        )
    cases = [c for c in oracles.TABLES[table] if c.fn == sql_name]
    ran = oracles.run_sql_kernels(spark, cases, tmpdir=str(tmp_path))
    assert ran and sum("uc" in c.surfaces for c in cases) == len(ran)
    assert [m for _, m in ran if m] == []


def test_every_uc_function_of_the_slice_has_its_uc_rows():
    """Each L4 function of the grid and vector tables has UC rows: a value, a NULL, and (for a geometry argument) a
    refusal or SRID row; the refusal of a BINARY for a GEOMETRY and the SRID of the BNG cell geometry are named rows.
    """
    man = manifest.load()
    for t in _UC_TABLES:
        for name in _uc_fns(t) + [
            p.values[1] for p in SQL_KERNEL_ROWS if p.values[0] == t
        ]:
            rows = [c for c in oracles.TABLES[t] if c.fn == name and "uc" in c.surfaces]
            kinds = {c.kind for c in rows}
            assert "null" in kinds or any(c.kind in ("no_rows",) for c in rows), name
            if any(
                p.uc == "GEOMETRY" for p in man.functions[name].params
            ) and name.split("_")[1] in ("bng", "h3", "quadbin", "custom"):
                assert "uc_refused" in kinds, name
    ids = {c.cid for c in oracles.TABLES["GRID_SCALAR"] if "uc" in c.surfaces}
    assert {"uc_wkb_into_geometry_refused", "uc_bng_aswkb_srid"} <= ids


# The bytes of gbx_bng_aswkb / gbx_bng_centroid for TQ3080 (little-endian EWKB, SRID 27700, the ring from the south-west
# corner): the UC Python handler here, the UC Scala handler in KernelFacadesSmokeTest, one expected hex.
TQ3080_ASWKB_HEX = (
    "0103000020346c0000010000000500000000000000a02c20410000000000f9054100000000703420410000000000f9054100"
    "00000070342041000000004018064100000000a02c2041000000004018064100000000a02c20410000000000f90541"
)
TQ3080_CENTROID_HEX = "0101000020346c0000000000008830204100000000a0080641"


def test_the_uc_python_handlers_write_the_bytes_the_uc_scala_handlers_write():
    from databricks.labs.gbx.uc import handlers

    assert handlers.bng_aswkb("TQ3080").hex() == TQ3080_ASWKB_HEX
    assert handlers.bng_centroid("TQ3080").hex() == TQ3080_CENTROID_HEX


SLICE = {
    "gbx_bng_pointascell", "gbx_bng_polyfill", "gbx_bng_aswkb", "gbx_bng_centroid", "gbx_bng_kring",
    "gbx_bng_geomkring", "gbx_bng_cellarea", "gbx_quadbin_pointascell", "gbx_quadbin_polyfill",
    "gbx_h3_geomkring", "gbx_st_explainvalidity", "gbx_st_legacyaswkb", "gbx_st_wrapx",
}  # fmt: skip
TVFS = {"gbx_bng_kringexplode", "gbx_bng_geomkringexplode", "gbx_h3_geomkringexplode"}


def test_the_uc_slice_probe_runs_every_slice_function_and_its_table_functions():
    """bench.probes.run_uc_slice (run on the warehouse and Serverless env 6 in Task 28) takes these rows."""
    from databricks.labs.gbx.bench import probes

    cases = probes.uc_slice_cases()
    assert SLICE | TVFS <= {c.fn for c in cases}
    # statements it would send: every scalar row once, with 3-level names
    sql = oracles.uc_sql([c for c in cases if c.fn in SLICE], "cat", "sch", "/tmp")
    assert len(sql) == len([c for c in cases if c.fn in SLICE])
    assert all("`cat`.`sch`.gbx_" in stmt for _, stmt in sql)
    refused = dict(sql)["uc_wkb_into_geometry_refused"]
    assert (
        "X'" in refused and "st_geomfromwkt" not in refused
    )  # a BINARY goes in as BINARY
    legacy = dict(sql)["leg_polygon_hole"]
    assert "named_struct('typeId', 5, 'srid', 0" in legacy and "st_asewkb(" in legacy


# --- The DataFrame helpers (`rasterx.helpers`): rows of the MVP_H table, which no manifest function owns -------------
_HELPERS = pytest.mark.skipif(
    bool(_ONLY) and "MVP_H" not in _ONLY, reason="GBX_ORACLE_TABLES leaves out MVP_H"
)


def _helpers_red(spark, tmp_path):
    ran = oracles.run_helpers(spark, oracles.MVP_H, tmpdir=str(tmp_path))
    assert len(ran) == len(oracles.MVP_H)
    return [m for _, m in ran if m]


@_HELPERS
def test_helpers_oracle_session(spark, tmp_path):
    assert _helpers_red(spark, tmp_path) == []


@_HELPERS
def test_helpers_oracle_connect(spark_connect_local, tmp_path):
    """The same rows through a Spark Connect client: the helpers touch no JVM-only API."""
    assert _helpers_red(spark_connect_local, tmp_path) == []


@_HELPERS
def test_a_virtual_member_is_not_decoded_into_a_row(spark, tmp_path, monkeypatch):
    """A virtual member goes to the merge by reference, never decoded into a row: only the result is bounded by the cap
    for bytes in a row, and the error says what to do with a result that does not fit.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")  # the cap is read on the driver
    tile = oracles.resolve_arg(oracles.Build("tile_virtual"), str(tmp_path))
    df = spark.createDataFrame([("a", tile)], f"key string, tile {TILE_SQL}")
    with pytest.raises(Exception, match="cap for the bytes of one row"):
        helpers.merge_by_key(df, ["key"], method="fold").collect()


def _frame(spark, tmp_path, name):
    tiles = oracles.resolve_arg(oracles.Build(name), str(tmp_path))
    return spark.createDataFrame(
        [("a", t) for t in tiles], f"key string, tile {TILE_SQL}"
    )


@_HELPERS
def test_a_group_over_the_materialize_cap_is_merged_not_held(
    spark, tmp_path, monkeypatch
):
    """A key's members are not held: their encoded bytes (2.2 KB here) pass the cap for bytes in a row (1.5 KB), and the
    merge still runs, its result (0.8 KB) being what the cap bounds. The group had a cap of its own once.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1500")  # the cap is read on the driver
    for frame in ("quad_tiles", "quad_paths"):
        rows = helpers.merge_by_key(_frame(spark, tmp_path, frame), ["key"]).collect()
        assert len(rows) == 1 and 0 < len(rows[0]["raster"]) < 1500


@_HELPERS
def test_a_result_over_the_cap_is_refused_with_the_mosaic_route(
    spark, tmp_path, monkeypatch
):
    """The merged result (0.8 KB here) is bounded by the cap for bytes in a row (0.5 KB), for tiles with bytes and for
    paths, and the Fix names the files route: `merge_by_key` is bounded by the same cap.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "500")
    for frame in ("quad_tiles", "quad_paths"):
        with pytest.raises(Exception, match=r"mint_vrt|cog_gbx writer") as err:
            helpers.merge_by_key(_frame(spark, tmp_path, frame), ["key"]).collect()
        assert "cap" in str(err.value)


@_HELPERS
def test_towers_to_crs_adds_the_counts_of_the_reprojected_viewshed(spark, tmp_path):
    """`to_crs` reprojects each tower's viewshed and counts it again: both counts are there and, on flat ground
    at the equator, within a few percent of the counts before the reprojection."""
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    manifest, towers, radius, height = (
        oracles.resolve_arg(oracles.Build("dsm_tiles_edge"), str(tmp_path)),
        (("t1", 100.0, 100.0),),
        80.0,
        20.0,
    )
    tiles = spark.createDataFrame(manifest, "tx int, ty int, path string")
    sites = spark.createDataFrame(
        list(towers), "tower_cellid string, x double, y double"
    )
    row = rst_viewshed_towers(
        sites,
        tiles,
        radius,
        height,
        x0=0.0,
        y0=0.0,
        tile_m=oracles.DSM_TILE_M,
        to_crs=4326,
    ).collect()[0]
    assert row["total_px_reproj"] > 0 and row["visible_px_reproj"] > 0
    assert row["visible_px_reproj"] <= row["total_px_reproj"]
    for after, before in (
        ("total_px_reproj", "total_px"),
        ("visible_px_reproj", "visible_px"),
    ):
        assert abs(row[after] - row[before]) <= 0.05 * row[before], (
            after,
            dict(row.asDict()),
        )
