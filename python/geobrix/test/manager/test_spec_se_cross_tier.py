"""The GDAL spec engine's oracle: every SPEC_SE row, and gbx_rst_transformcrs's MVP_S session rows, at L1 on the
engine (GdalSpecExpr), L3 on the engine's row-wise UDF (engine-c0) and on its batch UDF behind the wrapper
(engine-c1), through SQL; and L1 and L3 give the same pixels.

The L3 rows need the env wheel payload's osgeo (the container with GBX_NATIVE_ROOT); elsewhere they skip with the
engine's reason. The L1 rows need the staged JAR on a JVM running the payload's GDAL (conftest.payload_jvm_conf).
"""

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.manager import capabilities

MAN = manifest.load()
ENGINE_FNS = sorted(f.sql_name for f in MAN.functions.values() if f.gdal is not None)
LEVELS = [("L1", "engine"), ("L3", "engine-c0"), ("L3", "engine-c1")]


def _rows(fn):
    rows = [c for c in oracles.SPEC_SE if c.fn == fn]
    if fn == "gbx_rst_transformcrs":  # its own oracle rows, unchanged
        rows += [c for c in oracles.MVP_S if c.fn == fn and "session" in c.surfaces]
    return rows


def _session(request, level):
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    if level == "L3":
        return request.getfixturevalue("spark")
    spark = request.getfixturevalue("jvm_spark")
    caps = capabilities.detect(spark, need_jvm=True)
    if caps.jvm_gdal != MAN.gdal.pin:
        pytest.skip(
            f"PRECONDITION MISSING: the JVM runs GDAL {caps.jvm_gdal or caps.gdal_reason}, not "
            f"{MAN.gdal.pin}; it needs GBX_NATIVE_ROOT set to a payload whose bundled _jar is this session's JAR "
            "(gbx:test:parity refreshes the payload's _jar from the staged JAR; by hand, copy the JAR into its _jar/)"
        )
    return spark


def _run(request, tmp_path, fn, level, variant, rows):
    spark = _session(request, level)
    register(spark, only=[fn], _levels={fn: (level, variant)})
    results = oracles.run_session(spark, rows, tmpdir=str(tmp_path))
    assert len(results) == len(rows)
    return {case.cid: msg for case, msg in results if msg}


def _mark(level):
    return [pytest.mark.integration] if level == "L1" else []


@pytest.mark.parametrize(
    "fn,level,variant",
    [
        pytest.param(fn, lv, var, marks=_mark(lv), id=f"{fn}-{lv}-{var}")
        for fn in ENGINE_FNS
        for lv, var in LEVELS
    ],
)
def test_spec_se_oracle(request, tmp_path, fn, level, variant):
    assert _run(request, tmp_path, fn, level, variant, _rows(fn)) == {}


@pytest.mark.integration
def test_mapalgebra_on_its_heavy_companion_behind_the_tiles_wrapper(request, tmp_path):
    """L1 without the engine (as where the JVM runs another GDAL): mapalgebra runs on its RST_MapAlgebra companion
    behind the tiles dispatch wrapper. An array of materialized tiles goes to the companion, an array of virtual
    tiles to the light kernel the wrapper names, and each gives the tile."""
    from rasterio.io import MemoryFile

    spark = _session(request, "L1")
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
    "se_ma_norm_diff",
    "se_slope_png",
    "se_tcrs_projjson",
]


@pytest.mark.integration
def test_l1_and_l3_give_the_same_pixels(request, tmp_path):
    """L1 equals L3 (c0 and c1) pixel for pixel, NoData included; the bytes are equal too (same argv, same GDAL)."""
    import numpy as np

    _session(request, "L1")
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
    why = run.unavailable_reason()
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
    _session(request, "L1")
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
    spark = _session(request, "L1")
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
    spark = _session(request, "L3")
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
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    fn = MAN.functions["gbx_rst_slope"]
    png = oracles._build("dem_png_bytes", "")["raster"]
    vals = {"unit": "degrees", "xscale": None, "yscale": None}
    res = run.run(fn, [png], vals)
    assert res.error is None and res.raster[:4] in run._TIFF_MAGIC
    translations = []
    real = run._to_gtiff
    monkeypatch.setattr(run, "_to_gtiff", lambda *a: translations.append(a) or real(*a))
    monkeypatch.setattr(
        run, "decoded_budget", lambda: 1024
    )  # the 64 x 64 Byte PNG is 4 KiB decoded
    over = run.run(fn, [png], vals)
    assert over.raster is None and "Fix: retile" in over.error and translations == []
    assert not run._gdal().ReadDirRecursive(run.PREFIX)


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

    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    gdal = run._gdal()
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
    opened, res = _opened(gdal, lambda: run.run(fn, [data.encode()], vals))
    assert (
        res.raster is None and "Fix:" in res.error and "tile bytes" in res.error
    ), res.error
    # A failed open logs no GDALOpen line, so for vsicurl (the host never resolves) and gti (the driver is
    # deregistered) `opened` proves nothing: the refusal message is the proof that the header check, not an
    # open attempt, ended the call.
    assert REFUSAL[source] in res.error, res.error
    assert not [o for o in opened if not o.startswith(run.PREFIX)], opened
    assert not gdal.ReadDirRecursive(run.PREFIX)


def test_unreadable_tile_bytes_are_an_error_tile_and_leave_nothing():
    """Bytes no driver reads, and a PNG whose pixels are cut short (the translation fails), are error tiles."""
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    fn = MAN.functions["gbx_rst_slope"]
    vals = {"unit": "degrees", "xscale": None, "yscale": None}
    png = oracles._build("dem_png_bytes", "")["raster"]
    for data in (b"\x01\x02\x03\x04\x05", png[: len(png) // 2]):
        res = run.run(fn, [data], vals)
        assert res.raster is None and res.error, res
    assert not run._gdal().ReadDirRecursive(run.PREFIX)
