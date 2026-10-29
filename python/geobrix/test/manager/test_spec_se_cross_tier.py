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


# One success row per function: the same input through every engine level.
SAME_PIXELS = [
    "se_slope_deg",
    "se_slope_scales",
    "tcrs_epsg",
    "tcrs_esri102003",
    "tcrs_proj4",
    "tcrs_wkt",
    "se_ma_norm_diff",
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
