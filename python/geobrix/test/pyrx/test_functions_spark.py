import pytest
from pyspark.sql import functions as f

from databricks.labs.gbx.engine import run as _engine_run
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.rasterx import functions as rx

from .conftest import make_geotiff_bytes


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_no_jar_in_session(spark):
    # Guardrail: pyrx must work without the GeoBrix JAR.
    assert not spark.conf.get("spark.jars", "")


def _cellid_tile_view(spark, name, cellid):
    """Register a one-row view of a 3-band 4x4 tile carrying the given parent cellid."""
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    tile_t = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    raster = bytearray(make_geotiff_bytes(width=4, height=4, count=3))
    spark.createDataFrame(
        [{"tile": {"cellid": cellid, "raster": raster, "metadata": {}}}],
        StructType([StructField("tile", tile_t)]),
    ).createOrReplaceTempView(name)


_RETILE_FAMILY = [
    "gbx_rst_retile(tile, 2, 2)",
    "gbx_rst_tooverlappingtiles(tile, 2, 2, 10)",
    "gbx_rst_maketiles(tile, 1)",
    "gbx_rst_separatebands(tile)",
]


def test_retile_family_stamps_parent_cellid(spark):
    # Every output of the retile family carries the PARENT tile's cellid (as heavy does),
    # not a light-only emitted ordinal.
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_cellid_parent", 7)
    for fn in _RETILE_FAMILY:
        rows = spark.sql(
            f"SELECT t.cellid AS cellid FROM _ras_cellid_parent, LATERAL {fn} t"
        ).collect()
        assert rows, fn
        assert {r["cellid"] for r in rows} == {7}, (fn, [r["cellid"] for r in rows])


def test_retile_family_null_parent_cellid_is_zero(spark):
    # A null parent cellid maps to 0, matching heavy's getLong.
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_cellid_null", None)
    for fn in _RETILE_FAMILY:
        rows = spark.sql(
            f"SELECT t.cellid AS cellid FROM _ras_cellid_null, LATERAL {fn} t"
        ).collect()
        assert rows, fn
        assert {r["cellid"] for r in rows} == {0}, (fn, [r["cellid"] for r in rows])


def _window_rows(spark, view, fn):
    """The ``window`` provenance (col_off, row_off, width, height) of each output tile."""
    rows = spark.sql(
        f"SELECT t.window AS w, t.metadata AS m FROM {view}, LATERAL {fn} t"
    ).collect()
    assert rows, fn
    return rows, sorted(
        (r["w"]["col_off"], r["w"]["row_off"], r["w"]["width"], r["w"]["height"])
        for r in rows
        if r["w"] is not None
    )


def test_retile_family_stamps_window_provenance(spark):
    # A tile cut from a parent records its pixel window within the parent, so a writer
    # (or any consumer) can tell sibling windows apart: they share the parent's cellid.
    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx.pyrx.core import tiling

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_window_prov", 7)

    # rst_retile: 4x4 parent in 2x2 windows -> the four quadrants, exactly.
    rows, windows = _window_rows(
        spark, "_ras_window_prov", "gbx_rst_retile(tile, 2, 2)"
    )
    assert windows == [(0, 0, 2, 2), (0, 2, 2, 2), (2, 0, 2, 2), (2, 2, 2, 2)]
    assert len(rows) == 4

    # rst_tooverlappingtiles: the planned overlapping grid, edge windows clamped.
    rows, windows = _window_rows(
        spark, "_ras_window_prov", "gbx_rst_tooverlappingtiles(tile, 2, 2, 50)"
    )
    assert len(windows) == len(rows)
    assert windows == sorted(tiling.plan_grid_windows(4, 4, 2, 2, 50))

    # rst_maketiles: a 4x4 tile fits one MB, so the one output is the whole input and
    # keeps the input's own provenance (a plain tile has no window); see
    # test_single_output_covering_the_input_keeps_its_provenance.
    rows, windows = _window_rows(
        spark, "_ras_window_prov", "gbx_rst_maketiles(tile, 1)"
    )
    assert len(rows) == 1 and windows == []


def test_retile_window_is_relative_to_the_input_tile(spark):
    # The window is the output's place within the INPUT tile, so retiling a retiled
    # tile records offsets inside the 2x2 child, not inside the original 4x4.
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_window_nested", 7)
    spark.sql(
        "SELECT struct(t.*) AS tile FROM _ras_window_nested, "
        "LATERAL gbx_rst_retile(tile, 2, 2) t"
    ).createOrReplaceTempView("_ras_window_children")
    _, windows = _window_rows(
        spark, "_ras_window_children", "gbx_rst_retile(tile, 1, 1)"
    )
    assert len(windows) == 16  # four 2x2 children, four 1x1 windows each
    assert set(windows) == {(c, r, 1, 1) for c in (0, 1) for r in (0, 1)}


def _v2_input_view(spark, name, *, path, window):
    """One-row view of a 4x4 materialized v2 tile carrying the given provenance."""
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    win = (
        None
        if window is None
        else dict(zip(("col_off", "row_off", "width", "height"), window))
    )
    tile = {
        "cellid": 7,
        "raster": bytearray(make_geotiff_bytes(width=4, height=4, count=1)),
        "path": path,
        "path_mode": None,
        "window": win,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    spark.createDataFrame(
        [{"tile": tile}], StructType([StructField("tile", V2_TILE_SCHEMA)])
    ).createOrReplaceTempView(name)


def _provenance_rows(spark, view, fn):
    rows = spark.sql(
        f"SELECT t.path AS p, t.window AS w FROM {view}, LATERAL {fn} t"
    ).collect()
    return [
        (
            r["p"],
            (
                None
                if r["w"] is None
                else (
                    r["w"]["col_off"],
                    r["w"]["row_off"],
                    r["w"]["width"],
                    r["w"]["height"],
                )
            ),
        )
        for r in rows
    ]


# One output that covers its whole 4x4 input IS the input, so it keeps the input's
# provenance instead of stamping a fresh window.
_WHOLE_INPUT_FNS = [
    "gbx_rst_retile(tile, 4, 4)",
    "gbx_rst_tooverlappingtiles(tile, 4, 4, 0)",
    "gbx_rst_maketiles(tile, 1)",
]


def test_single_output_covering_the_input_keeps_its_provenance(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    # A whole-file reader tile: path plus the whole window.
    _v2_input_view(
        spark, "_ras_prov_whole", path="/data/scene.tif", window=(0, 0, 4, 4)
    )
    # A part of a parent: no path, its window within that parent.
    _v2_input_view(spark, "_ras_prov_part", path=None, window=(4, 0, 4, 4))
    # A tile with no provenance at all.
    _v2_input_view(spark, "_ras_prov_none", path=None, window=None)
    for fn in _WHOLE_INPUT_FNS:
        assert _provenance_rows(spark, "_ras_prov_whole", fn) == [
            ("/data/scene.tif", (0, 0, 4, 4))
        ], fn
        assert _provenance_rows(spark, "_ras_prov_part", fn) == [
            (None, (4, 0, 4, 4))
        ], fn
        assert _provenance_rows(spark, "_ras_prov_none", fn) == [(None, None)], fn


def test_single_output_covering_a_band_tile_keeps_its_band(spark):
    # A no-op retile of a band tile must stay marked as that band, or the bands would
    # all look like whole tiles of the same source.
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_band_parent", 7)
    spark.sql(
        "SELECT struct(t.*) AS tile FROM _ras_band_parent, "
        "LATERAL gbx_rst_separatebands(tile) t"
    ).createOrReplaceTempView("_ras_band_children")
    rows = spark.sql(
        "SELECT t.metadata AS m FROM _ras_band_children, "
        "LATERAL gbx_rst_retile(tile, 4, 4) t"
    ).collect()
    assert sorted(r["m"]["band"] for r in rows) == ["1", "2", "3"]


def test_several_outputs_stamp_windows_within_the_input_and_drop_its_path(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _v2_input_view(
        spark, "_ras_prov_multi", path="/data/scene.tif", window=(0, 0, 4, 4)
    )
    rows = _provenance_rows(spark, "_ras_prov_multi", "gbx_rst_retile(tile, 2, 2)")
    assert sorted(w for _, w in rows) == [
        (0, 0, 2, 2),
        (0, 2, 2, 2),
        (2, 0, 2, 2),
        (2, 2, 2, 2),
    ]
    assert {p for p, _ in rows} == {None}


def test_separatebands_stamps_band_marker(spark):
    # Each band tile is marked with its 1-based band number (metadata key "band"); the
    # bands share the parent's extent, so no window is stamped.
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    _cellid_tile_view(spark, "_ras_band_marker", 7)
    rows = spark.sql(
        "SELECT t.window AS w, t.metadata AS m "
        "FROM _ras_band_marker, LATERAL gbx_rst_separatebands(tile) t"
    ).collect()
    assert sorted(r["m"]["band"] for r in rows) == ["1", "2", "3"]
    assert all(r["w"] is None for r in rows)


# --- web-mercator XYZ tiling (rst_tilexyz / rst_xyzpyramid) -----------------


# --- Group 1: per-band statistics & accessors -------------------------------


# --- Operations (tryopen, setsrid, band, asformat, buildoverviews, sample) --


# ---------------------------------------------------------------------------
# Grouped aggregators (rst_*_agg) via .agg()
# ---------------------------------------------------------------------------
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402


def _ras_bytes(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[None, :, :]
    bands, h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=bands,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def _tiles_df(spark, rows):
    """rows: list of (key, raster_bytes). Builds a (k, tile struct) df."""
    df = spark.createDataFrame(rows, ["k", "raster"])
    return df.select("k", rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_rst_rasterize_agg_accepts_text_geometry(spark):
    """WKT and EWKT strings burn the same raster as WKB; NULL and unparseable text add no feature."""
    import shapely.wkb
    from shapely.geometry import box

    boxes = [(box(0, 0, 2, 4), 1.0), (box(1, 0, 4, 4), 2.0)]

    def burn(geoms, schema):
        rows = [("g", g, v) for g, (_, v) in zip(geoms, boxes)]
        rows.append(("g", None, 9.0))
        if "string" in schema.split("geom")[1].split(",")[0]:
            rows.append(("g", "not a geometry", 9.0))
        df = spark.createDataFrame(rows, schema=schema)
        tile = (
            df.groupBy("k")
            .agg(
                prx._light_rst_rasterize_agg(
                    "geom", "val", 0, 0, 4, 4, 4, 4, 32633
                ).alias("t")
            )
            .first()["t"]
        )
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return ds.read(1)

    wkb = burn(
        [shapely.wkb.dumps(b) for b, _ in boxes], "k string, geom binary, val double"
    )
    wkt = burn([b.wkt for b, _ in boxes], "k string, geom string, val double")
    ewkt = burn(
        [f"SRID=32633;{b.wkt}" for b, _ in boxes], "k string, geom string, val double"
    )
    assert np.all(wkb[:, 0] == 1.0) and np.all(wkb[:, 1] == 2.0)
    assert np.array_equal(wkt, wkb)
    assert np.array_equal(ewkt, wkb)


PYFUNC_SUM_AGG = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def _error_tile(reason):
    """An error tile with the keys a heavy one carries, so a rebuilt tile that kept the reason only shows."""
    tile = _serde.build_error_tile(reason, 7)
    tile["metadata"] = {
        "last_error": reason,
        "error_message": reason,
        "gdal_error": f"gdal says {reason}",
        "error_detail": f"detail of {reason}",
    }
    return tile


def _first_error(tiles):
    """The first of error tiles in canonical order: the cell id, then the metadata sorted."""
    return min(
        tiles,
        key=lambda t: (
            t["cellid"],
            "\0".join(f"{k}={v}" for k, v in sorted(t["metadata"].items())),
        ),
    )


def _error_tiles_df(spark, *reasons):
    from pyspark.sql.types import StringType, StructField, StructType

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    schema = StructType(
        [StructField("k", StringType()), StructField("tile", V2_TILE_SCHEMA)]
    )
    return spark.createDataFrame([("g", _error_tile(r)) for r in reasons], schema)


@pytest.mark.parametrize(
    "agg",
    [
        lambda d: prx._light_rst_merge_agg("tile"),
        lambda d: prx._light_rst_merge_agg("tile", virtualize_dir=str(d)),
        lambda d: prx._light_rst_combineavg_agg("tile"),
        lambda d: prx._light_rst_derivedband_agg("tile", PYFUNC_SUM_AGG, "addbands"),
    ],
    ids=["merge", "merge-virtualize", "combineavg", "derivedband"],
)
def test_an_aggregate_of_nothing_but_error_tiles_is_the_first_error_tile(
    spark, tmp_path, agg
):
    """A group whose every member is an error tile gives the first member's error tile as it is, for one member
    or several (every key, not a rebuilt tile that kept the reason), as on the JVM tier; one readable member
    makes it an ordinary group, the error tiles dropped."""
    for reasons in (("boom",), ("boom", "bang"), ("bang", "boom", "bang")):
        out = (
            _error_tiles_df(spark, *reasons)
            .groupBy("k")
            .agg(agg(tmp_path).alias("t"))
            .first()["t"]
        )
        want = _first_error([_error_tile(r) for r in reasons])
        assert out is not None and out["raster"] is None and out["path"] is None
        assert out["cellid"] == want["cellid"]
        assert dict(out["metadata"]) == want["metadata"]
    ok = _tiles_df(spark, [("g", _ras_bytes(np.full((2, 2), 3.0)))])
    mixed = (
        ok.union(_error_tiles_df(spark, "boom"))
        .groupBy("k")
        .agg(agg(tmp_path).alias("t"))
    )
    kept = mixed.first()["t"]  # the mosaic (a path only, when asked to virtualize it)
    assert kept["raster"] is not None or kept["path"] is not None


def _far_and_near(spark):
    """A group `far` (two tiles 120 degrees apart: a mosaic of terabytes) beside a group `near` (two adjacent)."""
    far = [
        _ras_bytes(np.full((16, 16), v), ulx=x, uly=y, px=1e-4, epsg=4326)
        for v, x, y in ((1.0, 0.0, 0.0), (2.0, 120.0, -60.0))
    ]
    near = [
        _ras_bytes(np.full((16, 16), v), ulx=x, uly=16.0)
        for v, x in ((3.0, 0.0), (4.0, 16.0))
    ]
    return _tiles_df(spark, [("far", b) for b in far] + [("near", b) for b in near])


def _surfaces(spark, df, d):
    """Each way to merge a group, as (is the result a tile?, the DataFrame of (k, t))."""
    from databricks.labs.gbx import register
    from databricks.labs.gbx.pyrx.functions import _merge_agg_udf

    df.createOrReplaceTempView("far_and_near")
    register(spark, only=["gbx_rst_merge_tvf"])
    by = df.groupBy("k")
    return {
        "api": (True, by.agg(prx._light_rst_merge_agg("tile").alias("t"))),
        "api-virtualize": (
            True,
            by.agg(prx._light_rst_merge_agg("tile", virtualize_dir=str(d)).alias("t")),
        ),
        "finalize": (
            True,
            by.agg(f.collect_list("tile").alias("ts")).select(
                "k", rx.rst_merge("ts").alias("t")
            ),
        ),
        "sql-bytes": (False, by.agg(_merge_agg_udf(f.col("tile")).alias("t"))),
        "tvf": (
            False,
            spark.sql(
                "SELECT key AS k, raster AS t FROM gbx_rst_merge_tvf("
                "TABLE(SELECT k AS key, tile FROM far_and_near) PARTITION BY (key))"
            ),
        ),
    }


@pytest.mark.parametrize("surface", ["api", "api-virtualize", "finalize"])
def test_a_merge_too_big_to_build_is_that_groups_answer_not_the_querys_failure(
    spark, tmp_path, surface
):
    """One group whose mosaic is past what a merge can stream does not fail the query where the result is a tile:
    that group is an error tile (a NULL raster and the reason, `Fix:` included), and the other group is merged.
    """
    _, out = _surfaces(spark, _far_and_near(spark), tmp_path)[surface]
    rows = {r["k"]: r["t"] for r in out.collect()}
    far, near = rows["far"], rows["near"]
    assert far is not None and far["raster"] is None and far["path"] is None
    assert "GiB" in far["metadata"]["last_error"]
    assert "Fix:" in far["metadata"]["last_error"]
    assert near["raster"] is not None or near["path"] is not None


@pytest.mark.parametrize("surface", ["sql-bytes", "tvf"])
def test_a_merge_too_big_to_build_raises_with_the_fix_where_the_result_is_bytes(
    spark, tmp_path, surface
):
    """Bytes have no place for a reason, and a NULL or a missing row reads as an empty group: the query raises, and
    the message names the route that can mosaic the files (`mint_vrt`: `merge_by_key` is bounded by the same cap).
    """
    _, out = _surfaces(spark, _far_and_near(spark), tmp_path)[surface]
    with pytest.raises(Exception, match="mint_vrt"):
        out.collect()


# The table functions read one partition's rows: a call not partitioned by `key`, a BINARY `tile` and a NULL canvas
# constant each raise with the Fix, and name what to change.
_BNG_ROWS = (
    "TABLE(SELECT 'a' AS key, 'TQ3080' AS cellid, 1.0D AS value) PARTITION BY (key)"
)
_BNG_CANVAS = "530000.0D, 180000.0D, 533000.0D, 182000.0D, 3, 2"


def test_a_table_function_raises_when_a_partition_holds_two_keys(spark):
    from databricks.labs.gbx import register

    register(spark, only=["gbx_rst_merge_tvf"])
    df = _tiles_df(
        spark,
        [
            ("a", _ras_bytes(np.ones((4, 4)))),
            ("b", _ras_bytes(np.ones((4, 4)), ulx=4.0)),
        ],
    )
    df.createOrReplaceTempView("two_keys")
    with pytest.raises(Exception, match=r"more than one key.*PARTITION BY \(key\)"):
        spark.sql(
            "SELECT * FROM gbx_rst_merge_tvf("
            "TABLE(SELECT k AS key, tile FROM two_keys) WITH SINGLE PARTITION)"
        ).collect()


@pytest.mark.parametrize(
    "cls_name,agg",
    [
        ("_RstMergeTvf", "gbx_rst_merge_agg"),
        ("_RstBngRasterizeTvf", "gbx_rst_bng_rasterize_agg"),
    ],
)
def test_a_table_function_on_serverless_names_the_aggregate_not_an_index_error(
    monkeypatch, cls_name, agg
):
    """Serverless's Python table-function engine fails `PARTITION BY` with `IndexError` on a task's second row (a
    plain pyspark UDTF fails the same way there), so the function says so, with the Fix, instead.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    cls = getattr(prx, cls_name).func
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.1")
    with pytest.raises(
        RuntimeError, match=rf"Serverless.*Fix: call {agg} on Serverless"
    ):
        cls()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "19.0")
    cls()  # a dedicated or standard cluster builds it


def test_a_binary_tile_column_of_a_table_function_raises_with_the_fix(spark):
    from databricks.labs.gbx import register

    register(spark, only=["gbx_rst_merge_tvf"])
    df = _tiles_df(spark, [("a", _ras_bytes(np.ones((4, 4))))])
    df.select("k", f.col("tile.raster").alias("tile")).createOrReplaceTempView(
        "binary_tiles"
    )
    with pytest.raises(Exception, match=r"BINARY.*gbx_rst_fromcontent"):
        spark.sql(
            "SELECT * FROM gbx_rst_merge_tvf("
            "TABLE(SELECT k AS key, tile FROM binary_tiles) PARTITION BY (key))"
        ).collect()


@pytest.mark.parametrize("name", ["xmin", "ymax", "width", "height"])
def test_a_null_canvas_constant_of_the_bng_tvf_raises_naming_it(spark, name):
    from databricks.labs.gbx import register

    register(spark, only=["gbx_rst_bng_rasterize_tvf"])
    canvas = dict(
        zip(
            ("xmin", "ymin", "xmax", "ymax", "width", "height"),
            _BNG_CANVAS.split(", "),
        )
    )
    canvas[name] = "CAST(NULL AS INT)" if name in ("width", "height") else "NULL"
    with pytest.raises(Exception, match=rf"canvas constant '{name}' is NULL"):
        spark.sql(
            f"SELECT * FROM gbx_rst_bng_rasterize_tvf({_BNG_ROWS}, {', '.join(canvas.values())})"
        ).collect()


# ---------------------------------------------------------------------------
# Heavyweight-only non-aggregator constructors / array operations
# (rst_merge, rst_combineavg, rst_frombands, rst_fromfile)
# ---------------------------------------------------------------------------


def test_rst_fromfile_strips_scheme_before_open(spark, tmp_path):
    # Columns may carry scheme-qualified paths (to_spark_uri -> dbfs:/file:); the
    # udf must strip the scheme back to the bare FUSE path before rasterio.open,
    # else the open fails (rasterio cannot open 'file:/...'). A bare tmp file
    # prefixed with 'file:' exercises that strip (file:/tmp/x -> /tmp/x). Use
    # materialize=True so the read actually happens.
    import rasterio

    p = str(tmp_path / "scene.tif")
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as src:
        profile = src.profile.copy()
        data = src.read()
    with rasterio.open(p, "w", **profile) as dst:
        dst.write(data)
    df = spark.createDataFrame([("file:" + p,)], ["path"])
    tile = df.select(
        rx.rst_fromfile("path", f.lit("GTiff"), materialize=True).alias("t")
    ).first()["t"]
    assert tile is not None  # scheme stripped -> open succeeded
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4 and ds.height == 3


# ---------------------------------------------------------------------------
# Geom-encoding parity: every pyrx function that accepts a user geometry must
# treat the SAME geometry expressed as WKB / EWKB / WKT / EWKT identically.
# These mirror test_rst_clip_geom_encodings / test_rst_sample_geom_encodings.
# ---------------------------------------------------------------------------


def _encode_geom(geom, srid=4326):
    """The same geometry in all four accepted encodings."""
    from shapely import set_srid, to_wkb

    return {
        "wkb": to_wkb(geom),
        "ewkb": to_wkb(set_srid(geom, srid), include_srid=True),
        "wkt": geom.wkt,
        "ewkt": f"SRID={srid};{geom.wkt}",
    }


def test_rst_rasterize_geom_encodings(spark):
    from shapely.geometry import box

    results = {}
    for name, geom in _encode_geom(box(1.0, 1.0, 3.0, 3.0)).items():
        df = spark.createDataFrame([(geom,)], ["g"])
        tile = df.select(
            rx.rst_rasterize("g", 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326).alias("t")
        ).first()["t"]
        assert tile is not None and tile["raster"] is not None, f"{name} null tile"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            results[name] = ds.read(1).tobytes()
    assert len(set(results.values())) == 1, "rst_rasterize encodings diverged"


@pytest.mark.skipif(
    bool(_engine_run.unavailable_reason()), reason=str(_engine_run.unavailable_reason())
)  # gbx_rst_viewshed runs only on the GDAL spec engine (rasterio's osgeo); a host venv without it skips
def test_rst_viewshed_geom_encodings(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from shapely.geometry import Point

    dem = np.zeros((7, 7), dtype="float64")
    dem[:, 3] = 100.0
    profile = dict(
        driver="GTiff",
        width=7,
        height=7,
        count=1,
        dtype="float64",
        crs="EPSG:32633",
        transform=from_origin(0.0, 7.0, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        src = mf.read()

    results = {}
    for name, geom in _encode_geom(Point(0.5, 3.5), srid=32633).items():
        df = spark.createDataFrame([(src, geom)], ["raster", "g"]).select(
            rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
            f.col("g"),
        )
        tile = df.select(
            rx.rst_viewshed("tile", f.col("g"), 1.0, 0.0, None).alias("t")
        ).first()["t"]
        assert tile is not None and tile["raster"] is not None, f"{name} null tile"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            results[name] = ds.read(1).tobytes()
    assert len(set(results.values())) == 1, "rst_viewshed encodings diverged"


def test_rst_gridfrompoints_geom_encodings(spark):
    from shapely.geometry import Point

    # IDW of p0=(0,0) v=10 and p1=(2,2) v=30 at the 1x1 center -> 20.
    results = {}
    for name in ("wkb", "ewkb", "wkt", "ewkt"):
        p0 = _encode_geom(Point(0.0, 0.0), srid=32633)[name]
        p1 = _encode_geom(Point(2.0, 2.0), srid=32633)[name]
        df = spark.createDataFrame([([p0, p1], [10.0, 30.0])], ["pts", "vals"])
        tile = df.select(
            rx.rst_gridfrompoints(
                "pts", "vals", 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, 2.0, 2
            ).alias("t")
        ).first()["t"]
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            v = ds.read(1)[0, 0]
        assert np.isclose(v, 20.0), f"{name} gridfrompoints -> {v}"
        results[name] = v
    assert all(np.isclose(v, 20.0) for v in results.values())


def test_rst_gridfrompoints_agg_geom_encodings(spark):
    from shapely.geometry import Point

    for name in ("wkb", "ewkb", "wkt", "ewkt"):
        p0 = _encode_geom(Point(0.0, 0.0), srid=32633)[name]
        p1 = _encode_geom(Point(2.0, 2.0), srid=32633)[name]
        df = spark.createDataFrame([("g", p0, 10.0), ("g", p1, 30.0)], ["k", "pt", "v"])
        tile = (
            df.groupBy("k")
            .agg(
                prx._light_rst_gridfrompoints_agg(
                    "pt", "v", 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, 2.0, 2
                ).alias("t")
            )
            .first()["t"]
        )
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            v = ds.read(1)[0, 0]
        assert np.isclose(v, 20.0), f"{name} gridfrompoints_agg -> {v}"


def _z_point_encodings(x, y, z, srid=32633):
    """Point-with-Z in all four encodings (WKT/EWKT carry 'POINT Z')."""
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point

    p = Point(x, y, z)
    return {
        "wkb": to_wkb(p, output_dimension=3),
        "ewkb": to_wkb(set_srid(p, srid), output_dimension=3, include_srid=True),
        "wkt": p.wkt,
        "ewkt": f"SRID={srid};{p.wkt}",
    }


def test_rst_dtmfromgeoms_geom_encodings(spark):
    # z = 2x + 3y + 1 plane; barycentric interpolation is exact in-hull.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    for name in ("wkb", "ewkb", "wkt", "ewkt"):
        pts = [_z_point_encodings(x, y, 2 * x + 3 * y + 1)[name] for x, y in corners]
        df = spark.createDataFrame([(pts,)], ["pts"])
        tile = df.select(
            rx.rst_dtmfromgeoms(
                "pts", f.lit(None), 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
            ).alias("t")
        ).first()["t"]
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            arr = ds.read(1)
            transform = ds.transform
        wx, wy = transform * (5 + 0.5, 5 + 0.5)
        assert np.isclose(
            arr[5, 5], 2 * wx + 3 * wy + 1, atol=1e-6
        ), f"{name} dtmfromgeoms diverged"


def test_rst_dtmfromgeoms_agg_geom_encodings(spark):
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    for name in ("wkb", "ewkb", "wkt", "ewkt"):
        rows = [
            ("g", _z_point_encodings(x, y, 2 * x + 3 * y + 1)[name]) for x, y in corners
        ]
        df = spark.createDataFrame(rows, ["k", "pt"])
        tile = (
            df.groupBy("k")
            .agg(
                prx._light_rst_dtmfromgeoms_agg(
                    "pt", f.lit(None), 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
                ).alias("t")
            )
            .first()["t"]
        )
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            arr = ds.read(1)
            transform = ds.transform
        wx, wy = transform * (5 + 0.5, 5 + 0.5)
        assert np.isclose(
            arr[5, 5], 2 * wx + 3 * wy + 1, atol=1e-6
        ), f"{name} dtmfromgeoms_agg diverged"


def test_rst_dtmfromgeoms_breakline_encodings(spark):
    # Breaklines are also user geometry input — accept all four encodings.
    from shapely import set_srid, to_wkb
    from shapely.geometry import LineString, Point

    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    pts = [
        to_wkb(Point(x, y, 2 * x + 3 * y + 1), output_dimension=3) for x, y in corners
    ]
    line = LineString([(2.0, 2.0, 11.0), (8.0, 8.0, 41.0)])
    bl_encodings = {
        "wkb": to_wkb(line, output_dimension=3),
        "ewkb": to_wkb(set_srid(line, 32633), output_dimension=3, include_srid=True),
        "wkt": line.wkt,
        "ewkt": f"SRID=32633;{line.wkt}",
    }
    results = {}
    for name, bl in bl_encodings.items():
        df = spark.createDataFrame([(pts, [bl])], ["pts", "bls"])
        tile = df.select(
            rx.rst_dtmfromgeoms(
                "pts", "bls", 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
            ).alias("t")
        ).first()["t"]
        assert tile is not None and tile["raster"] is not None, f"{name} null tile"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            results[name] = ds.read(1).tobytes()
    assert len(set(results.values())) == 1, "dtmfromgeoms breakline encodings diverged"


def test_h3_tessellate_mode_sql(spark):
    import pytest

    from databricks.labs.gbx import manager as gbx_manager

    df = _tile_df(spark, width=8, height=8, epsg=4326)
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    df.createOrReplaceTempView("_ras_tessellate_mode")
    # 2-arg (default assignment=centroid) — must still work.
    n_default = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4) t"
    ).count()
    # explicit covering — non-zero, may differ from default (centroid).
    n_cover = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'covering') t"
    ).count()
    # explicit centroid — same result as default.
    n_centroid = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'centroid') t"
    ).count()
    # Default is centroid: n_default == n_centroid; covering may differ.
    assert n_default == n_centroid and n_centroid > 0 and n_cover > 0
    # bad mode must raise.
    with pytest.raises(Exception):
        spark.sql(
            "SELECT t.* FROM _ras_tessellate_mode, "
            "LATERAL gbx_rst_h3_tessellate(tile, 4, 'bogus') t"
        ).count()


# ---------------------------------------------------------------------------
# Coverage/assignment on rastertogrid + tessellate surface
# ---------------------------------------------------------------------------


def test_light_rastertogrid_coverage_assignment_4arg_sql(spark):
    """4-arg rastertogrid SQL call: (tile, resolution, coverage, assignment).

    complete+centroid at fine resolution (7): the covering set has more cells
    than the 12 pixel-centroids, so complete mode emits NULL-measure rows for
    covered-but-empty cells.
    """
    from pyspark.sql.types import DoubleType

    from databricks.labs.gbx import manager as gbx_manager

    # 4x3 pixels at 0.5 deg each; at H3 res=7 (edge ~1.2 km) the covering set
    # has many more cells than the 12 pixel centroids -> NULL-measure rows appear.
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_t8_cov")
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    rows = spark.sql(
        "SELECT t.* FROM _ras_t8_cov, "
        "LATERAL gbx_rst_h3_rastertogridavg(tile, 7, 'complete', 'centroid') t"
    ).collect()
    # complete mode must emit covered-but-empty cells (NULL measure).
    assert any(
        r["measure"] is None for r in rows
    ), "complete coverage must yield at least one NULL-measure row"
    # measure column must be DoubleType even under complete mode.
    schema = spark.sql(
        "SELECT t.* FROM _ras_t8_cov, "
        "LATERAL gbx_rst_h3_rastertogridavg(tile, 7, 'complete', 'centroid') t"
    ).schema
    measure_field = next(fld for fld in schema.fields if fld.name == "measure")
    assert isinstance(
        measure_field.dataType, DoubleType
    ), f"measure must be DoubleType; got {measure_field.dataType}"


def test_light_rastertogrid_count_schema_is_double(spark):
    """The rastertogridcount measure column must be DoubleType (not Int/Long)."""
    from pyspark.sql.types import DoubleType

    from databricks.labs.gbx import manager as gbx_manager

    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_t8_cnt_schema")
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    for name, res in [
        ("gbx_rst_h3_rastertogridcount", 6),
        ("gbx_rst_quadbin_rastertogridcount", 10),
        ("gbx_rst_bng_rastertogridcount", 3),
    ]:
        schema = spark.sql(
            f"SELECT t.* FROM _ras_t8_cnt_schema, LATERAL {name}(tile, {res}) t"
        ).schema
        measure_field = next(fld for fld in schema.fields if fld.name == "measure")
        assert isinstance(
            measure_field.dataType, DoubleType
        ), f"{name}: measure must be DoubleType; got {measure_field.dataType}"


def test_light_tessellate_assignment_coverage_4arg_sql(spark):
    """4-arg tessellate SQL: (tile, resolution, assignment, coverage).

    assignment=centroid, coverage=complete should return rows (non-zero).
    """
    from databricks.labs.gbx import manager as gbx_manager

    df = _tile_df(spark, width=8, height=8, epsg=4326)
    df.createOrReplaceTempView("_ras_t8_tess")
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    rows = spark.sql(
        "SELECT t.* FROM _ras_t8_tess, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'centroid', 'complete') t"
    ).collect()
    assert len(rows) > 0, "centroid+complete tessellate must emit rows"


def test_light_tessellate_assignment_coverage_sparse(spark):
    """assignment=covering, coverage=sparse should return rows (non-zero)."""
    from databricks.labs.gbx import manager as gbx_manager

    df = _tile_df(spark, width=8, height=8, epsg=4326)
    df.createOrReplaceTempView("_ras_t8_tess_sparse")
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    rows = spark.sql(
        "SELECT t.* FROM _ras_t8_tess_sparse, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'covering', 'sparse') t"
    ).collect()
    assert len(rows) > 0, "covering+sparse tessellate must emit rows"
