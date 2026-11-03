"""Tests for explicit SQL registration of gbx_rst_* via prx.register(spark).

register(spark) should install the same SQL names the heavyweight rasterx
package uses, but bound to the pyspark/rasterio UDFs — so
``spark.sql("SELECT gbx_rst_width(tile) ...")`` works identically whether
the user registered the heavyweight (Scala) or lightweight (pyrx) package.
"""

from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.rasterx import functions as rx

from .conftest import make_geotiff_bytes


def _tile_view(spark, name="t", **kw):
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    df = df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    df.createOrReplaceTempView(name)


def test_register_enables_polygonize_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # polygonize is a LATERAL UDTF; count rows it yields.
    n = spark.sql(
        "SELECT COUNT(*) AS n FROM t, LATERAL gbx_rst_polygonize(tile, 1, 4) p"
    ).first()["n"]
    assert n >= 1


def _rgb_tile_view(spark, name="t"):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.03125, 0.03125),
    )
    data = (np.arange(64 * 64) % 256).astype("uint8").reshape(64, 64)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for b in range(1, 4):
                dst.write(data, b)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView(name)


def test_register_enables_tilexyz_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    # SQL callers must pass all args (no Python defaults in SQL UDFs).
    png = spark.sql(
        "SELECT gbx_rst_tilexyz(tile, 5, 16, 10, 'PNG', 256, 'bilinear') AS b FROM t"
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_register_enables_xyzpyramid_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    # xyzpyramid is a streaming UDTF; count the rows it yields.
    n = spark.sql(
        "SELECT count(*) AS n FROM t, "
        "LATERAL gbx_rst_xyzpyramid(tile, 1, 2, 'PNG', 64, 'bilinear') p"
    ).first()["n"]
    assert n >= 1


def test_register_enables_xyzpyramid_minimal_args_sql(spark):
    # Regression: before the fix, eval(self, tile, min_z, max_z, format, size,
    # resampling) required all 6 args — the 3-arg SQL call raised
    # UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE.
    # Now format/size/resampling default to PNG/256/bilinear in eval(), matching
    # the documented signature: gbx_rst_xyzpyramid(tile, min_z, max_z [,...]).
    prx.register(spark)
    _rgb_tile_view(spark)
    n_minimal = spark.sql(
        "SELECT count(*) AS n FROM t, " "LATERAL gbx_rst_xyzpyramid(tile, 1, 2) p"
    ).first()["n"]
    assert n_minimal > 0
    # Same tile-count as the explicit-format call — defaults match.
    n_explicit = spark.sql(
        "SELECT count(*) AS n FROM t, "
        "LATERAL gbx_rst_xyzpyramid(tile, 1, 2, 'PNG', 256, 'bilinear') p"
    ).first()["n"]
    assert n_minimal == n_explicit


# --- Operations via SQL -----------------------------------------------------


def test_register_enables_buildoverviews_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=64, height=64)
    # SQL callers pass all args (no Python defaults in SQL UDFs).
    n = spark.sql(
        "SELECT gbx_rst_numbands(gbx_rst_buildoverviews(tile, array(2, 4), 'average')) "
        "AS n FROM t"
    ).first()["n"]
    assert n == 1


# --- Group 1: statistics & accessors via SQL --------------------------------


# --- Grouped aggregators via SQL GROUP BY ---------------------------
def _agg_ras_bytes(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

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


def test_register_enables_merge_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    left = _agg_ras_bytes(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0)
    right = _agg_ras_bytes(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0)
    df = spark.createDataFrame([("g", left), ("g", right)], ["k", "raster"]).select(
        "k", rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    # Grouped-agg returns BINARY in SQL; accepts the tile STRUCT column directly.
    raster = spark.sql("SELECT gbx_rst_merge_agg(tile) AS b FROM v GROUP BY k").first()[
        "b"
    ]
    assert raster is not None and len(bytes(raster)) > 0
    # Wrap with gbx_rst_fromcontent to recover a tile struct.
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_fromcontent(gbx_rst_merge_agg(tile), 'GTiff')) "
        "AS w FROM v GROUP BY k"
    ).first()["w"]
    assert w == 4


def test_register_enables_combineavg_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    a = _agg_ras_bytes(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _agg_ras_bytes(np.array([[4.0, 8.0], [10.0, 12.0]]))
    df = spark.createDataFrame([("g", a), ("g", b)], ["k", "raster"]).select(
        "k", rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    avg = spark.sql(
        "SELECT gbx_rst_avg(gbx_rst_fromcontent(gbx_rst_combineavg_agg(tile), 'GTiff')) "
        "AS a FROM v GROUP BY k"
    ).first()["a"]
    # mean of all pixels [[3,6],[8,10]] = 6.75
    assert avg[0] == 6.75


def test_register_enables_frombands_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    b0 = _agg_ras_bytes(np.full((2, 2), 10.0))
    b1 = _agg_ras_bytes(np.full((2, 2), 20.0))
    df = spark.createDataFrame(
        [("g", b1, 1), ("g", b0, 0)], ["k", "raster", "band_index"]
    ).select(
        "k",
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
        "band_index",
    )
    df.createOrReplaceTempView("v")
    n = spark.sql(
        "SELECT gbx_rst_numbands("
        "gbx_rst_fromcontent(gbx_rst_frombands_agg(tile, band_index), 'GTiff')) "
        "AS n FROM v GROUP BY k"
    ).first()["n"]
    assert n == 2


def test_register_enables_rasterize_agg_sql(spark):
    import shapely.wkb
    from shapely.geometry import box

    prx.register(spark)
    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))
    spark.createDataFrame(
        [("g", g1, 1.0), ("g", g2, 2.0)], ["k", "geom", "val"]
    ).createOrReplaceTempView("v")
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_fromcontent("
        "gbx_rst_rasterize_agg(geom, val, 0, 0, 4, 4, 4, 4, 32633), 'GTiff')) "
        "AS w FROM v GROUP BY k"
    ).first()["w"]
    assert w == 4


def test_register_enables_derivedband_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    pyfunc = (
        "def addbands(in_ar, out_ar, *args, **kwargs):\n"
        "    import numpy as np\n"
        "    out_ar[:] = np.sum(in_ar, axis=0)\n"
    )
    a = _agg_ras_bytes(np.full((2, 2), 3.0))
    b = _agg_ras_bytes(np.full((2, 2), 4.0))
    df = spark.createDataFrame([("g", a), ("g", b)], ["k", "raster"]).select(
        "k", rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    mx = spark.sql(
        "SELECT gbx_rst_max(gbx_rst_fromcontent("
        f"gbx_rst_derivedband_agg(tile, '{pyfunc}', 'addbands'), 'GTiff')) "
        "AS m FROM v GROUP BY k"
    ).first()["m"]
    assert mx[0] == 7.0


def test_register_enables_gridfrompoints_agg_sql(spark):
    import shapely.wkb
    from shapely.geometry import Point

    prx.register(spark)
    p0 = shapely.wkb.dumps(Point(0.0, 0.0))
    p1 = shapely.wkb.dumps(Point(2.0, 2.0))
    spark.createDataFrame(
        [("g", p0, 10.0), ("g", p1, 30.0)], ["k", "pt", "v"]
    ).createOrReplaceTempView("v")
    val = spark.sql(
        "SELECT gbx_rst_avg(gbx_rst_fromcontent("
        "gbx_rst_gridfrompoints_agg(pt, v, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, 2.0, 2), "
        "'GTiff')) AS a FROM v GROUP BY k"
    ).first()["a"]
    assert val[0] == 20.0


def test_register_enables_dtmfromgeoms_agg_sql(spark):
    import shapely.wkb
    from shapely.geometry import Point

    prx.register(spark)
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    rows = [
        ("g", shapely.wkb.dumps(Point(x, y, 2 * x + 3 * y + 1), output_dimension=3))
        for x, y in corners
    ]
    spark.createDataFrame(rows, ["k", "pt"]).createOrReplaceTempView("v")
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_fromcontent("
        "gbx_rst_dtmfromgeoms_agg(pt, NULL, 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, "
        "32633), 'GTiff')) AS w FROM v GROUP BY k"
    ).first()["w"]
    assert w == 10


# ---------------------------------------------------------------------------
# Heavyweight-only non-aggregator constructors / array operations via SQL.
# Arrays are built with the SQL array(...) function over tile columns.
# ---------------------------------------------------------------------------
def _two_tile_view(spark, raster_a, raster_b, name="ab"):
    df = spark.createDataFrame([(raster_a, raster_b)], ["a", "b"]).select(
        rx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        rx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    df.createOrReplaceTempView(name)


def test_register_enables_merge_sql(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    def _ras(arr, ulx, uly):
        arr = np.asarray(arr, dtype="float32")
        profile = dict(
            driver="GTiff",
            width=arr.shape[1],
            height=arr.shape[0],
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(ulx, uly, 1.0, 1.0),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(arr, 1)
            return mf.read()

    prx.register(spark)
    _two_tile_view(
        spark, _ras([[1, 2], [3, 4]], 0.0, 2.0), _ras([[5, 6], [7, 8]], 2.0, 2.0)
    )
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_merge(array(ta, tb))) AS w FROM ab"
    ).first()["w"]
    assert w == 4


def test_register_enables_combineavg_sql(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    def _ras(arr):
        arr = np.asarray(arr, dtype="float32")
        profile = dict(
            driver="GTiff",
            width=arr.shape[1],
            height=arr.shape[0],
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(0.0, 2.0, 1.0, 1.0),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(arr, 1)
            return mf.read()

    prx.register(spark)
    _two_tile_view(
        spark, _ras([[2.0, 4.0], [6.0, 8.0]]), _ras([[4.0, 8.0], [10.0, 12.0]])
    )
    avg = spark.sql(
        "SELECT gbx_rst_avg(gbx_rst_combineavg(array(ta, tb))) AS a FROM ab"
    ).first()["a"]
    # Mean of all four means: (3+6+8+10)/4 = 6.75.
    assert avg[0] == 6.75


def test_register_enables_frombands_sql(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    def _ras(v):
        arr = np.full((2, 2), float(v), dtype="float32")
        profile = dict(
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(0.0, 2.0, 1.0, 1.0),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(arr, 1)
            return mf.read()

    prx.register(spark)
    _two_tile_view(spark, _ras(10.0), _ras(20.0))
    n = spark.sql(
        "SELECT gbx_rst_numbands(gbx_rst_frombands(array(ta, tb))) AS n FROM ab"
    ).first()["n"]
    assert n == 2


def test_register_enables_h3_tessellate_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=8, height=8, epsg=4326)
    # h3_tessellate is a streaming UDTF; count the cells it yields.
    n = spark.sql(
        "SELECT count(*) AS n FROM t, LATERAL gbx_rst_h3_tessellate(tile, 4) cell"
    ).first()["n"]
    assert n > 0


def test_register_enables_proximity_sql(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    prx.register(spark)
    data = np.zeros((5, 5), dtype="float32")
    data[0, 0] = 1.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 5, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        raster = mf.read()
    df = spark.createDataFrame([(raster,)], ["raster"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("p")
    # Compose: proximity then read its type in SQL.
    ty = spark.sql(
        "SELECT gbx_rst_type(gbx_rst_proximity(tile, NULL, 'PIXEL', NULL))[0] AS t FROM p"
    ).first()["t"]
    assert ty == "Float32"


def test_register_enables_cog_convert_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=64, height=64, epsg=4326)
    n = spark.sql(
        "SELECT gbx_rst_numbands(gbx_rst_cog_convert(tile, 'DEFLATE', 512, 'AVERAGE')) AS n FROM t"
    ).first()["n"]
    assert n == 1


def test_register_enables_contour_sql(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    prx.register(spark)
    band = np.tile(np.arange(6, dtype="float64"), (5, 1))
    profile = dict(
        driver="GTiff",
        width=6,
        height=5,
        count=1,
        dtype="float64",
        crs="EPSG:32633",
        transform=from_origin(100.0, 50.0, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(band, 1)
        raster = mf.read()
    df = spark.createDataFrame([(raster,)], ["raster"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("c")
    # Explode the ARRAY<struct(geom_wkb, value)> and count contours at level 2.5.
    # SQL-registered UDFs take fixed arity (no Python defaults): pass all 5 args.
    n = spark.sql(
        "SELECT count(*) AS n FROM "
        "(SELECT explode("
        "gbx_rst_contour(tile, array(cast(2.5 as double)), 0.0, 0.0, 'elev')"
        ") AS c FROM c)"
    ).first()["n"]
    assert n >= 1


def test_register_enables_viewshed_sql(spark):
    import numpy as np
    import shapely.wkb
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from shapely.geometry import Point

    prx.register(spark)
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
        raster = mf.read()
    obs = shapely.wkb.dumps(Point(0.5, 3.5))
    df = spark.createDataFrame([(raster, obs)], ["raster", "g"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"), f.col("g")
    )
    df.createOrReplaceTempView("v")
    ty = spark.sql(
        "SELECT gbx_rst_type(gbx_rst_viewshed(tile, g, 1.0, 0.0, NULL))[0] AS t FROM v"
    ).first()["t"]
    assert ty == "Byte"
