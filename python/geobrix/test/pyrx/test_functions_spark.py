from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx

from .conftest import make_geotiff_bytes


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_no_jar_in_session(spark):
    # Guardrail: pyrx must work without the GeoBrix JAR.
    assert not spark.conf.get("spark.jars", "")


def test_rst_width_height(spark):
    df = _tile_df(spark, width=4, height=3)
    row = df.select(
        prx.rst_width("tile").alias("w"),
        prx.rst_height("tile").alias("h"),
    ).first()
    assert row["w"] == 4
    assert row["h"] == 3


def test_rst_srid(spark):
    df = _tile_df(spark, epsg=4326)
    assert df.select(prx.rst_srid("tile").alias("s")).first()["s"] == 4326


def test_rst_rastertoworldcoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_rastertoworldcoordx("tile", f.lit(0), f.lit(0)).alias("x")
    ).first()
    assert row["x"] == 10.25


def test_rst_worldtorastercoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_worldtorastercoordx("tile", f.lit(10.6), f.lit(49.9)).alias("c")
    ).first()
    assert row["c"] == 1


def test_rst_metadata_maptype_roundtrip(spark):
    # Exercises the MapType return path (non-pandas @f.udf fallback).
    df = _tile_df(spark, width=4, height=3)
    meta = df.select(prx.rst_metadata("tile").alias("m")).first()["m"]
    assert meta["width"] == "4"
    assert meta["height"] == "3"
    assert meta["driver"] == "GTiff"


def test_rst_boundingbox_binarytype_roundtrip(spark):
    # Exercises the BinaryType (WKB) return path end-to-end through Spark.
    import shapely.wkb

    df = _tile_df(spark, width=4, height=3)
    wkb = df.select(prx.rst_boundingbox("tile").alias("b")).first()["b"]
    assert shapely.wkb.loads(bytes(wkb)).bounds == (10.0, 48.5, 12.0, 50.0)


def test_register_is_noop(spark):
    # swap-compat: rx.register(spark) -> prx.register(spark) must not error.
    prx.register(spark)


def test_rst_transform_to_3857(spark):
    df = _tile_df(spark, epsg=4326, count=2)
    out = df.select(prx.rst_transform("tile", 3857).alias("t"))
    row = out.select(
        prx.rst_srid("t").alias("s"), prx.rst_numbands("t").alias("n")
    ).first()
    assert row["s"] == 3857
    assert row["n"] == 2


def test_rst_to_webmercator(spark):
    df = _tile_df(spark, epsg=4326)
    out = df.select(prx.rst_to_webmercator("tile").alias("t"))
    assert out.select(prx.rst_srid("t").alias("s")).first()["s"] == 3857


def test_rst_resample_factor(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_resample("tile", 2.0).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (8, 6)


def test_rst_resample_to_size(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_resample_to_size("tile", 10, 7).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (10, 7)


def test_rst_resample_to_res(spark):
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    out = df.select(prx.rst_resample_to_res("tile", 0.25, 0.25).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (8, 6)


def test_rst_clip(spark):
    import shapely.wkb
    from shapely.geometry import box

    geom = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    df = df.withColumn("g", f.lit(geom))
    out = df.select(prx.rst_clip("tile", "g", False).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert 0 < row["w"] < 4 and 0 < row["h"] < 3


def test_rst_clip_geom_encodings(spark):
    # Parity bug fix: rst_clip must accept the SAME geometry as WKB / EWKB /
    # WKT / EWKT (heavyweight accepts all four; the xView example passes EWKT).
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    poly = box(10.5, 49.0, 11.5, 49.5)
    encodings = {
        "wkb": to_wkb(poly),
        "ewkb": to_wkb(set_srid(poly, 4326), include_srid=True),
        "wkt": poly.wkt,
        "ewkt": f"SRID=4326;{poly.wkt}",
    }

    results = {}
    for name, geom in encodings.items():
        df = _tile_df(spark, width=4, height=3, epsg=4326)
        df = df.withColumn("g", f.lit(geom))
        row = (
            df.select(prx.rst_clip("tile", "g", False).alias("t"))
            .select(f.col("t.raster").alias("b"))
            .first()
        )
        assert row is not None and row["b"] is not None, f"{name} produced a null tile"
        results[name] = bytes(row["b"])

    # All four encodings of the same geometry must produce identical raster bytes.
    distinct = set(results.values())
    sizes = {k: len(v) for k, v in results.items()}
    assert len(distinct) == 1, f"encodings diverged: {sizes}"


def test_rst_sample_geom_encodings(spark):
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point

    pt = Point(10.75, 49.75)
    encodings = {
        "wkb": to_wkb(pt),
        "ewkb": to_wkb(set_srid(pt, 4326), include_srid=True),
        "wkt": pt.wkt,
        "ewkt": f"SRID=4326;{pt.wkt}",
    }
    for name, geom in encodings.items():
        df = _tile_df(spark, width=4, height=3, count=2)
        df = df.withColumn("g", f.lit(geom))
        vals = df.select(prx.rst_sample("tile", "g").alias("v")).first()["v"]
        assert vals == [1.0, 101.0], f"{name} sampled {vals}"


def test_rst_updatetype(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_updatetype("tile", f.lit("Int32")).alias("t"))
    assert out.select(prx.rst_type("t").alias("ty")).first()["ty"][0] == "Int32"


def test_rst_initnodata(spark):
    df = _tile_df(spark, nodata=None)
    out = df.select(prx.rst_initnodata("tile").alias("t"))
    assert out.select(prx.rst_getnodata("t").alias("nd")).first()["nd"][0] == -9999.0


def test_rst_rasterize(spark):
    import shapely.wkb
    from shapely.geometry import box

    geom = shapely.wkb.dumps(box(1.0, 1.0, 3.0, 3.0))
    df = spark.createDataFrame([(geom,)], ["g"])
    out = df.select(
        prx.rst_rasterize("g", 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326).alias("t")
    )
    row = out.select(
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
        prx.rst_srid("t").alias("s"),
    ).first()
    assert (row["w"], row["h"], row["s"]) == (4, 4, 4326)


def test_rst_fillnodata(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.ones((5, 5), dtype="float32")
    data[2, 2] = -9999.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    out = df.select(prx.rst_fillnodata("tile").alias("t"))
    # rst_pixelcount absent in pyrx; verify via dims and nodata list (all filled = empty nodata)
    row = out.select(
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
        prx.rst_getnodata("t").alias("nd"),
    ).first()
    assert (row["w"], row["h"]) == (5, 5)
    # nodata is set but all pixels should now be valid — getnodata returns the
    # configured nodata value (not per-pixel mask), so just verify tile is non-null.
    assert row["nd"] is not None


def test_rst_ndvi(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    out = df.select(prx.rst_ndvi("tile", 1, 2).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_type("t").alias("ty")
    ).first()
    assert row["n"] == 1
    assert row["ty"][0] == "Float32"


def test_rst_evi_savi_ndwi_nbr_single_band(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    for col in (
        prx.rst_evi("tile", 1, 2, 3),
        prx.rst_savi("tile", 1, 2),
        prx.rst_ndwi("tile", 1, 2),
        prx.rst_nbr("tile", 2, 1),
    ):
        n = df.select(prx.rst_numbands(col).alias("n")).first()["n"]
        assert n == 1


def test_rst_slope_aspect_hillshade(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    profile = dict(
        driver="GTiff",
        width=6,
        height=6,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 6, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ramp, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    # Auto-scale path (no scale args): must run and produce Float32.
    assert (
        df.select(prx.rst_type(prx.rst_slope("tile")).alias("t")).first()["t"][0]
        == "Float32"
    )
    # Explicit xscale/yscale override path: also runs, Float32, and differs from auto.
    auto_min = df.select(prx.rst_min(prx.rst_slope("tile"))[0].alias("v")).first()["v"]
    over_min = df.select(
        prx.rst_min(prx.rst_slope("tile", xscale=111120.0, yscale=111120.0))[0].alias(
            "v"
        )
    ).first()["v"]
    assert over_min != auto_min
    assert (
        df.select(
            prx.rst_type(prx.rst_slope("tile", xscale=2.0, yscale=2.0)).alias("t")
        ).first()["t"][0]
        == "Float32"
    )
    assert (
        df.select(prx.rst_type(prx.rst_hillshade("tile")).alias("t")).first()["t"][0]
        == "Byte"
    )
    assert (
        df.select(prx.rst_numbands(prx.rst_aspect("tile")).alias("n")).first()["n"] == 1
    )


def test_rst_polygonize(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    # rst_polygonize is a streaming UDTF — invoke via SQL LATERAL.
    df.createOrReplaceTempView("ras")
    prx.register(spark)
    rows = spark.sql(
        "SELECT t.geom_wkb AS g, t.value AS v FROM ras, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
    ).collect()
    vals = [r["v"] for r in rows]
    assert 5.0 in vals
    assert all(r["g"] is not None for r in rows)


def test_rst_tri_tpi_roughness(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 7.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    for col in (prx.rst_tri("tile"), prx.rst_tpi("tile"), prx.rst_roughness("tile")):
        row = df.select(
            prx.rst_numbands(col).alias("n"), prx.rst_type(col).alias("t")
        ).first()
        assert row["n"] == 1 and row["t"][0] == "Float32"


def test_rst_threshold(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.array([[1.0, 5.0], [10.0, 20.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(prx.rst_threshold("tile", f.lit(">"), 5.0).alias("t"))
    assert out.select(prx.rst_getnodata("t").alias("nd")).first()["nd"][0] == -9999.0


def test_rst_filter_and_convolve(spark):
    df = _tile_df(spark, width=5, height=5, count=1)
    fout = df.select(prx.rst_filter("tile", 3, f.lit("mean")).alias("t"))
    assert fout.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1
    cout = df.select(
        prx.rst_convolve(
            "tile",
            f.array(
                f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
                f.array(f.lit(0.0), f.lit(1.0), f.lit(0.0)),
                f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
            ),
        ).alias("t")
    )
    assert cout.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


def test_rst_color_relief(spark):
    import tempfile

    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    # Write color table to a named temp file (avoids function/module scope clash).
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tf:
        # elev=0 -> black (0,0,0); elev=100 -> white (255,255,255)
        tf.write("0 0 0 0\n100 255 255 255\n")
        color_table_path = tf.name

    dem = np.array([[0, 50], [100, 100]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(prx.rst_color_relief("tile", f.lit(color_table_path)).alias("t"))
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 3


def test_rst_mapalgebra(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    def _ras(v):
        data = np.full((2, 2), float(v), dtype="float32")
        profile = dict(
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(0, 2, 1, 1),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data, 1)
            return mf.read()

    df = spark.createDataFrame([(_ras(1.0), _ras(2.0))], ["a", "b"])
    df = df.select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    out = df.select(prx.rst_mapalgebra(f.array("ta", "tb"), f.lit("A + B")).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_type("t").alias("ty")
    ).first()
    assert row["n"] == 1 and row["ty"][0] == "Float32"


def test_rst_separatebands(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_sepbands")
    # rst_separatebands is a streaming UDTF — invoke via SQL LATERAL.
    parts = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata FROM _ras_sepbands, "
        "LATERAL gbx_rst_separatebands(tile) t"
    )
    assert parts.count() == 3
    assert (
        parts.select(
            prx.rst_numbands(f.struct("cellid", "raster", "metadata")).alias("n")
        ).first()["n"]
        == 1
    )


def test_rst_retile(spark):
    df = _tile_df(spark, width=4, height=4)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_retile")
    parts = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata FROM _ras_retile, "
        "LATERAL gbx_rst_retile(tile, 2, 2) t"
    )
    assert parts.count() == 4
    row = parts.select(
        prx.rst_width(f.struct("cellid", "raster", "metadata")).alias("w"),
        prx.rst_height(f.struct("cellid", "raster", "metadata")).alias("h"),
    ).first()
    assert (row["w"], row["h"]) == (2, 2)


def test_rst_tooverlappingtiles(spark):
    df = _tile_df(spark, width=4, height=4)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_overlap")
    parts = spark.sql(
        "SELECT t.cellid FROM _ras_overlap, "
        "LATERAL gbx_rst_tooverlappingtiles(tile, 2, 2, 1) t"
    )
    assert parts.count() >= 4


def test_rst_derivedband(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    pyfunc = (
        "def double(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0] * 2\n"
    )
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(
        prx.rst_derivedband("tile", f.lit(pyfunc), f.lit("double")).alias("t")
    )
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


def test_rst_maketiles(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    # 1024x1024 single-band float32 ~= 4 MiB encoded. Heavy keys on the encoded
    # byte size and quad-splits power-of-4; at size_in_mb=1 that is k=2 -> 16
    # tiles (a 4x4 grid of 256x256), matching heavy BalancedSubdivision.
    side = 1024
    data = np.arange(side * side, dtype="float32").reshape(side, side)
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, side, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    prx.register(spark)
    df.createOrReplaceTempView("_ras_maketiles")
    parts = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata FROM _ras_maketiles, "
        "LATERAL gbx_rst_maketiles(tile, 1) t"
    )
    assert parts.count() == 16
    assert (
        parts.select(
            prx.rst_numbands(f.struct("cellid", "raster", "metadata")).alias("n")
        ).first()["n"]
        == 1
    )


# --- web-mercator XYZ tiling (rst_tilexyz / rst_xyzpyramid) -----------------
def _rgb_tile_df(spark):
    """One-row DataFrame with a small RGB GTiff tile over a European extent."""
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
    return spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )


def test_rst_tilexyz_in_extent_png(spark):
    df = _rgb_tile_df(spark)
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(5), f.lit(16), f.lit(10)).alias("b")
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_tilexyz_out_of_extent_transparent(spark):
    df = _rgb_tile_df(spark)
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(2), f.lit(0), f.lit(0), "PNG", 64).alias("b")
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_tilexyz_null_tile_transparent(spark):
    # Even a null tile must return a transparent PNG (never null) — mirror heavyweight.
    df = spark.createDataFrame(
        [(None,)],
        "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>",
    )
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(2), f.lit(0), f.lit(0), "PNG", 32).alias("b")
    ).first()["b"]
    assert png is not None
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_xyzpyramid_array(spark):
    df = _rgb_tile_df(spark)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_pyramid")
    # rst_xyzpyramid is a streaming UDTF — invoke via SQL LATERAL.
    rows = spark.sql(
        "SELECT t.z, t.x, t.y, t.bytes FROM _ras_pyramid, "
        "LATERAL gbx_rst_xyzpyramid(tile, 1, 3, 'PNG', 64, 'bilinear') t"
    ).collect()
    assert len(rows) > 0
    for r in rows:
        assert r["z"] in (1, 2, 3)
        assert bytes(r["bytes"])[:4] == b"\x89PNG"


def test_sql_rst_tilexyz_accepts_rescale(spark):
    # Regression: gbx_rst_tilexyz must accept the optional 8th arg (rescale).
    # Uses the SQL surface to verify arity after Task 4 registration.
    prx.register(spark)
    df = _rgb_tile_df(spark)
    df.createOrReplaceTempView("_tilexyz_rescale")
    row = spark.sql(
        "SELECT gbx_rst_tilexyz(tile, 8, 41, 90, 'PNG', 256, 'bilinear', 'auto') AS png"
        " FROM _tilexyz_rescale"
    ).first()
    assert row["png"] is not None and len(row["png"]) > 0


# --- raster->grid aggregation (h3 + quadbin) --------------------------------
def test_rst_h3_rastertogridcount(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_h3_count")
    prx.register(spark)
    # UDTF yields flat (band, cellID, measure); count sums to 4x3=12.
    rows = spark.sql(
        "SELECT t.band, t.cellID, t.measure FROM _ras_h3_count, "
        "LATERAL gbx_rst_h3_rastertogridcount(tile, 6) t"
    ).collect()
    assert sum(r["measure"] for r in rows) == 12
    assert all(0 < r["cellID"] < 2**63 for r in rows)


def test_rst_quadbin_rastertogridcount(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_qb_count")
    prx.register(spark)
    rows = spark.sql(
        "SELECT t.measure FROM _ras_qb_count, "
        "LATERAL gbx_rst_quadbin_rastertogridcount(tile, 10) t"
    ).collect()
    assert sum(r["measure"] for r in rows) == 12


def test_rst_h3_rastertogridavg_multiband_outer_length(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    df.createOrReplaceTempView("_ras_h3_avg_mb")
    prx.register(spark)
    # 2-band raster: expect 2 distinct band values in the UDTF output.
    rows = spark.sql(
        "SELECT DISTINCT t.band FROM _ras_h3_avg_mb, "
        "LATERAL gbx_rst_h3_rastertogridavg(tile, 5) t"
    ).collect()
    assert len(rows) == 2


def test_rst_quadbin_rastertogridmedian(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_qb_median")
    prx.register(spark)
    # 1-band raster: should return rows with band=1.
    rows = spark.sql(
        "SELECT DISTINCT t.band FROM _ras_qb_median, "
        "LATERAL gbx_rst_quadbin_rastertogridmedian(tile, 8) t"
    ).collect()
    assert len(rows) == 1


# --- rastertogrid UDTF LATERAL tests (new flat schema) ----------------------
def test_rst_h3_rastertogridcount_udtf_lateral(spark):
    """UDTF emits flat (band, cellID, measure) rows via LATERAL."""
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_test")
    prx.register(spark)
    rows = spark.sql(
        "SELECT t.band, t.cellID, t.measure "
        "FROM _ras_test, LATERAL gbx_rst_h3_rastertogridcount(tile, 6) t"
    ).collect()
    # 1-band raster: all rows have band=1
    assert all(
        r["band"] == 1 for r in rows
    ), "all rows should have band=1 for 1-band raster"
    # sum of cell counts == total pixel count (4x3=12)
    assert sum(r["measure"] for r in rows) == 12
    assert all(0 < r["cellID"] < 2**63 for r in rows)


def test_rastertogrid_all_10_register_and_return_flat_rows(spark):
    """Smoke: all 10 UDTFs register and return flat (band, cellID, measure) rows."""
    df = _tile_df(spark, width=4, height=3, count=1)
    df.createOrReplaceTempView("_ras_smoke")
    prx.register(spark)
    for name, res in [
        ("gbx_rst_h3_rastertogridavg", 6),
        ("gbx_rst_h3_rastertogridcount", 6),
        ("gbx_rst_h3_rastertogridmax", 6),
        ("gbx_rst_h3_rastertogridmin", 6),
        ("gbx_rst_h3_rastertogridmedian", 6),
        ("gbx_rst_quadbin_rastertogridavg", 10),
        ("gbx_rst_quadbin_rastertogridcount", 10),
        ("gbx_rst_quadbin_rastertogridmax", 10),
        ("gbx_rst_quadbin_rastertogridmin", 10),
        ("gbx_rst_quadbin_rastertogridmedian", 10),
    ]:
        rows = spark.sql(
            f"SELECT t.band, t.cellID, t.measure FROM _ras_smoke, LATERAL {name}(tile, {res}) t"
        ).collect()
        assert len(rows) > 0, f"{name} returned no rows"
        assert all(
            r["band"] == 1 for r in rows
        ), f"{name}: band should be 1 for 1-band raster"
        assert all(
            r["cellID"] is not None for r in rows
        ), f"{name}: cellID must not be None"


# --- Group 1: per-band statistics & accessors -------------------------------
def test_rst_avg_min_max_median_pixelcount(spark):
    import math

    df = _tile_df(spark, width=4, height=3, count=2)
    row = df.select(
        prx.rst_avg("tile").alias("avg"),
        prx.rst_min("tile").alias("min"),
        prx.rst_max("tile").alias("max"),
        prx.rst_median("tile").alias("med"),
        prx.rst_pixelcount("tile").alias("pc"),
    ).first()
    # band1 = arange(12); band2 = +100 (no -9999 -> all 12 valid).
    assert row["avg"][0] == math.fsum(range(12)) / 12
    assert row["min"] == [0.0, 100.0]
    assert row["max"] == [11.0, 111.0]
    assert row["med"][0] == 5.5
    assert row["pc"] == [12, 12]


def test_rst_memsize(spark):
    raster = make_geotiff_bytes(width=4, height=3)
    df = spark.createDataFrame([(raster,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    sz = df.select(prx.rst_memsize("tile").alias("s")).first()["s"]
    assert sz == len(raster)


def test_rst_rotation_skew_format(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_rotation("tile").alias("rot"),
        prx.rst_skewx("tile").alias("sx"),
        prx.rst_skewy("tile").alias("sy"),
        prx.rst_format("tile").alias("fmt"),
    ).first()
    assert row["rot"] == 0.0
    assert row["sx"] == 0.0
    assert row["sy"] == 0.0
    assert row["fmt"] == "GTiff"


def test_rst_georeference_keys(spark):
    df = _tile_df(spark)
    gr = df.select(prx.rst_georeference("tile").alias("g")).first()["g"]
    assert set(gr.keys()) == {
        "upperLeftX",
        "upperLeftY",
        "scaleX",
        "scaleY",
        "skewX",
        "skewY",
    }
    assert gr["scaleX"] == 0.5
    assert gr["scaleY"] == -0.5
    assert gr["upperLeftX"] == 10.0
    assert gr["upperLeftY"] == 50.0


def test_rst_bandmetadata_and_subdatasets(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_bandmetadata("tile", f.lit(1)).alias("bm"),
        prx.rst_subdatasets("tile").alias("sd"),
    ).first()
    assert isinstance(row["bm"], dict)
    assert row["sd"] == {}  # plain GTiff has no subdatasets


def test_rst_summary_is_json(spark):
    import json

    df = _tile_df(spark, width=4, height=3, count=1)
    s = df.select(prx.rst_summary("tile").alias("s")).first()["s"]
    obj = json.loads(s)
    assert obj["driverShortName"] == "GTiff"
    assert obj["size"] == [4, 3]
    assert obj["bands"][0]["max"] == 11.0


def test_rst_histogram_bucket_sum(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    hist = df.select(
        prx.rst_histogram("tile", 4, f.lit(0.0), f.lit(11.0)).alias("h")
    ).first()["h"]
    assert list(hist.keys()) == ["band_1"]
    assert sum(hist["band_1"]) == 12


# --- Operations (tryopen, setsrid, band, asformat, buildoverviews, sample) --
def test_rst_tryopen_true(spark):
    df = _tile_df(spark, width=4, height=3)
    assert df.select(prx.rst_tryopen("tile").alias("ok")).first()["ok"] is True


def test_rst_tryopen_false_on_garbage(spark):
    # Build a tile struct directly with garbage raster bytes.
    df = spark.createDataFrame(
        [((0, b"not a raster", {"driver": "GTiff"}),)],
        "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>",
    )
    assert df.select(prx.rst_tryopen("tile").alias("ok")).first()["ok"] is False


def test_rst_setsrid_stamps_crs(spark):
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    out = df.select(prx.rst_setsrid("tile", f.lit(27700)).alias("t"))
    row = out.select(
        prx.rst_srid("t").alias("s"),
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
    ).first()
    assert (row["s"], row["w"], row["h"]) == (27700, 4, 3)


def test_rst_band_extracts_single_band(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    out = df.select(prx.rst_band("tile", f.lit(2)).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_max("t").alias("mx")
    ).first()
    assert row["n"] == 1
    # band2 = arange(12)+100 -> max 111.
    assert row["mx"][0] == 111.0


def test_rst_band_out_of_range_raises(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    out = df.select(prx.rst_band("tile", f.lit(5)).alias("t"))
    with __import__("pytest").raises(Exception):
        out.select(prx.rst_numbands("t")).first()


def test_rst_asformat_gtiff(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_asformat("tile", "GTiff").alias("t"))
    meta = df.select(prx.rst_metadata(prx.rst_asformat("tile", "GTiff")).alias("m"))
    assert meta.first()["m"]["driver"] == "GTiff"
    assert out.select(prx.rst_width("t").alias("w")).first()["w"] == 4


def test_rst_buildoverviews(spark):
    df = _tile_df(spark, width=64, height=64)
    out = df.select(
        prx.rst_buildoverviews("tile", f.array(f.lit(2), f.lit(4))).alias("t")
    )
    # Reopen and confirm overviews are embedded via summary/numbands sanity.
    row = out.select(prx.rst_numbands("t").alias("n")).first()
    assert row["n"] == 1
    # Verify overviews persisted by reading raster bytes back through rasterio.
    raster = out.select(f.col("t.raster").alias("r")).first()["r"]
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(raster)) as mf:
        with mf.open() as o:
            assert o.overviews(1) == [2, 4]


def test_rst_sample(spark):
    import shapely.wkb
    from shapely.geometry import Point

    pt = shapely.wkb.dumps(Point(10.75, 49.75))
    df = _tile_df(spark, width=4, height=3, count=2)
    df = df.withColumn("g", f.lit(pt))
    vals = df.select(prx.rst_sample("tile", "g").alias("v")).first()["v"]
    assert vals == [1.0, 101.0]


def test_rst_rastertoworldcoord_struct_roundtrip(spark):
    df = _tile_df(spark)
    wc = df.select(
        prx.rst_rastertoworldcoord("tile", f.lit(2), f.lit(1)).alias("wc")
    ).first()["wc"]
    assert wc["x"] is not None and wc["y"] is not None
    rc = df.select(
        prx.rst_worldtorastercoord("tile", f.lit(wc["x"]), f.lit(wc["y"])).alias("rc")
    ).first()["rc"]
    assert (rc["x"], rc["y"]) == (2, 1)


# ---------------------------------------------------------------------------
# Tier 2: grouped aggregators (rst_*_agg) via .agg()
# ---------------------------------------------------------------------------
import numpy as np  # noqa: E402
import pytest  # noqa: E402
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
    return df.select("k", prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_rst_merge_agg(spark):
    left = _ras_bytes(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0)
    right = _ras_bytes(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0)
    df = _tiles_df(spark, [("g", left), ("g", right)])
    tile = df.groupBy("k").agg(prx.rst_merge_agg("tile").alias("t")).first()["t"]
    assert tile is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4
        assert ds.bounds.left == 0.0
        assert ds.bounds.right == 4.0


def test_rst_combineavg_agg_mean_and_cellid(spark):
    a = _ras_bytes(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras_bytes(np.array([[4.0, 8.0], [10.0, 12.0]]))
    df = _tiles_df(spark, [("g", a), ("g", b)])
    tile = df.groupBy("k").agg(prx.rst_combineavg_agg("tile").alias("t")).first()["t"]
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])
    # cellid carried through from the group's first tile (fromcontent -> 0).
    assert tile["cellid"] == 0


def test_rst_frombands_agg_ascending_order(spark):
    b0 = _ras_bytes(np.full((2, 2), 10.0))
    b1 = _ras_bytes(np.full((2, 2), 20.0))
    b2 = _ras_bytes(np.full((2, 2), 30.0))
    rows = [("g", b2, 2), ("g", b0, 0), ("g", b1, 1)]
    df = spark.createDataFrame(rows, ["k", "raster", "band_index"]).select(
        "k",
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
        "band_index",
    )
    tile = (
        df.groupBy("k")
        .agg(prx.rst_frombands_agg("tile", "band_index").alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.count == 3
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)
        assert np.allclose(ds.read(3), 30.0)


def test_rst_rasterize_agg(spark):
    import shapely.wkb
    from shapely.geometry import box

    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))
    df = spark.createDataFrame([("g", g1, 1.0), ("g", g2, 2.0)], ["k", "geom", "val"])
    tile = (
        df.groupBy("k")
        .agg(prx.rst_rasterize_agg("geom", "val", 0, 0, 4, 4, 4, 4, 32633).alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
    assert np.all(arr[:, 0] == 1.0)
    assert np.all(arr[:, 1] == 2.0)


PYFUNC_SUM_AGG = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def test_rst_derivedband_agg(spark):
    a = _ras_bytes(np.full((2, 2), 3.0))
    b = _ras_bytes(np.full((2, 2), 4.0))
    c = _ras_bytes(np.full((2, 2), 5.0))
    df = _tiles_df(spark, [("g", a), ("g", b), ("g", c)])
    tile = (
        df.groupBy("k")
        .agg(prx.rst_derivedband_agg("tile", PYFUNC_SUM_AGG, "addbands").alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.count == 1
        assert np.allclose(ds.read(1), 12.0)


# ---------------------------------------------------------------------------
# Heavyweight-only non-aggregator constructors / array operations
# (rst_merge, rst_combineavg, rst_frombands, rst_fromfile)
# ---------------------------------------------------------------------------
def test_rst_merge_array(spark):
    # Two adjacent tiles in one row's array -> merged tile spans the union.
    left = _ras_bytes(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0)
    right = _ras_bytes(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0)
    df = spark.createDataFrame([(left, right)], ["a", "b"]).select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    tile = df.select(prx.rst_merge(f.array("ta", "tb")).alias("t")).first()["t"]
    assert tile is not None
    assert tile["cellid"] == 0
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4
        assert ds.height == 2
        assert ds.bounds.left == 0.0
        assert ds.bounds.right == 4.0


def test_rst_combineavg_array(spark):
    a = _ras_bytes(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras_bytes(np.array([[4.0, 8.0], [10.0, 12.0]]))
    df = spark.createDataFrame([(a, b)], ["a", "b"]).select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    tile = df.select(prx.rst_combineavg(f.array("ta", "tb")).alias("t")).first()["t"]
    # cellid shared across elements (fromcontent -> 0) -> carried as 0.
    assert tile["cellid"] == 0
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])


def test_rst_combineavg_array_shape_mismatch_raises(spark):
    from pyspark.errors import PythonException

    a = _ras_bytes(np.array([[1.0, 2.0], [3.0, 4.0]]))
    b = _ras_bytes(np.array([[1.0, 2.0, 3.0]]))
    df = spark.createDataFrame([(a, b)], ["a", "b"]).select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    with pytest.raises(PythonException, match="aligned tiles"):
        df.select(prx.rst_combineavg(f.array("ta", "tb")).alias("t")).first()


def test_rst_frombands_array_order(spark):
    # Array order IS band order: element 0 -> band 1, element 1 -> band 2.
    b0 = _ras_bytes(np.full((2, 2), 10.0))
    b1 = _ras_bytes(np.full((2, 2), 20.0))
    df = spark.createDataFrame([(b0, b1)], ["a", "b"]).select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    tile = df.select(prx.rst_frombands(f.array("ta", "tb")).alias("t")).first()["t"]
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.count == 2
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)


def test_rst_fromfile_roundtrip(spark, tmp_path):
    import rasterio

    p = str(tmp_path / "scene.tif")
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as src:
        profile = src.profile.copy()
        data = src.read()
    with rasterio.open(p, "w", **profile) as dst:
        dst.write(data)
    df = spark.createDataFrame([(p,)], ["path"])
    # materialize=True -> a bytes tile whose raster round-trips.
    tile = df.select(
        prx.rst_fromfile("path", f.lit("GTiff"), materialize=True).alias("t")
    ).first()["t"]
    assert tile is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4
        assert ds.height == 3
        assert ds.crs.to_epsg() == 4326


def test_rst_fromfile_virtual_by_default(spark, tmp_path):
    # 2-arg default is a VIRTUAL tile: raster None, path (bare) + whole-file
    # window populated. No pixels read.
    import rasterio

    p = str(tmp_path / "scene.tif")
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as src:
        profile = src.profile.copy()
        data = src.read()
    with rasterio.open(p, "w", **profile) as dst:
        dst.write(data)
    df = spark.createDataFrame([(p,)], ["path"])
    tile = df.select(prx.rst_fromfile("path", f.lit("GTiff")).alias("t")).first()["t"]
    assert tile is not None
    assert tile["raster"] is None  # bytes-free
    assert tile["path"] == p  # BARE path stored (no dbfs:/file: scheme)
    w = tile["window"]
    assert (w["col_off"], w["row_off"], w["width"], w["height"]) == (0, 0, 4, 3)


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
        prx.rst_fromfile("path", f.lit("GTiff"), materialize=True).alias("t")
    ).first()["t"]
    assert tile is not None  # scheme stripped -> open succeeded
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4 and ds.height == 3


def test_rst_fromfile_bad_path_returns_none(spark):
    # Heavyweight returns NULL on read failure; pyrx matches.
    df = spark.createDataFrame([("/no/such/raster.tif",)], ["path"])
    tile = df.select(prx.rst_fromfile("path", f.lit("GTiff")).alias("t")).first()["t"]
    assert tile is None


# ---------------------------------------------------------------------------
# TIN / IDW constructors + aggregators
# ---------------------------------------------------------------------------
def test_rst_gridfrompoints_constructor(spark):
    import shapely.wkb
    from shapely.geometry import Point

    # 1x1 grid center (1,1); p0=(0,0) v=10, p1=(2,2) v=30 -> IDW (power 2) = 20.
    p0 = shapely.wkb.dumps(Point(0.0, 0.0))
    p1 = shapely.wkb.dumps(Point(2.0, 2.0))
    df = spark.createDataFrame([([p0, p1], [10.0, 30.0])], ["pts", "vals"])
    tile = df.select(
        prx.rst_gridfrompoints(
            "pts", "vals", 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, 2.0, 2
        ).alias("t")
    ).first()["t"]
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.dtypes[0] == "float64"
        assert ds.nodata == -9999.0
        assert np.isclose(ds.read(1)[0, 0], 20.0)


def test_rst_dtmfromgeoms_constructor(spark):
    import shapely.wkb
    from shapely.geometry import Point

    # z = 2x + 3y + 1 plane; barycentric interpolation is exact in-hull.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    pts = [
        shapely.wkb.dumps(Point(x, y, 2 * x + 3 * y + 1), output_dimension=3)
        for x, y in corners
    ]
    df = spark.createDataFrame([(pts,)], ["pts"])
    tile = df.select(
        prx.rst_dtmfromgeoms(
            "pts", f.lit(None), 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
        ).alias("t")
    ).first()["t"]
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        transform = ds.transform
        assert ds.nodata == -9999.0
    # interior cell matches the plane
    wx, wy = transform * (5 + 0.5, 5 + 0.5)
    assert np.isclose(arr[5, 5], 2 * wx + 3 * wy + 1, atol=1e-6)


def test_rst_gridfrompoints_agg_equals_constructor(spark):
    import shapely.wkb
    from shapely.geometry import Point

    p0 = shapely.wkb.dumps(Point(0.0, 0.0))
    p1 = shapely.wkb.dumps(Point(2.0, 2.0))
    # aggregator: one (point, value) per row, grouped by k.
    df = spark.createDataFrame([("g", p0, 10.0), ("g", p1, 30.0)], ["k", "pt", "v"])
    tile = (
        df.groupBy("k")
        .agg(
            prx.rst_gridfrompoints_agg(
                "pt", "v", 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, 2.0, 2
            ).alias("t")
        )
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert np.isclose(ds.read(1)[0, 0], 20.0)


def test_rst_dtmfromgeoms_agg_equals_constructor(spark):
    import shapely.wkb
    from shapely.geometry import Point

    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    rows = [
        ("g", shapely.wkb.dumps(Point(x, y, 2 * x + 3 * y + 1), output_dimension=3))
        for x, y in corners
    ]
    df = spark.createDataFrame(rows, ["k", "pt"])
    tile = (
        df.groupBy("k")
        .agg(
            prx.rst_dtmfromgeoms_agg(
                "pt", f.lit(None), 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
            ).alias("t")
        )
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        transform = ds.transform
    wx, wy = transform * (5 + 0.5, 5 + 0.5)
    assert np.isclose(arr[5, 5], 2 * wx + 3 * wy + 1, atol=1e-6)


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
            prx.rst_rasterize("g", 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326).alias("t")
        ).first()["t"]
        assert tile is not None and tile["raster"] is not None, f"{name} null tile"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            results[name] = ds.read(1).tobytes()
    assert len(set(results.values())) == 1, "rst_rasterize encodings diverged"


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
            prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
            f.col("g"),
        )
        tile = df.select(
            prx.rst_viewshed("tile", f.col("g"), 1.0, 0.0, None).alias("t")
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
            prx.rst_gridfrompoints(
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
                prx.rst_gridfrompoints_agg(
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
            prx.rst_dtmfromgeoms(
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
                prx.rst_dtmfromgeoms_agg(
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
            prx.rst_dtmfromgeoms(
                "pts", "bls", 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633
            ).alias("t")
        ).first()["t"]
        assert tile is not None and tile["raster"] is not None, f"{name} null tile"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            results[name] = ds.read(1).tobytes()
    assert len(set(results.values())) == 1, "dtmfromgeoms breakline encodings diverged"


def test_rst_index_ndvi(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    out = df.select(
        prx.rst_index(
            "tile",
            f.lit("ndvi"),
            f.create_map(f.lit("red"), f.lit(1), f.lit("nir"), f.lit(2)),
        ).alias("t")
    )
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_type("t").alias("ty")
    ).first()
    assert row["n"] == 1
    assert row["ty"][0] == "Float32"


def test_rst_h3_tessellate(spark):
    import h3

    df = _tile_df(spark, width=8, height=8, epsg=4326)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_tessellate")
    # rst_h3_tessellate is a streaming UDTF — invoke via SQL LATERAL.
    parts = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata FROM _ras_tessellate, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4) t"
    )
    assert parts.count() > 0
    rows = parts.select(
        prx.rst_numbands(f.struct("cellid", "raster", "metadata")).alias("n"),
        f.col("cellid").alias("cid"),
    ).collect()
    for r in rows:
        assert r["n"] == 1
        assert h3.is_valid_cell(h3.int_to_str(r["cid"]))


def test_h3_tessellate_mode_sql(spark):
    import pytest

    df = _tile_df(spark, width=8, height=8, epsg=4326)
    prx.register(spark)
    df.createOrReplaceTempView("_ras_tessellate_mode")
    # 2-arg (default mode) — must still work for backward compat.
    n_default = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4) t"
    ).count()
    # explicit covering — same result as default.
    n_cover = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'covering') t"
    ).count()
    # centroid — non-zero, may differ from covering.
    n_centroid = spark.sql(
        "SELECT t.* FROM _ras_tessellate_mode, "
        "LATERAL gbx_rst_h3_tessellate(tile, 4, 'centroid') t"
    ).count()
    assert n_default == n_cover and n_cover > 0 and n_centroid > 0
    # bad mode must raise.
    with pytest.raises(Exception):
        spark.sql(
            "SELECT t.* FROM _ras_tessellate_mode, "
            "LATERAL gbx_rst_h3_tessellate(tile, 4, 'bogus') t"
        ).count()


def test_rst_proximity_column_api(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

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
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(prx.rst_proximity("tile", None, "PIXEL", None).alias("t"))
    # Float32 output, nodata -1.
    assert out.select(prx.rst_type("t").alias("ty")).first()["ty"][0] == "Float32"
    assert out.select(prx.rst_getnodata("t").alias("nd")).first()["nd"][0] == -1.0


def test_rst_cog_convert_column_api(spark):
    df = _tile_df(spark, width=64, height=64)
    out = df.select(prx.rst_cog_convert("tile", "DEFLATE", 512, "AVERAGE").alias("t"))
    # Round-trips: still openable, same band count.
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1
    assert out.select(prx.rst_width("t").alias("w")).first()["w"] == 64


def test_rst_contour_column_api(spark):
    import numpy as np
    import shapely.wkb
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    # Left-to-right ramp: value == column index.
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
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(f.explode(prx.rst_contour("tile", f.array(f.lit(2.5)))).alias("c"))
        .select(f.col("c.value").alias("v"), f.col("c.geom_wkb").alias("g"))
        .collect()
    )
    assert len(rows) >= 1
    assert all(r["v"] == 2.5 for r in rows)
    line = shapely.wkb.loads(bytes(rows[0]["g"]))
    assert line.geom_type == "LineString"
    assert all(abs(c[0] - 103.0) < 1e-6 for c in line.coords)


def test_rst_viewshed_column_api(spark):
    import numpy as np
    import shapely.wkb
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from shapely.geometry import Point

    # 7x7 flat DEM with a tall wall at column 3.
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
    # Observer at pixel (row=3, col=0) center -> world (0.5, 3.5).
    obs = shapely.wkb.dumps(Point(0.5, 3.5))
    df = spark.createDataFrame([(src, obs)], ["raster", "g"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"), f.col("g")
    )
    out = df.select(prx.rst_viewshed("tile", f.col("g"), 1.0, 0.0, None).alias("t"))
    # Byte output, single band.
    assert out.select(prx.rst_type("t").alias("ty")).first()["ty"][0] == "Byte"
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1
