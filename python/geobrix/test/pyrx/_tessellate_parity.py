"""Shared helpers for the cross-tier tessellate parity suites.

Collect the chips both tiers emit for one raster, then compare them per cell:
chip width, height and geotransform, and the source pixels each chip carries.

Both tiers register the same ``gbx_rst_*_tessellate`` SQL names, so callers must
collect the light rows before the heavy tier registers (``collect()`` materialises
them first).
"""

import math

import numpy as np

# Relative tolerance for geotransform terms (the parity bar of these suites).
GT_TOL = 1e-9


def light_chips(spark, raster, sql_name, resolution, assignment, coverage, id_fn):
    """Light tier rows via SQL LATERAL -> ``[(cid, raster_bytes|None, metadata)]``."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_tess_chips_light")
    rows = spark.sql(
        f"SELECT t.cellid AS cid, t.raster AS r, t.metadata AS m "
        f"FROM _ras_tess_chips_light, "
        f"LATERAL {sql_name}(tile, {resolution}, '{assignment}', '{coverage}') t"
    ).collect()
    return [_row(r, id_fn) for r in rows]


def heavy_chips(spark, raster, fn_name, resolution, assignment, coverage, id_fn):
    """Heavy tier rows via the Python generator wrapper -> same shape as light."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(
            getattr(hx, fn_name)(
                f.col("tile"), f.lit(resolution), assignment, coverage
            ).alias("tt")
        )
        .select(
            f.col("tt.cellid").alias("cid"),
            f.col("tt.raster").alias("r"),
            f.col("tt.metadata").alias("m"),
        )
        .collect()
    )
    return [_row(r, id_fn) for r in rows]


def _row(r, id_fn):
    raster = bytes(r["r"]) if r["r"] is not None else None
    return id_fn(r["cid"]), raster, dict(r["m"] or {})


def error_rows(rows):
    """Rows with no raster or a non-empty ``last_error`` (the error-tile shape).

    Heavy stamps ``last_error`` on every tile, empty when nothing failed.
    """
    return [(cid, m) for cid, b, m in rows if b is None or m.get("last_error")]


def by_cell(rows, label):
    """``{cid: raster_bytes}``; fails if a cell id is emitted twice."""
    out = {}
    for cid, b, _ in rows:
        assert cid not in out, f"{label}: cell {cid} emitted twice"
        out[cid] = b
    return out


def source_transform(raster):
    from rasterio.io import MemoryFile

    with MemoryFile(raster) as mf, mf.open() as ds:
        return ds.transform


def chip_shape(raster_bytes):
    """``(width, height, gdal_geotransform, nodata)`` of one chip."""
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return ds.width, ds.height, ds.transform.to_gdal(), ds.nodata


def chip_pixels(raster_bytes, src_transform):
    """Valid chip pixels as ``{(src_row, src_col, band_values)}`` on the source grid.

    A pixel is valid if it is valid in any band (mask bands, so NaN-safe). The
    chip's pixel centres go through the chip geotransform and the inverse source
    geotransform, so the set is comparable across tiers whatever the chip window.
    """
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        valid = (ds.read_masks() != 0).any(axis=0)
        rows, cols = np.nonzero(valid)
        if rows.size == 0:
            return set()
        data = ds.read()
        xs, ys = ds.transform * (cols + 0.5, rows + 0.5)
        sc, sr = ~src_transform * (xs, ys)
        return {
            (int(math.floor(r)), int(math.floor(c)), tuple(float(v) for v in vals))
            for r, c, vals in zip(sr, sc, data[:, rows, cols].T)
        }


def _gt_close(a, b):
    return all(abs(x - y) <= GT_TOL * max(1.0, abs(y)) for x, y in zip(a, b))


def assert_centroid_chips_match(light, heavy, src_transform, label):
    """Centroid chips agree per cell: shape, geotransform and source pixels.

    ``light`` / ``heavy`` are ``{cid: raster_bytes}`` over the same cell set.
    Returns the cells whose chips hold no valid pixel (covered-but-empty).
    """
    assert set(light) == set(heavy), f"{label}: cell sets differ"
    empties = set()
    for cid in sorted(light, key=str):
        lw, lh, lgt, _ = chip_shape(light[cid])
        hw, hh, hgt, _ = chip_shape(heavy[cid])
        where = f"{label} cell {cid}"
        sizes = f"light={lw}x{lh} heavy={hw}x{hh}"
        assert (lw, lh) == (hw, hh), f"{where}: chip size {sizes}"
        assert _gt_close(lgt, hgt), f"{where}: geotransform light={lgt} heavy={hgt}"
        lpx = chip_pixels(light[cid], src_transform)
        hpx = chip_pixels(heavy[cid], src_transform)
        if lpx != hpx:
            raise AssertionError(
                f"{label} cell {cid}: pixel maps differ: "
                f"light_only={sorted(lpx - hpx)[:5]} heavy_only={sorted(hpx - lpx)[:5]}"
            )
        if not lpx:
            empties.add(cid)
    return empties
