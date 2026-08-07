"""Task 6 (RasterX CRS-100, Group B): out_srid/out_crs + Rule-2 reprojection.

Produce-new-raster functions (rst_rasterize, ...) take out_srid/out_crs (output
CRS, string or int); the geometry is reprojected from its source CRS into the
output CRS before burning; both out params -> error; neither -> source carried.
"""

import numpy as np
import pytest
import shapely
from rasterio.io import MemoryFile
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import features


def _bounds_of(raster_bytes):
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return ds.crs, ds.bounds


def test_rasterize_out_crs_string_stamps_output():
    """out_crs='EPSG:32633' -> output SR is EPSG:32633 (via canonical string)."""
    geom = box(499000.0, 4649000.0, 501000.0, 4651000.0)  # UTM-ish metres
    b = features.rasterize_geom(
        shapely.to_wkb(geom),
        1.0,
        499000,
        4649000,
        501000,
        4651000,
        16,
        16,
        out_crs="EPSG:32633",
    )
    crs, _ = _bounds_of(b)
    assert crs is not None and crs.to_epsg() == 32633


def test_rasterize_out_srid_esri_labels_esri():
    """out_srid=54008 stamps ESRI:54008 on the output (via the resolver)."""
    geom = box(0.0, 0.0, 1000.0, 1000.0)
    b = features.rasterize_geom(
        shapely.to_wkb(geom), 1.0, 0, 0, 1000, 1000, 8, 8, out_srid=54008
    )
    with MemoryFile(b) as mf, mf.open() as ds:
        auth = ds.crs.to_authority() if ds.crs else None
    assert auth == ("ESRI", "54008")


def test_rasterize_both_out_params_raises():
    geom = box(0.0, 0.0, 1.0, 1.0)
    with pytest.raises(ValueError, match="out_srid OR out_crs"):
        features.rasterize_geom(
            shapely.to_wkb(geom),
            1.0,
            0,
            0,
            1,
            1,
            4,
            4,
            out_srid=4326,
            out_crs="EPSG:3857",
        )


def test_rasterize_neither_out_param_crsless_when_source_crsless():
    """A plain WKB geom (no SRID) + no out param -> CRS-less output (not forced)."""
    geom = box(0.0, 0.0, 1.0, 1.0)
    b = features.rasterize_geom(shapely.to_wkb(geom), 1.0, 0, 0, 1, 1, 4, 4)
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.crs is None


def test_rasterize_reprojects_ewkb_source_into_output():
    """Rule-2: an EWKB-4326 geometry rasterized with out_crs='EPSG:32633'
    reprojects the geometry into 32633 before burning, so the burned pixels land
    inside the 32633-declared extent (not garbage from treating degrees as metres).
    """
    # A small polygon around (11.0E, 42.0N) in EPSG:4326, carried as EWKB (SRID set).
    geom4326 = shapely.set_srid(box(10.99, 41.99, 11.01, 42.01), 4326)
    wkb = shapely.to_wkb(geom4326, include_srid=True)
    # 11E,42N reprojects to ~(168701, 4657521) in UTM 33N (11E is west of the
    # zone-33 central meridian). Bracket that easting/northing in the extent.
    b = features.rasterize_geom(
        wkb, 5.0, 166000, 4655000, 172000, 4660000, 64, 64, out_crs="EPSG:32633"
    )
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 32633
        data = ds.read(1)
        # Some pixels burned (value 5.0 present) -> the reprojected geom landed in-extent.
        assert np.any(data == 5.0), "reprojected geometry should burn inside the extent"


# --- Group B (B4): grid rasterize_agg output CRS via cellraster ------------


def test_cellraster_grid_out_crs_string_and_int_and_esri():
    """cellraster (backing rst_{h3,quadbin}_rasterize_agg) accepts an int SRID OR
    a CRS string as the output-CRS spec, and labels ESRI codes correctly."""
    import h3

    from databricks.labs.gbx.pyrx.core import cellraster as cr

    cells = {h3.str_to_int(h3.latlng_to_cell(42.0, 11.0, 6)): 1.0}
    keys = list(cells.keys())
    for spec, want in [
        (4326, ("EPSG", "4326")),
        ("EPSG:3857", ("EPSG", "3857")),
        ("ESRI:54008", ("ESRI", "54008")),
    ]:
        gs = cr.compute_gridspec(keys, srid=spec, mode="centroids", kring_pad=1)
        b = cr.cells_to_raster(cells, *gs, resolution=6, grid="h3")
        with MemoryFile(b) as mf, mf.open() as ds:
            assert ds.crs is not None and ds.crs.to_authority() == want, (spec, ds.crs)


# --- Task 7: gbx_h3_cell_bbox + rst_h3_gridspec output CRS -----------------


def test_h3_cell_bbox_out_crs(spark):
    """gbx_h3_cell_bbox(out_crs='EPSG:3857') returns a bbox in web-mercator metres;
    out_srid=54008 (ESRI) resolves without error."""
    import h3
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx

    cell = h3.str_to_int(h3.latlng_to_cell(42.0, 11.0, 6))
    df = spark.createDataFrame([(cell,)], ["cellid"])
    # Web-mercator: |x|,|y| are metres (large), not degrees.
    row = df.select(
        prx.gbx_h3_cell_bbox("cellid", out_crs=F.lit("EPSG:3857")).alias("b")
    ).first()
    assert abs(row["b"]["xmax"]) > 1000.0  # metres, not ~11 degrees
    # ESRI out_srid resolves (no raise).
    row2 = df.select(
        prx.gbx_h3_cell_bbox("cellid", out_srid=F.lit(54008)).alias("b")
    ).first()
    assert row2["b"] is not None
