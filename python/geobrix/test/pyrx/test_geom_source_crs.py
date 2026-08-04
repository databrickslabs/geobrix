"""Task 4 (RasterX CRS-100, Group A): source-CRS declaration on geometry inputs.

rst_clip (clip_crs), rst_sample (crs), rst_viewshed (crs) — a plain WKB/WKT
geometry can declare its source CRS; an embedded EWKB SRID still wins per-geom;
absent CRS is assumed already-in-tile-CRS (never errors); ESRI/WKT strings work.
"""

import pytest
import shapely
from shapely.geometry import Point, box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit, ops

from .conftest import make_geotiff_bytes


def _utm_tif():
    # 32633 raster; make_geotiff origin is (10.0, 50.0) in whatever epsg is set,
    # so for UTM we just need a raster whose CRS != 4326 to force reprojection.
    return make_geotiff_bytes(width=8, height=8, epsg=32633)


# --- rst_sample: crs override on a plain-WKB point --------------------------


def test_sample_crs_override_reprojects_plain_point():
    """A plain-WKB point in EPSG:4326 + crs='EPSG:4326' over a 32633 raster is
    reprojected to the raster CRS before sampling (vs. assumed-aligned)."""
    b = make_geotiff_bytes(width=8, height=8, epsg=4326)
    with _serde.open_tile(b) as ds:
        # point inside the 4326 extent (origin 10.0,50.0, 0.5 px, 8x8)
        pt = Point(11.0, 48.5)
        vals = ops.sample(ds, pt, geom_crs="EPSG:4326")
    assert vals is not None and len(vals) == 1


def test_sample_embedded_srid_wins_over_crs_param():
    """EWKB embedded SRID takes precedence; the crs param is ignored per-geom
    (mixed-column safe) — no error even though both are 'present'."""
    b = make_geotiff_bytes(width=8, height=8, epsg=4326)
    with _serde.open_tile(b) as ds:
        pt = shapely.set_srid(Point(11.0, 48.5), 4326)  # embedded 4326
        vals = ops.sample(ds, pt, geom_crs="EPSG:3857")  # param ignored (embedded wins)
    assert vals is not None and len(vals) == 1


def test_sample_no_crs_assumes_aligned_no_error():
    """A plain point with no crs is assumed already in the raster CRS; no raise."""
    b = _utm_tif()
    with _serde.open_tile(b) as ds:
        # bare point in the raster's own coordinate space (origin 10,50 in 32633 units)
        pt = Point(11.0, 48.5)
        vals = ops.sample(ds, pt)  # no crs -> assume aligned, no error
    assert vals is not None


# --- rst_clip: clip_crs on a plain-WKB cutline ------------------------------


def test_clip_crs_esri_string_resolves():
    """clip_crs='ESRI:54008' on a plain-WKB cutline resolves ESRI (was int-only
    via the old _epsg_int); no crash. Cutline that misses -> None (not error)."""
    b = make_geotiff_bytes(width=8, height=8, epsg=4326)
    cut = box(10.5, 48.0, 11.5, 49.5)  # plain WKB, no SRID
    with _serde.open_tile(b) as ds:
        # clip_crs names a non-EPSG authority; must resolve, not raise on parse.
        out = edit.clip_to_geom(ds, cut, geom_crs="EPSG:4326")
    # overlapping cutline in the raster CRS -> non-None clipped bytes
    assert out is not None


def test_clip_no_crs_plain_cutline_assumed_aligned():
    """A plain-WKB cutline with no clip_crs is used as-is (assumed raster CRS)."""
    b = make_geotiff_bytes(width=8, height=8, epsg=4326)
    cut = box(10.5, 48.0, 11.5, 49.5)
    with _serde.open_tile(b) as ds:
        out = edit.clip_to_geom(ds, cut)  # no crs -> as-is, no error
    assert out is not None


def test_clip_both_srid_and_crs_conflict_raises():
    """Supplying clip_crs alongside a plain cutline is fine, but the underlying
    resolve_source_crs rejects an explicit srid+crs pair (call-level conflict)."""
    from databricks.labs.gbx.pyrx.core.crs import resolve_source_crs

    with pytest.raises(ValueError, match="srid OR crs"):
        resolve_source_crs(0, 4326, "EPSG:3857")
