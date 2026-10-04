"""Pure-function tests for pyrx Analysis ports: proximity (scipy EDT) and
cog_convert (rio-cogeo)."""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import analysis

from .conftest import make_geotiff_bytes


def _single_source_bytes(width=5, height=5, src_row=0, src_col=0, pixel=1.0):
    """A raster that is all-zero except one source pixel (value != 0).

    Pixel size 1x1 so PIXEL and GEO distances coincide; CRS EPSG:32633.
    """
    data = np.zeros((height, width), dtype="float32")
    data[src_row, src_col] = pixel
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


# --- proximity --------------------------------------------------------------
def test_proximity_distance_increases_with_pixel_distance():
    src = _single_source_bytes(width=5, height=5, src_row=0, src_col=0)
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # source pixel -> 0 distance
        assert arr[0, 0] == pytest.approx(0.0)
        # one pixel right / down -> 1.0
        assert arr[0, 1] == pytest.approx(1.0)
        assert arr[1, 0] == pytest.approx(1.0)
        # diagonal corner -> sqrt(2)
        assert arr[1, 1] == pytest.approx(math.sqrt(2.0), rel=1e-5)


def test_proximity_output_is_float32_nodata_minus1():
    src = _single_source_bytes()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        assert o.dtypes[0] == "float32"
        assert o.nodata == -1.0


def test_proximity_geo_units_scale_by_pixel_size():
    # pixel size 0.5 (make_geotiff_bytes); single source at (0,0).
    width, height = 5, 5
    data = np.zeros((height, width), dtype="float32")
    data[0, 0] = 1.0
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        geo = analysis.proximity(ds, None, "GEO", None)
    with _serde.open_tile(geo) as o:
        arr = o.read(1)
        # one pixel right in GEO = 0.5 ground units (pixel width)
        assert arr[0, 1] == pytest.approx(0.5)
        assert arr[1, 0] == pytest.approx(0.5)


def test_proximity_preserves_georef():
    src = _single_source_bytes(width=5, height=5)
    with _serde.open_tile(src) as ds:
        src_transform = ds.transform
        src_crs = ds.crs
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        assert o.transform == src_transform
        assert o.crs == src_crs
        assert (o.width, o.height) == (5, 5)


def test_proximity_max_distance_marks_far_pixels_nodata():
    src = _single_source_bytes(width=5, height=5, src_row=0, src_col=0)
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", 1.5)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # within 1.5: source (0), adjacent (1), diagonal sqrt2 ~1.41 kept
        assert arr[0, 0] == pytest.approx(0.0)
        assert arr[0, 1] == pytest.approx(1.0)
        assert arr[1, 1] == pytest.approx(math.sqrt(2.0), rel=1e-5)
        # far corner (4,4) dist ~5.66 > 1.5 -> nodata
        assert arr[4, 4] == pytest.approx(-1.0)


def test_proximity_default_source_rule_nonzero():
    # default (target_values=None): source = pixels with value != 0.
    width, height = 4, 1
    data = np.array([[0.0, 7.0, 0.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert list(arr[0]) == pytest.approx([1.0, 0.0, 1.0, 2.0])


def test_proximity_target_values_selects_specific_values():
    # source pixels = only those whose value is in target_values.
    width, height = 4, 1
    data = np.array([[0.0, 7.0, 9.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    # only value 9 is a source.
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "9", "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # distances to col=2 (the only 9): [2,1,0,1]
        assert list(arr[0]) == pytest.approx([2.0, 1.0, 0.0, 1.0])


def test_proximity_target_values_comma_separated():
    width, height = 5, 1
    data = np.array([[0.0, 7.0, 0.0, 9.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "7,9", "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # sources at col 1 and 3: distances [1,0,1,0,1]
        assert list(arr[0]) == pytest.approx([1.0, 0.0, 1.0, 0.0, 1.0])


def test_proximity_target_match_rounds_to_int_like_gdal():
    # GDAL ComputeProximity compares VALUES against the pixel value cast to int
    # via round-half-up (it copies the source scanline to a GInt32 buffer with
    # rounding). So VALUES=1 must match every pixel that rounds to 1 -- i.e. any
    # value in [0.5, 1.5) -- not only the pixel that is exactly 1.0. A continuous
    # float band (e.g. 0.6) is a target for VALUES=1, the way it is in the
    # heavyweight gbx_rst_proximity.
    width, height = 4, 1
    # col0=0.6 rounds->1 (target), col1=0.4 rounds->0 (not), col2=1.0 target,
    # col3=0.49 rounds->0 (not).
    data = np.array([[0.6, 0.4, 1.0, 0.49]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "1", "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # Targets are col0 and col2 (both round to 1). Distances: col0=0,
        # col1=1 (nearest target col0 or col2), col2=0, col3=1.
        assert list(arr[0]) == pytest.approx([0.0, 1.0, 0.0, 1.0])


def test_proximity_geo_units_with_rounded_target_and_nonunit_pixel():
    # End-to-end of the bug: a fractional band + non-unit (10 m) pixel size, GEO
    # units, VALUES=1. The single rounds-to-1 pixel is the only target; distances
    # must be Euclidean pixel distance * 10 (ground units), and the rounding rule
    # must make the 0.6 pixel -- not just an exact 1.0 -- the target.
    height, width = 1, 3
    data = np.array([[0.6, 0.1, 0.2]], dtype="float32")  # only col0 rounds to 1
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 10.0, 10.0),  # 10 m pixels
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "1", "GEO", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # target at col0: pixel distances [0,1,2] -> ground units * 10 = [0,10,20]
        assert list(arr[0]) == pytest.approx([0.0, 10.0, 20.0])


def test_proximity_default_source_rounds_to_int_like_gdal():
    # Default (no target_values): GDAL's source = pixels whose rounded value != 0.
    # A 0.6 pixel rounds to 1 (source); a 0.3 pixel rounds to 0 (not a source).
    width, height = 3, 1
    data = np.array([[0.3, 0.6, 0.3]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # only col1 (0.6 -> 1) is a source: distances [1, 0, 1]
        assert list(arr[0]) == pytest.approx([1.0, 0.0, 1.0])


def test_proximity_bad_distunits_raises():
    with _serde.open_tile(_single_source_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "BOGUS", None)


def test_proximity_bad_max_distance_raises():
    with _serde.open_tile(_single_source_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "PIXEL", 0.0)
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "PIXEL", -5.0)


# --- cog_convert ------------------------------------------------------------
def test_cog_convert_roundtrips_and_validates():
    src = make_geotiff_bytes(width=64, height=64)
    with _serde.open_tile(src) as ds:
        out = analysis.cog_convert(ds, "DEFLATE", 512, "AVERAGE")
    # reopens with rasterio
    with _serde.open_tile(out) as o:
        assert (o.width, o.height) == (64, 64)
    # validates as a COG
    try:
        from rio_cogeo.cogeo import cog_validate

        with MemoryFile(out) as mf:
            is_valid, errors, warnings = cog_validate(mf.name)
        assert is_valid, f"COG invalid: {errors}"
    except ImportError:
        pytest.skip("rio-cogeo not installed")


def test_cog_convert_honors_blocksize_and_compression():
    from rio_cogeo.cogeo import cog_info

    src = make_geotiff_bytes(width=256, height=256)
    with _serde.open_tile(src) as ds:
        out = analysis.cog_convert(ds, "LZW", 128, "NEAREST")
    with MemoryFile(out) as mf:
        info = cog_info(mf.name)
    assert tuple(info.IFD[0].Blocksize) == (128, 128)
    assert info.Compression.upper() == "LZW"


def test_cog_convert_bad_blocksize_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "DEFLATE", 0, "AVERAGE")


def test_cog_convert_empty_compression_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "", 512, "AVERAGE")


def test_cog_convert_unknown_compression_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "NOT_A_PROFILE", 512, "AVERAGE")


# --- contour ----------------------------------------------------------------
def _ramp_bytes(width=6, height=5, crs="EPSG:32633", ulx=100.0, uly=50.0, px=1.0):
    """A raster whose value equals the column index (a left-to-right ramp).

    A contour at level v is a near-vertical iso-line at column ~v; in world
    coords its x ~= ulx + (v + 0.5)*px (pixel-center offset).
    """
    band = np.tile(np.arange(width, dtype="float64"), (height, 1))
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float64",
        crs=crs,
        transform=from_origin(ulx, uly, px, px),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(band, 1)
        return mf.read()


def test_contour_fixed_level_yields_linestring_at_expected_world_x():
    import shapely.wkb

    src = _ramp_bytes(width=6, height=5, ulx=100.0, uly=50.0, px=1.0)
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [2.5], 0.0, 0.0, "elev")
    assert len(out) >= 1
    entry = out[0]
    assert set(entry.keys()) == {"geom_wkb", "value"}
    assert entry["value"] == pytest.approx(2.5)
    line = shapely.wkb.loads(entry["geom_wkb"])
    assert line.geom_type == "LineString"
    assert len(line.coords) >= 2
    # ramp -> iso x ~= ulx + (2.5 + 0.5) = 103.0 for every vertex
    xs = [c[0] for c in line.coords]
    assert all(x == pytest.approx(103.0) for x in xs)


def test_contour_each_value_matches_requested_level():
    src = _ramp_bytes(width=8, height=4)
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [1.5, 3.5, 5.5], 0.0, 0.0, "elev")
    produced = sorted({e["value"] for e in out})
    assert produced == pytest.approx([1.5, 3.5, 5.5])


def test_contour_empty_levels_with_interval_yields_multiple():
    src = _ramp_bytes(width=10, height=4)  # data range 0..9
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [], 2.0, 0.0, "elev")
    vals = sorted({e["value"] for e in out})
    # base 0 + k*2 within [0..9] inclusive: 0, 2, 4, 6, 8
    assert vals == pytest.approx([0.0, 2.0, 4.0, 6.0, 8.0])
    assert len(vals) > 1


def test_contour_empty_levels_honors_base_offset():
    src = _ramp_bytes(width=10, height=4)  # data range 0..9
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [], 3.0, 1.0, "elev")
    vals = sorted({e["value"] for e in out})
    # base 1 + k*3 within (0..9): 1, 4, 7
    assert vals == pytest.approx([1.0, 4.0, 7.0])


def test_contour_empty_levels_nonpositive_interval_raises():
    src = _ramp_bytes()
    with _serde.open_tile(src) as ds:
        with pytest.raises(ValueError):
            analysis.contour(ds, [], 0.0, 0.0, "elev")
        with pytest.raises(ValueError):
            analysis.contour(ds, [], -1.0, 0.0, "elev")


def test_contour_empty_attr_field_raises():
    src = _ramp_bytes()
    with _serde.open_tile(src) as ds:
        with pytest.raises(ValueError):
            analysis.contour(ds, [2.5], 0.0, 0.0, "")


def test_contour_returns_list_of_struct_dicts():
    src = _ramp_bytes()
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [2.5], 0.0, 0.0, "elev")
    assert isinstance(out, list)
    assert all(
        isinstance(e, dict)
        and isinstance(e["geom_wkb"], (bytes, bytearray))
        and isinstance(e["value"], float)
        for e in out
    )


# --- viewshed ---------------------------------------------------------------
def _dem_with_wall_bytes(crs="EPSG:32633"):
    """7x7 flat DEM with a tall wall at column 3 (blocks line of sight)."""
    h, w = 7, 7
    dem = np.zeros((h, w), dtype="float64")
    dem[:, 3] = 100.0
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float64",
        crs=crs,
        transform=from_origin(0.0, float(h), 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        return mf.read()


def _observer_world_xy(ds, col, row):
    """World (x, y) at the center of pixel (row, col)."""
    from rasterio.transform import xy as _xy

    x, y = _xy(ds.transform, row, col, offset="center")
    return float(x), float(y)


def test_viewshed_returns_byte_tile_binary_mask():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        out = analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        assert o.count == 1
        assert o.dtypes[0] == "uint8"
        arr = o.read(1)
        assert set(np.unique(arr)).issubset({0, 255})


def test_viewshed_observer_cell_visible():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        out = analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert arr[3, 0] == 255


def test_viewshed_cell_behind_wall_invisible():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        out = analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # column 5 sits behind the wall at column 3 -> not visible.
        assert arr[3, 5] == 0


def test_viewshed_preserves_georef_and_crs():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        src_transform = ds.transform
        src_crs = ds.crs
        ox, oy = _observer_world_xy(ds, col=3, row=3)
        out = analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        assert o.transform == src_transform
        assert o.crs == src_crs
        assert (o.width, o.height) == (7, 7)


def test_viewshed_negative_observer_height_raises():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        with pytest.raises(ValueError):
            analysis.viewshed(ds, ox, oy, -1.0, 0.0, None)


def test_viewshed_negative_target_height_raises():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        with pytest.raises(ValueError):
            analysis.viewshed(ds, ox, oy, 1.0, -2.0, None)


def test_viewshed_nonpositive_max_distance_raises():
    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        with pytest.raises(ValueError):
            analysis.viewshed(ds, ox, oy, 1.0, 0.0, 0.0)
        with pytest.raises(ValueError):
            analysis.viewshed(ds, ox, oy, 1.0, 0.0, -10.0)


# ---------------------------------------------------------------------------
# Fix 1: viewshed size-gate uses 500 B/px (xrspatial working set), not 4 B/px
# ---------------------------------------------------------------------------


def test_viewshed_medium_dem_routes_to_ctypes(monkeypatch):
    """A DEM where 500*h*w > monkeypatched budget must use ctypes, not xrspatial.

    Pre-fix the gate used the decoded size (4 B/px for float32), so a 2000×2000
    DEM at 16 MB decoded would pass under a 64 MiB budget and be routed to
    xrspatial — which needs ~500 B/px (~2 GB) and would raise MemoryError.
    Post-fix the gate uses _XRSPATIAL_BYTES_PER_PX * h * w, so the same DEM
    exceeds the budget and goes to the bounded ctypes engine.
    """
    h, w = 80, 80  # 500 * 80 * 80 = 3.2 MB; monkeypatch budget to 1.6 MB → ctypes
    dem = np.zeros((h, w), dtype="float64")
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float64",
        crs="EPSG:3857",
        transform=from_origin(0.0, float(h), 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        src = mf.read()

    ctypes_calls = []
    xrspatial_calls = []
    orig_ctypes = analysis._viewshed_gdal_ctypes
    orig_xrspatial = analysis._viewshed_xrspatial

    def spy_ctypes(ds, *args, **kwargs):
        ctypes_calls.append(1)
        return orig_ctypes(ds, *args, **kwargs)

    def spy_xrspatial(ds, *args, **kwargs):
        xrspatial_calls.append(1)
        return orig_xrspatial(ds, *args, **kwargs)

    # Budget 1.6 MB < 500*80*80 = 3.2 MB → ctypes path
    monkeypatch.setattr(analysis, "_viewshed_gdal_ctypes", spy_ctypes)
    monkeypatch.setattr(analysis, "_viewshed_xrspatial", spy_xrspatial)
    monkeypatch.setattr(analysis, "decoded_budget_bytes", lambda s: 1_600_000)

    with _serde.open_tile(src) as ds:
        from rasterio.transform import xy as _xy

        ox, oy = _xy(ds.transform, h // 2, w // 2, offset="center")
        out = analysis.viewshed(ds, float(ox), float(oy), 2.0, 0.0, None)

    assert len(ctypes_calls) == 1, "medium DEM must route to ctypes engine"
    assert len(xrspatial_calls) == 0, "medium DEM must not invoke xrspatial"

    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert set(np.unique(arr)).issubset({0, 255})


# --- ctypes GDALViewshedGenerate engine -------------------------------------


def _dem_ctypes_wall_bytes():
    """9x9 flat DEM (elev 0) with a tall wall at column 5 (one east of centre col 4).

    EPSG:3857, 1 m pixels, origin (0, 9).  Centre pixel (col=4, row=4) has
    world coords (4.5, 4.5).
    """
    h, w = 9, 9
    dem = np.zeros((h, w), dtype="float64")
    dem[:, 5] = 100.0
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float64",
        crs="EPSG:3857",
        transform=from_origin(0.0, float(h), 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        return mf.read()


def test_viewshed_gdal_ctypes_occlusion():
    """ctypes engine: observer at centre, wall one column east.

    Visible: observer cell and cells west of the wall.
    Invisible: cells east of (behind) the wall.
    """
    src = _dem_ctypes_wall_bytes()
    with _serde.open_tile(src) as ds:
        from rasterio.transform import xy as _xy

        ox, oy = _xy(ds.transform, 4, 4, offset="center")
        out_arr = analysis._viewshed_gdal_ctypes(ds, float(ox), float(oy), 2.0, 0.0, None)

    assert out_arr.dtype == np.dtype("uint8"), f"expected uint8, got {out_arr.dtype}"
    unique = set(np.unique(out_arr).tolist())
    assert unique.issubset({0, 255}), f"unexpected pixel values: {unique}"
    assert out_arr[4, 4] == 255, "observer cell should be visible (255)"
    assert out_arr[4, 3] == 255, "cell just west of observer (col 3) should be visible"
    assert out_arr[4, 6] == 0, "cell east of wall (col 6) should be invisible (0)"


def test_viewshed_engines_agree_small():
    """ctypes and xrspatial engines agree on interior cells of a small DEM."""
    pytest.importorskip("xrspatial")

    src = _dem_with_wall_bytes()
    with _serde.open_tile(src) as ds:
        ox, oy = _observer_world_xy(ds, col=0, row=3)
        ctypes_arr = analysis._viewshed_gdal_ctypes(ds, ox, oy, 1.0, 0.0, None)
        xrs_arr = analysis._viewshed_xrspatial(ds, ox, oy, 1.0, 0.0, None)

    # Trim 1-pixel border to allow edge-handling differences.
    interior_c = ctypes_arr[1:-1, 1:-1]
    interior_x = xrs_arr[1:-1, 1:-1]

    mismatches = int(np.sum(interior_c != interior_x))
    total = interior_c.size
    mismatch_frac = mismatches / total if total > 0 else 0.0

    assert mismatch_frac < 0.15, (
        f"ctypes and xrspatial disagree on {mismatches}/{total} interior cells "
        f"({mismatch_frac:.1%}); expected < 15% mismatch"
    )


# ---------------------------------------------------------------------------
# Child-process script for test_viewshed_gdal_bounds_peak_rss.
# Runs in a fresh subprocess so ru_maxrss is not pre-inflated by pytest imports.
# ---------------------------------------------------------------------------
_VIEWSHED_RSS_CHILD = """
import gc, os, platform, resource, sys, tempfile

# Cap GDAL block cache before rasterio initialises GDAL.
os.environ.setdefault("GDAL_CACHEMAX", "4")

import numpy as np
import rasterio
import rasterio.windows
from rasterio.transform import from_origin, xy as _xy
from databricks.labs.gbx.pyrx.core.analysis import _viewshed_gdal_ctypes

H, W = 4000, 4000
# Write the DEM block-by-block so the full band never lives as one Python array.
dem_fd, dem_file = tempfile.mkstemp(suffix=".tif")
os.close(dem_fd)
try:
    profile = dict(
        driver="GTiff", width=W, height=H, count=1, dtype="float32",
        crs="EPSG:3857", transform=from_origin(0.0, float(H), 1.0, 1.0),
    )
    block_h = 256
    with rasterio.open(dem_file, "w", **profile) as dst:
        for row_off in range(0, H, block_h):
            bh = min(block_h, H - row_off)
            blk = np.zeros((bh, W), dtype="float32")
            win = rasterio.windows.Window(0, row_off, W, bh)
            dst.write(blk, 1, window=win)
            del blk

    # Baseline: DEM on disk, no large array in Python memory.
    gc.collect()
    _scale = 1024 if platform.system() == "Linux" else 1
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _scale

    with rasterio.open(dem_file) as ds:
        ox, oy = _xy(ds.transform, H // 2, W // 2, offset="center")
        _viewshed_gdal_ctypes(ds, float(ox), float(oy), 2.0, 0.0, None)

    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _scale
    print(rss_after - rss_before, flush=True)
finally:
    try:
        os.unlink(dem_file)
    except OSError:
        pass
"""


def test_viewshed_gdal_bounds_peak_rss():
    """ctypes engine peak RSS delta stays below the decoded DEM size (subprocess proof).

    Runs in a fresh child process so ``ru_maxrss`` starts from a clean high-water
    mark, not pre-inflated by numba/xarray/earlier-test imports.  The DEM is
    block-written so the baseline is free of any full-band Python allocation.

    A streaming engine must not materialise the full band as a Python array;
    peak RSS delta should stay below the decoded DEM size (4000×4000 float32 = 64 MB).
    Calling ``ds.read(1)`` in the engine would add ~64 MB on top of the output
    array and FAIL this threshold — that is the intended failure mode for Fix A.
    """
    import subprocess

    try:
        import resource  # noqa: F401 — availability check only
    except ImportError:
        pytest.skip("resource module unavailable")

    proc = subprocess.run(
        [__import__("sys").executable, "-c", _VIEWSHED_RSS_CHILD],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"viewshed RSS child exited {proc.returncode}:\n{proc.stderr[-2000:]}"
        )

    delta_bytes = int(proc.stdout.strip())
    # 4000×4000 float32 = 64 MB; streaming engine should not allocate a full DEM array.
    decoded_dem_bytes = 4000 * 4000 * np.dtype("float32").itemsize
    assert delta_bytes < decoded_dem_bytes, (
        f"peak RSS delta {delta_bytes / 1e6:.1f} MB ≥ decoded DEM "
        f"{decoded_dem_bytes / 1e6:.0f} MB — engine may be loading the full band"
    )
