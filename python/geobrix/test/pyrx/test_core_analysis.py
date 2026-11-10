"""Pure-function tests for pyrx Analysis ports: proximity (scipy EDT) and
cog_convert (GDAL's COG driver)."""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import analysis

from .conftest import cog_errors, make_geotiff_bytes


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


def test_proximity_targets_that_are_not_numbers_are_a_checked_argument():
    with pytest.raises(
        ValueError, match="target_values must be comma-separated numbers"
    ):
        analysis.check_proximity(target_values="a,b")
    # float("inf") parses, then int(round(inf)) overflows: the same argument error, not an OverflowError
    for bad in ("1,inf", "-inf", "1e999"):
        with pytest.raises(
            ValueError, match="target_values must be comma-separated numbers"
        ):
            analysis.check_proximity(target_values=bad)
    analysis.check_proximity(target_values=None)
    analysis.check_proximity(target_values="1, 2,")
    with _serde.open_tile(_single_source_bytes()) as ds:
        with pytest.raises(ValueError, match="target_values must be"):
            analysis.proximity(ds, "x", "PIXEL", None)


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
    errors = cog_errors(out, strict=False)
    assert not errors, f"COG invalid: {errors}"


def test_cog_convert_honors_blocksize_and_compression():
    src = make_geotiff_bytes(width=256, height=256)
    with _serde.open_tile(src) as ds:
        out = analysis.cog_convert(ds, "LZW", 128, "NEAREST")
    with _serde.open_tile(out) as o:
        assert o.block_shapes[0] == (128, 128)
        assert o.compression.value == "LZW"


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


def test_cog_convert_profile_names():
    """The COG profile names a caller may pass: rio-cogeo 7's list, kept as a local table."""
    assert analysis._comp.COG_PROFILES == {
        "deflate",
        "jpeg",
        "lerc",
        "lerc_deflate",
        "lerc_zstd",
        "lzma",
        "lzw",
        "packbits",
        "raw",
        "webp",
        "zstd",
    }
    src = make_geotiff_bytes(width=64, height=64)
    with _serde.open_tile(src) as ds:
        assert analysis.cog_convert(ds, "lzw", 256, "AVERAGE")[:2] in (b"II", b"MM")
        with pytest.raises(ValueError, match="valid profiles: deflate"):
            analysis.cog_convert(ds, "NOT_A_PROFILE", 256, "AVERAGE")


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


def test_contour_levels_are_sorted_and_without_repeats_and_nulls():
    src = _ramp_bytes(width=8, height=4)
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [5.5, None, 1.5, 5.5], 0.0, 0.0, "elev")
    assert sorted({e["value"] for e in out}) == [1.5, 5.5]
    assert [e["value"] for e in out] == sorted(e["value"] for e in out)


def test_contour_infinite_level_raises_naming_it():
    src = _ramp_bytes()
    with _serde.open_tile(src) as ds:
        with pytest.raises(ValueError, match="levels must be finite"):
            analysis.contour(ds, [float("inf")], 0.0, 0.0, "elev")


def test_contour_levels_of_only_nan_fall_back_to_the_interval_check():
    src = _ramp_bytes()
    with _serde.open_tile(src) as ds:
        with pytest.raises(ValueError, match="interval must be > 0"):
            analysis.contour(ds, [float("nan")], 0.0, 0.0, "elev")
        assert analysis.contour(ds, [float("nan")], 2.0, 0.0, "elev")


def test_contour_does_not_trace_the_9999_sentinel_of_a_tile_with_no_nodata():
    band = np.tile(np.arange(10, dtype="float64"), (10, 1))
    band[3:6, 4:6] = -9999.0
    profile = dict(
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype="float64",
        crs="EPSG:32633",
        transform=from_origin(0.0, 10.0, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(band, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.contour(ds, [-5000.0, 2.5], 0.0, 0.0, "elev")
    assert {e["value"] for e in out} == {2.5}
