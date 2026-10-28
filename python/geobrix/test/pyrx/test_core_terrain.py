import math

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import terrain


def _dem(data):
    data = np.asarray(data, dtype="float32")
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_slope_flat_is_zero():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.slope(ds)
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"
        # Interior is flat (slope 0); the 1-px kernel border is NoData.
        assert np.allclose(o.read(1)[1:-1, 1:-1], 0.0, atol=1e-4)


def test_slope_45deg_ramp():
    # ramp rising 1 unit per 1 unit in +x (projected, 1m px) -> 45 degrees slope interior
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))  # value == column index
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="degrees")
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        assert np.allclose(interior, 45.0, atol=1.0)


def test_aspect_ramp_faces_west_or_known():
    # value increases with column (slopes up toward +x); downslope faces -x (west=270 compass)
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.aspect(ds)
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        # all interior cells share one aspect; assert it's a finite compass value in [0,360)
        assert np.all((interior >= 0) & (interior < 360))
        assert np.allclose(interior, interior[0, 0], atol=1.0)


def test_aspect_flat_is_nodata():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.aspect(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # flat surface: all cells should be nodata (-9999)
        assert np.allclose(arr, -9999.0, atol=1e-3)


def test_aspect_flat_zero_for_flat():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.aspect(ds, zero_for_flat=True)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # Interior flat cells -> 0; the 1-px kernel border is NoData.
        assert np.allclose(arr[1:-1, 1:-1], 0.0, atol=1e-3)


def test_aspect_trigonometric():
    # N-facing slope: value decreases with row (higher values at top, lower at bottom)
    # -> dzdy > 0 (Horn: g+2h+i - a+2b+c)
    # In trig mode, arctan2(dzdy, -dzdx) with dzdx=0 -> 90 degrees (east)
    # But for a slope increasing toward bottom (south), dzdy<0 → trig gives -90
    col_ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(col_ramp)) as ds:
        out_trig = terrain.aspect(ds, trigonometric=True)
        out_compass = terrain.aspect(ds, trigonometric=False)
    with _serde.open_tile(out_trig) as o:
        trig_interior = o.read(1)[1:-1, 1:-1]
    with _serde.open_tile(out_compass) as o:
        compass_interior = o.read(1)[1:-1, 1:-1]
    # trig and compass should differ (different conventions)
    assert not np.allclose(trig_interior, compass_interior, atol=0.1)


def test_hillshade_byte_range():
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "uint8"
        arr = o.read(1)
        assert arr.min() >= 0 and arr.max() <= 255


def test_hillshade_flat_uniform():
    # A flat DEM should produce uniform hillshade (no slope variation)
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # Interior is uniform; the 1-px kernel border is NoData (0).
        assert np.all(arr[1:-1, 1:-1] == arr[1, 1])


def test_slope_percent():
    # 45-degree slope -> 100% grade
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="percent")
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        assert np.allclose(interior, 100.0, atol=2.0)


def test_tri_tpi_roughness_flat_zero():
    flat = np.full((5, 5), 50.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        tri_b, tpi_b, rough_b = terrain.tri(ds), terrain.tpi(ds), terrain.roughness(ds)
    for out in (tri_b, tpi_b, rough_b):
        with _serde.open_tile(out) as o:
            assert o.count == 1 and o.dtypes[0] == "float32"
            # Interior flat -> 0; the 1-px kernel border is NoData.
            assert np.allclose(o.read(1)[1:-1, 1:-1], 0.0, atol=1e-4)


def test_roughness_known_step():
    # a single high cell in a flat field -> roughness around it = the step height
    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 10.0
    with _serde.open_tile(_dem(data)) as ds:
        out = terrain.roughness(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # the peak cell and its neighbors see a 10-unit max-min spread
        assert abs(arr[2, 2] - 10.0) < 1e-4
        assert (
            abs(arr[1, 1] - 10.0) < 1e-4
        )  # diagonal neighbor's window includes the peak


def test_tpi_peak_positive():
    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 9.0
    with _serde.open_tile(_dem(data)) as ds:
        out = terrain.tpi(ds)
    with _serde.open_tile(out) as o:
        # center is higher than its (zero) neighbors -> positive TPI
        assert o.read(1)[2, 2] > 0


def test_slope_border_is_nodata():
    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="degrees")
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert o.nodata == -9999.0
        assert (r[0, :] == -9999.0).all() and (r[-1, :] == -9999.0).all()
        assert (r[:, 0] == -9999.0).all() and (r[:, -1] == -9999.0).all()
        assert r[2, 2] != -9999.0


def test_slope_input_nodata_propagates_to_neighbors():
    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    ramp[3, 3] = -9999.0
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert (r[2:5, 2:5] == -9999.0).all()


def test_hillshade_border_is_nodata_zero():
    flat = np.full((6, 6), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        assert o.nodata == 0.0
        r = o.read(1)
        assert (r[0, :] == 0).all()
        assert r[2, 2] != 0


def test_tri_uses_riley_sqrt_sum_sq():
    # 3x3 interior pixel: center=0, all 8 neighbors=1 -> Riley = sqrt(8*1^2)=sqrt(8);
    # Wilson (old) would be mean(|0-1|)=1.0. Assert Riley.
    dem = np.array(
        [
            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            [1, 1, 0, 1, 1],  # center low point at [2,2]
            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
        ],
        dtype="float32",
    )
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.tri(ds)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert abs(r[2, 2] - np.sqrt(8.0)) < 1e-4  # Riley, not 1.0 (Wilson)


def test_hillshade_dark_pixels_floor_to_1_not_0():
    # A steep DEM produces some self-shadowed (cang<=0) pixels; they must be 1, not 0
    # (0 is reserved for nodata). Assert no INTERIOR pixel is 0 and the min interior is >=1.
    steep = (
        np.tile(np.arange(8, dtype="float32"), (8, 1)) ** 2
    ) * 50.0  # strong relief
    with _serde.open_tile(_dem(steep)) as ds:
        out = terrain.hillshade(ds, azimuth=315.0, altitude=45.0)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        interior = r[1:-1, 1:-1]
        assert interior.min() >= 1  # valid floor is 1, never 0
        assert interior.max() <= 255


def test_hillshade_flat_matches_gdal_rational_form():
    # Flat DEM, default sun (az=315, alt=45, z=1): GDAL rational form -> 1+254*sin(45deg)=180.62 -> 181.
    # The OLD arctan form gave 255*cos(45)=180.3 -> 180; assert the GDAL value 181.
    flat = np.full((6, 6), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        interior = r[1:-1, 1:-1]
        assert (interior == 181).all()  # 1+254*sin(45deg) rounded


def test_hillshade_matches_gdal_rational_form_on_slope():
    # On a non-trivial gradient the output must equal GDAL's rational form evaluated
    # directly on pyrx's own Horn gradients (this pins the formula, incl. sign/azimuth convention).
    dem = (
        np.tile(np.arange(8, dtype="float32"), (8, 1))
        + np.arange(8, dtype="float32")[:, None]
    ) * 7.0
    az, alt, z = 315.0, 45.0, 1.0
    with _serde.open_tile(_dem(dem)) as ds:
        dzdx, dzdy, _ = terrain._horn_gradients(ds)
        out = terrain.hillshade(ds, azimuth=az, altitude=alt, z_factor=z)
    alt_r, az_r = np.radians(alt), np.radians(az)
    cang = (
        np.sin(alt_r) + np.cos(alt_r) * z * (dzdy * np.cos(az_r) - dzdx * np.sin(az_r))
    ) / np.sqrt(1.0 + z * z * (dzdx * dzdx + dzdy * dzdy))
    expected = np.where(cang <= 0.0, 1.0, 1.0 + 254.0 * cang)
    expected = np.clip(np.rint(expected), 0, 255).astype("uint8")
    with _serde.open_tile(out) as o:
        r = o.read(1)
    # compare interior (border is NoData=0 by propagate_invalid)
    assert np.array_equal(r[1:-1, 1:-1], expected[1:-1, 1:-1])


def _tile_with_crs(arr, crs, transform):
    profile = dict(
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr.astype("float32"), 1)
        return mf.read()


def test_gdaldem_scale_geographic_4326():
    arr = np.zeros((10, 8), dtype="float32")
    tr = from_origin(-73.99, 40.75, 0.0001, 0.0001)  # lon, lat, xsize, ysize (deg)
    with _serde.open_tile(_tile_with_crs(arr, "EPSG:4326", tr)) as ds:
        xs, ys = terrain._gdaldem_scale(ds)
    ang = math.pi / 180.0
    yscale = ang * 6378137.0
    mean_lat = (tr.f + 10 * tr.e / 2.0) * ang
    assert abs(ys - yscale) < 1e-3
    assert abs(xs - yscale * math.cos(mean_lat)) < 1e-3
    assert xs < ys


def test_gdaldem_scale_projected_metre():
    arr = np.zeros((10, 8), dtype="float32")
    tr = from_origin(500000.0, 4500000.0, 30.0, 30.0)
    with _serde.open_tile(_tile_with_crs(arr, "EPSG:32618", tr)) as ds:
        xs, ys = terrain._gdaldem_scale(ds)
    assert abs(xs - 1.0) < 1e-9 and abs(ys - 1.0) < 1e-9


def test_gdaldem_scale_no_crs_is_unit():
    arr = np.zeros((6, 6), dtype="float32")
    tr = from_origin(0.0, 0.0, 1.0, 1.0)
    with _serde.open_tile(_tile_with_crs(arr, None, tr)) as ds:
        xs, ys = terrain._gdaldem_scale(ds)
    assert (xs, ys) == (1.0, 1.0)


def test_slope_auto_scales_geographic_not_saturated():
    dem = (np.tile(np.arange(12, dtype="float32"), (12, 1))) * 5.0
    tr = from_origin(-73.99, 40.75, 0.0001, 0.0001)
    with _serde.open_tile(_tile_with_crs(dem, "EPSG:4326", tr)) as ds:
        out = terrain.slope(ds)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        interior = r[1:-1, 1:-1]
        assert interior.max() < 80.0
        assert interior.max() > 0.0


def test_slope_explicit_xyscale_overrides_auto():
    dem = (np.tile(np.arange(12, dtype="float32"), (12, 1))) * 5.0
    tr = from_origin(-73.99, 40.75, 0.0001, 0.0001)
    with _serde.open_tile(_tile_with_crs(dem, "EPSG:4326", tr)) as ds:
        dzdx, dzdy, _ = terrain._horn_gradients(ds)
        out = terrain.slope(ds, xscale=111120.0, yscale=111120.0)
    mag = np.sqrt((dzdx / 111120.0) ** 2 + (dzdy / 111120.0) ** 2)
    expected = np.degrees(np.arctan(mag)).astype("float32")
    with _serde.open_tile(out) as o:
        r = o.read(1)
    assert np.allclose(r[1:-1, 1:-1], expected[1:-1, 1:-1], atol=1e-3)


def test_aspect_scale_invariant_matches_gdaldem():
    """gdaldem aspect ignores the horizontal CRS scale (aspect is a pure
    direction). pyrx must match: the angle comes from the raw gradient ratio,
    not the anisotropic geographic scale. This guards against re-introducing the
    dzdx/xs, dzdy/ys division, which shifted the geographic aspect distribution
    away from heavyweight GDAL (the divergence the tier parity gate caught)."""
    dem = (
        np.tile(np.arange(12, dtype="float32"), (12, 1))
        + np.arange(12, dtype="float32")[:, None]
    ) * 5.0
    tr = from_origin(-73.99, 40.75, 0.0001, 0.0001)
    with _serde.open_tile(_tile_with_crs(dem, "EPSG:4326", tr)) as ds:
        dzdx, dzdy, _ = terrain._horn_gradients(ds)
        # Auto-scale would be anisotropic on this geographic tile; aspect must
        # NOT apply it.
        xs, ys = terrain._gdaldem_scale(ds)
        assert abs(xs - ys) > 1.0
        out = terrain.aspect(ds)
    # Expected = raw, unscaled gradient ratio (what gdaldem aspect computes).
    arad = np.arctan2(dzdy, -dzdx)
    expected = (90.0 - np.degrees(arad)) % 360.0
    with _serde.open_tile(out) as o:
        r = o.read(1)
    assert np.allclose(r[1:-1, 1:-1], expected[1:-1, 1:-1].astype("float32"), atol=1e-3)


def test_hillshade_auto_scale_geographic_realistic():
    dem = (np.tile(np.arange(12, dtype="float32"), (12, 1))) * 2.0
    tr = from_origin(-73.99, 40.75, 0.0001, 0.0001)
    with _serde.open_tile(_tile_with_crs(dem, "EPSG:4326", tr)) as ds:
        dzdx, dzdy, _ = terrain._horn_gradients(ds)
        xs, ys = terrain._gdaldem_scale(ds)
        out = terrain.hillshade(ds)
    gx, gy = dzdx / xs, dzdy / ys
    alt_r, az_r = np.radians(45.0), np.radians(315.0)
    cang = (
        np.sin(alt_r) + np.cos(alt_r) * (gy * np.cos(az_r) - gx * np.sin(az_r))
    ) / np.sqrt(1.0 + gx * gx + gy * gy)
    expected = np.where(cang <= 0.0, 1.0, 1.0 + 254.0 * cang)
    expected = np.clip(np.rint(expected), 0, 255).astype("uint8")
    with _serde.open_tile(out) as o:
        r = o.read(1)
    assert np.array_equal(r[1:-1, 1:-1], expected[1:-1, 1:-1])
