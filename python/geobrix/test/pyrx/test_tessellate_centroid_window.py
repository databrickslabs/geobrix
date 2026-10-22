"""Light centroid tessellate chips are cropped to each cell's pixel window.

Each centroid chip covers only the bounding pixel window of the pixels assigned to
its cell (not the whole source raster), synthetic covered-but-empty chips are 1x1 at
the pixel holding the cell's centroid, validity comes from the mask bands (NaN-safe),
and a source without declared NoData gets the shared dtype fallback. The BNG warp of
such a source marks its fill as NoData, so no valid chip pixel is invented outside
the source footprint.
"""

import math
import warnings

import h3
import numpy as np
import pytest
import shapely.geometry
from rasterio.enums import ColorInterp
from rasterio.errors import NodataShadowWarning
from rasterio.io import MemoryFile
from rasterio.transform import Affine, from_bounds
from rasterio.warp import transform as warp_transform
from rasterio.windows import Window

from databricks.labs.gbx.core.crs import authority_srid_of, get_transformer
from databricks.labs.gbx.pygx import _bng, _custom, _quadbin
from databricks.labs.gbx.pyrx.core import tessellate as T
from databricks.labs.gbx.pyrx.core._nodata import valid_mask

BOUNDS = (-122.60, 37.60, -122.40, 37.80)  # ~0.2 deg square; ~70 H3 res-7 cells


def _raster(
    w,
    h,
    *,
    bounds=BOUNDS,
    crs="EPSG:4326",
    dtype="uint16",
    nodata=0,
    count=1,
    transform=None,
    hole=None,
    band_holes=None,
    zero_block=None,
    blockysize=None,
):
    """hole: (r0, r1, c0, c1) set to nodata in every band; band_holes: {band: (r0, r1, c0, c1)}
    set to nodata in that band only; zero_block: (r0, r1, c0, c1) set to real 0 in every band;
    blockysize: rows per strip of a striped (untiled) GTiff, e.g. ``h`` for a single strip.
    The strips are DEFLATE-compressed: GDAL reads an uncompressed strip as ~8 KB virtual
    blocks, so only a compressed strip reports its real height in ``block_shapes``.
    """
    base = (np.arange(w * h, dtype="uint32").reshape(h, w) % 4000 + 1).astype(dtype)
    prof = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=count,
        dtype=dtype,
        crs=crs,
        transform=transform or from_bounds(*bounds, w, h),
        nodata=nodata,
    )
    if blockysize is not None:
        prof.update(tiled=False, blockysize=blockysize, compress="deflate")
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            for b in range(1, count + 1):
                a = base.copy()
                if zero_block is not None:
                    r0, r1, c0, c1 = zero_block
                    a[r0:r1, c0:c1] = 0
                if hole is not None:
                    r0, r1, c0, c1 = hole
                    a[r0:r1, c0:c1] = nodata
                if band_holes and b in band_holes:
                    r0, r1, c0, c1 = band_holes[b]
                    a[r0:r1, c0:c1] = nodata
                d.write(a, b)
        return mf.read()


def _chips(b, res=7, grid="h3", coverage="sparse"):
    with MemoryFile(b) as mf, mf.open() as ds:
        return (
            list(T.iter_tessellate(ds, res, grid, mode="centroid", coverage=coverage)),
            ds.transform,
            ds.width,
            ds.height,
        )


def _open(b):
    m = MemoryFile(b)
    return m, m.open()


def _cell_window_px(cellid_h3, src_tf):
    """Pixel-space bbox (w, h) of an H3 cell polygon on the source grid."""
    lnglat = [(lng, lat) for lat, lng in h3.cell_to_boundary(cellid_h3)]
    minx, miny, maxx, maxy = shapely.geometry.Polygon(lnglat).bounds
    c0, r0 = ~src_tf * (minx, maxy)
    c1, r1 = ~src_tf * (maxx, miny)
    return abs(c1 - c0), abs(r1 - r0)


def test_centroid_chips_are_window_sized():
    chips, tf, W, H = _chips(_raster(2048, 2048))
    assert len(chips) > 25
    for cellid, b in chips:
        m, c = _open(b)
        with m, c:
            assert c.width < W / 4 and c.height < H / 4
            h3s = h3.int_to_str(cellid & 0xFFFFFFFFFFFFFFFF)
            ww, wh = _cell_window_px(h3s, tf)
            assert c.width <= ww + 2 and c.height <= wh + 2


def test_centroid_chip_size_scales_with_pixel_density_not_source_size():
    def max_w(n):
        chips, *_ = _chips(_raster(n, n))
        out = 0
        for _, b in chips:
            m, c = _open(b)
            with m, c:
                out = max(out, c.width)
        return out

    w512, w2048 = max_w(512), max_w(2048)
    assert 3.5 <= w2048 / w512 <= 4.5 and w2048 < 2048 and w512 < 512


def _oracle_cell(grid, x, y, res, conf):
    """The raw cell of one pixel centre by the grid's scalar encoder; None = dropped."""
    if grid == "h3":
        return h3.latlng_to_cell(y, x, res)
    if grid == "quadbin":
        return _quadbin.point_as_cell(x, y, res)
    if grid == "bng":
        cell = _bng.point_to_cell_id(x, y, res)
        return cell if _bng.is_valid(cell) else None
    return _custom.point_to_cell_id_or_none(conf, x, y, res)


def _oracle_and_check(b, res, grid, conf=None):
    """Centroid chips equal the per-pixel definition, pixel by pixel and in order.

    Each valid pixel centre goes through the geotransform, then (h3/quadbin on a
    raster not in EPSG:4326) a per-pixel reprojection to EPSG:4326, then the grid's
    scalar encoder. bng needs an EPSG:27700 raster (no warp here); custom uses
    grid-native coordinates. Chips must come in first-seen pixel order (row-major).
    """
    with MemoryFile(b) as mf, mf.open() as ds:
        data = ds.read(1)
        valid = (ds.read_masks() != 0).any(axis=0)
        rows, cols = np.nonzero(valid)
        xs, ys = ds.transform * (cols + 0.5, rows + 0.5)
        if grid == "bng":
            assert authority_srid_of(ds.crs) == 27700, "the bng oracle does not warp"
        if (
            grid in ("h3", "quadbin")
            and ds.crs is not None
            and authority_srid_of(ds.crs) != 4326
        ):
            xs, ys = warp_transform(ds.crs, "EPSG:4326", xs.tolist(), ys.tolist())
        rres = T._resolve_resolution(res, grid)
        expected = {}
        for r, c, x, y in zip(rows, cols, xs, ys):
            cell = _oracle_cell(grid, float(x), float(y), rres, conf)
            if cell is not None:
                encoded = T._encode_cellid(cell, grid, conf=conf)
                expected[(int(r), int(c))] = (encoded, int(data[r, c]))
        inv = ~ds.transform
        chips = list(
            T.iter_tessellate(
                ds, res, grid, mode="centroid", coverage="sparse", conf=conf
            )
        )
    first_seen = list(dict.fromkeys(cell for cell, _ in expected.values()))
    assert [cellid for cellid, _ in chips] == first_seen, "emission order changed"
    seen = {}
    for cellid, cb in chips:
        m, c = _open(cb)
        with m, c:
            cm = (c.read_masks() != 0).any(axis=0)
            rr, cc = np.nonzero(cm)
            gx, gy = c.transform * (cc + 0.5, rr + 0.5)
            sc, sr = inv * (gx, gy)
            vals = c.read(1)[rr, cc]
            for r, col, v in zip(
                np.floor(sr).astype(int), np.floor(sc).astype(int), vals
            ):
                assert (r, col) not in seen, "pixel in two chips"
                seen[(r, col)] = (cellid, int(v))
    assert seen == expected
    return chips


@pytest.mark.parametrize("grid,res", [("h3", 7), ("quadbin", 13)])
def test_centroid_partition_matches_per_pixel_oracle(grid, res):
    _oracle_and_check(_raster(512, 512), res, grid)


def test_centroid_partition_oracle_on_rotated_geotransform():  # review focus 1
    tf = Affine(0.0004, 0.00005, -122.60, 0.00005, -0.0004, 37.80)
    _oracle_and_check(_raster(256, 256, transform=tf), 7, "h3")


def test_pixel_valid_if_any_band_is_valid():  # review focus 2
    b = _raster(
        256, 256, count=3, band_holes={2: (0, 256, 0, 128)}
    )  # band 2 nodata, left half
    chips, *_ = _chips(b)
    total = 0
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            total += int((c.read_masks() != 0).any(axis=0).sum())
    assert total == 256 * 256


def test_synthetic_empties_are_1x1_at_the_cell_centroid_pixel():
    b = _raster(
        1024, 1024, hole=(0, 1024, 0, 400)
    )  # left ~40% NoData: wider than a res-7 cell
    with MemoryFile(b) as mf, mf.open() as ds:
        chips = list(
            T.iter_tessellate(ds, 7, "h3", mode="centroid", coverage="complete")
        )
        empties = 0
        for cellid, cb in chips:
            m, c = _open(cb)
            with m, c:
                if (c.read_masks() != 0).any():
                    continue
                empties += 1
                assert (c.width, c.height) == (1, 1)
                h3s = h3.int_to_str(cellid & 0xFFFFFFFFFFFFFFFF)
                cx, cy = T._cell_geom(h3s, "h3").centroid.coords[
                    0
                ]  # D2: polygon centroid, grid CRS
                col_f, row_f = ~ds.transform * (cx, cy)
                col = min(max(math.floor(col_f), 0), ds.width - 1)
                row = min(max(math.floor(row_f), 0), ds.height - 1)
                assert c.transform == ds.window_transform(Window(col, row, 1, 1))
        assert empties > 0


def test_uint16_without_nodata_keeps_real_zeros_and_uses_fallback():
    b = _raster(256, 256, nodata=None, zero_block=(0, 16, 0, 16))
    chips, *_ = _chips(b)
    valid = 0
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            assert c.nodata == 65535.0
            valid += int((c.read_masks(1) != 0).sum())
    assert valid == 256 * 256


def test_float_without_nodata_uses_float32_min():
    chips, *_ = _chips(_raster(128, 128, dtype="float32", nodata=None))
    m, c = _open(chips[0][1])
    with m, c:
        assert c.nodata == float(np.finfo(np.float32).min)


@pytest.mark.parametrize("mode", ["centroid", "covering"])
def test_sparse_drops_nan_only_cells(mode):
    a = np.random.default_rng(3).random((128, 128)).astype("float32")
    a[:, :64] = np.nan
    prof = dict(
        driver="GTiff",
        width=128,
        height=128,
        count=1,
        dtype="float32",
        nodata=float("nan"),
        crs="EPSG:4326",
        transform=from_bounds(*BOUNDS, 128, 128),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a, 1)
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        chips = list(T.iter_tessellate(ds, 7, "h3", mode=mode, coverage="sparse"))
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            assert (c.read_masks(1) != 0).any()


def _empties(b, res, grid):
    """(cellid, chip bytes) of the cells "complete" adds over "sparse": the synthetic empties."""
    with MemoryFile(b) as mf, mf.open() as ds:
        sparse = {
            c
            for c, _ in T.iter_tessellate(
                ds, res, grid, mode="centroid", coverage="sparse"
            )
        }
        complete = T.iter_tessellate(
            ds, res, grid, mode="centroid", coverage="complete"
        )
        return [(c, cb) for c, cb in complete if c not in sparse]


def test_synthetic_empties_on_a_projected_raster_sit_at_the_cell_centroid_pixel():
    """An h3 centroid is moved 4326 -> raster CRS before its empty's pixel is chosen."""
    # UTM 18N, 20 km square at ~39 m; the left 8 km is NoData: wider than a res-7 cell.
    b = _raster(
        512,
        512,
        bounds=(500000, 4400000, 520000, 4420000),
        crs="EPSG:32618",
        hole=(0, 512, 0, 205),
    )
    to_utm = get_transformer(4326, 32618)
    empties = _empties(b, 7, "h3")
    assert empties
    inside = 0
    with MemoryFile(b) as mf, mf.open() as ds:
        for cellid, cb in empties:
            m, c = _open(cb)
            with m, c:
                assert (c.width, c.height) == (1, 1)
                assert not (c.read_masks() != 0).any(), "an empty must be all NoData"
                h3s = h3.int_to_str(cellid & 0xFFFFFFFFFFFFFFFF)
                lon, lat = T._cell_geom(h3s, "h3").centroid.coords[0]
                col_f, row_f = ~ds.transform * to_utm.transform(lon, lat)
                inside += 0 <= col_f < ds.width and 0 <= row_f < ds.height
                col = min(max(math.floor(col_f), 0), ds.width - 1)
                row = min(max(math.floor(row_f), 0), ds.height - 1)
                assert c.transform == ds.window_transform(Window(col, row, 1, 1))
    assert inside, "no empty's centroid fell inside the raster: placement untested"


def test_empty_chip_of_a_source_without_nodata_uses_the_uint16_fallback():
    """The empty of a uint16 source with no declared nodata carries 65535."""
    # The east edge reaches 0.3 px into the next quadbin cell: that cell is covered
    # (positive-area overlap) but holds no pixel centre, so "complete" adds an empty.
    res, px = 10, 0.001
    cell = _quadbin.point_as_cell(-122.5, 37.7, res)
    _, miny, maxx, maxy = T._cell_geom(cell, "quadbin").bounds
    east, cy = maxx + 0.3 * px, (miny + maxy) / 2
    b = _raster(
        16, 16, bounds=(east - 16 * px, cy - 8 * px, east, cy + 8 * px), nodata=None
    )
    empties = _empties(b, res, "quadbin")
    assert empties
    for _, cb in empties:
        m, c = _open(cb)
        with m, c:
            assert c.nodata == 65535.0
            assert (c.width, c.height) == (1, 1)
            assert not (c.read_masks() != 0).any()


_FILL_N = 40
# Norfolk, far from the 27700 central meridian (-2): the warp rotates the grid ~2.4
# degrees, so the EPSG:27700 raster has corners outside the source footprint.
_FILL_BOUNDS = (1.00, 52.40, 1.20, 52.60)


def _bng_fill_source():
    """uint16 EPSG:4326 source, no NoData: pixel p holds p + 1 (no zeros), row 3 holds 65535."""
    n = _FILL_N
    data = (np.arange(n * n, dtype="uint32").reshape(n, n) + 1).astype("uint16")
    data[3, :] = 65535
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_bounds(*_FILL_BOUNDS, n, n),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(data, 1)
        return mf.read()


def _bng_fill_stats(b, assignment, coverage):
    """``(valid, invented, fallback_valid, chip_nodatas)`` of the BNG chips of ``b``.

    A valid chip pixel is invented when its value is not a source value (a 0, or the
    fallback) or its centre is not on the source pixel that value names. Value ``v``
    sits at source row/col ``divmod(v - 1, n)``.
    """
    n = _FILL_N
    to_src = get_transformer(27700, 4326).transform
    valid = invented = fallback_valid = 0
    nodatas = set()
    with MemoryFile(b) as mf, mf.open() as ds:
        inv = ~ds.transform
        for _, cb in T.iter_tessellate(
            ds, 3, "bng", mode=assignment, coverage=coverage
        ):
            m, c = _open(cb)
            with m, c:
                nodatas.add(c.nodata)
                vals = c.read(1)
                rr, cc = np.nonzero(c.read_masks(1) != 0)
                xs, ys = c.transform * (cc + 0.5, rr + 0.5)
                for r, k, x, y in zip(rr, cc, xs, ys):
                    valid += 1
                    v = int(vals[r, k])
                    if v == 65535:
                        fallback_valid += 1
                        continue
                    if not 1 <= v <= n * n:
                        invented += 1
                        continue
                    lon, lat = to_src(float(x), float(y))
                    sc, sr = inv * (lon, lat)
                    er, ec = divmod(v - 1, n)
                    # 1.5 px of slack for the warp's approximate transformer at the edge
                    if abs(sc - (ec + 0.5)) > 1.5 or abs(sr - (er + 0.5)) > 1.5:
                        invented += 1
    return valid, invented, fallback_valid, nodatas


@pytest.mark.parametrize(
    "assignment,coverage",
    [("centroid", "sparse"), ("centroid", "complete"), ("covering", "sparse")],
)
def test_bng_warp_of_a_source_without_nodata_marks_fill_as_nodata(assignment, coverage):
    """The warp fill outside the source footprint is NoData, not valid zero pixels."""
    valid, invented, fallback_valid, nodatas = _bng_fill_stats(
        _bng_fill_source(), assignment, coverage
    )
    assert valid > 0, "the chips hold no valid pixel"
    assert invented == 0, f"{invented} of {valid} valid chip pixels are warp fill (0)"
    assert fallback_valid == 0, f"{fallback_valid} pixels equal to 65535 stayed valid"
    assert nodatas == {65535.0}


# name -> (grid, source CRS, source bounds, resolution): the 4326 source is warped to
# EPSG:27700 for bng and tessellated as is for h3; the 27700 source is not warped.
_ALPHA_CASES = {
    "bng_warped": ("bng", "EPSG:4326", _FILL_BOUNDS, 3),
    "bng_native": ("bng", "EPSG:27700", (620000, 290000, 640000, 310000), 3),
    "h3": ("h3", "EPSG:4326", _FILL_BOUNDS, 7),
}


def _banded_source(case, *, alpha):
    """uint8 four-band source, no NoData, rows of interest: 4-8 have RGB (or RGB+NIR) that
    is still non-zero but alpha 0 (``alpha``), 12 has a zero fourth band under non-zero
    colour bands, and 20 has zero colour bands (opaque with ``alpha``, else all zero).
    """
    _, crs, bounds, _ = _ALPHA_CASES[case]
    n = _FILL_N
    p = np.arange(n * n, dtype="int64").reshape(n, n)
    bands = [((p * k) % 199 + 1).astype("uint8") for k in (1, 3, 7, 11)]
    for band in bands[:3]:
        band[20, :] = 0
    if alpha:
        bands[3][:] = 255
        bands[3][4:9, :] = 0
    else:
        bands[3][12, :] = 0
        bands[3][20, :] = 0
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=4,
        dtype="uint8",
        crs=crs,
        transform=from_bounds(*bounds, n, n),
        photometric="RGB",
    )
    if alpha:
        prof["alpha"] = "YES"
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            for i, band in enumerate(bands, 1):
                d.write(band, i)
            if not alpha:
                d.colorinterp = (
                    ColorInterp.red,
                    ColorInterp.green,
                    ColorInterp.blue,
                    ColorInterp.undefined,
                )
        return mf.read()


def _banded_chips(case, *, alpha, assignment, coverage):
    """Tessellate the source of ``case`` (NodataShadowWarning is an error); return the chips
    and a function giving the fractional source ``(col, row)`` of a chip pixel centre.
    """
    grid, crs, _, res = _ALPHA_CASES[case]
    b = _banded_source(case, alpha=alpha)
    with warnings.catch_warnings():
        warnings.simplefilter("error", NodataShadowWarning)
        with MemoryFile(b) as mf, mf.open() as ds:
            inv = ~ds.transform
            chips = list(
                T.iter_tessellate(ds, res, grid, mode=assignment, coverage=coverage)
            )
    warped = grid == "bng" and crs == "EPSG:4326"
    to_src = get_transformer(27700, 4326).transform if warped else None

    def source_colrow(x, y):
        return inv * (to_src(float(x), float(y)) if to_src else (x, y))

    return chips, source_colrow


def _chip_stats(chips, source_colrow):
    """Count the valid pixels (``valid_mask``) of ``chips`` by where their source pixel
    lies: off the source footprint, in the transparent rows (4-8), or with zero colour
    bands (all of them, and those outside row 20) or a zero fourth band (row 12 or not).
    """
    n = _FILL_N
    out = dict(valid=0, outside=0, transparent=0, zero_rgb=0, zero_rgb_off=0)
    out.update(zero_b4=0, zero_b4_off=0)
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            data = c.read()
            rr, cc = np.nonzero(valid_mask(c))
            xs, ys = c.transform * (cc + 0.5, rr + 0.5)
            for r, k, x, y in zip(rr, cc, xs, ys):
                out["valid"] += 1
                sc, sr = source_colrow(x, y)
                if not (-1.5 <= sc <= n + 1.5 and -1.5 <= sr <= n + 1.5):
                    out["outside"] += 1
                elif 4.75 <= sr <= 8.25:
                    out["transparent"] += 1
                if (data[:3, r, k] == 0).all():
                    out["zero_rgb"] += 1
                    out["zero_rgb_off"] += not 19.25 <= sr <= 21.75
                if data[3, r, k] == 0:
                    out["zero_b4"] += 1
                    out["zero_b4_off"] += not 11.25 <= sr <= 13.75
    return out


@pytest.mark.parametrize("coverage", ["sparse", "complete"])
@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("case", list(_ALPHA_CASES))
def test_tessellate_of_an_rgba_source_keeps_its_alpha_band(case, assignment, coverage):
    """Chips keep the alpha band and declare no NoData over it: a transparent pixel (alpha 0,
    RGB still non-zero) is NoData, an opaque pixel whose RGB is 0 stays valid, none lies off
    the source footprint, and reading a chip's masks never warns that NoData shadows alpha.
    """
    chips, source_colrow = _banded_chips(
        case, alpha=True, assignment=assignment, coverage=coverage
    )
    assert chips
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            assert c.nodatavals == (None,) * 4
            assert c.colorinterp[3] == ColorInterp.alpha
    with warnings.catch_warnings():
        warnings.simplefilter("error", NodataShadowWarning)
        st = _chip_stats(chips, source_colrow)
    assert st["valid"] > 0, "the chips hold no valid pixel"
    assert st["outside"] == 0, f"{st['outside']} valid pixels off the source footprint"
    assert st["transparent"] == 0, f"{st['transparent']} transparent pixels are valid"
    assert st["zero_rgb"] > 0, "the opaque all-zero pixels must stay valid"
    assert st["zero_rgb_off"] == 0, f"{st['zero_rgb_off']} zero pixels not in row 20"


@pytest.mark.parametrize(
    "assignment,coverage",
    [("centroid", "sparse"), ("centroid", "complete"), ("covering", "sparse")],
)
@pytest.mark.parametrize("case", ["bng_warped", "h3"])
def test_tessellate_of_a_four_band_source_without_alpha_keeps_band_four_data(
    case, assignment, coverage
):
    """The fourth band of RGB+NIR is not alpha in the chips: a zero there under non-zero colour
    bands is valid, a pixel zero in every band is NoData (the uint8 fallback), and reading the
    masks never warns that NoData shadows an alpha band."""
    chips, source_colrow = _banded_chips(
        case, alpha=False, assignment=assignment, coverage=coverage
    )
    assert chips
    for _, cb in chips:
        m, c = _open(cb)
        with m, c:
            assert c.nodatavals == (0.0,) * 4
            assert ColorInterp.alpha not in c.colorinterp
    with warnings.catch_warnings():
        warnings.simplefilter("error", NodataShadowWarning)
        st = _chip_stats(chips, source_colrow)
    assert st["valid"] > 0 and st["outside"] == 0
    assert st["zero_b4"] > 0, "the zero NIR pixels under colour must stay valid"
    # row 20 is zero in every band: NoData, so a valid zero-NIR pixel comes from row 12
    assert st["zero_b4_off"] == 0 and st["zero_rgb"] == 0
