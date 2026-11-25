"""A raster in another CRS than the grid's is gridded against its own pixels, with no warp.

H3 and quadbin cells are polygons in EPSG:4326 (straight edges in lon/lat), BNG cells squares in EPSG:27700. For a
raster in another CRS, ``raster_to_grid*`` moves each cell into the raster's CRS and shares every valid pixel over the
cells by the exact overlap area (``covering``), or bins each valid pixel by its own centre (``centroid``). The private
references below do the same with shapely, each cell edge walked with 64 points, so they are independent of the
library's kernel, its candidate cells and its edge walk.
"""

import contextlib

import h3
import numpy as np
import pytest
import quadbin
import shapely
from rasterio.io import MemoryFile
from rasterio.transform import Affine, from_origin
from shapely import STRtree
from shapely.geometry import Polygon

from databricks.labs.gbx.core.crs import get_transformer
from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import gridagg
from databricks.labs.gbx.pyrx.core._nodata import band_valid_mask

# NWS HRAP (Stage IV): polar stereographic on a sphere of radius 6,371,200 m, 4762.5 m pixels
HRAP = "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=-105 +x_0=0 +y_0=0 +R=6371200 +units=m +no_defs"
_REF_EDGE_POINTS = 64


def _ramp(h, w, holes=True):
    y, x = np.mgrid[0:h, 0:w].astype("float64")
    a = 1.0 + (x * 7 + y * 3) % 17
    if holes:
        a[(x + 2 * y) % 5 == 0] = -9999.0
    return a


def _tif(data, crs, transform, nodata=-9999.0):
    h, w = data.shape
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=w,
            height=h,
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
            nodata=nodata,
        ) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


@contextlib.contextmanager
def _open(b):
    with MemoryFile(b) as mf, mf.open() as ds:
        yield ds


def _hrap_origin(lon, lat, px=4762.5):
    """The HRAP grid corner (a multiple of the pixel from the Stage IV origin) at or west/north of ``lon, lat``."""
    x, y = get_transformer(4326, HRAP).transform(lon, lat)
    x0, y0 = -1904912.11, -3423783.70
    return x0 + np.floor((x - x0) / px) * px, y0 + np.ceil((y - y0) / px) * px


_HX, _HY = _hrap_origin(-94.2, 42.3)
_UTM_T = from_origin(446000.0, 4650000.0, 30.0, 30.0)
_UTM31_X, _UTM31_Y = (
    round(v / 250.0) * 250.0 for v in get_transformer(4326, 32631).transform(1.0, 52.7)
)

# label -> (raster bytes, CRS)
RASTERS = {
    "hrap": (_tif(_ramp(12, 14), HRAP, from_origin(_HX, _HY, 4762.5, 4762.5)), HRAP),
    "utm": (_tif(_ramp(10, 12), "EPSG:32615", _UTM_T), "EPSG:32615"),
    "utm-rotated": (
        _tif(
            _ramp(10, 12),
            "EPSG:32615",
            Affine(30.0, 6.0, 446000.0, 6.0, -30.0, 4650000.0),
        ),
        "EPSG:32615",
    ),
    "norfolk-4326": (
        _tif(_ramp(16, 16), "EPSG:4326", from_origin(1.0, 52.7, 0.005, 0.005)),
        "EPSG:4326",
    ),
    "norfolk-utm31": (
        _tif(
            _ramp(12, 12), "EPSG:32631", from_origin(_UTM31_X, _UTM31_Y, 250.0, 250.0)
        ),
        "EPSG:32631",
    ),
    # EPSG:32660 (UTM 60N, central meridian 177E) at 45N: x 700-780 km crosses 180E
    "across-180": (
        _tif(
            _ramp(20, 40),
            "EPSG:32660",
            from_origin(700000.0, 5000000.0, 2000.0, 2000.0),
        ),
        "EPSG:32660",
    ),
    # x 600-730 km stays west of 180E, but res-3 cells (~60 km edges) along its east edge cross it
    "near-180": (
        _tif(
            _ramp(20, 65),
            "EPSG:32660",
            from_origin(600000.0, 5000000.0, 2000.0, 2000.0),
        ),
        "EPSG:32660",
    ),
    # MODIS sinusoidal tile h25v03 (10 km pixels): edges bend strongly far from the central meridian
    "sinu": (
        _tif(
            _ramp(111, 111),
            "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs",
            from_origin(7 * 1111950.5197665233, 6 * 1111950.5197665233, 10e3, 10e3),
        ),
        "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs",
    ),
    # EPSG:3413 (NSIDC north polar stereographic): around the pole
    "pole": (
        _tif(
            _ramp(50, 50), "EPSG:3413", from_origin(-100000.0, 100000.0, 4000.0, 4000.0)
        ),
        "EPSG:3413",
    ),
}


# --- the exact native reference --------------------------------------------------------------------------------------


def _pixel_quads(ds):
    """Every pixel of ``ds`` as a shapely quad in its own CRS (the full geotransform, rotation included), row-major."""
    t = ds.transform
    rows, cols = np.mgrid[0 : ds.height, 0 : ds.width]
    rows, cols = rows.ravel(), cols.ravel()
    corners = [
        t * (cols + dc, rows + dr) for dc, dr in ((0, 0), (1, 0), (1, 1), (0, 1))
    ]
    xy = np.stack([np.stack(c, axis=-1) for c in corners], axis=1)
    return shapely.polygons(xy), rows, cols


def _raster_outline(ds):
    t = ds.transform
    return Polygon(
        [t * (0, 0), t * (ds.width, 0), t * (ds.width, ds.height), t * (0, ds.height)]
    )


def _grid_cells(ds, src_crs, grid, res):
    """``(cell id as the library gives it, ring in the grid CRS)`` of every cell near the raster: a superset."""
    grid_crs = 27700 if grid == "bng" else 4326
    t = ds.transform
    f = np.linspace(0.0, 1.0, 41)
    cols = np.concatenate(
        [f * ds.width, np.full(41, ds.width), f * ds.width, np.zeros(41)]
    )
    rows = np.concatenate(
        [np.zeros(41), f * ds.height, np.full(41, ds.height), f * ds.height]
    )
    xs, ys = t * (cols, rows)
    gx, gy = get_transformer(src_crs, grid_crs).transform(xs, ys)
    w, s, e, n = min(gx), min(gy), max(gx), max(gy)
    if grid in ("h3", "quadbin"):
        # the cells of points sampled over the raster (padded by a pixel) at a third of a cell's edge, and their
        # neighbours: no lon/lat box, which would break across the antimeridian or around a pole
        if grid == "h3":
            edge = h3.average_hexagon_edge_length(res, "m")
        else:
            edge = 40075016.0 / 2**res * np.cos(np.radians(max(abs(s), abs(n))))
        step = min(abs(t.a), edge / 3.0) / abs(t.a)
        cc, rr = np.meshgrid(
            np.arange(-1.0, ds.width + 1.0 + 1e-9, step),
            np.arange(-1.0, ds.height + 1.0 + 1e-9, step),
        )
        lon, lat = get_transformer(src_crs, 4326).transform(
            *(t * (cc.ravel(), rr.ravel()))
        )
        if grid == "h3":
            seen = {h3.latlng_to_cell(la, lo, res) for lo, la in zip(lon, lat)}
            for c in set().union(*(h3.grid_disk(c, 1) for c in seen)):
                yield h3.str_to_int(c), [
                    (lng, lat) for lat, lng in h3.cell_to_boundary(c)
                ]
        else:
            seen = set()
            for c in {int(c) for c in _quadbin.point_as_cell_vec(lon, lat, res)}:
                x, y, z = quadbin.cell_to_tile(
                    c
                )  # its 3 x 3 tiles, x wrapping at the antimeridian
                seen |= {
                    quadbin.tile_to_cell(((x + i) % 2**z, y + j, z))
                    for i in (-1, 0, 1)
                    for j in (-1, 0, 1)
                    if 0 <= y + j < 2**z
                }
            for c in seen:
                cw, cs, ce, cn = quadbin.cell_to_bounding_box(int(c))
                yield int(c), [(cw, cs), (ce, cs), (ce, cn), (cw, cn)]
    else:
        edge = _bng.get_edge_size(_bng.get_resolution(res))
        for x in np.arange(np.floor(w / edge) - 2, np.ceil(e / edge) + 2) * edge:
            for y in np.arange(np.floor(s / edge) - 2, np.ceil(n / edge) + 2) * edge:
                cid = _bng.point_to_cell_id(x + edge / 2, y + edge / 2, res)
                if _bng.is_valid(cid):
                    yield _bng.format(cid), [
                        (x, y),
                        (x + edge, y),
                        (x + edge, y + edge),
                        (x, y + edge),
                    ]


def _native_cells(ds, src_crs, grid, res, unwrap=None, forward=None):
    """``{cell id: the cell's polygon in the raster CRS}``: each grid-CRS edge walked with 64 points (or 16 per pixel
    side of the edge's moved length, up to 2048: a long edge near a pole bends far), then moved. ``unwrap`` (moved
    x -> x) makes the raster CRS continuous past its seam, for a cell across it.
    """
    # the inverse of the raster -> grid operation, as the library moves cells (PROJ may pick another one for the
    # grid -> raster direction)
    forward = forward or get_transformer(src_crs, 27700 if grid == "bng" else 4326)

    def move(x, y):
        return forward.transform(x, y, direction="INVERSE")

    def to_native(x, y):
        x, y = move(x, y)
        return (x, y) if unwrap is None else (unwrap(np.asarray(x)), y)

    side = abs(ds.transform.determinant) ** 0.5
    out = {}
    for cid, ring in _grid_cells(ds, src_crs, grid, res):
        ring = np.asarray(ring, dtype="float64")
        nxt = np.roll(ring, -1, axis=0)
        if grid != "bng":  # an edge's longitude step, the short way round
            nxt[:, 0] = ring[:, 0] + (nxt[:, 0] - ring[:, 0] + 180.0) % 360.0 - 180.0
        ex, ey = to_native(ring[:, 0], ring[:, 1])
        longest = np.hypot(
            np.diff(np.append(ex, ex[0])), np.diff(np.append(ey, ey[0]))
        ).max()
        k = int(np.clip(np.ceil(16 * longest / side), _REF_EDGE_POINTS, 2048))
        t = np.arange(k)[None, :, None] / k
        pts = (ring[:, None, :] + t * (nxt - ring)[:, None, :]).reshape(-1, 2)
        x, y = to_native(pts[:, 0], pts[:, 1])
        out[cid] = Polygon(zip(x, y))
    return out


def _native_reference(b, src_crs, grid, res, unwrap=None, forward=None):
    """``{cell id: (count, sum, m2, min, max, smallest pair weight, partial pairs)}``: the valid pixels shared over the
    cells by exact overlap area in the raster's CRS (shapely), weight = overlap / pixel area; a partial pair is one
    with a weight below 1 (a pixel on the cell's edge)."""
    with _open(b) as ds:
        band = ds.read(1).astype("float64").ravel()
        mask = band_valid_mask(ds, 1).ravel()
        quads, _, _ = _pixel_quads(ds)
        cells = _native_cells(ds, src_crs, grid, res, unwrap, forward)
    quads, vals = quads[mask], band[mask]
    px_area = shapely.area(quads[0])
    tree = STRtree(quads)
    ref = {}
    for cid, poly in cells.items():
        idx = tree.query(poly, predicate="intersects")
        if idx.size == 0:
            continue
        w = shapely.area(shapely.intersection(quads[idx], poly)) / px_area
        keep = w > 0
        if not keep.any():
            continue
        w, v = w[keep], vals[idx][keep]
        n, s = w.sum(), (w * v).sum()
        partial = int((w < 1.0 - 1e-9).sum())
        ref[cid] = (
            n,
            s,
            (w * (v - s / n) ** 2).sum(),
            v.min(),
            v.max(),
            w.min(),
            partial,
        )
    return ref


def _centroid_reference(b, src_crs, grid, res):
    """``{cell id: (count, sum)}`` of the valid pixels binned by their own centres moved to the grid CRS."""
    with _open(b) as ds:
        band = ds.read(1).astype("float64")
        rr, cc = np.nonzero(band_valid_mask(ds, 1))
        x, y = ds.transform * (cc + 0.5, rr + 0.5)
    gx, gy = get_transformer(src_crs, 27700 if grid == "bng" else 4326).transform(x, y)
    out = {}
    for lon, lat, v in zip(gx, gy, band[rr, cc]):
        if grid == "h3":
            cid = h3.str_to_int(h3.latlng_to_cell(lat, lon, res))
        elif grid == "quadbin":
            cid = int(_quadbin.point_as_cell(lon, lat, res))
        else:
            raw = _bng.point_to_cell_id(lon, lat, res)
            if not _bng.is_valid(raw):
                continue
            cid = _bng.format(raw)
        n, s = out.get(cid, (0, 0.0))
        out[cid] = (n + 1, s + v)
    return out


def _partial(b, grid, res, coverage="sparse", assignment="covering", **kw):
    with _open(b) as ds:
        (band,) = gridagg.raster_to_grid_partial(
            ds, res, grid, coverage=coverage, assignment=assignment, **kw
        )
    return {c["cellID"]: c for c in band}


# --- covering against the exact reference ----------------------------------------------------------------------------

# A pair below this share of a pixel is a sliver: the library and the reference may disagree on whether it exists.
_SLIVER = 1e-6
# The library walks a cell edge until its chords stay within 1e-4 of the shorter of the edge and the pixel side
# (gridagg._NATIVE_EDGE_TOLERANCE); the reference walks 64 points. The weight of a pixel on a cell's edge agrees to
# about that fraction, so a cell's count to that fraction times its edge pixels.
_W_TOL = 2e-4

COVERING_CASES = {
    # label: (raster, grid, resolution) -- the comment says which covering path the library takes
    "hrap-h3-r7": (
        "hrap",
        "h3",
        7,
    ),  # vectorised: res-7 cells (5.2 km2) are smaller than the 18 km2 pixels
    "hrap-h3-r5": ("hrap", "h3", 5),  # per-pixel loop: cells larger than the pixels
    "utm-h3-r12": ("utm", "h3", 12),  # vectorised (HLS-like 30 m)
    "utm-h3-r10": ("utm", "h3", 10),  # per-pixel loop
    "utm-rotated-h3-r12": (
        "utm-rotated",
        "h3",
        12,
    ),  # rotated geotransform: per-pixel loop
    "utm-rotated-h3-r10": ("utm-rotated", "h3", 10),
    "hrap-quadbin-z12": ("hrap", "quadbin", 12),
    "utm-quadbin-z17": (
        "utm",
        "quadbin",
        17,
    ),  # cells larger than pixels: interior pixels take weight 1
    "utm-quadbin-z20": ("utm", "quadbin", 20),  # cells about the pixel's size
    "utm-rotated-quadbin-z17": ("utm-rotated", "quadbin", 17),
    "norfolk-4326-bng-r3": (
        "norfolk-4326",
        "bng",
        3,
    ),  # 1 km cells over ~340 x 555 m pixels
    "norfolk-4326-bng-r4": (
        "norfolk-4326",
        "bng",
        4,
    ),  # 100 m cells, smaller than the pixels
    "norfolk-utm31-bng-r3": ("norfolk-utm31", "bng", 3),
    "across-180-h3-r8": ("across-180", "h3", 8),  # vectorised, across the antimeridian
    "across-180-h3-r5": ("across-180", "h3", 5),
    "near-180-h3-r3": (
        "near-180",
        "h3",
        3,
    ),  # cells across the antimeridian, the raster not
    "pole-h3-r7": ("pole", "h3", 7),  # vectorised, the pole cell included
    "pole-h3-r3": ("pole", "h3", 3),
    "across-180-quadbin-z12": ("across-180", "quadbin", 12),
    "sinu-quadbin-z4": (
        "sinu",
        "quadbin",
        4,
    ),  # cells far larger than the pixels, edges bent
    "sinu-h3-r3": ("sinu", "h3", 3),
}


@pytest.mark.parametrize("case", list(COVERING_CASES))
def test_covering_matches_the_exact_native_reference(case):
    raster, grid, res = COVERING_CASES[case]
    b, src_crs = RASTERS[raster]
    ref = _native_reference(b, src_crs, grid, res)
    lib = _partial(b, grid, res)
    real_ref = {c for c, r in ref.items() if r[0] > _SLIVER}
    real_lib = {c for c, r in lib.items() if r["count"] > _SLIVER}
    assert real_ref <= set(
        lib
    ), f"{case}: cells missing: {sorted(real_ref - set(lib))[:5]}"
    assert real_lib <= set(
        ref
    ), f"{case}: cells the reference lacks: {sorted(real_lib - set(ref))[:5]}"
    for cid in real_ref:
        n, s, m2, lo, hi, w_min, partial = ref[cid]
        got = lib[cid]
        tol = _W_TOL * max(partial, 1)
        assert got["count"] == pytest.approx(n, abs=tol), (case, cid)
        # the weighted mean, against the value range (1 to 17)
        assert got["sum"] / got["count"] == pytest.approx(s / n, abs=17.0 * tol / n), (
            case,
            cid,
        )
        if w_min > _SLIVER:
            assert (got["min"], got["max"]) == (lo, hi), (case, cid)


@pytest.mark.parametrize(
    "case", ["hrap-h3-r7", "utm-h3-r12", "utm-quadbin-z17", "norfolk-4326-bng-r3"]
)
def test_each_valid_pixel_is_shared_out_whole(case):
    # the cells of a grid tile the plane, so the weights of one pixel sum to 1: the total count is the valid pixels
    raster, grid, res = COVERING_CASES[case]
    b, _ = RASTERS[raster]
    with _open(b) as ds:
        valid = band_valid_mask(ds, 1)
        n_valid = int(valid.sum())
        total = float(ds.read(1)[valid].astype("float64").sum())
    lib = _partial(b, grid, res)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n_valid, rel=1e-9)
    assert sum(c["sum"] for c in lib.values()) == pytest.approx(total, rel=1e-9)


@pytest.mark.parametrize("case", ["hrap-h3-r7", "utm-h3-r12"])
def test_the_vectorised_pass_equals_the_per_pixel_loop(case, monkeypatch):
    raster, grid, res = COVERING_CASES[case]
    b, _ = RASTERS[raster]
    calls = []
    real = gridagg._covering_band_h3_native

    def spy(*a, **k):
        calls.append(1)
        return real(*a, **k)

    monkeypatch.setattr(gridagg, "_covering_band_h3_native", spy)
    new = _partial(b, grid, res, coverage="complete")
    assert calls, "the vectorised native pass did not run"
    monkeypatch.setattr(gridagg, "_H3_COVERING_VECTORISED", False)
    old = _partial(b, grid, res, coverage="complete")
    assert set(new) == set(old)
    for cid, o in old.items():
        for field in gridagg.PARTIAL_FIELDS:
            if o[field] is None:
                assert new[cid][field] is None
            else:
                assert new[cid][field] == pytest.approx(
                    o[field], rel=1e-9, abs=1e-10
                ), (cid, field)


# --- complete coverage, centroid -------------------------------------------------------------------------------------


@pytest.mark.parametrize("assignment", ["covering", "centroid"])
@pytest.mark.parametrize(
    "case",
    ["hrap-h3-r7", "utm-rotated-h3-r10", "utm-quadbin-z17", "norfolk-4326-bng-r3"],
)
def test_complete_coverage_adds_the_cells_the_raster_overlaps(case, assignment):
    raster, grid, res = COVERING_CASES[case]
    b, src_crs = RASTERS[raster]
    with _open(b) as ds:
        outline = _raster_outline(ds)
        px_area = abs(ds.transform.determinant)
        cells = _native_cells(ds, src_crs, grid, res)
    overlap = {
        c: shapely.area(shapely.intersection(p, outline)) / px_area
        for c, p in cells.items()
    }
    lib = _partial(b, grid, res, coverage="complete", assignment=assignment)
    assert {c for c, a in overlap.items() if a > _SLIVER} <= set(lib)
    assert set(lib) <= {c for c, a in overlap.items() if a > 0}
    sparse = _partial(b, grid, res, coverage="sparse", assignment=assignment)
    for cid, c in lib.items():
        if cid not in sparse:
            assert c["count"] == 0.0 and c["sum"] is None


@pytest.mark.parametrize(
    "raster,grid,res",
    [
        ("hrap", "h3", 7),
        ("hrap", "h3", 9),
        ("utm-rotated", "h3", 12),
        ("utm", "quadbin", 20),
        ("norfolk-4326", "bng", 4),
        ("norfolk-utm31", "bng", 3),
    ],
)
def test_centroid_bins_each_valid_pixel_by_its_own_centre(raster, grid, res):
    b, src_crs = RASTERS[raster]
    ref = _centroid_reference(b, src_crs, grid, res)
    lib = _partial(b, grid, res, assignment="centroid")
    assert set(lib) == set(ref)
    for cid, (n, s) in ref.items():
        assert lib[cid]["count"] == n
        assert lib[cid]["sum"] == pytest.approx(s, rel=1e-12)


def test_a_crs_override_grids_a_crsless_raster_in_that_crs():
    b, src_crs = RASTERS["hrap"]
    with _open(b) as ds:
        bare = _tif(ds.read(1), None, ds.transform)
    for assignment in ("covering", "centroid"):
        want = _partial(b, "h3", 7, coverage="complete", assignment=assignment)
        got = _partial(
            bare, "h3", 7, coverage="complete", assignment=assignment, crs=HRAP
        )
        assert got == want


def test_a_masked_source_keeps_its_mask():
    import rasterio

    data = _ramp(12, 14, holes=False)
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(
                driver="GTiff",
                width=14,
                height=12,
                count=1,
                dtype="float32",
                crs=HRAP,
                transform=from_origin(_HX, _HY, 4762.5, 4762.5),
            ) as dst:
                dst.write(data.astype("float32"), 1)
                m = np.full((12, 14), 255, dtype="uint8")
                m[3:6, :] = 0
                dst.write_mask(m)
            b = mf.read()
    expected = float(data[m > 0].sum())
    for assignment in ("covering", "centroid"):
        lib = _partial(b, "h3", 7, assignment=assignment)
        assert sum(c["count"] for c in lib.values()) == pytest.approx(
            int((m > 0).sum()), rel=1e-9
        )
        assert sum(c["sum"] for c in lib.values()) == pytest.approx(expected, rel=1e-9)


# --- grid-native rasters, and rasters the native frame cannot hold ---------------------------------------------------


def test_a_grid_native_raster_never_enters_the_native_frame(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("native frame built for a grid-native raster")

    monkeypatch.setattr(gridagg, "_grid_native", boom)
    ll = _tif(_ramp(10, 12), "EPSG:4326", from_origin(-93.70, 42.05, 0.001, 0.001))
    bng = _tif(_ramp(8, 8), "EPSG:27700", from_origin(600000.0, 320000.0, 100.0, 100.0))
    for assignment in ("covering", "centroid"):
        assert _partial(ll, "h3", 12, coverage="complete", assignment=assignment)
        assert _partial(ll, "quadbin", 18, coverage="complete", assignment=assignment)
        assert _partial(bng, "bng", 4, coverage="complete", assignment=assignment)


_SEAM_3857 = 20037508.342789244
# EPSG:3857 rasters whose east edge sits on its seam (180E), at 2 to 156 km pixels, and a zoom-2 world raster
_SEAM_RASTERS = {
    f"seam-{int(px / 1000)}km": _tif(
        _ramp(10, 10, holes=False),
        "EPSG:3857",
        from_origin(_SEAM_3857 - px * 10, 5.6e6, px, px),
    )
    for px in (2000.0, 20000.0, 50000.0, 156543.0)
}
_SEAM_RASTERS["world-z2"] = _tif(
    _ramp(256, 256, holes=False),
    "EPSG:3857",
    from_origin(-_SEAM_3857, _SEAM_3857, 2 * _SEAM_3857 / 256, 2 * _SEAM_3857 / 256),
)
_SEAM_CASES = [
    (name, grid, res)
    for name in _SEAM_RASTERS
    for grid, res in (
        [("h3", 0), ("h3", 1), ("h3", 2), ("quadbin", 3)]
        if name == "world-z2"
        else [("h3", 1), ("h3", 2), ("h3", 3), ("h3", 4), ("quadbin", 4)]
    )
]


@pytest.mark.parametrize("assignment", ["covering", "centroid"])
@pytest.mark.parametrize("name,grid,res", _SEAM_CASES)
def test_cells_across_the_crs_seam_are_cut_there(
    name, grid, res, assignment, monkeypatch
):
    # EPSG:3857 wraps at 180E: a cell across it is cut on that meridian and each piece moved on its own side, so the
    # raster grids natively at any pixel size (the warp gave no cells, wrong totals or a GEOS error here)
    def no_warp(*a, **k):
        raise AssertionError("the warp ran")

    monkeypatch.setattr(gridagg, "_warp_to_4326_if_needed", no_warp)
    b = _SEAM_RASTERS[name]
    with _open(b) as ds:
        data = ds.read(1).astype("float64")
    lib = _partial(b, grid, res, coverage="complete", assignment=assignment)
    # every pixel shared out whole, once
    assert sum(c["count"] for c in lib.values()) == pytest.approx(data.size, rel=1e-6)
    assert sum(c["sum"] or 0.0 for c in lib.values()) == pytest.approx(
        data.sum(), rel=1e-6
    )
    if assignment == "centroid":
        ref = _centroid_reference(b, "EPSG:3857", grid, res)
        assert {c: r["count"] for c, r in lib.items() if r["count"]} == {
            c: n for c, (n, _) in ref.items()
        }


def test_the_per_pixel_pass_holds_a_bounded_number_of_pairs(monkeypatch):
    # memory: the pass shares pixels out in batches of at most _NATIVE_LOOP_PAIRS (cell, pixel) pairs (plus the last
    # pixel's), and the batching does not change the states
    b, _ = RASTERS["utm"]
    want = _partial(b, "h3", 10, coverage="complete")
    sizes = []
    real = gridagg._native_pair_weights

    def spy(frame, cells, pair_cell, pair_pix, *a, **k):
        sizes.append(len(pair_cell))
        return real(frame, cells, pair_cell, pair_pix, *a, **k)

    monkeypatch.setattr(gridagg, "_native_pair_weights", spy)
    monkeypatch.setattr(gridagg, "_NATIVE_LOOP_PAIRS", 40)
    got = _partial(b, "h3", 10, coverage="complete")
    assert len(sizes) > 3 and max(sizes) <= 40 + 20
    assert set(got) == set(want)
    for cid, w in want.items():
        for field in gridagg.PARTIAL_FIELDS:
            if w[field] is None:
                assert got[cid][field] is None
            else:
                assert got[cid][field] == pytest.approx(w[field], rel=1e-12, abs=1e-12)


@pytest.mark.parametrize(
    "box,res,cell",
    [
        # a 30 m box inside a res-9 cell in Iowa: h3's overlap polyfill returns only a neighbour
        (
            (
                -93.52824545164846,
                41.96729256462714,
                -93.52787375537152,
                41.96756995970309,
            ),
            9,
            617664615818723327,
        ),
        # a box inside a res-5 cell near 180W: the same call returns only a neighbour
        (
            (
                -179.6341563941033,
                44.850711029530416,
                -179.606921728288,
                44.87008132933918,
            ),
            5,
            599583555923738623,
        ),
    ],
    ids=["small-box-iowa", "near-180w"],
)
def test_candidate_cells_hold_the_cell_around_a_box(box, res, cell):
    from h3.api import basic_int as h3i

    w, s, e, n = box
    assert h3i.latlng_to_cell((s + n) / 2, (w + e) / 2, res) == cell
    raw = h3i.polygon_to_cells_experimental(
        h3i.LatLngPoly([(s, w), (n, w), (n, e), (s, e)]), res, contain="overlap"
    )
    assert cell not in raw  # the h3 call this works around
    assert cell in gridagg._box_cells([box], res, "h3")


@pytest.mark.parametrize(
    "key,crs",
    [("r3_8v", "EPSG:32626"), ("r3_7v", "EPSG:3413"), ("r13_8v", "EPSG:32626")],
)
def test_the_weight_floor_holds_for_distorted_rings_in_a_projected_crs(key, crs):
    # a real distorted H3 ring moved into a projected CRS (walked into up to ~40 vertices, slightly concave), and a
    # 25 x 25 grid of boxes 1.5x the cell's extent: the kernel matches rational arithmetic to 1e-14 of a box, and
    # the pairs above gridagg._COVER_WEIGHT_FLOOR are exactly those that overlap
    from .test_core_gridagg_vectorized import _DISTORTED, _box_grid, _exact_overlap

    cell = _DISTORTED[key]
    res = h3.get_resolution(cell)
    lat, lon = h3.cell_to_latlng(cell)
    x0, y0 = get_transformer(4326, crs).transform(lon, lat)
    px = 1.5 * 2 * h3.average_hexagon_edge_length(res, "m")
    with _open(_tif(np.zeros((8, 8)), crs, from_origin(x0, y0, px, px))) as ds:
        vx, vy, _ = gridagg._native_rings(
            gridagg._Frame(ds, crs, "h3"), [h3.str_to_int(cell)]
        )
    ring = list(zip(vx[0], vy[0]))
    boxes = _box_grid(ring, 25, 0.1)
    got = gridagg._box_overlap_area(vx, vy, np.zeros(len(boxes), np.intp), *boxes.T)
    pa = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    exact = np.array([float(_exact_overlap(ring, tuple(b))) for b in boxes])
    assert (exact > 0).sum() > 100 and (exact == 0).sum() > 100
    assert (np.abs(got - exact) / pa).max() <= 1e-14
    assert ((got / pa > gridagg._COVER_WEIGHT_FLOOR) == (exact > 0)).all()


def test_a_bent_edge_is_walked_to_the_tolerance():
    # the walk takes its points from the moved midpoint's distance from the chord's midpoint, which bounds the bend
    # where the moved edge also runs unevenly along its chord: a sinusoidal quadbin z4 cell holds ~3,000 pixels and
    # its count is within 3e-3 of a pixel of the exact one
    b, crs = RASTERS["sinu"]
    ref = _native_reference(b, crs, "quadbin", 4)
    lib = _partial(b, "quadbin", 4)
    assert max(abs(lib[c]["count"] - r[0]) for c, r in ref.items()) <= 3e-3


# +proj=merc +lon_0=180: its seam is the Greenwich meridian, across England
_MERC180 = "+proj=merc +lon_0=180 +datum=WGS84 +units=m +no_defs"
_MERC180_SEAM = get_transformer(4326, _MERC180).transform(-1e-9, 0.0)[0]
_GREENWICH = _tif(
    _ramp(20, 20),
    _MERC180,
    from_origin(
        _MERC180_SEAM - 20 * 2000.0,
        get_transformer(4326, _MERC180).transform(-0.5, 52.0)[1],
        2000.0,
        2000.0,
    ),
)


@pytest.mark.parametrize("res", [2, 3])
def test_bng_cells_across_the_raster_crs_seam_match_the_exact_reference(res):
    # BNG cells (EPSG:27700 squares) across the raster CRS's seam: the cut runs on the seam meridian, found in
    # lon/lat, not in eastings. The reference moves each cell continuously past the seam (closed form: a moved x
    # below 0 is on the far side, one turn east of the raster)
    def unwrap(x):
        return np.where(x < 0.0, x + 2.0 * _MERC180_SEAM, x)

    # one operation both ways (for this CRS pair PROJ picks different ones per direction, 122 m apart), as the library
    from databricks.labs.gbx.core.crs import get_operation_transformer

    forward = get_operation_transformer(_MERC180, 27700, (-1.0, 51.5, 0.5, 52.5))
    ref = _native_reference(_GREENWICH, _MERC180, "bng", res, unwrap, forward)
    lib = _partial(_GREENWICH, "bng", res)
    assert {c for c, r in ref.items() if r[0] > _SLIVER} <= set(lib)
    for cid, r in ref.items():
        if r[0] > _SLIVER:
            assert lib[cid]["count"] == pytest.approx(
                r[0], abs=_W_TOL * max(r[6], 1)
            ), cid
            assert lib[cid]["sum"] == pytest.approx(
                r[1], rel=1e-4, abs=17 * _W_TOL * max(r[6], 1)
            ), cid
    # the cut keeps 1e-6 degrees (~0.1 m) clear of the seam on each side: 7e-5 of a 2 km pixel along it
    with _open(_GREENWICH) as ds:
        data = ds.read(1)[band_valid_mask(ds, 1)].astype("float64")
    assert sum(c["count"] for c in lib.values()) == pytest.approx(data.size, rel=1e-5)
    assert sum(c["sum"] for c in lib.values()) == pytest.approx(data.sum(), rel=1e-5)


@pytest.mark.parametrize(
    "name,x0,width",
    [
        ("world", -_SEAM_3857, 128),
        ("west-half", -_SEAM_3857, 64),
        ("east-half", 0.0, 64),
    ],
)
@pytest.mark.parametrize("z", [0, 1])
def test_quadbin_cells_half_the_globe_wide_keep_their_way_round(name, x0, width, z):
    # a quadbin z1 cell is 180 degrees wide, z0 360: its edges are walked east, never the "short way". In EPSG:3857 a
    # z1 tile of a 128 x 128 world raster is exactly 64 x 64 pixels
    px = 2 * _SEAM_3857 / 128
    b = _tif(np.ones((128, width)), "EPSG:3857", from_origin(x0, _SEAM_3857, px, px))
    lib = _partial(b, "quadbin", z)
    got = {
        quadbin.cell_to_tile(c): r["count"]
        for c, r in lib.items()
        if r["count"] > _SLIVER
    }
    if z == 0:
        want = {(0, 0, 0): 128.0 * width}
    else:
        cols = {"world": (0, 1), "west-half": (0,), "east-half": (1,)}[name]
        want = {(x, y, 1): 4096.0 for x in cols for y in (0, 1)}
    assert set(got) == set(want)
    for tile, n in want.items():
        assert got[tile] == pytest.approx(n, abs=1e-6), tile


_OMERC = (
    "+proj=omerc +lat_0=4 +lonc=102.25 +alpha=323.0257905 +k=0.99984 +x_0=804671 +y_0=0 +no_uoff "
    "+gamma=323.1301023611111 +ellps=GRS80 +units=m +no_defs"
)


def test_a_seam_that_is_no_meridian_is_refused_with_a_fix():
    # an 8,000 km oblique Mercator raster: its seam is not a meridian, so a cell across it cannot be cut there
    b = _tif(np.ones((40, 40)), None, from_origin(-12e6, 12e6, 6e5, 6e5))
    with pytest.raises(ValueError) as err:
        _partial(b, "h3", 1, crs=_OMERC)
    msg = str(err.value)
    assert msg.startswith("gbx_rst_h3_rastertogrid_partial:") and "Fix:" in msg
    assert "PROJCS" not in msg and len(msg) < 400


def test_points_that_do_not_transform_take_the_warp():
    # a Mollweide raster whose padded outline leaves the globe's edge (no lon/lat there): covering takes the warp to
    # EPSG:4326, as before; centroid needs no outline and stays native
    moll = "+proj=moll +lon_0=0 +datum=WGS84 +units=m +no_defs"
    edge = get_transformer(4326, moll).transform(180.0, 1.0)[0]
    b = _tif(
        _ramp(10, 10, holes=False),
        moll,
        from_origin(edge - 10 * 20000.0 - 5000.0, 100000.0, 20000.0, 20000.0),
    )
    with _open(b) as ds:
        warped = gridagg._warp_to_4326_if_needed(ds, None)
    assert _partial(b, "h3", 5) == _partial(warped, "h3", 5)
    ref = _centroid_reference(b, moll, "h3", 5)
    lib = _partial(b, "h3", 5, assignment="centroid")
    assert {c: r["count"] for c, r in lib.items()} == {
        c: n for c, (n, _) in ref.items()
    }
