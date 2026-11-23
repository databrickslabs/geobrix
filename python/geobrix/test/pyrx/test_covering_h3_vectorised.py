"""The vectorised H3 covering path (cells smaller than pixels) equals the per-pixel loop (spec §5.3)."""

import contextlib

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import gridagg


def _bytes(data, crs, origin, px, nodata=-9999.0):
    h, w = data.shape
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=w,
            height=h,
            count=1,
            dtype="float32",
            crs=crs,
            transform=from_origin(origin[0], origin[1], px, px),
            nodata=nodata,
        ) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


@contextlib.contextmanager
def _open(b):
    # the MemoryFile reads its bytes in place: it, and so the bytes, must outlive every read
    with MemoryFile(b) as mf, mf.open() as ds:
        yield ds


def _ramp(h, w, holes=True):
    y, x = np.mgrid[0:h, 0:w].astype("float64")
    a = 1.0 + (x * 7 + y * 3) % 17
    if holes:
        a[(x + 2 * y) % 5 == 0] = -9999.0
    return a


LL = _bytes(
    _ramp(10, 12), "EPSG:4326", (-93.70, 42.05), 0.001
)  # ~83 m x 111 m pixels at 42 N
UTM = _bytes(
    _ramp(10, 10), "EPSG:32615", (446000.0, 4650000.0), 30.0
)  # HLS-like, warped to 4326 inside


def _run(b, res, fn, vectorised, monkeypatch, **kw):
    monkeypatch.setattr(gridagg, "_H3_COVERING_VECTORISED", vectorised)
    with _open(b) as ds:
        return fn(ds, res, "h3", **kw)


def _as_map(band, key="measure"):
    return {c["cellID"]: c[key] for c in band}


def _assert_same(new, old):
    assert len(new) == len(old)
    for nb, ob in zip(new, old):
        nm, om = _as_map(nb), _as_map(ob)
        assert set(nm) == set(om)
        for cid, v in om.items():
            if v is None:
                assert nm[cid] is None
            else:
                assert nm[cid] == pytest.approx(v, rel=1e-9, abs=1e-12)


@pytest.mark.parametrize(
    "b,res", [(LL, 11), (LL, 12), (UTM, 12)], ids=["ll_r11", "ll_r12", "utm_r12"]
)
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
def test_partial_states_equal_the_per_pixel_loop(b, res, coverage, monkeypatch):
    kw = dict(coverage=coverage, assignment="covering")
    new = _run(b, res, gridagg.raster_to_grid_partial, True, monkeypatch, **kw)
    old = _run(b, res, gridagg.raster_to_grid_partial, False, monkeypatch, **kw)
    for field in gridagg.PARTIAL_FIELDS:
        _assert_same(
            [
                [{"cellID": c["cellID"], "measure": c[field]} for c in band]
                for band in new
            ],
            [
                [{"cellID": c["cellID"], "measure": c[field]} for c in band]
                for band in old
            ],
        )


@pytest.mark.parametrize("stat", gridagg._AGGS)
def test_every_statistic_equals_the_per_pixel_loop(stat, monkeypatch):
    kw = dict(coverage="sparse", assignment="covering")
    new = _run(
        LL, 12, gridagg.raster_to_grid_single, True, monkeypatch, stat=stat, **kw
    )
    old = _run(
        LL, 12, gridagg.raster_to_grid_single, False, monkeypatch, stat=stat, **kw
    )
    _assert_same(new, old)


def test_the_vectorised_path_is_taken_only_for_finer_cells(monkeypatch):
    calls = []
    real = gridagg._covering_band_h3_vectorised

    def spy(*a, **k):
        calls.append(a[2])
        return real(*a, **k)

    monkeypatch.setattr(gridagg, "_covering_band_h3_vectorised", spy)
    with _open(LL) as ds:
        gridagg.raster_to_grid_partial(
            ds, 12, "h3", coverage="sparse", assignment="covering"
        )
        gridagg.raster_to_grid_partial(
            ds, 9, "h3", coverage="sparse", assignment="covering"
        )  # cells > pixels
        gridagg.raster_to_grid_partial(
            ds, 12, "h3", coverage="sparse", assignment="centroid"
        )
    assert calls == [12]


def test_weights_of_each_valid_pixel_sum_to_one(monkeypatch):
    # area conservation: one valid pixel, then all pixels valid
    one = np.full((6, 6), -9999.0)
    one[2, 3] = 5.0
    for data, n_valid in ((one, 1), (np.ones((6, 6)), 36)):
        b = _bytes(data, "EPSG:4326", (-93.70, 42.05), 0.001)
        (band,) = _run(
            b,
            12,
            gridagg.raster_to_grid_partial,
            True,
            monkeypatch,
            coverage="sparse",
            assignment="covering",
        )
        assert sum(c["count"] for c in band) == pytest.approx(n_valid, rel=1e-9)


def test_vectorised_states_merge_across_subtiles(monkeypatch):
    # Review Focus 2: summing count and sum per cell over two sub-tiles gives the whole tile's states, seams included
    data = _ramp(10, 12)
    whole = _bytes(data, "EPSG:4326", (-93.70, 42.05), 0.001)
    left = _bytes(data[:, :6], "EPSG:4326", (-93.70, 42.05), 0.001)
    right = _bytes(data[:, 6:], "EPSG:4326", (-93.70 + 6 * 0.001, 42.05), 0.001)
    kw = dict(coverage="sparse", assignment="covering")
    (w,) = _run(whole, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw)
    merged = {}
    for part in (left, right):
        (band,) = _run(
            part, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw
        )
        for c in band:
            n, s = merged.get(c["cellID"], (0.0, 0.0))
            merged[c["cellID"]] = (n + c["count"], s + c["sum"])
    assert set(merged) == {c["cellID"] for c in w}
    for c in w:
        assert merged[c["cellID"]][0] == pytest.approx(c["count"], rel=1e-9)
        assert merged[c["cellID"]][1] == pytest.approx(c["sum"], rel=1e-9)


def test_no_valid_pixel_gives_no_cell(monkeypatch):
    b = _bytes(np.full((5, 5), -9999.0), "EPSG:4326", (-93.70, 42.05), 0.001)
    (band,) = _run(
        b,
        12,
        gridagg.raster_to_grid_partial,
        True,
        monkeypatch,
        coverage="sparse",
        assignment="covering",
    )
    assert band == []


def _spy(monkeypatch, name):
    """Record the first argument of every call of ``gridagg.<name>``, and call through."""
    calls = []
    real = getattr(gridagg, name)

    def spy(*a, **k):
        calls.append(a[0] if a else None)
        return real(*a, **k)

    monkeypatch.setattr(gridagg, name, spy)
    return calls


def test_no_valid_pixel_builds_no_candidate(monkeypatch):
    built = _spy(monkeypatch, "_h3_candidate_cells")
    hexes = _spy(monkeypatch, "_h3_hexagons")
    b = _bytes(np.full((5, 5), -9999.0), "EPSG:4326", (-93.70, 42.05), 0.001)
    kw = dict(coverage="sparse", assignment="covering")
    assert _run(b, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw) == [[]]
    assert built == [] and hexes == []


@pytest.mark.parametrize("coverage", ["sparse", "complete"])
def test_mostly_nodata_tile_equals_the_loop_and_builds_few_cells(coverage, monkeypatch):
    # a 3 x 3 block of valid pixels in a 40 x 40 NoData tile: only the block's padded bbox is polyfilled
    data = np.full((40, 40), -9999.0)
    data[30:33, 5:8] = _ramp(3, 3, holes=False)
    sparse = _bytes(data, "EPSG:4326", (-93.70, 42.05), 0.001)
    full = _bytes(_ramp(40, 40, holes=False), "EPSG:4326", (-93.70, 42.05), 0.001)
    kw = dict(coverage=coverage, assignment="covering")
    hexes = _spy(monkeypatch, "_h3_hexagons")
    new = _run(sparse, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw)
    n_sparse = sum(len(c) for c in hexes)
    hexes.clear()
    _run(full, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw)
    n_full = sum(len(c) for c in hexes)
    old = _run(sparse, 12, gridagg.raster_to_grid_partial, False, monkeypatch, **kw)
    for field in gridagg.PARTIAL_FIELDS:
        _assert_same(
            [[{"cellID": c["cellID"], "measure": c[field]} for c in bd] for bd in new],
            [[{"cellID": c["cellID"], "measure": c[field]} for c in bd] for bd in old],
        )
    assert 0 < n_sparse * 20 < n_full


@pytest.mark.parametrize("stat", ["partial", "median"])
def test_chunk_boundaries_do_not_change_the_states(stat, monkeypatch):
    # 7 cells per chunk: every cell's pixels and the seams between chunks go through the merge of chunk outputs
    monkeypatch.setattr(gridagg, "_H3_VEC_CHUNK_CELLS", 7)
    kw = dict(coverage="sparse", assignment="covering")
    if stat == "partial":
        fn, field_sets = gridagg.raster_to_grid_partial, gridagg.PARTIAL_FIELDS
    else:
        fn, field_sets = gridagg.raster_to_grid_single, ("measure",)
        kw["stat"] = "median"
    new = _run(LL, 12, fn, True, monkeypatch, **kw)
    old = _run(LL, 12, fn, False, monkeypatch, **kw)
    for field in field_sets:
        _assert_same(
            [[{"cellID": c["cellID"], "measure": c[field]} for c in bd] for bd in new],
            [[{"cellID": c["cellID"], "measure": c[field]} for c in bd] for bd in old],
        )


def test_a_rotated_geotransform_keeps_the_per_pixel_loop(monkeypatch):
    from rasterio.transform import Affine

    data = _ramp(8, 8).astype("float32")
    t = Affine(0.001, 0.0002, -93.70, 0.0002, -0.001, 42.05)  # rotation terms b, d != 0
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=8,
            height=8,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=t,
            nodata=-9999.0,
        ) as dst:
            dst.write(data, 1)
        b = mf.read()
    with _open(b) as ds:
        assert not gridagg._use_h3_vectorised(ds, 12, ds.transform.to_gdal())
    calls = _spy(monkeypatch, "_covering_band_h3_vectorised")
    kw = dict(coverage="sparse", assignment="covering")
    (band,) = _run(b, 12, gridagg.raster_to_grid_partial, True, monkeypatch, **kw)
    assert calls == [] and band  # the loop ran and gave cells


@pytest.mark.parametrize("res", [-1, 16])
def test_the_gate_refuses_an_out_of_range_resolution(res):
    with _open(LL) as ds:
        with pytest.raises(ValueError, match="resolution"):
            gridagg._use_h3_vectorised(ds, res, ds.transform.to_gdal())
