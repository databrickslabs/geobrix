"""Pure-function tests for core/agg.py reducers (Spark-free) + Spark grouped-agg tests."""

import numpy as np
import pytest
import shapely.wkb
from pyspark.sql.types import (
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import agg
from databricks.labs.gbx.pyrx.functions import (
    _combineavg_agg_sql_udf,
    _combineavg_agg_udf,
    _combineavg_bytes,
    _derivedband_agg_udf,
    _frombands_agg_udf,
    _frombands_bytes,
    _merge_agg_udf,
    _merge_bytes,
)


def _ras(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    """GTiff bytes from a 2-D or 3-D numpy array with a known georef."""
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


# --- merge_tiles ------------------------------------------------------------
def test_merge_tiles_union_extent():
    # Two adjacent 2x2 tiles side by side -> 2x4 mosaic spanning the union.
    left = _ras(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0, px=1.0)
    right = _ras(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        assert ds.width == 4
        assert ds.height == 2
        b = ds.bounds
        assert b.left == pytest.approx(0.0)
        assert b.right == pytest.approx(4.0)


def test_merge_tiles_single_passthrough():
    one = _ras(np.array([[1, 2], [3, 4]]))
    assert agg.merge_tiles([one]) == one


def test_merge_tiles_overlap_last_wins():
    # Two 4x4 tiles overlapping in x=[2,4]: union mosaic is 4x6, overlap = cols
    # 2 and 3. Heavyweight MergeRasters builds a GDAL VRT (gdalbuildvrt), where
    # overlapping pixels take the LAST listed source. The order passed here is
    # [left, right], so the overlap must take the RIGHT tile's value (20), not
    # the left's (10) -- pre-fix rasterio defaults to first-wins and returns 10.
    left = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    right = _ras(np.full((4, 4), 20.0), ulx=2.0, uly=4.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        assert arr.shape == (4, 6)
        # Non-overlap left cols (0,1) -> 10 ; overlap cols (2,3) -> 20 (last wins)
        assert np.all(arr[:, 0:2] == 10.0)
        assert np.all(arr[:, 2:4] == 20.0)
        # Non-overlap right cols (4,5) -> 20
        assert np.all(arr[:, 4:6] == 20.0)


def test_merge_tiles_overlap_winner_order_invariant():
    # A Spark groupBy().agg() gives no row-arrival-order guarantee, so a last-wins
    # mosaic must not depend on the order tiles are passed. merge_tiles sorts by the
    # raw GTiff bytes, so one tile reliably wins the overlap whether it is listed
    # first or last.
    left = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    right = _ras(np.full((4, 4), 20.0), ulx=2.0, uly=4.0, px=1.0)
    out_lr = agg.merge_tiles([left, right])
    out_rl = agg.merge_tiles([right, left])
    # Bitwise-identical output regardless of input order.
    assert out_lr == out_rl
    with _serde.open_tile(out_lr) as ds:
        arr = ds.read(1)
        # Overlap cols (2,3) resolve to a single canonical winner regardless of order.
        overlap = arr[:, 2:4]
        winner = overlap.flat[0]
        assert winner in (10.0, 20.0)
        assert np.all(overlap == winner)


def test_merge_tiles_same_origin_overlap_winner_order_invariant():
    # The residual nondeterminism hole the content-byte sort closes: two tiles with
    # the SAME geotransform origin but different content fully overlap. A geotransform
    # -origin key cannot separate them (they tie on origin), so the old key fell back
    # to a per-open /vsimem/<uuid> description -- random, so the winner varied run to
    # run and the two tiers disagreed. Sorting on raw GTiff bytes is a total order with
    # no tie, so the winner is fixed and identical regardless of input order.
    a = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    b = _ras(np.full((4, 4), 20.0), ulx=0.0, uly=4.0, px=1.0)
    out_ab = agg.merge_tiles([a, b])
    out_ba = agg.merge_tiles([b, a])
    # Bitwise-identical regardless of order -- this is the case the origin key failed.
    assert out_ab == out_ba
    with _serde.open_tile(out_ab) as ds:
        arr = ds.read(1)
    # Fully overlapping tiles -> one constant wins everywhere (10.0 or 20.0).
    winner = arr.flat[0]
    assert winner in (10.0, 20.0)
    assert np.all(arr == winner)


# --- merge_tiles streaming path ------------------------------------


def _force_streaming(monkeypatch):
    """The merge streams whatever the budget: the estimate of what the in-RAM merge would hold is over any budget (the
    read cap, which a member to be decoded whole is judged by, stays as it is)."""
    monkeypatch.setattr(agg, "_in_ram_bytes", lambda *a, **k: 1 << 62)


def test_merge_tiles_refuses_a_mosaic_it_cannot_stream_before_building_it(monkeypatch):
    """Two tiles a continent apart make a mosaic of terabytes that is nearly all NoData. The size comes from the
    headers, and past the COG ceiling the merge stops with a Fix: instead of streaming for ever or allocating.
    """

    def never(_rasters):
        raise AssertionError("streamed a mosaic it cannot hold")

    monkeypatch.setattr(agg, "_merge_tiles_streaming", never)
    near = _ras(np.full((16, 16), 1.0), ulx=0.0, uly=0.0, px=1e-4, epsg=4326)
    far = _ras(np.full((16, 16), 2.0), ulx=120.0, uly=-60.0, px=1e-4, epsg=4326)
    with pytest.raises(ValueError, match=r"GiB.*Fix:"):
        agg.merge_tiles([near, far])


def test_merge_streaming_matches_in_ram(monkeypatch):
    """Streaming path produces same extent, band count, and pixel values as in-RAM path.

    Forces the streaming path (the in-RAM estimate is over any budget), then compares
    the result with the default in-RAM result: extent (within pixel tolerance),
    band count, and per-pixel values must all match.  Includes one overlapping tile
    pair to exercise last-wins determinism on both paths.
    """
    # Three adjacent tiles; tile_b overlaps tile_a at x=[1,2].
    # After byte-sorting the overlap winner is deterministic on both paths.
    tile_a = _ras(np.full((2, 2), 1.0), ulx=0.0, uly=2.0, px=1.0)
    tile_b = _ras(np.full((2, 2), 2.0), ulx=1.0, uly=2.0, px=1.0)
    tile_c = _ras(np.full((2, 2), 3.0), ulx=3.0, uly=2.0, px=1.0)
    tiles = [tile_a, tile_b, tile_c]

    # In-RAM path (default budget).
    in_ram_bytes = agg.merge_tiles(tiles)

    # Streaming path (the in-RAM estimate patched over any budget → always streams).
    _force_streaming(monkeypatch)
    streaming_bytes = agg.merge_tiles(tiles)

    assert streaming_bytes is not None, "streaming merge returned None"

    with MemoryFile(in_ram_bytes) as mf1, mf1.open() as ds1:
        with MemoryFile(streaming_bytes) as mf2, mf2.open() as ds2:
            assert ds1.count == ds2.count, "band count must match"
            # Extent must agree within one-pixel tolerance (COG may round the envelope).
            assert ds1.bounds.left == pytest.approx(ds2.bounds.left, abs=1.0)
            assert ds1.bounds.right == pytest.approx(ds2.bounds.right, abs=1.0)
            assert ds1.bounds.top == pytest.approx(ds2.bounds.top, abs=1.0)
            assert ds1.bounds.bottom == pytest.approx(ds2.bounds.bottom, abs=1.0)
            arr1 = ds1.read(1)
            arr2 = ds2.read(1)
            assert (
                arr1.shape == arr2.shape
            ), f"shape mismatch: {arr1.shape} vs {arr2.shape}"
            assert np.allclose(
                arr1, arr2, equal_nan=True
            ), "pixel values differ between in-RAM and streaming paths"


# ---------------------------------------------------------------------------
# Child-process script for test_merge_streaming_bounds_peak_rss.
# Runs in a fresh subprocess so ru_maxrss is not pre-inflated by pytest imports.
# ---------------------------------------------------------------------------
_MERGE_RSS_CHILD = """
import gc, platform, resource, sys

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx.core import agg

# Force the streaming path regardless of the runtime cgroup budget (the in-RAM estimate is over any budget).
agg._in_ram_bytes = lambda *a, **k: 1 << 62

W, H = 3000, 3000
tile_bytes_list = []
for row in range(2):
    for col in range(2):
        ulx = float(col * W)
        uly = float((row + 1) * H)
        val = float(row * 2 + col + 1)
        data = np.full((H, W), val, dtype="float32")
        profile = dict(
            driver="GTiff", width=W, height=H, count=1, dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(ulx, uly, 1.0, 1.0),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data[None])
            b = mf.read()
        tile_bytes_list.append(b)
        del data
        gc.collect()

# tile_bytes_list holds 4 compressed GTiff blobs (small); all large arrays freed.
gc.collect()
_scale = 1024 if platform.system() == "Linux" else 1
rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _scale

result = agg.merge_tiles(tile_bytes_list)

rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _scale
assert result is not None and len(result) > 0, "merge_tiles returned empty"
print(rss_after - rss_before, flush=True)
"""


def test_merge_streaming_bounds_peak_rss():
    """Streaming merge peak RSS delta stays below 60% of the full-mosaic size (subprocess proof).

    Runs in a fresh child process so ``ru_maxrss`` is not pre-inflated by pytest
    imports.  Four 3000×3000 float32 tiles form a 6000×6000 union (144 MB
    uncompressed).  With the budget forced to 1 byte, the streaming path must not
    allocate a full-mosaic array; peak RSS delta must stay below 60% of that union.

    A ``rasterio.merge``-style full-mosaic in-RAM merge would allocate ~144 MB and
    FAIL this threshold — that is the intended failure mode for a regression.
    """
    import subprocess

    try:
        import resource  # noqa: F401 — availability check only
    except ImportError:
        pytest.skip("resource module not available on this platform")

    proc = subprocess.run(
        [__import__("sys").executable, "-c", _MERGE_RSS_CHILD],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(f"merge RSS child exited {proc.returncode}:\n{proc.stderr[-2000:]}")

    delta_bytes = int(proc.stdout.strip())
    # Union decoded: 6000×6000 × 1 band × 4 bytes = 144 MB.
    union_uncompressed = (2 * 3000) * (2 * 3000) * 1 * np.dtype("float32").itemsize
    assert delta_bytes < 0.6 * union_uncompressed, (
        f"peak RSS delta {delta_bytes / 1e6:.1f} MB ≥ 60% of union "
        f"{union_uncompressed / 1e6:.0f} MB — possible full-mosaic allocation"
    )


def test_merge_streaming_error_propagates(monkeypatch):
    """Streaming path errors must propagate as the original exception, not IndexError.

    Pre-fix: ``_merge_tiles_streaming`` was called inside the size-gate's
    ``try/except Exception``, so any streaming failure (VRT build, cog_convert_file,
    disk-full) was caught, mislogged as "size-gate computation failed", and then fell
    through to the in-RAM path with empty ``datasets`` → ``IndexError`` at
    ``merge_ds[0]``, masking the real error.

    Post-fix: the try/except is scoped to size-estimation math only; the streaming
    return is outside the except so a streaming exception propagates to the caller.
    """
    tile_a = _ras(np.full((2, 2), 1.0), ulx=0.0, uly=2.0, px=1.0)
    tile_b = _ras(np.full((2, 2), 2.0), ulx=2.0, uly=2.0, px=1.0)

    _force_streaming(monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(agg, "cog_convert_file", boom)

    with pytest.raises(RuntimeError, match="boom"):
        agg.merge_tiles([tile_a, tile_b])


def test_reproject_dataset_target_res_honored():
    """_reproject_dataset with target_res produces output at the specified pixel size.

    Without target_res the output uses the natural transform resolution (which varies
    by CRS).  With target_res the output pixel size must snap exactly to the requested
    value — used by _merge_tiles_streaming to align CRS-mismatched tiles to the same
    grid.
    """
    data = np.ones((10, 10), dtype="float32")
    profile = dict(
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(500000.0, 5500000.0, 1000.0, 1000.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        tile_bytes = mf.read()

    with MemoryFile(tile_bytes) as mf:
        with mf.open() as src:
            # Without target_res: EPSG:32633 → EPSG:4326 gives natural ~deg resolution
            rep_mf_nat, rep_ds_nat = agg._reproject_dataset(src, "EPSG:4326")
            natural_xres = abs(rep_ds_nat.transform.a)
            rep_ds_nat.close()
            rep_mf_nat.close()

            # With target_res=(0.01, 0.01): output must use exactly 0.01 deg pixels
            rep_mf_snp, rep_ds_snp = agg._reproject_dataset(
                src, "EPSG:4326", target_res=(0.01, 0.01)
            )
            snapped_xres = abs(rep_ds_snp.transform.a)
            rep_ds_snp.close()
            rep_mf_snp.close()

    # Snapped pixel must be exactly the requested 0.01 deg.
    assert snapped_xres == pytest.approx(0.01, rel=1e-5)
    # Natural resolution (1000m UTM → deg ≈ 0.009 deg at 50°N) differs from 0.01.
    assert abs(natural_xres - 0.01) > 1e-4


@pytest.mark.perf
def test_merge_streaming_multicrs_produces_valid_output(monkeypatch):
    """Streaming merge with CRS-mismatched tiles returns valid single-CRS output.

    Two tiles with different CRS metadata are force-streamed.  The output must be
    non-empty, have a CRS, and contain at least some valid (non-nodata) pixels.
    This exercises the _reproject_dataset(target_res=...) code path added in Fix 5.
    """
    tile_a = _ras(np.full((5, 5), 1.0), ulx=0.0, uly=5.0, px=1.0, epsg=32633)

    data_b = np.full((5, 5), 2.0, dtype="float32")
    profile_b = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=from_origin(5.0, 5.0, 1.0, 1.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile_b) as dst:
            dst.write(data_b, 1)
        tile_b = mf.read()

    _force_streaming(monkeypatch)
    result = agg.merge_tiles([tile_a, tile_b])

    assert result is not None and len(result) > 0, "streaming merge must return bytes"
    with MemoryFile(result) as mf:
        with mf.open() as ds:
            assert ds.crs is not None, "output must have a CRS"
            arr = ds.read(1)
            nd = ds.nodata
            valid_pixels = arr[arr != nd] if nd is not None else arr.ravel()
            assert len(valid_pixels) > 0, "output must contain valid pixels"


@pytest.mark.parametrize("streamed", [False, True], ids=["in-ram", "streamed"])
def test_a_tile_in_another_crs_leaves_its_neighbours_pixels_alone(
    monkeypatch, streamed
):
    """A tile with no NoData, reprojected across a UTM zone boundary, is a rotated footprint inside a
    larger box: the box pixels that no source pixel reaches are not data, and the tile that folds last
    must not overwrite the valid pixels of the tile beside it with them."""
    from rasterio.warp import transform as warp_xy

    def tile(epsg, x, y, w, h, value):
        profile = dict(
            driver="GTiff",
            width=w,
            height=h,
            count=1,
            dtype="uint8",
            crs=f"EPSG:{epsg}",
            transform=from_origin(x, y, 100.0, 100.0),
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(np.full((1, h, w), value, dtype="uint8"))
            return mf.read()

    (x32,), (y32,) = warp_xy("EPSG:4326", "EPSG:32632", [11.0], [48.0])
    (x33,), (y33,) = warp_xy("EPSG:4326", "EPSG:32633", [12.0], [48.0])
    ref = tile(32632, x32, y32 + 20000, 900, 400, 100)  # in the reference CRS
    other = tile(32633, x33, y33 + 20000, 1000, 400, 200)  # reprojected
    assert (
        sorted([ref, other])[0] == ref
    ), "fixture: the reprojected tile must fold last"
    if streamed:
        _force_streaming(monkeypatch)

    with MemoryFile(agg.merge_tiles([ref, other])) as mf, mf.open() as ds:
        left, top = ds.transform.c, ds.transform.f
        col, row = round((x32 - left) / 100.0), round((top - (y32 + 20000)) / 100.0)
        beside = ds.read(1)[row : row + 400, col : col + 900]
    assert set(np.unique(beside)) == {
        100,
        200,
    }  # no zero fill inside the reference tile


def test_a_merge_across_crs_of_striped_tiles_logs_no_blockxsize_warning(caplog):
    """A striped GeoTIFF reports its strip as blockxsize; copied into the reprojected tile and the
    mosaic it made GDAL log "BLOCKXSIZE can only be used with TILED=YES" for each dataset.
    """
    import logging

    a = _ras(
        np.arange(16).reshape(4, 4), ulx=500000.0, uly=4000000.0, px=10.0, epsg=32633
    )
    b = _ras(
        np.arange(16).reshape(4, 4), ulx=500100.0, uly=4000000.0, px=10.0, epsg=32634
    )
    for merge in (agg.merge_tiles, agg._merge_tiles_streaming):
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            merge([a, b])
        noisy = [
            r.getMessage() for r in caplog.records if "BLOCKXSIZE" in r.getMessage()
        ]
        assert noisy == [], (merge.__name__, noisy)


def test_merge_small_unchanged():
    """Small 2-tile merge uses the in-RAM path (default budget) and returns the correct mosaic.

    Regression guard: the size gate must not break small merges.
    """
    left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
    right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
    out = agg.merge_tiles([left, right])
    assert out is not None
    with MemoryFile(out) as mf:
        with mf.open() as ds:
            assert ds.width == 4
            assert ds.height == 2
            arr = ds.read(1)
            # Left half should be left tile values; right half should be right tile values.
            assert np.allclose(arr[:, 0:2], [[1.0, 2.0], [3.0, 4.0]])
            assert np.allclose(arr[:, 2:4], [[5.0, 6.0], [7.0, 8.0]])


# --- the budget the merge and the combines work in ------------------------------------------------------------------


def _tile_in(arr, crs, ulx, uly, px, nodata=0):
    """A one-band uint8 tile in *crs*, with its pixel size *px* in that CRS's units."""
    arr = np.asarray(arr, dtype="uint8")
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=arr.shape[1],
            height=arr.shape[0],
            count=1,
            dtype="uint8",
            crs=crs,
            transform=from_origin(ulx, uly, px, px),
            nodata=nodata,
        ) as dst:
            dst.write(arr, 1)
        return mf.read()


def _grid_and_pixels(raster):
    with MemoryFile(raster) as mf, mf.open() as ds:
        return (
            (ds.width, ds.height, str(ds.crs)),
            ds.transform,
            ds.read(1),
            ds.read_masks(1),
        )


def _streamed_calls(monkeypatch):
    """Record the calls of the streaming merge (it still runs)."""
    calls, real = [], agg._merge_tiles_streaming

    def spy(rasters, **kwargs):
        calls.append(len(rasters))
        return real(rasters, **kwargs)

    monkeypatch.setattr(agg, "_merge_tiles_streaming", spy)
    return calls


@pytest.mark.parametrize("fx, fy", [(0.0, 0.0), (0.3, 0.7), (0.55, 0.85), (0.9, 0.5)])
def test_a_merge_whose_mosaic_fits_but_whose_working_set_does_not_streams_to_the_same_grid(
    monkeypatch, fx, fy
):
    """The mosaic of a mixed-CRS group fits the budget (`materialize_decision` says in RAM), but the in-RAM merge also
    holds the reprojected tile, the masks and the members' bytes (`_in_ram_bytes`): over the budget, it streams. The
    streamed mosaic has the grid and the pixels of the in-RAM one, whatever the offset of the reprojected tile from the
    reference grid (a tile at a fractional offset is where a 1-pixel difference between the two merges shows), and
    one budget exactly at the estimate does not stream."""
    from databricks.labs.gbx import _gdal_policy

    px = 50000.0
    rng = np.random.default_rng(7)
    ref = _tile_in(
        rng.integers(1, 200, (12, 12)),
        "EPSG:3857",
        1.0e6 + fx * px,
        5.0e6 + fy * px,
        px,
    )
    other = _tile_in(
        rng.integers(1, 200, (12, 12)), "EPSG:4326", 9.0 + fx, 44.0 + fy, 0.4
    )
    rasters = sorted([ref, other])
    hdrs = agg._scan_headers(rasters)
    ref_crs = agg._pick_ref_crs(hdrs)
    union = agg._union_decoded_bytes(hdrs, ref_crs)
    estimate = agg._in_ram_bytes(hdrs, ref_crs, union)
    assert union < estimate - 1, (union, estimate)  # the mosaic alone fits the budget

    in_ram = agg.merge_tiles(rasters)
    calls = _streamed_calls(monkeypatch)
    with _gdal_policy.carried(None, estimate):
        assert agg.merge_tiles(rasters) == in_ram and calls == []
    with _gdal_policy.carried(None, estimate - 1):
        streamed = agg.merge_tiles(rasters)
    assert calls == [2]

    (ga, ta, pa, ma), (gb, tb, pb, mb) = (
        _grid_and_pixels(in_ram),
        _grid_and_pixels(streamed),
    )
    assert ga == gb
    assert tuple(tb) == pytest.approx(tuple(ta), rel=0, abs=1e-6)
    assert np.array_equal(pa, pb) and np.array_equal(ma, mb)


def test_the_in_ram_estimate_counts_the_copies_the_merge_holds():
    """Each term of the estimate moves it: an alpha member adds its decoded copy, a member in another CRS its
    reprojected tile at the target size, a group with an own mask a union-sized mask."""
    base = [
        _ras(np.ones((8, 8)), ulx=0.0, uly=8.0),
        _ras(np.ones((8, 8)), ulx=8.0, uly=8.0),
    ]

    def estimate(rasters):
        hdrs = agg._scan_headers(rasters)
        ref_crs = agg._pick_ref_crs(hdrs)
        return agg._in_ram_bytes(hdrs, ref_crs, agg._union_decoded_bytes(hdrs, ref_crs))

    plain = estimate(base)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=8,
            height=8,
            count=2,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(0.0, 8.0, 1.0, 1.0),
            alpha="YES",
        ) as dst:
            dst.write(np.ones((2, 8, 8), dtype="float32"))
        alpha = mf.read()
    alpha_size = 8 * 8 * 2 * 4
    assert estimate([base[0], alpha]) >= plain + alpha_size
    other_crs = _ras(np.ones((8, 8)), ulx=0.0, uly=8.0, epsg=32733)
    assert estimate([base[0], other_crs]) > plain + 8 * 8 * 4


def test_a_member_decoded_whole_is_judged_at_its_output_size_against_the_merge_budget():
    """In the streamed merge a member that is reprojected, or rewritten with its mask, is the only one decoded whole: it
    is refused with the Fix when it is over the MERGE budget at the size it is decoded at (the reprojected tile, not the
    source), and a member copied as it is is never refused, whatever its size."""
    from databricks.labs.gbx import _gdal_policy

    ref = _tile_in(np.ones((2, 2)), "EPSG:3857", 1.0e6, 5.0e6, 100.0)
    # 4 x 4 pixels of 0.01 degrees: 16 bytes at the source, about 46 x 46 once on the 100 m reference grid
    small_source = _tile_in(np.ones((4, 4)), "EPSG:4326", 9.0, 44.0, 0.01)
    with _gdal_policy.carried(None, 1500):
        with pytest.raises(agg.ReadCapRefused, match="merge_by_key"):
            agg._merge_tiles_streaming(sorted([ref, small_source]))
    with _gdal_policy.carried(None, 5000):
        assert agg._merge_tiles_streaming(sorted([ref, small_source]))

    # alpha band: rewritten whole with its mask, so judged by its size; a tile that needs neither is copied as it is
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=32,
            height=32,
            count=2,
            dtype="uint8",
            crs="EPSG:32633",
            transform=from_origin(0.0, 32.0, 1.0, 1.0),
            alpha="YES",
        ) as dst:
            dst.write(np.full((2, 32, 32), 5, dtype="uint8"))
        alpha = mf.read()
    plain = _tile_in(np.full((32, 32), 9), "EPSG:32633", 32.0, 32.0, 1.0)  # 1 KiB
    with _gdal_policy.carried(None, 1500):
        with pytest.raises(agg.ReadCapRefused, match="merge_by_key"):
            agg._merge_tiles_streaming(sorted([alpha, plain]))
    big = _tile_in(np.full((64, 64), 9), "EPSG:32633", 0.0, 64.0, 1.0)  # 4 KiB
    with _gdal_policy.carried(None, 1500):
        assert agg._merge_tiles_streaming(sorted([big, plain]))


def test_a_member_over_the_read_cap_falls_to_streaming_not_a_refusal(monkeypatch):
    """The in-RAM path is chosen against the merge budget, the read gate of a member judges by the same cap: a member the
    gate refuses is merged by the streaming path (which copies it as it is) instead of failing the merge.
    """
    big = _tile_in(np.full((64, 64), 9), "EPSG:32633", 0.0, 64.0, 1.0)  # 4 KiB decoded
    other = _tile_in(np.full((64, 64), 7), "EPSG:32633", 64.0, 64.0, 1.0)
    calls = _streamed_calls(monkeypatch)
    # the size estimate is taken to fit; the gate, which judges each member by the read cap, refuses one
    monkeypatch.setattr(agg, "_in_ram_bytes", lambda *a, **k: 0)
    monkeypatch.setattr(agg, "_merge_budget", lambda: 1 << 30)
    real_open_all = agg._open_all

    def refusing(rasters, gate=True):
        if gate:
            raise agg.ReadCapRefused("over the read cap")
        return real_open_all(rasters, gate=gate)

    monkeypatch.setattr(agg, "_open_all", refusing)
    out = agg.merge_tiles([big, other])
    assert calls == [2]
    with MemoryFile(out) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (128, 64)


def test_the_encoded_result_of_a_bytes_aggregate_is_bounded():
    """The bytes of a merged or combined result come back whole in one row: over the cap `max_out`, both raise with the
    Fix that names their own route (the merge: the VRT-mosaic route, `mint_vrt`, since `merge_by_key` is bounded by the
    same cap; the combine: retile first, or `combine_by_key`).
    """
    left = _ras(np.arange(64 * 64).reshape(64, 64) % 251, ulx=0.0, uly=64.0)
    right = _ras(np.arange(64 * 64).reshape(64, 64) % 241, ulx=64.0, uly=64.0)
    assert agg.merge_tiles([left, right], max_out=1 << 20)
    with pytest.raises(
        agg.OutputOverBudget, match=r"rasterx\.helpers\.mint_vrt\(paths"
    ) as err:
        agg.merge_tiles([left, right], max_out=100)
    assert "merge_by_key" not in str(err.value)
    with pytest.raises(agg.OutputOverBudget, match="mint_vrt") as err:
        agg._merge_tiles_streaming(
            sorted([left, right]), max_out=100
        )  # checked before the COG is read
    assert "merge_by_key" not in str(err.value)
    assert agg.combineavg_tiles([left, left], max_out=1 << 20)
    with pytest.raises(agg.OutputOverBudget, match="combine_by_key") as err:
        agg.combineavg_tiles([left, left], max_out=100)
    assert "merge_by_key" not in str(err.value) and "gbx_rst_retile" in str(err.value)
    with pytest.raises(agg.OutputOverBudget, match="combine_by_key"):
        agg._combine_stat_tiles([left, left], "max", max_out=100)


def test_the_fold_windows_are_whole_output_blocks_and_the_refusal_names_the_combine_route():
    """A window is a whole number of the output's blocks (each compressed block is written by one window), unless the
    budget cannot hold one; a row too wide for the budget is refused with the combine route, not the mosaic one.
    """
    windows = agg._fold_windows((1, 100, 10), "avg", 1, 10 * 40 * 37 * 2, block_rows=16)
    assert [w.row_off for w in windows] == [0, 32, 64, 96]
    assert [w.height for w in windows] == [32, 32, 32, 4]
    # fewer rows fit than a block: the budget decides, the windows still cover every row once
    short = agg._fold_windows((1, 100, 10), "avg", 1, 10 * 40 * 3 * 2, block_rows=16)
    assert sum(w.height for w in short) == 100 and short[0].height == 3
    with pytest.raises(agg.ArrayOverBudget, match="combine_by_key") as err:
        agg._fold_windows((1, 4, 1000), "avg", 1, 1000, block_rows=1)
    assert "merge_by_key" not in str(err.value)


@pytest.mark.parametrize("stat", ["min", "max"])
def test_the_running_combines_fold_over_several_windows(stat):
    """min and max keep a running value per window: folded over many windows (one row each) they equal the one-window
    fold."""
    from databricks.labs.gbx import _gdal_policy

    rng = np.random.default_rng(3)
    tiles = [_ras(rng.integers(0, 90, (6, 8)).astype("float32")) for _ in range(3)]
    combine = {"min": agg.combine_min_tiles, "max": agg.combine_max_tiles}[stat]
    whole = combine(tiles)
    seen = []
    real = agg._running_stat

    def spy(datasets, nodatas, stat, fallback, window=None):
        seen.append(window)
        return real(datasets, nodatas, stat, fallback, window)

    agg._running_stat = spy
    try:
        with _gdal_policy.carried(None, 1000):
            folded = combine(tiles)
    finally:
        agg._running_stat = real
    assert len(seen) > 1
    with (
        MemoryFile(whole) as a,
        a.open() as da,
        MemoryFile(folded) as b,
        b.open() as db,
    ):
        assert np.array_equal(da.read(), db.read())


def test_a_spooled_member_keeps_the_byte_open_driver_policy(tmp_path):
    """A group too large to open from memory is spooled to files and opened by path: a member of a driver that can name
    other files (a VRT) is refused with the byte-open policy's error before it is written, as `open_bytes` refuses it.
    """
    from databricks.labs.gbx._gdal_policy import DriverPolicyError

    a = _ras(np.ones((8, 8)), ulx=0.0, uly=8.0)
    b = _ras(np.ones((8, 8)), ulx=8.0, uly=8.0)
    with agg._members([a, b], 10) as datasets:  # 10 bytes: the group is spooled
        assert [
            str(d.name).endswith(f"member_{i}.tif") for i, d in enumerate(datasets)
        ] == [True, True]
        assert datasets[0].read().shape == (1, 8, 8)
    vrt = (
        b'<VRTDataset rasterXSize="2" rasterYSize="2"><VRTRasterBand dataType="Byte" band="1">'
        b'<SimpleSource><SourceFilename relativeToVRT="0">/etc/hostname</SourceFilename>'
        b"<SourceBand>1</SourceBand></SimpleSource></VRTRasterBand></VRTDataset>"
    )
    with pytest.raises(DriverPolicyError, match="can name other files"):
        with agg._members([a, vrt], 10):
            pass


# --- combineavg_tiles -------------------------------------------------------
def test_combineavg_tiles_mean():
    a = _ras(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])


def test_combineavg_tiles_ignores_nodata():
    # Where one input is NoData, the mean is taken over the valid input only.
    a = _ras(np.array([[2.0, -9999.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 10.0], [-9999.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    # (2+4)/2=3 ; only-b=10 ; only-a=6 ; (8+12)/2=10
    assert np.allclose(got, [[3.0, 10.0], [6.0, 10.0]])


def test_combineavg_tiles_all_nodata_pixel_gets_fallback():
    a = _ras(np.array([[-9999.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[-9999.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    assert got[0, 0] == pytest.approx(-9999.0)


def test_combineavg_tiles_shape_mismatch_raises():
    a = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))
    b = _ras(np.array([[1.0, 2.0, 3.0]]))
    with pytest.raises(ValueError, match="aligned tiles"):
        agg.combineavg_tiles([a, b])


def test_combineavg_tiles_streaming_many_tiles_with_nodata():
    # Exercises the streaming sum+count accumulation over N>2 tiles (the memory rewrite):
    # the per-pixel mean must use ONLY the valid (non-NoData) inputs at each pixel, no matter
    # how the tiles are folded one-at-a-time.
    tiles = [
        _ras(np.array([[10.0, -9999.0], [1.0, 5.0]])),
        _ras(np.array([[20.0, 4.0], [2.0, -9999.0]])),
        _ras(np.array([[30.0, 8.0], [-9999.0, -9999.0]])),
        _ras(np.array([[40.0, -9999.0], [4.0, 5.0]])),
        _ras(np.array([[50.0, 12.0], [-9999.0, 5.0]])),
    ]
    out = agg.combineavg_tiles(tiles)
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    # pixel(0,0): mean(10,20,30,40,50)=30 ; (0,1): mean(4,8,12)=8 ;
    # (1,0): mean(1,2,4)=7/3 ; (1,1): mean(5,5,5)=5
    assert np.allclose(got, [[30.0, 8.0], [7.0 / 3.0, 5.0]])


def test_combineavg_tiles_streaming_no_nodata_declared():
    # When no input declares NoData, every value counts (the valid=None fast path).
    tiles = [_ras(np.array([[v, v]]), nodata=None) for v in (1.0, 2.0, 3.0, 6.0)]
    out = agg.combineavg_tiles(tiles)
    with _serde.open_tile(out) as ds:
        assert np.allclose(ds.read(1), [[3.0, 3.0]])  # mean(1,2,3,6)=3


def test_open_all_closes_and_raises_on_corrupt_tile():
    # A corrupt tile mid-group must raise cleanly (not hang/crash) -- exercises the _open_all
    # partial-open failure path that closes the already-opened buffers before re-raising.
    good = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))
    with pytest.raises(Exception):  # noqa: B017 — rasterio raises its own IO error type
        agg.merge_tiles([good, b"not a valid geotiff", good])


# --- frombands_tiles --------------------------------------------------------
def test_frombands_tiles_ascending_order():
    # Provide out of order: index 2 then index 0 then index 1.
    b0 = _ras(np.full((2, 2), 10.0))
    b1 = _ras(np.full((2, 2), 20.0))
    b2 = _ras(np.full((2, 2), 30.0))
    out = agg.frombands_tiles([(2, b2), (0, b0), (1, b1)])
    with _serde.open_tile(out) as ds:
        assert ds.count == 3
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)
        assert np.allclose(ds.read(3), 30.0)


# --- rasterize_features -----------------------------------------------------
def test_rasterize_features_burns_values():
    # Extent 0..4 x 0..4, 4x4 px (1 unit/px). Two boxes, second overlaps first.
    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))  # left half -> value 1
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))  # overlaps col 1 -> value 2 (last wins)
    out = agg.rasterize_features([(g1, 1.0), (g2, 2.0)], 0, 0, 4, 4, 4, 4, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # Column 0 only g1 -> 1 ; columns 1..3 -> g2 last-wins -> 2.
    assert np.all(arr[:, 0] == 1.0)
    assert np.all(arr[:, 1] == 2.0)


def test_rasterize_features_overlap_winner_order_invariant():
    # A Spark groupBy().agg() gives no feature-arrival-order guarantee, so a
    # last-wins burn must not depend on feature order. rasterize_features burns in
    # a canonical (geom_wkb, value) order, so the overlap pixel is identical
    # whichever order the features are supplied.
    g1 = shapely.wkb.dumps(box(0, 0, 3, 4))  # left band, value 1
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))  # overlaps cols 1..2, value 2
    out_ab = agg.rasterize_features([(g1, 1.0), (g2, 2.0)], 0, 0, 4, 4, 4, 4, 32633)
    out_ba = agg.rasterize_features([(g2, 2.0), (g1, 1.0)], 0, 0, 4, 4, 4, 4, 32633)
    assert out_ab == out_ba  # bitwise-identical regardless of order
    with _serde.open_tile(out_ab) as ds:
        arr = ds.read(1)
    # Overlap cols 1..2 resolve to a single canonical winner (1.0 or 2.0).
    overlap = arr[:, 1:3]
    winner = overlap.flat[0]
    assert winner in (1.0, 2.0)
    assert np.all(overlap == winner)


def test_rasterize_features_empty_returns_none():
    assert agg.rasterize_features([], 0, 0, 4, 4, 4, 4, 32633) is None


# --- derivedband_tiles ------------------------------------------------------
PYFUNC_SUM = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def test_derivedband_tiles_sum_across_group():
    a = _ras(np.full((2, 2), 3.0))
    b = _ras(np.full((2, 2), 4.0))
    c = _ras(np.full((2, 2), 5.0))
    out = agg.derivedband_tiles([a, b, c], PYFUNC_SUM, "addbands")
    with _serde.open_tile(out) as ds:
        assert ds.count == 1
        assert np.allclose(ds.read(1), 12.0)


# --- corrupt-member skip in light-tier helpers --------------------------------
# These tests exercise the skip-and-count behaviour added to _merge_bytes,
# _combineavg_bytes, and _frombands_bytes.  Each helper now wraps the
# per-member open/materialize in try/except so a corrupt-but-non-empty member is
# dropped instead of raising.


def _tile_struct(raster_bytes, cellid=0):
    """Minimal materialized v1 tile input dict (cellid, raster, metadata)."""
    return {"cellid": cellid, "raster": raster_bytes, "metadata": {}}


def _corrupt_tile():
    """Non-empty tile whose raster bytes are invalid GTiff (causes open failure)."""
    return _tile_struct(b"not a valid geotiff")


def _valid_tile(data=None, cellid=0):
    """Valid single-band 2x2 tile struct."""
    if data is None:
        data = np.array([[1.0, 2.0], [3.0, 4.0]])
    raster_bytes = _ras(data)
    return _tile_struct(raster_bytes, cellid=cellid)


class TestMergeBytesSkipsCorrupt:
    # _merge_bytes now returns (bytes, dropped: int) or None.
    # dropped > 0 when at least one corrupt member was skipped.

    def test_corrupt_member_does_not_raise(self):
        # Before the fix _merge_bytes raised when it hit the corrupt tile.
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _merge_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_tuple_bytes_and_dropped(self):
        good = _valid_tile(np.array([[7.0, 8.0], [9.0, 10.0]]))
        corrupt = _corrupt_tile()
        result = _merge_bytes([good, corrupt])
        new_bytes, dropped = result
        assert new_bytes is not None
        assert dropped == 1
        with _serde.open_tile(new_bytes) as ds:
            assert ds.count >= 1

    def test_all_corrupt_returns_none(self):
        result = _merge_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        # No corrupt members → dropped == 0.
        a = _valid_tile(np.array([[1.0, 2.0], [3.0, 4.0]]))
        b = _valid_tile(np.array([[5.0, 6.0], [7.0, 8.0]]))
        new_bytes, dropped = _merge_bytes([a, b])
        assert new_bytes is not None
        assert dropped == 0


class TestCombineavgBytesSkipsCorrupt:
    # _combineavg_bytes now returns (bytes, cellid, dropped: int) or None.

    def test_corrupt_member_does_not_raise(self):
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _combineavg_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_triple_with_dropped(self):
        good = _valid_tile(np.array([[2.0, 4.0], [6.0, 8.0]]))
        corrupt = _corrupt_tile()
        result = _combineavg_bytes([good, corrupt])
        new_bytes, cellid, dropped = result
        assert new_bytes is not None
        assert dropped == 1
        with _serde.open_tile(new_bytes) as ds:
            assert ds.count >= 1

    def test_all_corrupt_returns_none(self):
        result = _combineavg_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        a = _valid_tile(np.array([[2.0, 4.0], [6.0, 8.0]]))
        b = _valid_tile(np.array([[4.0, 8.0], [10.0, 12.0]]))
        new_bytes, _cellid, dropped = _combineavg_bytes([a, b])
        assert new_bytes is not None
        assert dropped == 0


class TestFrombandsBytesSkipsCorrupt:
    # _frombands_bytes now returns (bytes, cellid, dropped: int) or None.

    def test_corrupt_member_does_not_raise(self):
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _frombands_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_triple_with_dropped(self):
        good = _valid_tile(np.array([[5.0, 6.0], [7.0, 8.0]]))
        corrupt = _corrupt_tile()
        result = _frombands_bytes([good, corrupt])
        new_bytes, _cellid, dropped = result
        assert new_bytes is not None
        assert dropped == 1

    def test_all_corrupt_returns_none(self):
        result = _frombands_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        b0 = _valid_tile(np.full((2, 2), 1.0))
        b1 = _valid_tile(np.full((2, 2), 2.0))
        new_bytes, _cellid, dropped = _frombands_bytes([b0, b1])
        assert new_bytes is not None
        assert dropped == 0


# ---------------------------------------------------------------------------
# Spark grouped-agg corrupt-member skip tests
#
# These exercise the ACTUAL pandas_udfs via a Spark groupBy().agg() with one
# valid + one corrupt tile struct in the same group, asserting:
#   (a) no raise on .collect()
#   (b) non-null aggregate over the good member
#
# The drop-count has no metadata carrier at the pandas_udf layer (bare-bytes
# return type), so we assert (a)+(b) only — not a last_error value.
# ---------------------------------------------------------------------------

_PYFUNC_IDENTITY = """
def identity(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = in_ar[0]
"""


def _valid_raster():
    """Valid GTiff bytes for Spark-based tests."""
    return _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))


def _corrupt_raster():
    """Corrupt (non-GTiff) bytes for Spark-based tests."""
    return b"not a valid geotiff"


# A 3-field (v1) tile input schema, defined locally to inject raw/corrupt bytes.
# These tests deliberately feed a v1 tile to the aggregators: the light-tier
# `_open` front-door accepts v1 tiles on INPUT indefinitely (the aggregators emit
# v2), so a v1 input row is a realistic corrupt-skip scenario. Defined here rather
# than importing a production constant so the test owns its fixture shape.
_V1_TILE_INPUT_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


def _spark_tile_df_raw(spark, raster_bytes_seq):
    """Create a one-group DataFrame of tile structs by injecting raw raster bytes.

    Uses a schematized createDataFrame (v1 tile input rows) instead of
    rst_fromcontent so that corrupt bytes can be injected without rst_fromcontent
    raising on open_tile during fromcontent. This matches how the heavy Scala tests
    inject corrupt bytes: they use rst_fromcontent on the heavy tier where build_tile
    is a GDAL open (which tolerates the inject path differently), but the light tier's
    rst_fromcontent raises on corrupt bytes before the agg_udf is ever called. The
    aggregators accept a v1 tile on input (front-door contract) and emit v2.
    """
    from pyspark.sql import functions as sf

    rows = [{"cellid": 0, "raster": rb, "metadata": {}} for rb in raster_bytes_seq]
    df = spark.createDataFrame(rows, schema=_V1_TILE_INPUT_SCHEMA)
    return df.select(sf.lit(1).alias("g"), sf.struct("*").alias("tile"))


class TestGroupedAggUdfSkipsCorrupt:
    """Spark grouped-agg pandas_udfs skip corrupt members instead of raising."""

    def test_merge_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(_merge_agg_udf(sf.col("tile")).alias("agg_bytes"))
        # Must not raise and must produce a non-null aggregate.
        rows = result.collect()
        assert rows, "no rows returned"
        assert rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_combineavg_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _combineavg_agg_udf(sf.col("tile")).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        # combineavg prepends 8-byte cellid envelope; total > 8 bytes for a real tile.
        agg_b = rows[0]["agg_bytes"]
        assert agg_b is not None and len(agg_b) > 8, "aggregate bytes must be non-null"

    def test_derivedband_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _derivedband_agg_udf(
                sf.col("tile"),
                sf.lit(_PYFUNC_IDENTITY),
                sf.lit("identity"),
            ).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        assert rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_frombands_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf
        from pyspark.sql.types import IntegerType

        # Build a two-row group: valid tile → band 1, corrupt bytes → band 2.
        # Reuses the local v1 tile input schema (aggregators accept v1 on input).
        schema_with_band = StructType(
            list(_V1_TILE_INPUT_SCHEMA.fields)
            + [StructField("band_idx", IntegerType(), True)]
        )
        rows_data = [
            {"cellid": 0, "raster": _valid_raster(), "metadata": {}, "band_idx": 1},
            {"cellid": 0, "raster": _corrupt_raster(), "metadata": {}, "band_idx": 2},
        ]
        df_raw = spark.createDataFrame(rows_data, schema=schema_with_band)
        df = df_raw.select(
            sf.lit(1).alias("g"),
            sf.struct("cellid", "raster", "metadata").alias("tile"),
            sf.col("band_idx"),
        )
        result = df.groupBy("g").agg(
            _frombands_agg_udf(sf.col("tile"), sf.col("band_idx")).alias("agg_bytes")
        )
        out_rows = result.collect()
        assert out_rows, "no rows returned"
        assert out_rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_combineavg_agg_sql_udf_no_raise(self, spark):
        """_combineavg_agg_sql_udf (backs gbx_rst_combineavg_agg) skips corrupt members.

        SQL registration variant: returns raw GTiff bytes (no cellid envelope),
        so we assert bytes are non-null and openable (not just non-empty).
        """
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _combineavg_agg_sql_udf(sf.col("tile")).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        agg_b = rows[0]["agg_bytes"]
        assert agg_b is not None, "aggregate bytes must be non-null"
        # Confirm the output is a valid (openable) GTiff, not a corrupt passthrough.
        from databricks.labs.gbx.pyrx import _serde

        with _serde.open_tile(bytes(agg_b)):
            pass  # raises if bytes are not a valid raster


# ---------------------------------------------------------------------------
# Aggregators never inject try_to_file (Spark 4 nondeterminism guard)
# ---------------------------------------------------------------------------
# try_to_file is nondeterministic; Spark 4 rejects nondeterministic expressions
# as aggregate function arguments (AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION).
# rst_merge_agg and rst_combineavg_agg must always use the FUSE-opening UDF path,
# never file_ref_arg(). These tests document the invariant.
# ---------------------------------------------------------------------------


def test_merge_agg_no_try_to_file(spark):
    """rst_merge_agg must not inject try_to_file into the column expression tree.

    On a FILE-capable cluster, the pre-fix code conditionally injected
    file_ref_arg (= try_to_file(tile.path)) which is nondeterministic and
    causes Spark 4 to reject the .agg() call.  After the fix the expression
    tree must be free of try_to_file regardless of cluster capability.

    ``spark`` is required so the JVM is up (Column._jc.toString() needs the
    JVM running).
    """
    from databricks.labs.gbx.pyrx.functions import _light_rst_merge_agg

    expr = _light_rst_merge_agg("tile")._jc.toString()
    assert "try_to_file" not in expr.lower()


def test_combineavg_agg_no_try_to_file(spark):
    """rst_combineavg_agg must not inject try_to_file (same rationale as merge)."""
    from databricks.labs.gbx.pyrx.functions import (
        _light_rst_combineavg_agg as rst_combineavg_agg,
    )

    expr = rst_combineavg_agg("tile")._jc.toString()
    assert "try_to_file" not in expr.lower()


class TestAggPublicFunctionsParity:
    """Public rst_merge_agg / rst_combineavg_agg produce correct output for materialized tiles.

    Parity check: dropping the file-ref branch must not regress materialized-tile groups.
    """

    def test_rst_merge_agg_non_null_mosaic(self, spark):
        """Two adjacent materialized tiles merge into a non-null union-extent mosaic."""
        import databricks.labs.gbx.pyrx.functions as prx

        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        result = df.groupBy("g").agg(prx._light_rst_merge_agg("tile").alias("merged"))
        rows = result.collect()
        assert rows, "no rows returned"
        merged = rows[0]["merged"]
        assert merged is not None, "merged tile must be non-null"
        raster_bytes = merged["raster"]
        assert raster_bytes is not None and len(raster_bytes) > 0
        # Union extent: 4 pixels wide, left edge 0.0, right edge 4.0.
        with _serde.open_tile(bytes(raster_bytes)) as ds:
            assert ds.width == 4
            b = ds.bounds
            assert b.left == pytest.approx(0.0)
            assert b.right == pytest.approx(4.0)

    def test_rst_combineavg_agg_non_null_mean(self, spark):
        """Two aligned materialized tiles produce a correct per-pixel mean."""
        import databricks.labs.gbx.pyrx.functions as prx

        a = _ras(np.array([[2.0, 4.0], [6.0, 8.0]]))
        b = _ras(np.array([[4.0, 8.0], [10.0, 12.0]]))
        df = _spark_tile_df_raw(spark, [a, b])
        result = df.groupBy("g").agg(prx._light_rst_combineavg_agg("tile").alias("avg"))
        rows = result.collect()
        assert rows, "no rows returned"
        avg_tile = rows[0]["avg"]
        assert avg_tile is not None, "avg tile must be non-null"
        raster_bytes = avg_tile["raster"]
        assert raster_bytes is not None and len(raster_bytes) > 0
        with _serde.open_tile(bytes(raster_bytes)) as ds:
            got = ds.read(1)
        # pixel-wise mean: [[3, 6], [8, 10]]
        assert np.allclose(got, [[3.0, 6.0], [8.0, 10.0]])


# ---------------------------------------------------------------------------
# rst_merge_agg force-output (virtualize_dir) path
# ---------------------------------------------------------------------------


class TestMergeAggVirtualizeDir:
    """rst_merge_agg(tile, virtualize_dir=...) writes merged tile to disk and
    returns a path-only virtual tile — avoids Arrow serialisation of the full
    mosaic bytes (OOM guard for large DSMs on Serverless)."""

    def test_virtualize_dir_returns_path_only_tile(self, spark, tmp_path):
        """Merged tile is path-only (raster=None, path set); file exists on disk."""
        import databricks.labs.gbx.pyrx.functions as prx

        out_dir = str(tmp_path / "magg_vout")
        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        result = df.groupBy("g").agg(
            prx._light_rst_merge_agg("tile", virtualize_dir=out_dir).alias("merged")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        merged = rows[0]["merged"]
        assert merged is not None, "merged tile must be non-null"

        # Path-only: raster bytes must be absent.
        assert merged["raster"] is None, "raster bytes must be None for a virtual tile"
        path = merged["path"]
        assert path is not None and path.endswith(
            ".tif"
        ), f"expected .tif path, got {path!r}"

        # The file must exist on disk.
        import os

        assert os.path.exists(path), f"written file not found: {path}"
        assert os.path.getsize(path) > 0, "written file is empty"

    def test_the_public_api_takes_virtualize_dir_and_prefix(self, spark, tmp_path):
        """`rasterx.functions.rst_merge_agg(..., virtualize_dir=, virtualize_prefix=)` writes the mosaic and
        returns a path-only tile (the SQL form returns BINARY and has no such keywords).
        """
        import os

        from databricks.labs.gbx.rasterx import functions as rx

        out_dir = str(tmp_path / "magg_public")
        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        merged = (
            df.groupBy("g")
            .agg(
                rx.rst_merge_agg(
                    "tile", virtualize_dir=out_dir, virtualize_prefix="pub"
                ).alias("merged")
            )
            .first()["merged"]
        )
        assert merged["raster"] is None and os.path.getsize(merged["path"]) > 0
        assert os.path.basename(merged["path"]).startswith("pub_")

    @pytest.mark.parametrize("materialize", [True, False])
    def test_materialize_alone_gives_the_tile_and_false_is_the_unforced_path(
        self, spark, materialize
    ):
        """`materialize=True` (no directory) is the merged tile struct with its raster bytes and no path;
        `materialize=False` is the unforced path, the same BINARY as no keyword. Both span the 4x2 union extent.
        """
        from databricks.labs.gbx.rasterx import functions as rx

        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        merged = (
            df.groupBy("g")
            .agg(rx.rst_merge_agg("tile", materialize=materialize).alias("m"))
            .first()["m"]
        )
        if materialize:
            assert merged["raster"] is not None and merged["path"] is None
            merged = merged["raster"]
        else:
            plain = (
                df.groupBy("g").agg(rx.rst_merge_agg("tile").alias("m")).first()["m"]
            )
            assert bytes(merged) == bytes(plain)
        with MemoryFile(bytes(merged)) as mf, mf.open() as ds:
            assert (ds.width, ds.height) == (4, 2)

    def test_virtualize_dir_reopens_to_correct_extent(self, spark, tmp_path):
        """Re-opening the written file yields the correct merged extent/dims."""
        import databricks.labs.gbx.pyrx.functions as prx
        from databricks.labs.gbx.pyrx.core import open_tile as ot

        out_dir = str(tmp_path / "magg_extent")
        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])

        # Materialized reference for extent comparison.
        ref_rows = (
            df.groupBy("g")
            .agg(prx._light_rst_merge_agg("tile").alias("merged"))
            .collect()
        )
        ref_merged = ref_rows[0]["merged"]
        with _serde.open_tile(bytes(ref_merged["raster"])) as ds_ref:
            exp_w, exp_h = ds_ref.width, ds_ref.height
            exp_left, exp_right = ds_ref.bounds.left, ds_ref.bounds.right

        # Virtualized result.
        virt_rows = (
            df.groupBy("g")
            .agg(
                prx._light_rst_merge_agg("tile", virtualize_dir=out_dir).alias("merged")
            )
            .collect()
        )
        merged = virt_rows[0]["merged"]
        with ot._open(merged.asDict()) as ds:
            assert ds.width == exp_w
            assert ds.height == exp_h
            assert ds.bounds.left == pytest.approx(exp_left)
            assert ds.bounds.right == pytest.approx(exp_right)

    def test_default_path_still_materializes(self, spark):
        """Default (no virtualize_dir) still returns an in-memory merged tile — unchanged."""
        import databricks.labs.gbx.pyrx.functions as prx

        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        result = df.groupBy("g").agg(prx._light_rst_merge_agg("tile").alias("merged"))
        rows = result.collect()
        merged = rows[0]["merged"]
        assert merged is not None
        raster_bytes = merged["raster"]
        assert (
            raster_bytes is not None and len(raster_bytes) > 0
        ), "default path must return in-memory raster bytes"

    def test_virtualize_prefix_is_used_in_filename(self, spark, tmp_path):
        """When virtualize_prefix is set the written filename begins with that prefix."""
        import os

        import databricks.labs.gbx.pyrx.functions as prx

        out_dir = str(tmp_path / "magg_prefix")
        left = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]), ulx=0.0, uly=2.0, px=1.0)
        right = _ras(np.array([[5.0, 6.0], [7.0, 8.0]]), ulx=2.0, uly=2.0, px=1.0)
        df = _spark_tile_df_raw(spark, [left, right])
        result = df.groupBy("g").agg(
            prx._light_rst_merge_agg(
                "tile", virtualize_dir=out_dir, virtualize_prefix="run42"
            ).alias("merged")
        )
        rows = result.collect()
        path = rows[0]["merged"]["path"]
        assert path is not None
        assert os.path.basename(path).startswith(
            "run42_"
        ), f"filename does not start with prefix: {os.path.basename(path)!r}"
