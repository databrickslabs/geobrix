"""Local verify for the grouped FILE-amortization bench leg (bench/grouped_file.py).

Runs on local[2] Spark against a synthesized 2-COG x 8-window corpus (16 virtual
tiles sharing 2 sources). Verifies:

1. All three tile modes (materialized / virtual-file-off / virtual-file-on) RUN
   and emit ok ResultRows with the expected shape.
2. Cross-mode PIXEL PARITY: the three modes produce byte-for-byte identical
   decoded pixels for the canonical rst_clip_grouped (correctness across modes).
3. The OPEN-cost AMORTIZATION MECHANISM, at the OpenResourceLRU level that
   grouped_tile_map actually uses: 16 windows across 2 sources open each source
   ONCE (lru.opens == 2), while the FILE-off fallback opens the source per-tile
   (16 rasterio.open(source) calls) -- proving fewer opens FILE-on than FILE-off,
   with fast-path output equal to the fallback output per tile.

Note on local FILE: plain local Spark has no ``try_to_file``, so
``file_supported()`` is False and mode C degrades to the same per-tile fallback
as mode B end-to-end (correct output, no open amortization). The amortization
WIN is therefore asserted at the LRU level (test 3), which is the exact object
and opener grouped_tile_map drives on-cluster; the Spark run (tests 1-2) proves
the three modes are wired and agree.
"""

import io

import numpy as np
import pytest
import rasterio

from databricks.labs.gbx.bench import grouped_file as gf


@pytest.fixture(scope="module")
def corpus(tmp_path_factory):
    """A tiny 2-COG x 8-window multiwindow corpus; returns the manifest path."""
    out = tmp_path_factory.mktemp("gf_corpus")
    return gf.synthesize_multiwindow_corpus(
        out,
        cog_count=2,
        windows_per_cog=8,
        cog_px=512,
        window_px=128,
        srid=4326,
        bands=1,
        dtype="float32",
        seed=13,
    )


def _decode(raster_bytes):
    """Decode a tile's GeoTIFF bytes to a numpy array (bands, h, w)."""
    with rasterio.io.MemoryFile(raster_bytes) as mf:
        with mf.open() as ds:
            return ds.read()


# ---------------------------------------------------------------------------
# 1. Three modes run and produce rows
# ---------------------------------------------------------------------------


def test_three_modes_run_and_produce_rows(spark, corpus):
    rows = gf.run_grouped_file(
        spark,
        manifest_path=corpus,
        fns=["rst_clip_grouped", "rst_transform_grouped"],
        warmup=0,
        measured=1,
        progress=False,
    )
    # 3 modes x 2 fns = 6 rows.
    assert len(rows) == 6
    modes_seen = {r.split_strategy for r in rows}
    assert modes_seen == set(gf.MODES)

    for r in rows:
        assert r.status == "ok", f"{r.fn}/{r.split_strategy} failed: {r.note}"
        assert r.rows == 16, "corpus is 2 COGs x 8 windows = 16 tiles"
        assert r.mode == "spark-path"
        assert r.api == "lightweight"
        assert r.measured_iters == 1 and r.warmup_iters == 0
        assert r.per_tile_avg_ms > 0.0
        assert r.note.startswith("grouped-file/")

    # input_tile is materialized only for the materialized mode.
    by_mode = {r.split_strategy: r for r in rows if r.fn == "rst_clip_grouped"}
    assert by_mode["materialized"].input_tile == "materialized"
    assert by_mode["virtual-file-off"].input_tile == "virtual"
    assert by_mode["virtual-file-on"].input_tile == "virtual"


# ---------------------------------------------------------------------------
# 2. Cross-mode pixel parity
# ---------------------------------------------------------------------------


def _clip_outputs_by_cellid(spark, corpus, mode):
    """Apply rst_clip_grouped in `mode`; return {cellid: decoded pixels}."""
    from databricks.labs.gbx.pyrx import functions as prx

    manifest_rows = gf.read_manifest_rows(corpus)
    geom_wkb, geom_crs = gf._clip_geom_from_source(manifest_rows[0]["path"])
    prior = gf._set_file_env(mode)
    try:
        df = gf.build_tile_df(spark, manifest_rows, mode=mode)
        out = prx.rst_clip_grouped(df, geom_wkb, clip_crs=geom_crs)
        collected = out.collect()
    finally:
        gf._restore_file_env(prior)
    result = {}
    for row in collected:
        tile = row["tile"]
        assert tile["raster"] is not None, "clip over full extent must be non-null"
        result[int(tile["cellid"])] = _decode(bytes(tile["raster"]))
    return result


def test_cross_mode_pixel_parity(spark, corpus):
    mat = _clip_outputs_by_cellid(spark, corpus, "materialized")
    off = _clip_outputs_by_cellid(spark, corpus, "virtual-file-off")
    on = _clip_outputs_by_cellid(spark, corpus, "virtual-file-on")

    assert set(mat) == set(off) == set(on)
    assert len(mat) == 16
    for cid in mat:
        np.testing.assert_array_equal(
            mat[cid], off[cid], err_msg=f"materialized vs file-off differ @cell {cid}"
        )
        np.testing.assert_array_equal(
            off[cid], on[cid], err_msg=f"file-off vs file-on differ @cell {cid}"
        )


# ---------------------------------------------------------------------------
# 3. OPEN-cost amortization mechanism (OpenResourceLRU level)
# ---------------------------------------------------------------------------


class _FakeFileRef:
    """Minimal FileRef stand-in: seekable stream + size + local path.

    Mirrors the pyspark FileRef contract that ``_open_via_file_ref`` requires
    (``.size``, ``.open()`` -> seekable stream, ``.as_local_file()``), letting
    the FILE fast path be exercised in-process without a Databricks runtime.
    """

    def __init__(self, path):
        self._path = str(path)
        self._bytes = open(self._path, "rb").read()

    @property
    def size(self):
        return len(self._bytes)

    def open(self):
        return io.BytesIO(self._bytes)  # seekable, rasterio-openable

    def as_local_file(self):
        return self._path


def _core_sum(ds, cellid):  # noqa: ARG001 - cellid unused, contract arg
    a = ds.read()
    return (int(ds.count), int(ds.width), int(ds.height), float(np.nan_to_num(a).sum()))


def test_lru_amortizes_source_opens(corpus, monkeypatch):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    from databricks.labs.gbx.pyrx.grouped_exec import (
        OpenResourceLRU,
        _OpenerContext,
        _run_fallback_tile,
        _run_file_fast_path,
    )

    rows = gf.read_manifest_rows(corpus)
    # align_partitions sorts by path so a source's windows are contiguous.
    rows = sorted(rows, key=lambda r: r["path"])
    n_windows = len(rows)
    src_paths = {r["path"] for r in rows}
    n_sources = len(src_paths)
    assert n_windows == 16 and n_sources == 2  # the amortization opportunity

    def _tile(i, r):
        return VirtualTile(cellid=i, path=r["path"], window=tuple(r["window"])).to_row()

    # --- FILE-on: one shared LRU across the partition (the real object) ---
    ctx = _OpenerContext()
    lru = OpenResourceLRU(opener=ctx.open, closer=ctx.close, weigher=ctx.weigh)
    fast_results = []
    try:
        for i, r in enumerate(rows):
            fr = _FakeFileRef(r["path"])
            fast_results.append(
                _run_file_fast_path(
                    ctx.fr_holder,
                    ctx.size_holder,
                    lru,
                    r["path"],
                    fr,
                    _tile(i, r),
                    i,
                    "pixels",
                    _core_sum,
                )
            )
    finally:
        lru.close_all()

    # THE amortization number: 16 windows, 2 distinct sources -> 2 opens.
    assert lru.opens == n_sources
    assert lru.opens < n_windows

    # --- FILE-off: per-tile fallback opens the source path each time ---
    real_open = rasterio.open
    counter = {"n": 0}

    def _spy(fp, *a, **k):
        if isinstance(fp, str) and fp in src_paths:
            counter["n"] += 1
        return real_open(fp, *a, **k)

    monkeypatch.setattr(rasterio, "open", _spy)
    fb_results = [
        _run_fallback_tile(_tile(i, r), i, "pixels", _core_sum)
        for i, r in enumerate(rows)
    ]
    monkeypatch.undo()

    # FILE-off opens the source once per window -> 16 opens (no amortization).
    assert counter["n"] == n_windows
    assert lru.opens < counter["n"]  # FILE-on strictly fewer source opens

    # Correctness: the amortized fast path yields the same pixels as the
    # per-tile fallback for every window.
    assert fast_results == fb_results
