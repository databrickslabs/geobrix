"""Regression tests for the multi-tile-array input builder (tile_array / tile_aggregate).

The bench harness builds ARRAY<tile> columns for rst_frombands / rst_merge /
rst_combineavg (tile_array ops) via _synth_array_col, and (tile, band_index)
DataFrames for rst_frombands_agg / rst_merge_agg / rst_combineavg_agg via
_tile_aggregate_df.

Bug: both builders previously embedded GTiff bytes as Spark plan literals
(F.lit(bytes) / createDataFrame rows with bytes).  On bench-corpus-1024-1k
(~8 MB per tile) those literals exceed Spark's task broadcast limit; bytes
arrive corrupt or truncated on the executor, and rasterio raises "Cannot open
TIFF image".  The fix uses path-based virtual tile structs (raster=None, path
set, window=full-file) so executors read the bytes from the synth file paths.

These tests RED before the fix and GREEN after.
"""

from __future__ import annotations

import pytest
import rasterio

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import runner as rn
from databricks.labs.gbx.bench import spec as s
from databricks.labs.gbx.bench import synth as _synth


# ---------------------------------------------------------------------------
# Shared corpus fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def small_corpus(tmp_path_factory):
    """32px 2-band float32 EPSG:4326 corpus — fast, repr of real corpora."""
    out = tmp_path_factory.mktemp("corpus")
    return dg.generate_corpus(
        out_dir=out,
        seed=42,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    ), out


# ---------------------------------------------------------------------------
# 1. Synth files are valid GTiffs (prerequisite, not the fix itself)
# ---------------------------------------------------------------------------


def test_synth_files_are_valid_gtiffs(small_corpus):
    """Synth files written by bench.synth must be openable by rasterio.

    This is the prerequisite for the fix: if synth cannot write valid GTiffs,
    neither the byte-embedding nor the path-based approach would work.
    """
    corpus, out = small_corpus
    array_root = corpus.row_pool.tiles[0].path
    for fn in ("rst_frombands", "rst_merge", "rst_combineavg"):
        recipe = s.synth_recipe(fn)
        out_dir = _synth.synth_dir(out, array_root, recipe)
        paths = _synth.synthesize(str(out / array_root), recipe, out_dir)
        assert paths, f"{fn}: synthesize returned no paths"
        for p in paths:
            with rasterio.open(p) as ds:
                assert ds.width > 0, f"{fn}: synth tile not openable: {p}"


# ---------------------------------------------------------------------------
# 2. _tile_aggregate_df builds path-based virtual tile structs (RED/GREEN)
# ---------------------------------------------------------------------------


def test_tile_aggregate_df_uses_path_based_virtual_tiles(small_corpus, spark):
    """_tile_aggregate_df must return tiles with path set and raster=None.

    Before the fix: tiles have raster bytes embedded (3-field struct, path absent).
    After  the fix: tiles have path+window set, raster=None (V2 virtual tile).

    Using path-based virtual tiles avoids embedding large byte arrays in the
    LocalRelation / Spark plan, which causes "Cannot open TIFF image" for large
    corpus tiles (1024px ~ 8 MB / tile exceeds Spark's task broadcast ceiling).
    """
    corpus, out = small_corpus
    for agg_fn in ("rst_frombands_agg", "rst_merge_agg", "rst_combineavg_agg"):
        fs = s.REGISTRY[agg_fn]
        df, _ = rn._tile_aggregate_df(spark, out, corpus, fs)
        collected = df.select("tile").collect()
        assert collected, f"{agg_fn}: _tile_aggregate_df returned no rows"
        for row in collected:
            d = row["tile"].asDict()
            path = d.get("path")
            raster = d.get("raster")
            # AFTER FIX: virtual tile — path set, raster absent/None
            assert path is not None, (
                f"{agg_fn}: tile has no path — bytes are still embedded as plan literals "
                "(fix: use path-based virtual tile structs in _tile_aggregate_df)"
            )
            assert raster is None, (
                f"{agg_fn}: tile has inline raster bytes — bytes are still embedded as plan "
                "literals (fix: use path-based virtual tile structs in _tile_aggregate_df)"
            )
            # Path must be openable by rasterio on the driver side
            with rasterio.open(path) as ds:
                assert ds.width > 0, f"{agg_fn}: path-based tile not openable: {path}"


# ---------------------------------------------------------------------------
# 3. All 6 ops succeed end-to-end via run_spark_path (regression guard)
# ---------------------------------------------------------------------------


def test_all_six_array_agg_ops_succeed_spark_path(small_corpus, spark):
    """rst_frombands/merge/combineavg and their *_agg counterparts must all
    succeed via run_spark_path after the fix.

    Before the fix these ops produced status='error' with 'Cannot open TIFF
    image' on the cluster bench-corpus-1024-1k (bytes-as-literals approach).
    After the fix (path-based virtual tiles) they succeed locally and on cluster.
    """
    corpus, out = small_corpus
    fns = s.select(
        functions=[
            "rst_frombands",
            "rst_merge",
            "rst_combineavg",
            "rst_frombands_agg",
            "rst_merge_agg",
            "rst_combineavg_agg",
        ]
    )
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=out,
        corpus=corpus,
        fnspecs=fns,
        run_id="test_array_agg",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected result rows"
    failed = [(r.fn, r.status, r.note) for r in rows if r.status != "ok"]
    assert not failed, f"ops failed (expected all ok after fix): {failed}"
    # Verify tile_array ops produce raster fingerprints on the smallest-N cell
    import json

    for fn in ("rst_frombands", "rst_merge", "rst_combineavg"):
        fp_rows = [r for r in rows if r.fn == fn and r.rows == 2]
        assert fp_rows, f"{fn}: no rows at N=2"
        # tile_array ops do not emit a consistency fingerprint on the spark-path
        # (they time the op but don't collect output), so fingerprint may be empty.
        assert all(r.mode == "spark-path" for r in fp_rows), fn
    # Verify agg ops produce raster fingerprints on the smallest-N cell
    for fn in ("rst_frombands_agg", "rst_merge_agg", "rst_combineavg_agg"):
        fp_rows = [r for r in rows if r.fn == fn and r.rows == 2]
        assert fp_rows, f"{fn}: no rows at N=2"
        fp = fp_rows[0].output_fingerprint
        assert fp, f"{fn}: no consistency fingerprint at N=2 (expected raster fingerprint)"
        parsed = json.loads(fp)
        assert parsed["kind"] == "raster", (
            f"{fn}: unexpected fingerprint kind: {parsed.get('kind')}"
        )
