"""T2 fail-on-revert wiring tests for bench compression consumers.

NOTE: test/bench is de-scoped from the light CI suite (_LIGHT_TEST_DIRS in
.github/actions/pyrx_build does not include test/bench).  Run directly:
    gbx:test:python --path python/geobrix/test/bench/test_bench_compression_wiring.py
"""


def test_compression_sweep_predictor_for_is_canonical():
    """Fail-on-revert: compression_sweep.predictor_for must be the canonical function.

    Before the fold, compression_sweep.py defined its own `predictor_for` at lines
    66-72.  After the fold it imports the canonical one.  If reverted, the local
    definition is restored and `is` identity fails.
    """
    from databricks.labs.gbx.bench import compression_sweep
    from databricks.labs.gbx.pyrx.core import compression as C

    assert compression_sweep.predictor_for is C.predictor_for, (
        "compression_sweep.predictor_for must be the canonical function imported "
        "from pyrx.core.compression, not a local re-implementation. "
        "Reverting the fold restores lines 66-72 which this check catches."
    )


def test_datagen_cog_write_large_raster_has_predictor(tmp_path):
    """Fail-on-revert (GAP-3 / COG is_cog key-mapping): datagen COG output must
    carry ZSTD + predictor, set via creation_opts(driver='COG').

    Old code: {"COMPRESS": compress} — no predictor, no LEVEL key.
    After fold: creation_opts("float32", compress="zstd", driver="COG") adds
    predictor=3 and LEVEL=6.  This test fails if reverted.
    """
    import rasterio
    from rasterio.enums import Compression

    from databricks.labs.gbx.bench.datagen import write_large_raster_streamed

    dest = tmp_path / "wiring_cog.tif"
    write_large_raster_streamed(
        dest,
        width=64,
        height=64,
        bands=1,
        dtype="float32",
        srid=4326,
        tiled=True,
        block_size=64,
        compress="ZSTD",
        seed=0,
    )
    with rasterio.open(str(dest)) as ds:
        assert ds.compression == Compression.zstd, (
            f"Expected ZSTD-compressed COG from datagen, got {ds.compression}. "
            "Reverting the fold loses the creation_opts call."
        )
        pred = ds.profile.get("predictor") or ds.tags(ns="IMAGE_STRUCTURE").get(
            "PREDICTOR"
        )
        assert pred is not None and int(pred) == 3, (
            f"GAP-3 guard: expected predictor=3 for float32 ZSTD COG from "
            f"write_large_raster_streamed, got {pred!r}. "
            "Old code used 'COMPRESS': compress with no predictor key."
        )
