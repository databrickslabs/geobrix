"""Task 1 (SDD — Phase A native mini-COG mosaic): MosaicOptions parser + validator.

Pure-Python tests — no Spark, no JAR required.  Validates the parser for all
mosaic-mode option combinations and their error conditions.

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/ds/test_mosaic_options.py \
        --log mosaic-options.log
"""

from __future__ import annotations

import pytest

from databricks.labs.gbx.ds.cog_writer import MosaicOptions, parse_mosaic_options

# ── helpers ──────────────────────────────────────────────────────────────────


def _parse(**kwargs) -> MosaicOptions | None:
    """Call parse_mosaic_options with keyword-style args for readability."""
    return parse_mosaic_options(kwargs)


def _parse_str(**kwargs) -> MosaicOptions | None:
    """Call parse_mosaic_options with string values, mirroring Spark option dicts."""
    return parse_mosaic_options({k: str(v) for k, v in kwargs.items()})


# ── single-COG mode (no mosaic trigger) ─────────────────────────────────────


def test_no_mosaic_option_returns_none():
    """Absent mosaic and gridSystem → single-COG mode, returns None."""
    assert parse_mosaic_options({}) is None


def test_non_mosaic_options_return_none():
    """Options relevant to single-COG mode only → None (existing path unaffected)."""
    # These are cog_gbx options that do NOT trigger mosaic mode.
    result = parse_mosaic_options(
        {"compress": "zstd", "blockSize": "512", "cogBlockSize": "512"}
    )
    assert result is None


def test_mosaic_false_returns_none():
    """vrtMosaic='false' is an explicit opt-out → None (single-COG mode)."""
    assert _parse_str(vrtMosaic="false") is None


def test_mosaic_false_bool_returns_none():
    """vrtMosaic=False (Python bool) → None."""
    assert _parse(vrtMosaic=False) is None


# ── mosaic mode — valid native (gridSystem='none') ───────────────────────────


def test_grid_system_none_tile_size_parses_ok():
    """gridSystem='none' + tileSize=1024 → valid MosaicOptions with correct values."""
    opts = _parse(vrtMosaic="true", gridSystem="none", tileSize=1024)
    assert opts is not None
    assert opts.grid_system == "none"
    assert opts.tile_size == 1024


def test_grid_system_none_with_overlap_parses_ok():
    """gridSystem='none' + overlapPercent=10.5 → valid MosaicOptions."""
    opts = _parse(
        vrtMosaic="true", gridSystem="none", tileSize=512, overlapPercent=10.5
    )
    assert opts is not None
    assert opts.overlap_percent == 10.5
    assert opts.tile_size == 512


def test_grid_system_none_defaults():
    """mosaic='true' + gridSystem='none' → all optional fields use documented defaults."""
    opts = _parse(vrtMosaic="true", gridSystem="none")
    assert opts is not None
    assert opts.grid_system == "none"
    assert opts.tile_size is None
    assert opts.overlap_percent == 0.0
    assert opts.merge_strategy == "none"
    assert opts.prune_empty is True
    assert opts.write_vrt is True
    assert opts.vrt_paths == "relative"
    assert opts.grid_min_resolution is None
    assert opts.grid_max_resolution is None
    assert opts.grid_step_resolution is None


def test_mosaic_trigger_grid_system_alone():
    """Supplying gridSystem alone (no mosaic key) also triggers mosaic mode."""
    opts = _parse(gridSystem="none")
    assert opts is not None
    assert opts.grid_system == "none"


def test_merge_strategy_variants():
    """mergeStrategy values min/max/avg/first/last/none are all accepted."""
    for strategy in ("none", "min", "max", "avg", "first", "last"):
        opts = _parse(vrtMosaic="true", mergeStrategy=strategy)
        assert opts is not None, f"mergeStrategy={strategy!r} should be accepted"
        assert opts.merge_strategy == strategy


def test_write_vrt_false():
    """writeVrt=False stores correctly."""
    opts = _parse(vrtMosaic="true", writeVrt=False)
    assert opts is not None
    assert opts.write_vrt is False


def test_vrt_paths_absolute():
    """vrtPaths='absolute' is accepted."""
    opts = _parse(vrtMosaic="true", vrtPaths="absolute")
    assert opts is not None
    assert opts.vrt_paths == "absolute"


def test_string_values_parsed_correctly():
    """String-encoded options (as Spark passes them) parse to correct types."""
    opts = _parse_str(
        vrtMosaic="true",
        gridSystem="none",
        tileSize="256",
        overlapPercent="5.0",
        pruneEmpty="false",
        writeVrt="false",
    )
    assert opts is not None
    assert opts.tile_size == 256
    assert opts.overlap_percent == 5.0
    assert opts.prune_empty is False
    assert opts.write_vrt is False


# ── per-tile encoding options pass through (compression/blockSize) ────────────


def test_compression_with_mosaic_is_ok():
    """compress + mosaic mode → no error (encoding options pass through)."""
    opts = _parse(vrtMosaic="true", gridSystem="none", compress="zstd")
    assert opts is not None


def test_blocksize_with_mosaic_is_ok():
    """blockSize + mosaic mode → no error (encoding options pass through)."""
    opts = _parse(vrtMosaic="true", gridSystem="none", blockSize=512)
    assert opts is not None


def test_predictor_with_mosaic_is_ok():
    """predictor + mosaic mode → no error (encoding options pass through)."""
    opts = _parse(vrtMosaic="true", gridSystem="none", predictor=2)
    assert opts is not None


# ── Phase A: DGGS rejection ──────────────────────────────────────────────────


def test_quadbin_raises_not_supported():
    """gridSystem='quadbin' → ValueError (not supported in this release)."""
    with pytest.raises(ValueError, match="quadbin"):
        _parse(gridSystem="quadbin")


def test_quadbin_error_message_has_no_internal_phase_wording():
    """ValueError message for DGGS must not mention 'Phase B' or similar internal terms."""
    with pytest.raises(ValueError) as exc_info:
        _parse(gridSystem="quadbin")
    msg = str(exc_info.value).lower()
    assert "phase" not in msg, f"Internal 'phase' wording leaked into error: {msg!r}"
    assert "wave" not in msg, f"Internal 'wave' wording leaked into error: {msg!r}"


# ── cross-option validation: native-only vs DGGS-only ────────────────────────


def test_tile_size_with_dggs_raises():
    """tileSize + a DGGS gridSystem → ValueError (native-only option)."""
    # Phase A: quadbin is rejected before cross-option checks, but it's still ValueError.
    with pytest.raises(ValueError):
        _parse(gridSystem="quadbin", tileSize=1024)


def test_overlap_percent_with_dggs_raises():
    """overlapPercent + a DGGS gridSystem → ValueError."""
    with pytest.raises(ValueError):
        _parse(gridSystem="h3", overlapPercent=5.0)


def test_grid_min_resolution_with_none_raises():
    """gridMinResolution + gridSystem='none' → ValueError (DGGS-only option)."""
    with pytest.raises(ValueError, match="gridMinResolution"):
        _parse(vrtMosaic="true", gridSystem="none", gridMinResolution=4)


def test_grid_max_resolution_with_none_raises():
    """gridMaxResolution + gridSystem='none' → ValueError (DGGS-only option)."""
    with pytest.raises(ValueError, match="gridMaxResolution"):
        _parse(vrtMosaic="true", gridSystem="none", gridMaxResolution=8)


def test_grid_step_resolution_with_none_raises():
    """gridStepResolution + gridSystem='none' → ValueError (DGGS-only option)."""
    with pytest.raises(ValueError, match="gridStepResolution"):
        _parse(vrtMosaic="true", gridSystem="none", gridStepResolution=1)


# ── driverMode + mosaic contradiction ────────────────────────────────────────


def test_driver_mode_true_with_mosaic_raises():
    """driverMode=True + mosaic=True → ValueError (contradictory options)."""
    with pytest.raises(ValueError, match="driverMode"):
        _parse(vrtMosaic="true", driverMode=True)


def test_driver_mode_string_true_with_mosaic_raises():
    """driverMode='true' (string, as from Spark) + mosaic='true' → ValueError."""
    with pytest.raises(ValueError, match="driverMode"):
        _parse_str(vrtMosaic="true", driverMode="true")


def test_driver_mode_false_with_mosaic_is_ok():
    """driverMode=False + mosaic → no error (contradiction only when driverMode is True)."""
    opts = _parse(vrtMosaic="true", driverMode=False)
    assert opts is not None


def test_driver_mode_string_false_with_mosaic_is_ok():
    """driverMode='false' (string) + mosaic → no error."""
    opts = _parse_str(vrtMosaic="true", driverMode="false")
    assert opts is not None


# ── unknown gridSystem value ──────────────────────────────────────────────────


def test_unrecognised_grid_system_raises():
    """gridSystem with an unrecognised value → ValueError."""
    with pytest.raises(ValueError, match="gridSystem"):
        _parse(gridSystem="hexbin")


# ── quadbin acceptance (Phase B) ─────────────────────────────────────────────


def test_quadbin_requires_grid_resolution():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    with pytest.raises(ValueError, match="gridResolution"):
        parse_mosaic_options({"vrtMosaic": "true", "gridSystem": "quadbin"})


def test_quadbin_accepts_grid_resolution():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    o = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "quadbin", "gridResolution": "12"}
    )
    assert o.grid_system == "quadbin" and o.grid_resolution == 12


def test_quadbin_rejects_native_only_options():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    for bad in ({"tileSize": "512"}, {"overlapPercent": "5"}):
        with pytest.raises(ValueError, match="only valid with gridSystem='none'"):
            parse_mosaic_options(
                {
                    "vrtMosaic": "true",
                    "gridSystem": "quadbin",
                    "gridResolution": "12",
                    **bad,
                }
            )


def test_quadbin_downsamplefactor_errors():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    with pytest.raises(ValueError, match="downsampleFactor.*quadbin"):
        parse_mosaic_options(
            {
                "vrtMosaic": "true",
                "gridSystem": "quadbin",
                "gridResolution": "12",
                "downsampleFactor": "2",
            }
        )


def test_quadbin_pyramid_options_deferred():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    with pytest.raises(ValueError, match="resolution pyramid not yet"):
        parse_mosaic_options(
            {
                "vrtMosaic": "true",
                "gridSystem": "quadbin",
                "gridResolution": "12",
                "gridMinResolution": "5",
            }
        )


# ── h3 acceptance (Task 2) ───────────────────────────────────────────────────


def test_h3_requires_grid_resolution():
    import pytest

    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    with pytest.raises(ValueError, match="gridResolution"):
        parse_mosaic_options({"vrtMosaic": "true", "gridSystem": "h3"})


def test_h3_accepts_grid_resolution():
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    o = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "7"}
    )
    assert o.grid_system == "h3"
    assert o.grid_resolution == 7


def test_h3_rejects_native_only_options():
    import pytest

    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    base = {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "7"}
    for bad in ({"tileSize": "512"}, {"overlapPercent": "5"}):
        with pytest.raises(ValueError, match="only valid with gridSystem='none'"):
            parse_mosaic_options({**base, **bad})


def test_h3_allows_downsample_factor():
    """Unlike quadbin, h3 ALLOWS downsampleFactor."""
    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    # Should NOT raise — downsampleFactor is allowed with h3.
    o = parse_mosaic_options(
        {
            "vrtMosaic": "true",
            "gridSystem": "h3",
            "gridResolution": "7",
            "downsampleFactor": "2",
        }
    )
    assert o.grid_system == "h3"


def test_h3_pyramid_options_deferred():
    import pytest

    from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options

    base = {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "7"}
    with pytest.raises(ValueError, match="resolution pyramid not yet"):
        parse_mosaic_options({**base, "gridMinResolution": "5"})


# ── bng acceptance (Task 2) ──────────────────────────────────────────────────


def test_bng_gridsystem_accepted_int_index():
    opts = parse_mosaic_options({"gridSystem": "bng", "gridResolution": "3"})
    assert opts.grid_system == "bng"
    assert opts.grid_resolution == 3  # 1km


def test_bng_gridsystem_accepted_string_key():
    opts = parse_mosaic_options({"gridSystem": "bng", "gridResolution": "1km"})
    assert opts.grid_system == "bng"
    assert opts.grid_resolution == 3


def test_bng_rejects_metres_as_int():
    import pytest
    with pytest.raises(ValueError, match="resolution"):
        parse_mosaic_options({"gridSystem": "bng", "gridResolution": "1000"})


def test_bng_rejects_pyramid_options():
    import pytest
    with pytest.raises(ValueError, match="pyramid"):
        parse_mosaic_options({"gridSystem": "bng", "gridResolution": "3",
                              "gridMinResolution": "2"})


def test_bng_downsamplefactor_errors():
    """gridSystem='bng' + downsampleFactor → ValueError (cell-based, not pixel downsampling)."""
    with pytest.raises(ValueError, match="downsampleFactor"):
        parse_mosaic_options(
            {
                "vrtMosaic": "true",
                "gridSystem": "bng",
                "gridResolution": "3",
                "downsampleFactor": "2",
            }
        )
