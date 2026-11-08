"""Writer option tables: the mosaic options of the COG writer, one row per rule.

Pure Python (no Spark, no JAR). ``parse_mosaic_options`` takes the raw option mapping a Spark
writer receives (all strings) and returns the validated ``MosaicOptions``, ``None`` when mosaic
mode is not asked for, or raises ``ValueError`` with a sentence that says what to change.
"""

from __future__ import annotations

import re

import pytest

from databricks.labs.gbx.ds.cog_writer import MosaicOptions, parse_mosaic_options

_PYRAMID = (
    "{}: DGGS resolution pyramid not yet implemented (deferred to a follow-on release)."
)
_ONLY_NONE = (
    "{} is only valid with gridSystem='none' (native tiling); got gridSystem={!r}"
)
_ONLY_DGGS = (
    "{} is only valid with a DGGS gridSystem (quadbin, bng, h3); got gridSystem='none'"
)
_CONTRADICTION = (
    "driverMode=True and mosaic mode are contradictory: driverMode produces a single driver-side COG "
    "while mosaic mode produces per-tile mini-COGs on executors. Use one or the other, not both."
)
_BNG = {"gridSystem": "bng", "gridResolution": "3"}
_QB = {"vrtMosaic": "true", "gridSystem": "quadbin", "gridResolution": "12"}
_H3 = {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "7"}
_NATIVE = {"vrtMosaic": "true", "gridSystem": "none"}

# Options that do not ask for mosaic mode: the single-COG path is unchanged.
NOT_MOSAIC = [
    {},
    {"compress": "zstd", "blockSize": "512", "cogBlockSize": "512"},
    {"vrtMosaic": "false"},
    {"vrtMosaic": False},
]


@pytest.mark.parametrize(
    "opts",
    NOT_MOSAIC,
    ids=lambda o: ",".join(f"{k}={v}" for k, v in o.items()) or "empty",
)
def test_options_that_do_not_ask_for_a_mosaic_give_none(opts):
    assert parse_mosaic_options(opts) is None


# (options, the MosaicOptions fields they must produce); fields not listed keep the documented default.
DEFAULTS = dict(
    grid_system="none", tile_size=None, overlap_percent=0.0, merge_strategy="none", prune_empty=True,
    write_vrt=True, vrt_paths="relative", grid_resolution=None, grid_min_resolution=None,
    grid_max_resolution=None, grid_step_resolution=None,
)  # fmt: skip
ACCEPT = {
    "native_defaults": (_NATIVE, {}),
    "gridsystem_alone_triggers": ({"gridSystem": "none"}, {}),
    "native_tile_and_overlap": ({**_NATIVE, "tileSize": 1024, "overlapPercent": 10.5}, {"tile_size": 1024, "overlap_percent": 10.5}),
    "strings_as_spark_passes_them": (
        {**_NATIVE, "tileSize": "256", "overlapPercent": "5.0", "pruneEmpty": "false", "writeVrt": "false"},
        {"tile_size": 256, "overlap_percent": 5.0, "prune_empty": False, "write_vrt": False},
    ),
    "write_vrt_false": ({"vrtMosaic": "true", "writeVrt": False}, {"write_vrt": False}),
    "vrt_paths_absolute": ({"vrtMosaic": "true", "vrtPaths": "absolute"}, {"vrt_paths": "absolute"}),
    "per_tile_encoding_passes_through": ({**_NATIVE, "compress": "zstd", "blockSize": 512, "predictor": 2}, {}),
    "driver_mode_false": ({"vrtMosaic": "true", "driverMode": False}, {}),
    "driver_mode_false_string": ({"vrtMosaic": "true", "driverMode": "false"}, {}),
    "quadbin_resolution": (_QB, {"grid_system": "quadbin", "grid_resolution": 12}),
    "h3_resolution": (_H3, {"grid_system": "h3", "grid_resolution": 7}),
    "h3_allows_downsample_factor": ({**_H3, "downsampleFactor": "2"}, {"grid_system": "h3", "grid_resolution": 7}),
    "bng_index": (_BNG, {"grid_system": "bng", "grid_resolution": 3}),
    "bng_key": ({"gridSystem": "bng", "gridResolution": "1km"}, {"grid_system": "bng", "grid_resolution": 3}),
    **{
        f"merge_{s}": ({"vrtMosaic": "true", "mergeStrategy": s}, {"merge_strategy": s})
        for s in ("none", "min", "max", "avg", "first", "last")
    },
}  # fmt: skip


@pytest.mark.parametrize("opts,fields", ACCEPT.values(), ids=ACCEPT.keys())
def test_mosaic_options_accepted(opts, fields):
    got = parse_mosaic_options(opts)
    assert isinstance(got, MosaicOptions)
    for name, want in {**DEFAULTS, **fields}.items():
        assert getattr(got, name) == want, name


# (options, the whole error sentence)
REJECT = {
    "driver_mode_true": ({"vrtMosaic": "true", "driverMode": True}, _CONTRADICTION),
    "driver_mode_true_string": ({"vrtMosaic": "true", "driverMode": "true"}, _CONTRADICTION),
    "unknown_grid_system": (
        {"gridSystem": "hexbin"},
        "gridSystem='hexbin' is not a recognised value; must be one of ['bng', 'h3', 'none', 'quadbin']",
    ),
    "unknown_merge_strategy": (
        {"vrtMosaic": "true", "mergeStrategy": "median"},
        "mergeStrategy='median' is not recognised; must be one of ['avg', 'first', 'last', 'max', 'min', 'none']",
    ),
    "unknown_vrt_paths": ({"vrtMosaic": "true", "vrtPaths": "mixed"}, "vrtPaths='mixed' must be 'relative' or 'absolute'"),
    "grid_resolution_with_none": (
        {**_NATIVE, "gridResolution": "3"},
        "gridResolution is only valid with a DGGS gridSystem (quadbin, bng, h3); got gridSystem='none'",
    ),
    "quadbin_needs_resolution": (
        {"vrtMosaic": "true", "gridSystem": "quadbin"},
        "gridResolution is required when gridSystem='quadbin'; supply an integer resolution level.",
    ),
    "h3_needs_resolution": (
        {"vrtMosaic": "true", "gridSystem": "h3"},
        "gridResolution is required when gridSystem='h3'; supply an integer resolution level.",
    ),
    "bng_needs_resolution": (
        {"gridSystem": "bng"},
        "gridResolution is required when gridSystem='bng'; supply an integer resolution index (1-6) or a string key "
        "(e.g. '1km', '100m').".replace("(1-6)", "(1–6)"),
    ),
    "bng_metres_as_int": ({"gridSystem": "bng", "gridResolution": "1000"}, "resolution"),
    **{
        f"{g}_native_only_{o}": ({**base, o: v}, _ONLY_NONE.format(o, g))
        for g, base in (("quadbin", _QB), ("h3", _H3))
        for o, v in (("tileSize", "512"), ("overlapPercent", "5"))
    },
    **{
        f"native_dggs_only_{o}": ({**_NATIVE, o: 4}, _ONLY_DGGS.format(o))
        for o in ("gridMinResolution", "gridMaxResolution", "gridStepResolution")
    },
    **{
        f"{g}_pyramid_{o}": ({**base, o: "5"}, _PYRAMID.format(o))
        for g, base in (("quadbin", _QB), ("h3", _H3), ("bng", _BNG))
        for o in ("gridMinResolution", "gridMaxResolution", "gridStepResolution")
    },
    **{
        f"{g}_downsample_factor": (
            {**base, "downsampleFactor": "2"},
            f"downsampleFactor is not supported with gridSystem='{g}'; "
            f"{g} mosaic uses cell-based resolution, not pixel downsampling.",
        )
        for g, base in (("quadbin", _QB), ("bng", _BNG))
    },
}  # fmt: skip


@pytest.mark.parametrize("opts,sentence", REJECT.values(), ids=REJECT.keys())
def test_mosaic_options_rejected_with_the_rule_stated(opts, sentence):
    with pytest.raises(ValueError, match=re.escape(sentence)) as ei:
        parse_mosaic_options(opts)
    assert not re.search(
        r"\b(phase|wave)\b", str(ei.value).lower()
    ), "internal wording leaked into the message"
