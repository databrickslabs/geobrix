"""The `gdal` blocks of the manifest: a closed vocabulary, a closed allowlist, and the values the engine refuses.

Pure Python (no Spark, no GDAL): every row runs on the host.
"""

import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import spec

MAN = manifest.load()
ENGINE_FNS = sorted(
    (f for f in MAN.functions.values() if f.gdal is not None), key=lambda f: f.sql_name
)
REPO = Path(__file__).resolve().parents[4]


def test_the_engine_rows_carry_a_gdal_block_and_the_pin_is_the_build_pin():
    assert {f.sql_name for f in ENGINE_FNS} >= {
        "gbx_rst_slope",
        "gbx_rst_transformcrs",
        "gbx_rst_mapalgebra",
    }
    pin = re.search(
        r"^GDAL_VERSION=(\S+)$", (REPO / "scripts/native/pins.conf").read_text(), re.M
    ).group(1)
    assert MAN.gdal.pin == pin


@pytest.mark.parametrize("fn", ENGINE_FNS, ids=lambda f: f.sql_name)
def test_every_gdal_block_validates(fn):
    spec.validate(fn, MAN.gdal)
    assert "engine" in fn.levels["L1"] and "engine" in fn.levels["L3"]


def _row(**over):
    """The slope block as raw JSON, with `over` merged in (None deletes a key)."""
    raw = {
        "alg": "raster slope",
        "in": {"tile": "raster"},
        "out": {"kind": "raster", "dtype": "Float32"},
        "write": "tile",
        "args": [
            {
                "arg": "unit",
                "from": "unit",
                "type": "choice",
                "map": {"degrees": "degree"},
            },
            {
                "arg": "xscale",
                "from": "xscale",
                "type": "double",
                "null": "omit",
                "with": "yscale",
            },
            {
                "arg": "yscale",
                "from": "yscale",
                "type": "double",
                "null": "omit",
                "with": "xscale",
            },
            {"arg": "no-edges", "const": True},
        ],
    }
    raw.update(over)
    return {k: v for k, v in raw.items() if v is not None}


SLOPE = MAN.functions["gbx_rst_slope"]
REFUSED_BLOCKS = [
    ("unknown block key", _row(engine="fast"), "engine"),
    (
        "unknown arg key",
        _row(args=[{"arg": "unit", "from": "unit", "type": "choice", "pad": 1}]),
        "pad",
    ),
    (
        "unknown binder",
        _row(args=[{"arg": "unit", "from": "unit", "type": "path"}]),
        "path",
    ),
    ("unknown input kind", _row(**{"in": {"tile": "dataset"}}), "dataset"),
    (
        "unknown null rule",
        _row(args=[{"arg": "unit", "from": "unit", "type": "choice", "null": "zero"}]),
        "zero",
    ),
    (
        "const and from together",
        _row(args=[{"arg": "unit", "const": "degree", "from": "unit"}]),
        "unit",
    ),
    ("a VRT write profile", _row(write="vrt"), "vrt"),
    ("a GDALG write profile", _row(write="gdalg"), "gdalg"),
]


@pytest.mark.parametrize(
    "why,raw,named", REFUSED_BLOCKS, ids=[r[0] for r in REFUSED_BLOCKS]
)
def test_a_block_outside_the_vocabulary_is_refused_at_load(why, raw, named):
    with pytest.raises(ValueError, match=re.escape(named)):
        manifest.parse_gdal_spec(raw)


REFUSED_SPECS = [
    ("an algorithm off the allowlist", _row(alg="raster tile"), "raster tile"),
    (
        "an argument off the allowlist",
        _row(args=[{"arg": "zfactor", "const": 2}]),
        "zfactor",
    ),
    (
        "a param the function lacks",
        _row(args=[{"arg": "unit", "from": "units", "type": "choice"}]),
        "units",
    ),
    (
        "a one-sided with group",
        _row(
            args=[
                {
                    "arg": "xscale",
                    "from": "xscale",
                    "type": "double",
                    "null": "omit",
                    "with": "yscale",
                }
            ]
        ),
        "yscale",
    ),
    ("an input the function lacks", _row(**{"in": {"dem": "raster"}}), "dem"),
] + [
    (f"the deny-listed {a}", _row(args=[{"arg": a, "const": "x"}]), a)
    for a in sorted(MAN.gdal.deny)
]


@pytest.mark.parametrize(
    "why,raw,named", REFUSED_SPECS, ids=[r[0] for r in REFUSED_SPECS]
)
def test_a_spec_the_allowlist_does_not_cover_is_refused(why, raw, named):
    block = manifest.parse_gdal_spec(raw)
    with pytest.raises(spec.SpecError, match=re.escape(named)):
        spec.validate(SLOPE, MAN.gdal, block)


@pytest.mark.parametrize(
    "value",
    [
        "/etc/passwd",
        "\\\\host\\share",
        "/vsicurl/http://x/a.tif",
        "@file.txt",
        "http://x/crs.json",
        "a/../b",
    ],
)
def test_a_value_that_names_a_file_or_url_is_refused(value):
    with pytest.raises(spec.ParamError, match=re.escape(value)):
        spec.check_value("gbx_rst_transformcrs", "target_crs", value)


@pytest.mark.parametrize(
    "value", ["EPSG:3857", "3857", "+proj=merc +nadgrids=@null", "(A-B)/(A+B)"]
)
def test_an_ordinary_value_passes(value):
    spec.check_value("gbx_rst_transformcrs", "target_crs", value)


@pytest.mark.parametrize(
    "tokens,named",
    [
        (["-of=VRT"], "-of"),  # a short flag
        (["--output-format=VRT"], "output-format"),  # a deny-listed argument
        (["--zfactor=2"], "zfactor"),  # an argument off the allowlist
        (["--unit=degree", "!", "--unit=percent"], "!"),  # a step separator
    ],
)
def test_the_engine_core_refuses_tokens_the_spec_did_not_make(tokens, named):
    with pytest.raises(spec.SpecError, match=re.escape(named)):
        spec.guard_tokens(MAN.gdal, "raster slope", tokens)
    with pytest.raises(spec.SpecError, match="raster tile"):
        spec.guard_tokens(MAN.gdal, "raster tile", ["--unit=degree"])
