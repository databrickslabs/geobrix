"""(spec, bound values, input properties) -> the exact argv, and the write profile's creation options.

The fixture is the engine contract on both tiers: GdalSpecSmokeTest (Scala) reads the same file. Pure Python
plus the canonical CRS resolver (rasterio): every row runs on the host.
"""

import json
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import argv, spec
from databricks.labs.gbx.pyrx.core.compression import engine_creation_options

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "argv_golden.json").read_text()
)
MAN = manifest.load()


@pytest.fixture(autouse=True)
def _no_level_knob(monkeypatch):
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)


def _props(case):
    return [argv.RasterProps(w, h, b, t) for w, h, b, t in case["inputs"]]


@pytest.mark.parametrize("case", GOLDEN["argv"], ids=lambda c: c["id"])
def test_the_argv_is_the_golden_one(case):
    fn = MAN.functions[case["fn"]]
    assert argv.build(fn, case["values"], _props(case)) == case["argv"]


@pytest.mark.parametrize("case", GOLDEN["creation_options"], ids=lambda c: c["id"])
def test_the_write_profile_is_the_golden_one(case):
    got = engine_creation_options(
        case["dtype"], case["decoded_bytes"], case["width"], case["height"]
    )
    assert got == case["options"]


@pytest.mark.parametrize("case", GOLDEN["refused"], ids=lambda c: c["id"])
def test_a_refused_binding_names_the_value(case):
    fn = MAN.functions[case["fn"]]
    err = spec.ParamError if case["error"] == "param" else spec.DataError
    with pytest.raises(err) as exc:
        argv.build(fn, case["values"], _props(case))
    assert case["names"] in str(exc.value) and fn.sql_name in str(exc.value)


def test_a_null_required_value_is_a_null_result():
    fn = MAN.functions["gbx_rst_slope"]
    assert (
        argv.build(
            fn,
            {"unit": None, "xscale": None, "yscale": None},
            _props(GOLDEN["argv"][0]),
        )
        is None
    )
