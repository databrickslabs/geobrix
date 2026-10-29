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


@pytest.mark.parametrize("case", GOLDEN["zstd_knob"], ids=lambda c: repr(c["knob"]))
def test_the_level_knob_reads_as_on_the_jvm(case, monkeypatch):
    """GBX_ZSTD_LEVEL: fast, max, or plain digits 1..22; anything else is the size ladder (both tiers)."""
    import warnings

    from databricks.labs.gbx.pyrx.core.compression import auto_level

    monkeypatch.setenv("GBX_ZSTD_LEVEL", case["knob"])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert auto_level(case["decoded_bytes"]) == case["level"]


def test_literal_arguments_are_bound_once(monkeypatch):
    """The argv of one (spec, values, input props) is bound once and reused per row with that row's file names."""
    fn = MAN.functions["gbx_rst_transformcrs"]
    calls = []
    real = argv._template
    monkeypatch.setattr(argv, "_template", lambda *a: calls.append(a) or real(*a))
    monkeypatch.setattr(argv, "_templates", argv.OrderedDict())
    props = [argv.RasterProps(64, 48, 1, "Float32")]
    rows = [
        argv.build(
            fn,
            {"target_crs": "EPSG:3857"},
            props,
            [f"/vsimem/r{i}/in0.tif"],
            f"/vsimem/r{i}/out.tif",
        )
        for i in range(3)
    ]
    assert len(calls) == 1
    assert [r[0] for r in rows] == [f"--input=/vsimem/r{i}/in0.tif" for i in range(3)]
