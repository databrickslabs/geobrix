"""(spec, bound values, input properties) -> the exact argv, and the write profile's creation options.

The fixture is the engine contract (the engine has no JVM level). Pure Python
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
    # (width, height, bands, GDAL type) and, optionally, band 1's declared NoData value
    return [argv.RasterProps(*props) for props in case["inputs"]]


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


def test_a_byte_hillshade_is_written_striped_not_tiled_and_compressed():
    """GDAL 3.13.3's `raster hillshade` leaves the NoData neighbours of a hole unmasked in a tiled, compressed Byte
    GeoTIFF (see `engine_creation_options`): the hillshade output is striped, every other output keeps its tiling.
    """
    props = [argv.RasterProps(64, 48, 1, "Float32")]
    values = {
        "azimuth": 315.0,
        "altitude": 45.0,
        "z_factor": 1.0,
        "xscale": None,
        "yscale": None,
    }
    hill = argv.build(MAN.functions["gbx_rst_hillshade"], values, props)
    assert "--creation-option=COMPRESS=ZSTD" in hill
    assert not [
        t
        for t in hill
        if t.startswith(("--creation-option=TILED", "--creation-option=BLOCK"))
    ]
    tri = argv.build(MAN.functions["gbx_rst_tri"], {}, props)
    assert "--creation-option=TILED=YES" in tri
    assert "TILED=YES" in engine_creation_options("Byte", 4096, 64, 48, "raster tri")
    assert "TILED=YES" not in engine_creation_options(
        "Byte", 4096, 64, 48, "raster hillshade"
    )


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


@pytest.mark.parametrize(
    "fn, values",
    [
        ("gbx_rst_ndvi", {"red_idx": 3, "nir_idx": None}),
        ("gbx_rst_savi", {"red_idx": 3, "nir_idx": 4, "l": None}),
        ("gbx_rst_threshold", {"op": None, "value": 1.0}),
        ("gbx_rst_index", {"formula_name": None, "band_map": {"red": 3, "nir": 4}}),
        ("gbx_rst_index", {"formula_name": "ndvi", "band_map": None}),
    ],
)
def test_a_null_value_of_a_formula_is_a_null_result(fn, values):
    """A NULL band, constant, operator, index name or band map gives a NULL result, never a stand-in."""
    assert (
        argv.build(MAN.functions[fn], values, [argv.RasterProps(64, 48, 4, "Float32")])
        is None
    )


def test_only_the_binders_values_reach_a_formula():
    """A threshold operator is one of the closed list; text that would extend the expression is refused."""
    fn = MAN.functions["gbx_rst_threshold"]
    props = [argv.RasterProps(64, 48, 1, "Float32")]
    for op in (">; 1", "> 0 ? A : 1 #", "A"):
        with pytest.raises(spec.ParamError):
            argv.build(fn, {"op": op, "value": 1.0}, props)


def test_a_color_table_is_written_to_a_file_of_its_own_in_a_canonical_form():
    """The table's text is checked line by line and handed to GDAL as the canonical lines: commas, tabs, comments and
    case are normalized. A table with no `nv` line gets one (transparent for RGBA, black for RGB), because GDAL
    colors a NoData pixel like the lowest value otherwise; the file name fills the placeholder of the argv.
    """
    fn = MAN.functions["gbx_rst_color_relief"]
    table = "# ramp\n100,0,0,255\n\n 150\t0 255 0 \r\n50% RED"
    assert argv.text_values(fn, {"color_table": table}) == [
        "100 0 0 255\n150 0 255 0\n50% red\nnv 0 0 0\n"
    ]
    assert argv.text_values(fn, {"color_table": "1 0 0 0 255\nnv 9 9 9"}) == [
        "1 0 0 0 255\nnv 9 9 9\n"
    ]
    assert argv.text_values(fn, {"color_table": "1 0 0 0 255"}) == [
        "1 0 0 0 255\nnv 0 0 0 0\n"
    ]
    assert argv.text_values(fn, {"color_table": None}) == []
    props = [argv.RasterProps(64, 48, 1, "Float32")]
    got = argv.build(
        fn,
        {"color_table": table},
        props,
        ["/vsimem/r/in0.tif"],
        "/vsimem/r/out.tif",
        texts=["/vsimem/r/arg_0.txt"],
    )
    assert "--color-map=/vsimem/r/arg_0.txt" in got
    assert argv.build(fn, {"color_table": None}, props) is None


def test_a_malformed_color_table_names_its_first_bad_line_and_carries_a_fix():
    fn = MAN.functions["gbx_rst_color_relief"]
    with pytest.raises(spec.ParamError) as exc:
        argv.text_values(fn, {"color_table": "100 0 0 255\n# c\n150 0 255"})
    assert "line 3: '150 0 255'" in str(exc.value) and "Fix:" in str(exc.value)


@pytest.mark.parametrize("case", GOLDEN["zstd_knob"], ids=lambda c: repr(c["knob"]))
def test_the_level_knob_reads_as_the_size_ladder_defines_it(case, monkeypatch):
    """GBX_ZSTD_LEVEL: fast, max, or plain digits 1..22; anything else is the size ladder."""
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


def test_the_engine_settings_table_is_the_golden_one():
    """The engine's GDAL settings, applied thread-local per call: the golden table. Removing a setting from
    either table fails here.
    """
    from databricks.labs.gbx.engine import run

    assert run.ENGINE_SETTINGS == GOLDEN["engine_settings"]


def test_a_contour_level_is_given_back_from_the_layers_rounded_value():
    """GDAL's output layer holds a level rounded to six decimals; the engine gives the level the call asked for."""
    fn = MAN.functions["gbx_rst_contour"]
    fixed = {"levels": [0.1 + 0.2, 1 / 3, 99.5], "interval": 0.0, "base": 0.0}
    field, snap = argv.level_snapper(fn, fixed)
    assert field == "value"
    assert snap(0.3) == 0.1 + 0.2 and snap(0.333333) == 1 / 3 and snap(99.5) == 99.5
    assert snap(7.25) == 7.25 and snap(None) is None  # no level near it: kept as read
    field, snap = argv.level_snapper(
        fn, {"levels": [], "interval": 0.7, "base": 0.15, "attr_field": "elev"}
    )
    assert snap(2.25) == 0.15 + 3 * 0.7 and snap(4.35) == 0.15 + 6 * 0.7
    assert argv.level_snapper(MAN.functions["gbx_rst_slope"], {}) is None
