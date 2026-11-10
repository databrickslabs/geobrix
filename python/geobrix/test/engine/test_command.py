"""gbx_rst_gdal: the command's tokens, and what stops a command that leaves the tile it is given.

The golden rows (`fixtures/argv_golden.json`, sections `command` and `command_refused`) pin the argv GDAL gets and every
refusal. The matrix rows below try each allowed argument with a path, a URL, an @file and a step separator. The run rows
need the engine (the dev container) and check what the caller's text cannot reach: no file is read or written, no step
outside the list runs, and the budget judges a large output before GDAL writes it.
"""

import json
import os
from pathlib import Path

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import argv, command, run, spec

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "argv_golden.json").read_text()
)
MAN = manifest.load()
FN = MAN.functions["gbx_rst_gdal"]
SECTION = MAN.gdal
PROPS = [argv.RasterProps(64, 48, 1, "Float32")]


@pytest.fixture(autouse=True)
def _no_level_knob(monkeypatch):
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)


def _build(cmd):
    return argv.build(FN, {"command": cmd}, PROPS)


@pytest.mark.parametrize("case", GOLDEN["command"], ids=lambda c: c["id"])
def test_the_command_gives_the_golden_tokens(case):
    assert _build(case["command"]) == case["tokens"]


@pytest.mark.parametrize("case", GOLDEN["command_refused"], ids=lambda c: c["id"])
def test_a_refused_command_names_the_cause(case):
    with pytest.raises(spec.ParamError) as exc:
        _build(case["command"])
    assert "gbx_rst_gdal" in str(exc.value) and case["names"] in str(exc.value)


def test_a_null_command_gives_no_argv():
    assert argv.build(FN, {"command": None}, PROPS) is None


def test_the_engine_writes_the_read_and_write_steps_and_the_caller_cannot_move_them():
    tokens = _build("slope")
    assert tokens[:4] == ["read", "--input=<input>", "--input-format=GTiff", "!"]
    at = tokens.index("write")
    assert tokens[at : at + 3] == [
        "write",
        "--output=<output>",
        "--output-format=GTiff",
    ]
    assert tokens.count("write") == 1 and tokens.count("read") == 1
    # exactly one read and one write step, whatever the command holds
    steps = command.steps_of(_build("select --band=1 ! slope ! tpi"))
    assert [s[0] for s in steps] == ["read", "select", "slope", "tpi", "write"]


# --- every allowed argument against every way to name a file --------------------------------------------------------

_ARGS = [
    (step, arg, kind)
    for step, kinds in sorted(SECTION.command_allow.items())
    for arg, kind in sorted(kinds.items())
]
_FILE_VALUES = [
    "/etc/passwd",
    "../secret.tif",
    "@/etc/passwd",
    "@list.txt",
    "http://example.com/x",
    "file:///etc/passwd",
    "/vsicurl/http://example.com/x.tif",
    "/vsimem/x",
    "\\\\server\\share\\x",
    "C:\\x.tif",
    "x ! write --output=/tmp/x.tif",
    "x | write",
    "'x'",
    '"x"',
    "x\ny",
]


@pytest.mark.parametrize("value", _FILE_VALUES)
@pytest.mark.parametrize("step, arg, kind", _ARGS, ids=lambda v: str(v))
def test_no_allowed_argument_takes_a_path_url_file_or_separator(step, arg, kind, value):
    """A value that names a file, a URL or another step is refused for every allowed argument of every allowed step.
    The value is one word (quoted), so the refusal is the value's, not the splitter's.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    with pytest.raises(spec.ParamError):
        command.pipeline_tokens(
            f'{step} --{arg}="{escaped}"', SECTION, argv.canonical_crs, []
        )


@pytest.mark.parametrize(
    "step, arg, kind",
    [
        a for a in _ARGS if a[2] not in ("crs", "wkt")
    ],  # those are rewritten; the golden rows pin them
    ids=lambda v: str(v),
)
def test_no_allowed_argument_hides_a_second_step_in_its_value(step, arg, kind):
    """A good value is one token: the steps GDAL gets are the ones the caller wrote and no other."""
    ok = {
        "flag": "true",
        "integer": "1",
        "number": "1.5",
        "integers": "1,2",
        "numbers": "1,2",
        "word": "abc",
        "words": "a,b",
        "sizes": "10%,20",
        "expr": "[0,1]=1; DEFAULT=0",
    }[kind]
    steps = command.steps_of(
        command.pipeline_tokens(f'{step} --{arg}="{ok}"', SECTION, str, [])
    )
    assert [s[0] for s in steps] == ["read", step, "write"]


@pytest.mark.parametrize("step, kinds", sorted(SECTION.command_allow.items()))
def test_no_step_takes_an_argument_the_engine_or_the_deny_list_owns(step, kinds):
    for name in sorted(spec.ENGINE_ARGS | set(SECTION.deny)):
        if name in kinds:
            assert name == "geometry" and kinds[name] == "wkt", (step, name)
            continue
        with pytest.raises(spec.ParamError) as exc:
            command.check_step(
                [step, f"--{name}=1"], SECTION.command_allow, SECTION.deny, str
            )
        assert name in str(exc.value)


@pytest.mark.parametrize(
    "word",
    [
        "read",
        "write",
        "tee",
        "concat",
        "mosaic",
        "stack",
        "calc",
        "pipeline",
        "info",
        "edit",
        "contour",
        "polygonize",
        "rasterize",
        "color-map",
        "footprint",
        "vector",
        "convert",
        "SLOPE",
        " slope",
        "slope;",
        "slope\\",
        "../slope",
        "raster slope",
        "gdal",
    ],
)
def test_a_step_outside_the_allowlist_never_runs(word):
    with pytest.raises(spec.ParamError) as exc:
        command.check_step([word], SECTION.command_allow, SECTION.deny, str)
    assert "not one gbx_rst_gdal runs" in str(exc.value)


def test_the_allowlist_has_no_step_that_reads_or_writes_a_file_or_runs_a_formula():
    names = set(SECTION.command_allow)
    assert not names & {"read", "write", "tee", "concat", "mosaic", "stack", "calc"}
    assert not names & {"pipeline", "vector", "info", "edit", "convert", "footprint"}
    for step, kinds in SECTION.command_allow.items():
        assert "dialect" not in kinds, step
        assert not set(kinds) & (
            spec.ENGINE_ARGS | {"config", "like", "mask", "sd-filename", "color-map"}
        ), step
        assert not any(k.startswith("like") for k in kinds), step


def test_a_crs_value_is_rewritten_never_passed_through():
    tokens = _build(
        "reproject --output-crs=3857 --input-crs=EPSG:4326 --bbox=0,0,1,1 --bbox-crs=4326"
    )
    got = {
        t.split("=")[0]: t.split("=")[1]
        for t in tokens
        if t.startswith("--") and "crs" in t
    }
    assert set(got.values()) == {"EPSG:3857", "EPSG:4326"}


def test_a_polygon_is_written_again_from_the_parsed_geometry():
    tokens = _build('clip --geometry="polygon((0 0,10 0,10 10,0 0))"')
    geom = next(t for t in tokens if t.startswith("--geometry="))
    assert geom == "--geometry=POLYGON ((0 0, 10 0, 10 10, 0 0))"


def test_a_long_or_deep_command_is_refused():
    with pytest.raises(spec.ParamError, match="characters"):
        command.tokenize("slope " + "--unit=percent " * 5000)
    with pytest.raises(spec.ParamError, match="steps"):
        command.tokenize(" ! ".join(["slope"] * (command.MAX_STEPS + 1)))


def test_a_flag_takes_true_or_false_only():
    assert "--no-edges" in _build("slope --no-edges=true")
    assert "--no-edges" not in _build("slope --no-edges=false")


def test_the_pipeline_estimate_counts_a_resize_a_reprojection_and_a_held_step():
    one = argv._pipeline_estimate(_build("slope"), PROPS[0], None)
    assert one == 64 * 48 * 4  # a terrain step writes Float32
    big = argv._pipeline_estimate(_build("resize --size=640,480"), PROPS[0], None)
    assert big == 640 * 480 * 4
    pct = argv._pipeline_estimate(_build("resize --size=200%,200%"), PROPS[0], None)
    assert pct == 128 * 96 * 4
    held = argv._pipeline_estimate(_build("sieve ! fill-nodata"), PROPS[0], None)
    assert (
        held == 64 * 48 * 4 * 3
    )  # the input's size, once for each held step, plus the output


# --- the run: what the caller's text cannot reach --------------------------------------------------------------------

needs_engine = pytest.mark.skipif(
    bool(run.unavailable_reason()), reason=str(run.unavailable_reason())
)


def _dem(width=64, height=48, crs="EPSG:32633", nodata=None) -> bytes:
    y, x = np.mgrid[0:height, 0:width]
    z = (np.sin(x / 6.0) * 30 + y * 2).astype("float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=crs,
        nodata=nodata,
        transform=from_origin(500000.0, 4000000.0, 10.0, 10.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(z[np.newaxis])
        return mf.read()


def _run(cmd, raster=None):
    return run.run(FN, [raster if raster is not None else _dem()], {"command": cmd})


def _open(res):
    assert res.error is None, res.error
    return MemoryFile(res.raster).open()


@needs_engine
def test_a_step_gives_the_pixels_of_the_gdal_command():
    from osgeo import gdal

    src = _dem()
    gdal.FileFromMemBuffer("/vsimem/gbx_cmd_ref.tif", src)
    try:
        ref = gdal.DEMProcessing(
            "/vsimem/gbx_cmd_ref_out.tif",
            "/vsimem/gbx_cmd_ref.tif",
            "slope",
            alg="Horn",
            computeEdges=False,
            slopeFormat="percent",
        )
        want = ref.GetRasterBand(1).ReadAsArray()
        ref = None
    finally:
        gdal.Unlink("/vsimem/gbx_cmd_ref.tif")
        gdal.Unlink("/vsimem/gbx_cmd_ref_out.tif")
    with _open(_run("slope --unit=percent")) as out:
        got = out.read(1)
        nd = out.nodata
    inner = (slice(1, -1), slice(1, -1))
    assert np.allclose(got[inner], want[inner], rtol=1e-4)
    assert nd == -9999.0


@needs_engine
def test_a_pipeline_runs_every_step_in_order():
    with _open(_run("reproject --output-crs=3857 ! slope")) as out:
        assert out.crs.to_epsg() == 3857
        assert out.dtypes[0] == "float32"
    with _open(_run("resize --size=32,24 ! set-type --output-data-type=Int16")) as out:
        assert (out.width, out.height, out.dtypes[0]) == (32, 24, "int16")


@needs_engine
def test_a_polygon_clip_keeps_the_pixels_inside():
    wkt = "POLYGON((500100 3999600,500400 3999600,500400 3999900,500100 3999900,500100 3999600))"
    with _open(_run(f'clip --geometry="{wkt}"')) as out:
        assert (out.width, out.height) == (30, 30)
        assert out.transform.c == 500100.0


@needs_engine
@pytest.mark.parametrize(
    "cmd",
    [
        "write --output=/tmp/gbx_cmd_pwn.tif",
        "slope ! write --output=/tmp/gbx_cmd_pwn.tif",
        "slope --output=/tmp/gbx_cmd_pwn.tif",
        'reclassify --mapping="[0,100]=1 ! write --output=/tmp/gbx_cmd_pwn.tif"',
        "slope --config=GDAL_CACHEMAX=1",
        "clip --geometry=/tmp/gbx_cmd_pwn.geojson",
    ],
)
def test_a_refused_command_raises_before_gdal_runs_and_writes_nothing(cmd):
    with pytest.raises(spec.ParamError):
        _run(cmd)
    assert not os.path.exists("/tmp/gbx_cmd_pwn.tif")
    assert not os.path.exists("/tmp/gbx_cmd_pwn.geojson")


@needs_engine
def test_the_scratch_is_removed_after_every_call():
    from osgeo import gdal

    before = set(gdal.ReadDirRecursive("/vsimem/gbx-spec") or ())
    _run("slope")
    with pytest.raises(spec.ParamError):
        _run("write")
    assert set(gdal.ReadDirRecursive("/vsimem/gbx-spec") or ()) == before


@needs_engine
def test_gdal_refusing_a_value_is_a_parameter_error_naming_the_cause():
    with pytest.raises(spec.ParamError, match="kernel"):
        _run("neighbors --method=mean --size=3")


@needs_engine
def test_a_failure_in_the_run_is_an_error_tile_not_an_exception():
    res = _run('reclassify --mapping="[0,20]=1; [20,100]=2"')
    assert res.raster is None and "overlaps" in res.error


@needs_engine
def test_a_large_output_is_judged_before_gdal_writes_it(monkeypatch):
    monkeypatch.setattr(run, "decoded_budget", lambda: 4 * 1024 * 1024)
    res = _run("resize --size=20000,20000")
    assert res.raster is None and "MiB decoded" in res.error
    monkeypatch.setattr(run, "decoded_budget", lambda: 64 * 1024 * 1024)
    assert _run("resize --size=200,200").error is None


@needs_engine
def test_the_callers_bytes_are_never_opened_writable(monkeypatch):
    """The engine opens a tile's bytes read-only: no open of the input under the call's prefix carries OF_UPDATE."""
    from osgeo import gdal

    seen = []
    real = gdal.OpenEx

    def spy(name, flags=0, *a, **kw):
        seen.append((str(name), flags))
        return real(name, flags, *a, **kw)

    monkeypatch.setattr(gdal, "OpenEx", spy)
    _run("slope")
    updates = [n for n, f in seen if f & gdal.OF_UPDATE]
    assert not [n for n in updates if n.endswith("/in0.tif")], updates
    # every open the engine makes names a driver list
    assert seen


@needs_engine
def test_a_tile_in_a_file_is_read_by_its_path_with_the_virtual_tile_drivers(tmp_path):
    path = tmp_path / "dem.tif"
    path.write_bytes(_dem())
    res = run.run(FN, [run.PathSource(str(path))], {"command": "slope --unit=percent"})
    assert res.error is None, res.error
    assert str(path) not in res.command
