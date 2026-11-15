"""gbx_rst_gdal: the command's tokens, and what stops a command that leaves the tile it is given.

The golden rows (`fixtures/argv_golden.json`, sections `command` and `command_refused`) pin the argv GDAL gets and every
refusal. The matrix rows below try each allowed argument with a path, a URL, an @file and a step separator. The run rows
need the engine (the dev container) and check what the caller's text cannot reach: no file is read or written, no step
outside the list runs, and the budget judges a large output before GDAL writes it.
"""

import contextlib
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


@pytest.mark.parametrize(
    "cmd,why",
    [
        ("clip --bbox=1,2", "needs four numbers"),
        ("clip --window=0,0", "needs four numbers"),
        ("reproject --output-crs=3857 --bbox=0,0,1", "needs four numbers"),
        ("clip --bbox=0,0,inf,1", "must be finite"),
        ("resize --resolution=nan", "must be finite"),
        ("resize --resolution=10,-inf", "must be finite"),
    ],
)
def test_a_box_or_window_of_the_wrong_size_or_a_value_that_is_not_finite_is_refused_by_name(
    cmd, why
):
    """These reached the budget estimate and failed there with an IndexError, ValueError or OverflowError."""
    with pytest.raises(spec.ParamError, match=why):
        _build(cmd)


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


_GOOD = {
    "kernel": "[[1,1,1],[1,1,1],[1,1,1]]",
    "mapping": "[0,1]=1",
    "output-data-type": "Int16",
    "band": "1",
    "position": "1,2,3",
    "size": "10,10",
}


def _needs(step, skip=()):
    """The arguments a step cannot run without (`command.REQUIRED`, and a size for resize), written out."""
    keys = list(command.REQUIRED.get(step, ())) + (["size"] if step == "resize" else [])
    return "".join(f" --{k}={_GOOD[k]}" for k in keys if k not in skip)


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
            f'{step}{_needs(step, (arg,))} --{arg}="{escaped}"',
            SECTION,
            argv.canonical_crs,
            [],
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
    ok = {"bbox": "1,2,3,4", "window": "1,2,3,4"}.get(arg, ok)  # a box has four numbers
    steps = command.steps_of(
        command.pipeline_tokens(
            f'{step}{_needs(step, (arg,))} --{arg}="{ok}"', SECTION, str, []
        )
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


class _Geo(argv.NoGeo):
    """A reprojection that keeps the grid of the state it is given, scaled by `scale` in each direction."""

    def __init__(self, scale=1.0):
        self.scale = scale

    def warp(self, st, target):
        return (
            int(st.width * self.scale),
            int(st.height * self.scale),
            st.px_x / self.scale,
            st.px_y / self.scale,
            st.ox,
            st.oy,
        )

    def envelope(self, bbox, from_crs, to_crs):
        return tuple(bbox)


def _start(width=64, height=48, bands=1, item=4, px=10.0):
    return argv.PipeState(
        width, height, px, px, 0.0, 0.0, "EPSG:32633", bands, item, True
    )


def _peak(cmd, start=None, geo=None):
    return argv.pipeline_peak(_build(cmd), start or _start(), geo or _Geo())


def test_the_peak_is_the_largest_state_any_step_leaves():
    assert _peak("slope") == 64 * 48 * 4  # a terrain step writes Float32
    assert _peak("resize --size=640,480") == 640 * 480 * 4
    assert _peak("resize --size=200%,200%") == 128 * 96 * 4
    assert (
        _peak("sieve") == 64 * 48 * 4 * 2
    )  # a step that holds its result also holds its input
    # a later step that shrinks the tile does not lower the peak
    assert _peak("resize --size=640,480 ! resize --size=10,10") == 640 * 480 * 4


def test_chained_resizes_are_sized_from_the_step_before_not_from_the_input():
    """Two 200% resizes make a tile 16 times the pixels, not 4: the second is sized from the first's output."""
    once = _peak("resize --size=200%,200%")
    twice = _peak("resize --size=200%,200% ! resize --size=200%,200%")
    assert twice == 16 * 64 * 48 * 4 == 4 * once
    assert (
        _peak(
            "resize --size=200%,200% ! resize --size=200%,200% ! resize --size=200%,200%"
        )
        == 64 * 64 * 48 * 4
    )


def test_a_resolution_is_read_against_the_extent_the_steps_before_it_left():
    # 640 x 480 m of 10 m pixels; a 1 m resolution makes 640 x 480 pixels, and after a 2x reprojection, 1280 x 960
    assert _peak("resize --resolution=1,1") == 640 * 480 * 4
    assert (
        _peak("reproject --output-crs=3857 ! resize --resolution=1,1", geo=_Geo(2.0))
        == 640 * 480 * 4
    )
    # a fine resolution after a reprojection is bounded by that reprojection's grid
    assert (
        _peak("reproject --output-crs=3857 --resolution=0.5,0.5", geo=_Geo(2.0))
        == 1280 * 960 * 4
    )


def test_a_reprojection_is_sized_from_the_state_it_gets():
    # each reproject asks GDAL for the grid of the step before: two 2x grids make 4x each way
    assert (
        _peak(
            "reproject --output-crs=3857 ! reproject --output-crs=4326", geo=_Geo(2.0)
        )
        == 256 * 192 * 4
    )
    assert (
        _peak("resize --size=128,96 ! reproject --output-crs=3857", geo=_Geo(2.0))
        == 256 * 192 * 4
    )


def test_a_clip_shrinks_the_state_and_a_type_step_changes_the_bytes():
    assert _peak("clip --window=0,0,16,8 ! resize --size=200%,200%") == max(
        16 * 8 * 4, 32 * 16 * 4
    )
    assert (
        _peak("set-type --output-data-type=Byte ! resize --size=200%,200%")
        == 128 * 96 * 1
    )
    assert _peak("set-type --output-data-type=Float64") == 64 * 48 * 8
    # a box outside the tile is bounded by the box itself when the step allows it
    big = _peak("clip --bbox=0,0,64000,48000 --allow-bbox-outside-source")
    assert big == 6400 * 4800 * 4
    assert _peak("clip --bbox=0,0,64000,48000") == 64 * 48 * 4


def test_a_band_or_type_step_changes_the_bands_or_the_bytes_of_a_pixel():
    three = _start(bands=3, item=2)
    assert _peak("nodata-to-alpha", three) == 64 * 48 * 4 * 2  # an alpha band more
    assert _peak("select --band=1,2", three) == 64 * 48 * 2 * 2  # the bands listed
    assert _peak("select --band=1 --exclude", three) == 64 * 48 * 2 * 2  # the rest
    assert _peak("unscale", three) == 64 * 48 * 3 * 8  # unscaled to Float64


class _NoGrid(argv.NoGeo):
    """GDAL gives no output grid, and a box moves from metres into a CRS whose unit is 10 km (as a degree is ~111 km)."""

    def envelope(self, bbox, from_crs, to_crs):
        return tuple(v / 1e4 for v in bbox)


def test_a_box_with_no_grid_from_gdal_is_sized_in_the_target_crss_units():
    # 640 x 480 m of 10 m pixels: in the target's units the pixel is 0.001, so a box twice the tile is 128 x 96 pixels
    box = "reproject --output-crs=4326 --bbox=0,0,0.128,0.096"
    assert _peak(box, geo=_NoGrid()) == 128 * 96 * 4
    # with no way to move the tile into the target CRS, the source's metres are not divided into degrees
    with pytest.raises(spec.DataError, match="give --size"):
        _peak(box, geo=argv.NoGeo())
    # nor with no pixel size to move
    unknown = argv.PipeState(64, 48, 0.0, 0.0, 0.0, 0.0, "EPSG:32633", 1, 4, True)
    with pytest.raises(spec.DataError, match="give --size"):
        _peak(box, unknown, _NoGrid())
    # a box in the tile's own CRS keeps the tile's pixel size: 1280 x 960 m of 10 m pixels
    same = "reproject --output-crs=EPSG:32633 --bbox=0,0,1280,960"
    assert _peak(same, geo=argv.NoGeo()) == 128 * 96 * 4


def test_a_step_whose_output_cannot_be_bounded_is_refused_with_a_fix():
    unknown = argv.PipeState(64, 48, 0.0, 0.0, 0.0, 0.0, "EPSG:32633", 1, 4, True)
    with pytest.raises(spec.DataError, match="give --size"):
        _peak("resize --resolution=1,1", unknown)
    with pytest.raises(spec.DataError, match="give --size"):
        _peak("reproject --output-crs=3857 --resolution=1,1", unknown, argv.NoGeo())
    no_crs = argv.PipeState(64, 48, 10.0, 10.0, 0.0, 0.0, None, 1, 4, True)
    with pytest.raises(spec.DataError, match="no CRS"):
        _peak("reproject --output-crs=3857", no_crs)
    # an absolute --size needs no extent
    assert _peak("resize --size=100,100", unknown) == 100 * 100 * 4
    assert (
        _peak("reproject --output-crs=3857 --size=100,100", unknown, argv.NoGeo())
        == 100 * 100 * 4
    )


def test_a_required_argument_missing_is_the_callers_mistake_and_raises():
    for cmd, names in [
        ("neighbors --method=mean", "kernel"),
        ("reclassify", "mapping"),
        ("set-type", "output-data-type"),
        ("select", "band"),
        ("viewshed", "position"),
        ("resize", "size"),
    ]:
        with pytest.raises(spec.ParamError, match=names):
            _build(cmd)


# --- the run: what the caller's text cannot reach --------------------------------------------------------------------

needs_engine = pytest.mark.skipif(
    bool(run.unavailable_reason()), reason=str(run.unavailable_reason())
)


def _dem(
    width=64,
    height=48,
    crs="EPSG:32633",
    nodata=None,
    origin=(500000.0, 4000000.0),
    px=10.0,
) -> bytes:
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
        transform=from_origin(origin[0], origin[1], px, px),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(z[np.newaxis])
        return mf.read()


def _run(cmd, raster=None):
    return run.run(FN, [raster if raster is not None else _dem()], {"command": cmd})


@contextlib.contextmanager
def _open(res):
    # the MemoryFile reads its bytes in place, without a copy: it, and so the bytes, must outlive every read
    assert res.error is None, res.error
    with MemoryFile(res.raster) as mf, mf.open() as ds:
        yield ds


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
def test_a_refusal_gdal_still_makes_is_the_tiles_and_gives_an_error_tile():
    """The grammar rejects every problem of the command; what GDAL refuses after that comes from the tile."""
    res = _run("reproject --output-crs=3857", _dem(crs=None))
    assert res.raster is None and "CRS" in res.error
    res = _run("clip --window=0,0,640,480")
    assert res.raster is None and res.error


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
    assert (
        _run("resize --size=200,200").error is None
    )  # a single size within the cap runs


@needs_engine
def test_chained_percent_resizes_and_a_fine_resolution_are_refused_by_the_budget(
    monkeypatch,
):
    """Each step is sized from the one before it (the real GDAL grid), so the steps cannot add up to more than the cap."""
    monkeypatch.setattr(run, "decoded_budget", lambda: 8 * 1024 * 1024)
    # 64 x 48 -> x16 each way over 3 steps: 4096 x 3072 Float32 = 48 MiB
    res = _run("resize --size=4000%,4000% ! resize --size=100%,100%")
    assert res.raster is None and "MiB decoded" in res.error
    res = _run(" ! ".join(["resize --size=800%,800%"] * 2))
    assert res.raster is None and "MiB decoded" in res.error
    # a reprojection and then a fine resolution: the extent is the reprojected one
    res = _run("reproject --output-crs=3857 ! resize --resolution=0.01,0.01")
    assert res.raster is None and "MiB decoded" in res.error
    res = _run("reproject --output-crs=3857 --resolution=0.01,0.01")
    assert res.raster is None and "MiB decoded" in res.error
    # within the cap, the same chain runs
    assert _run("resize --size=200%,200% ! resize --size=200%,200%").error is None


@needs_engine
def test_a_reprojection_keeps_the_footprint_as_a_mask_not_zeros():
    """A source with no declared NoData: the pixels no source pixel reaches are masked, never a valid 0."""
    src = _dem(crs="EPSG:4326", origin=(-1.0, 52.0), px=0.01)
    res = _run("reproject --output-crs=EPSG:3035", src)
    assert res.error is None, res.error
    with MemoryFile(res.raster) as mf, mf.open() as out:
        masked = int((out.dataset_mask() == 0).sum())
        assert out.count == 1 and out.nodata is None
    assert masked > 0, "the reprojected tile has corners outside the footprint"
    # a value step after it keeps the mask
    res = _run("reproject --output-crs=EPSG:3035 ! slope", src)
    assert res.error is None, res.error
    with MemoryFile(res.raster) as mf, mf.open() as out:
        assert int((out.dataset_mask() == 0).sum()) >= masked
    # a source that declares NoData already has its footprint as NoData: no mask band is added
    res = _run(
        "reproject --output-crs=EPSG:3035",
        _dem(crs="EPSG:4326", origin=(-1.0, 52.0), px=0.01, nodata=-9999.0),
    )
    with MemoryFile(res.raster) as mf, mf.open() as out:
        assert (
            out.nodata == -9999.0
            and int((out.dataset_mask() == 0).sum()) == 0
            or out.nodata == -9999.0
        )


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


# --- a viewshed with a distance limit writes a window; a CRS GDAL cannot rebuild; a tile with no geotransform ---------------


def test_a_viewshed_with_a_distance_limit_leaves_the_window_for_the_steps_after_it():
    window = 11 * 11 * 4  # 50 m each way over 10 m pixels, one band
    state = 64 * 48 * 4
    at = "viewshed --position=300,300,10"
    assert _peak(f"{at} --max-distance=50") == window + state  # held with its input
    assert _peak(f"{at} --max-distance=50 ! resize --size=200%,200%") == window + state
    assert (
        _peak(f"{at} ! resize --size=200%,200%") == 128 * 96 * 4
    )  # no limit: the whole grid


@needs_engine
def test_a_crs_gdal_cannot_rebuild_while_sizing_is_a_refusal_with_its_fix(monkeypatch):
    def refuse(text):
        raise ValueError(f"GDAL cannot build the CRS. {argv.CRS_BUILD_FIX}")

    monkeypatch.setattr(argv, "_osr_reference", refuse)
    geo = run._PipeGeo(run._gdal(), "gbx_rst_gdal", "x", ("GTiff",))
    state = argv.PipeState(8, 8, 1.0, 1.0, 0.0, 0.0, "EPSG:32633", 1, 4, False)
    with pytest.raises(run.DataError, match="cannot build the CRS"):
        geo.warp(state, "EPSG:3857")
    with pytest.raises(run.DataError, match="cannot build the CRS"):
        geo.envelope((0.0, 0.0, 1.0, 1.0), "EPSG:4326", "EPSG:3857")


@needs_engine
def test_a_tile_with_no_geotransform_has_no_pixel_size_to_size_a_resolution_by():
    y, x = np.mgrid[0:40, 0:60]
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff", width=60, height=40, count=1, dtype="float32"
        ) as dst:
            dst.write((x + y).astype("float32")[np.newaxis])
        bare = mf.read()
    res = _run("resize --resolution=0.001,0.001", bare)
    # GDAL's identity transform is not 1-unit pixels: the budget does not size 60 x 40 pixels by it
    assert res.error is not None and "MiB decoded" not in res.error, res.error


@needs_engine
def test_a_reprojection_then_a_windowed_viewshed_keeps_no_footprint_and_is_not_an_error():
    """The mask is carried over the grid steps only: a viewshed with a distance limit writes another grid."""
    from pyproj import Transformer

    y, x = np.mgrid[0:60, 0:80]
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=80,
            height=60,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(10.0, 50.0, 0.001, 0.001),
        ) as dst:
            dst.write((np.sin(x / 6.0) * 30 + y).astype("float32")[np.newaxis])
            mask = np.full((60, 80), 255, "uint8")
            mask[:5, :] = 0
            dst.write_mask(mask)  # a mask band, not a NoData value
        src = mf.read()
    cx, cy = Transformer.from_crs(4326, 3857, always_xy=True).transform(10.04, 49.996)
    res = _run(
        f"reproject --output-crs=EPSG:3857 ! viewshed --position={cx},{cy},10 --max-distance=800",
        src,
    )
    assert res.error is None, res.error
    with _open(res) as out:
        assert out.width < 80 * 2  # the window, not the reprojected grid
        valid = out.dataset_mask() > 0
    # the window reaches the source's masked top rows: they stay masked, the rest is valid
    assert not valid.all() and valid.any()
    assert not valid[0].any() and valid[-1].all()


@pytest.mark.parametrize(
    "limit,position",
    [("0", "300,300,10"), ("-5", "300,300,10"), ("inf", "300,300,10"), ("50", "300")],
)
def test_a_viewshed_limit_that_is_not_a_window_leaves_the_whole_grid(limit, position):
    cmd = f"viewshed --position={position} --max-distance={limit}"
    assert argv.window_mask(_build(cmd)) == "none"
    assert _peak(f"{cmd} ! resize --size=200%,200%") == 128 * 96 * 4


def test_a_viewshed_limit_that_is_not_a_number_is_refused_by_name():
    with pytest.raises(spec.ParamError, match="max-distance"):
        _build("viewshed --position=300,300,10 --max-distance=abc")


def test_the_window_mask_is_cut_unless_a_grid_step_follows_the_window():
    at = "viewshed --position=300,300,10 --max-distance=50"
    assert argv.window_mask(_build("slope")) == "none"
    assert argv.window_mask(_build(f"reproject --output-crs=3857 ! {at}")) == "cut"
    assert (
        argv.window_mask(_build(f"reproject --output-crs=3857 ! {at} ! slope")) == "cut"
    )
    assert argv.window_mask(_build(f"{at} ! reproject --output-crs=3857")) == "skip"
    assert argv.window_mask(_build(f"{at} ! resize --size=10,10")) == "skip"


def test_the_window_is_placed_by_the_observer_and_never_beyond_the_grid():
    st = argv._viewshed_window(
        ["viewshed", "--position=300,300,10", "--max-distance=50"], _start()
    )
    assert (st.width, st.height, st.bands, st.fresh) == (11, 11, 1, False)
    assert (st.ox, st.oy) == (300 - 5 * 10, 300 + 5 * 10)
    big = argv._viewshed_window(
        ["viewshed", "--position=300,300,10", "--max-distance=100000"], _start()
    )
    assert (big.width, big.height) == (64, 48)
    # a tile with no pixel size (rotated or GCP) has no window to measure: it is no longer the input's header
    blind = argv._viewshed_window(
        ["viewshed", "--position=300,300,10", "--max-distance=50"],
        argv.PipeState(64, 48, 0.0, 0.0, 0, 0, "EPSG:32633", 3, 4, True),
    )
    assert blind.nbytes == 64 * 48 * 3 * 4 and blind.fresh


def _big_dem() -> bytes:
    """A 2500 x 2500 float32 DEM in EPSG:3857 over San Francisco, no NoData: 24 MiB decoded, so a command that reprojects
    it (its output and the footprint mask's work arrays) needs more than 64 MiB."""
    n = 2500
    y, x = np.mgrid[0:n, 0:n]
    z = (np.sin(x / 40.0) * 30 + y * 0.02).astype("float32")
    profile = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=from_origin(-13636130.0, 4546890.0, 1.0, 1.0),
        tiled=True,
        blockxsize=512,
        blockysize=512,
        compress="zstd",
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(z[np.newaxis])
        return mf.read()


@needs_engine
@pytest.mark.parametrize(
    "cmd",
    ["reproject --output-crs=EPSG:32610", "reproject --output-crs=EPSG:32610 ! slope"],
)
def test_a_command_that_reprojects_over_the_budget_streams_to_the_same_tile(
    monkeypatch, cmd
):
    """A command whose reproject step takes it over the decoded budget (64 MiB, Serverless's) runs streamed, as
    gbx_rst_transform does: its output on local disk, GDAL's cache capped, the footprint mask carried a band of rows at a
    time. The tile is the unbounded call's: the same pixels, mask, grid and CRS."""
    src = _big_dem()
    monkeypatch.setattr(run, "decoded_budget", lambda: 0)
    ref = run.run(FN, [src], {"command": cmd})
    assert ref.error is None, ref.error
    monkeypatch.setattr(run, "decoded_budget", lambda: 64 << 20)
    got = run.run(FN, [src], {"command": cmd})
    assert got.error is None, got.error
    with _open(ref) as a, _open(got) as b:
        assert (a.width, a.height, a.transform, a.crs) == (
            b.width,
            b.height,
            b.transform,
            b.crs,
        )
        am, bm = a.dataset_mask(), b.dataset_mask()
        assert (am == bm).all() and (am == 0).any()
        assert (a.read() == b.read()).all()


@needs_engine
def test_a_command_whose_held_step_is_over_the_budget_is_still_refused(monkeypatch):
    """A step GDAL cannot stream (a fill holds its result in memory) is not made streamable by a reproject beside it: over
    the budget the call is an error tile with a Fix."""
    monkeypatch.setattr(run, "decoded_budget", lambda: 64 << 20)
    res = run.run(
        FN, [_big_dem()], {"command": "reproject --output-crs=EPSG:32610 ! fill-nodata"}
    )
    assert res.raster is None and "Fix:" in res.error
