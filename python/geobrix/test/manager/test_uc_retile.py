"""The retile family on Unity Catalog (gbx_rst_retile, gbx_rst_maketiles, gbx_rst_tooverlappingtiles,
gbx_rst_separatebands): what the oracle rows (bench/oracle_tables/raster_generator.py, oracles.MVP_G) do not say. The
rows hold the sub-tiles, the refusals and the NULLs; this file holds the mechanisms: the window arithmetic, the array
estimate that never falls under what the kernel makes and refuses before a window is cut, the input cap, the per-tile
and running-total checks behind it, the provenance of a sub-tile, the one-statement-per-tile independence, and the DDL
text that only the workspace can run.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core.compression import write_gtiff
from databricks.labs.gbx.uc import ddl, forms, handlers, tier_b
from databricks.labs.gbx.uc.handlers import _Refusal

MAN = manifest.load()
FAMILY = {
    "gbx_rst_retile": "gbx__rst_retile_tiles",
    "gbx_rst_maketiles": "gbx__rst_maketiles_tiles",
    "gbx_rst_tooverlappingtiles": "gbx__rst_tooverlappingtiles_tiles",
    "gbx_rst_separatebands": "gbx__rst_separatebands_tiles",
}


def _raster(width, height, count=1, dtype="uint8", seed=1, ones=False):
    """GTiff bytes of random (incompressible) pixels, or of ones."""
    rng = np.random.default_rng(seed)
    shape = (count, height, width)
    if ones:
        data = np.ones(shape, dtype=dtype)
    elif np.dtype(dtype).kind == "f":
        data = rng.random(shape).astype(dtype)
    else:
        data = rng.integers(0, np.iinfo(dtype).max, shape).astype(dtype)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype=dtype,
        crs="EPSG:32633",
        transform=from_origin(5e5, 5e6, 10.0, 10.0),
    )
    return write_gtiff(profile, data)


def _tile(raster, **fields):
    return {**oracles._tile(raster), **fields}


# --- the window arithmetic -----------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "width,height,tw,th,overlap",
    [
        (64, 48, 32, 32, 0),
        (64, 48, 32, 32, 50),
        (64, 48, 100, 100, 0),
        (64, 48, 100, 100, 99),
        (7, 5, 1, 1, 0),
        (7, 5, 3, 2, 34),
        (101, 99, 10, 7, 90),
        (1, 1, 1, 1, 0),
        (33, 1, 32, 1, 3),
        (1200, 1200, 600, 600, 99),
    ],
)
def test_the_window_totals_equal_the_enumeration(width, height, tw, th, overlap):
    """`tiling.window_stats` is the count of windows and the sum of their areas that `plan_grid_windows` lists, for a grid
    of whole windows, one with clamped edges, and one with a step of a pixel."""
    windows = tiling.plan_grid_windows(width, height, tw, th, overlap)
    assert tiling.window_stats(width, height, tw, th, overlap) == (
        len(windows),
        sum(w * h for _, _, w, h in windows),
    )


def test_the_window_totals_need_no_loop_over_the_windows():
    """A window of a pixel over a raster of 100,000 x 100,000 is judged at once (the enumeration would be 10^10 windows)."""
    assert tiling.window_stats(100_000, 100_000, 1, 1) == (10**10, 10**10)


# --- the array estimate --------------------------------------------------------------------------------------------------
def _spy_sizes(monkeypatch):
    """Every size `_check_bytes` is asked about, in order: the estimate first, then the running total after each tile."""
    sizes = []
    real = handlers._check_bytes

    def spy(fn, size, what, **kw):
        sizes.append((what, size))
        return real(fn, size, what, **kw)

    monkeypatch.setattr(handlers, "_check_bytes", spy)
    return sizes


CASES = [
    # (id, width, height, count, dtype, call, args after the tile)
    ("retile_1px", 48, 40, 1, "uint8", forms.rst_retile_tiles, (1, 1)),
    ("retile_2px_64_bands", 32, 32, 64, "uint8", forms.rst_retile_tiles, (2, 2)),
    ("retile_1px_200_bands", 16, 16, 200, "float32", forms.rst_retile_tiles, (1, 1)),
    ("retile_7px_f64", 200, 200, 4, "float64", forms.rst_retile_tiles, (7, 7)),
    ("retile_whole", 300, 300, 3, "int16", forms.rst_retile_tiles, (300, 300)),
    ("retile_wide", 4096, 16, 1, "uint8", forms.rst_retile_tiles, (4096, 4)),
    ("retile_tall", 1, 20000, 1, "uint8", forms.rst_retile_tiles, (1, 10000)),
    ("overlap_half", 200, 200, 2, "int16", forms.rst_tooverlappingtiles_tiles, (64, 64, 50)),
    ("overlap_deep", 90, 90, 1, "uint8", forms.rst_tooverlappingtiles_tiles, (16, 16, 90)),
    ("maketiles_split", 1200, 1200, 1, "float32", forms.rst_maketiles_tiles, (1,)),
    ("separate_13", 100, 100, 13, "uint8", forms.rst_separatebands_tiles, ()),
    ("separate_500", 16, 16, 500, "uint8", forms.rst_separatebands_tiles, ()),
]  # fmt: skip


@pytest.mark.parametrize(
    "name,w,h,count,dtype,call,args", CASES, ids=[c[0] for c in CASES]
)
def test_the_estimate_never_falls_under_the_bytes_the_kernel_makes(
    monkeypatch, name, w, h, count, dtype, call, args
):
    """On pixels the codec cannot compress, with the tiles as small as one pixel and as many as 500 bands: the estimate asked
    about before the first window is at least the bytes (and rows) of the tiles the kernel made.
    """
    sizes = _spy_sizes(monkeypatch)
    out = call(_tile(_raster(w, h, count, dtype)), *args)
    assert out, name
    (what, estimate), *running = sizes
    assert "upper estimate" in what
    made = sum(len(r["raster"]) for r in out)
    assert estimate >= made + len(out) * forms._TILE_ROW_BYTES, (name, estimate, made)
    assert running and estimate >= running[-1][1]


def test_an_array_over_the_cap_is_refused_before_any_window_is_cut(monkeypatch):
    """The refusal comes from the arithmetic: no window is cut for 1.44 million one-pixel tiles. Replacing the estimate with
    the running total alone makes this fail, because the kernel would cut a window first.
    """

    def cut(*_a, **_k):
        raise AssertionError("a window was cut before the size was judged")

    monkeypatch.setattr(tiling, "cut_window", cut)
    big = _tile(_raster(1200, 1200, 1, "float32"))
    for call, args in (
        (forms.rst_retile_tiles, (1, 1)),
        (forms.rst_tooverlappingtiles_tiles, (600, 600, 99)),
    ):
        with pytest.raises(
            _Refusal, match=r"upper estimate.*over the 256 MiB.*Fix: pass a larger"
        ):
            call(big, *args)


def test_the_refusal_names_the_remedy_of_each_function(monkeypatch):
    monkeypatch.setattr(tiling, "cut_window", lambda *a, **k: pytest.fail("cut"))
    tile = _tile(_raster(1200, 1200, 1, "float32"))
    with pytest.raises(
        _Refusal,
        match=r"gbx_rst_retile:.*Fix: pass a larger tile_width and tile_height, or use the session function",
    ):
        forms.rst_retile_tiles(tile, 1, 1)
    with pytest.raises(
        _Refusal,
        match=r"gbx_rst_tooverlappingtiles:.*Fix: pass a larger tile size or a smaller overlap",
    ):
        forms.rst_tooverlappingtiles_tiles(tile, 1, 1, 0)
    # a cap far under the product's: the 500 one-band tiles of 256 pixels (128 KB) cost 2.4 MB of headers in the estimate
    monkeypatch.setattr(forms, "DECODED_CAP", 200_000)
    many = _tile(_raster(16, 16, 500, "uint8"))
    with pytest.raises(
        _Refusal, match=r"gbx_rst_separatebands:.*Fix: split the raster by band"
    ):
        forms.rst_separatebands_tiles(many)
    # 1.44 MB of pixels the codec cannot shrink: over 1 MiB, so one MB cuts it in four; the cap allows the pixels, not the headers
    monkeypatch.setattr(forms, "DECODED_CAP", 1_450_000)
    monkeypatch.setattr(forms, "WINDOWED_DECODED_CAP", 1_450_000)
    with pytest.raises(
        _Refusal, match=r"gbx_rst_maketiles:.*Fix: pass a larger size_in_mb"
    ):
        forms.rst_maketiles_tiles(_tile(_raster(1200, 1200, 1, "uint8")), 1)


def test_the_running_total_is_a_backstop_that_does_not_need_the_estimate(monkeypatch):
    """With an estimate of zero the array is still refused once the tiles made pass the cap, and the Fix is the same."""
    monkeypatch.setattr(forms, "tiles_estimate", lambda *a: 0)
    tile = _tile(_raster(256, 256, 1, "uint8"))
    with pytest.raises(_Refusal, match=r"the tiles it returns is .*Fix: pass a larger"):
        forms._retile_tiles(
            tile, 128, 128, 66_000
        )  # 64 KiB decoded input; the four tiles and their rows are 69 KB
    out = forms._retile_tiles(tile, 128, 128, 1 << 30)
    assert len(out) == 4


def test_a_tile_made_over_the_cap_is_refused_with_a_fix():
    raster = _raster(64, 64, 1, "uint8")
    with pytest.raises(
        _Refusal,
        match=r"the result would be 4 KiB decoded, over the 1 KiB.*Fix: run gbx_rst_retile in a notebook",
    ):
        forms._sub_tile("gbx_rst_retile", _tile(raster), raster, 1024)
    assert (
        forms._sub_tile("gbx_rst_retile", _tile(raster), raster, 8192)["raster"]
        == raster
    )


# --- the input cap -------------------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def over_cap():
    return oracles._build("rg_over_cap", "/tmp")


@pytest.fixture(scope="module")
def over_windowed_cap():
    return oracles._build("rg_over_windowed_cap", "/tmp")


def test_an_input_over_the_windowed_cap_is_refused_and_the_probe_copy_takes_it(
    over_windowed_cap,
):
    """The product function refuses a tile decoded over 256 MiB before it opens a window; the probe copy, with the cap raised,
    cuts the same tile (this is how the cap is measured above its value, never through a product function).
    """
    with pytest.raises(
        _Refusal,
        match=r"gbx_rst_retile: the tile is 259 MiB decoded, over the 256 MiB.*Fix: run gbx_rst_retile in a notebook or job session",
    ):
        forms.rst_retile_tiles(over_windowed_cap, 4125, 4125)
    out = forms.rst_retile_tiles_probe(over_windowed_cap, 4125, 4125)
    assert len(out) == 16
    assert out[0]["window"] == {
        "col_off": 0,
        "row_off": 0,
        "width": 4125,
        "height": 4125,
    }


def test_the_probe_copy_still_refuses_what_is_over_its_own_cap():
    assert forms.PROBE_DECODED_CAP > forms.WINDOWED_DECODED_CAP > forms.DECODED_CAP
    tile = _tile(_raster(64, 64, 1, "uint8"))
    with pytest.raises(_Refusal, match="upper estimate"):
        forms._retile_tiles(tile, 1, 1, 40_000)


def test_every_function_of_the_family_refuses_the_over_cap_input(
    over_cap, over_windowed_cap
):
    """separatebands decodes a whole band at once, so it keeps the 64 MiB cap; the three window readers refuse at 256 MiB."""
    with pytest.raises(_Refusal, match=r"decoded, over the 64 MiB.*Fix:"):
        forms.rst_separatebands_tiles(over_cap)
    for call, args in (
        (forms.rst_retile_tiles, (512, 512)),
        (forms.rst_maketiles_tiles, (1,)),
        (forms.rst_tooverlappingtiles_tiles, (512, 512, 0)),
    ):
        with pytest.raises(_Refusal, match=r"decoded, over the 256 MiB.*Fix:"):
            call(over_windowed_cap, *args)


# --- the windowed cap: the window readers decode up to 256 MiB, because they read it a window at a time ---------------
def test_the_windowed_cap_is_its_own_named_value_and_every_other_cap_stays():
    from databricks.labs.gbx.pyrx.core import budget

    assert budget.UC_WINDOWED_DECODED_CAP == 256 * 1024 * 1024
    assert tier_b.WINDOWED_DECODED_CAP == budget.UC_WINDOWED_DECODED_CAP
    assert tier_b.DECODED_CAP == budget.UC_DECODED_CAP == 64 * 1024 * 1024


@pytest.fixture
def between_caps(monkeypatch):
    """Forced sizes, not real ones: a 1.44 MB tile (1200 x 1200 uint8, incompressible) sits between a pixel cap of 500,000
    and a windowed cap of 4 MiB, as a 100 MiB tile sits between 64 and 256 MiB."""
    monkeypatch.setattr(forms, "DECODED_CAP", 500_000)
    monkeypatch.setattr(forms, "WINDOWED_DECODED_CAP", 4 * 1024 * 1024)
    return _tile(_raster(1200, 1200, 1, "uint8"))


def test_a_tile_between_the_two_caps_passes_for_the_window_readers(between_caps):
    for call, args in (
        (forms.rst_retile_tiles, (600, 600)),
        (forms.rst_maketiles_tiles, (1,)),
        (forms.rst_tooverlappingtiles_tiles, (600, 600, 0)),
    ):
        out = call(between_caps, *args)
        assert len(out) == 4, call.__name__


def test_a_tile_between_the_two_caps_is_still_refused_by_separatebands(between_caps):
    with pytest.raises(_Refusal, match=r"decoded, over the 488 KiB.*Fix:"):
        forms.rst_separatebands_tiles(between_caps)


def test_a_tile_over_the_windowed_cap_is_refused_with_the_fix(
    between_caps, monkeypatch
):
    monkeypatch.setattr(forms, "WINDOWED_DECODED_CAP", 1_000_000)
    for call, args in (
        (forms.rst_retile_tiles, (600, 600)),
        (forms.rst_maketiles_tiles, (1,)),
        (forms.rst_tooverlappingtiles_tiles, (600, 600, 0)),
    ):
        with pytest.raises(
            _Refusal,
            match=r"the tile is 1 MiB decoded, over the 976 KiB.*Fix: run gbx_rst_\w+ in a notebook or job session",
        ):
            call(between_caps, *args)


def test_a_window_is_never_read_over_the_pixel_cap(between_caps):
    """A window that would hold the whole 256 MiB tile is a whole-raster decode, so it is refused before it is cut: the
    windowed cap is for tiles read in windows, and a window stays under the pixel cap.
    """
    for call, args in (
        (forms.rst_retile_tiles, (1200, 1200)),
        (forms.rst_maketiles_tiles, (0,)),
        (forms.rst_tooverlappingtiles_tiles, (1200, 1200, 0)),
    ):
        with pytest.raises(_Refusal, match=r"a window of 1 MiB.*Fix: pass a smaller"):
            call(between_caps, *args)


# --- NULL, error tile, virtual tile, corrupt bytes, a bad argument --------------------------------------------------------
def test_null_error_virtual_and_corrupt():
    good = _tile(_raster(64, 48))
    err = {**oracles._tile(None), "metadata": {"last_error": "boom"}}
    virtual = _tile(None, path="/Volumes/x/y/z.tif")
    corrupt = _tile(b"not a tiff at all")
    calls = [
        (forms.rst_retile_tiles, (32, 32)),
        (forms.rst_maketiles_tiles, (1,)),
        (forms.rst_tooverlappingtiles_tiles, (32, 32, 50)),
        (forms.rst_separatebands_tiles, ()),
    ]
    for call, args in calls:
        assert call(None, *args) is None
        assert call(good, *(None for _ in args)) is None if args else True
        assert call(corrupt, *args) is None
        (e,) = call(err, *args)
        assert e["raster"] is None and e["metadata"]["last_error"] == "boom"
        with pytest.raises(_Refusal, match=r"virtual tiles .* are session-only.*Fix:"):
            call(virtual, *args)


def test_a_bad_argument_raises_with_a_fix_whatever_the_tile_holds():
    good = _tile(_raster(64, 48))
    with pytest.raises(ValueError, match=r"tile_width must be 1 or more pixels.*Fix:"):
        forms.rst_retile_tiles(good, 0, 32)
    with pytest.raises(ValueError, match=r"tile_height must be 1 or more pixels.*Fix:"):
        forms.rst_tooverlappingtiles_tiles(None, 32, -1, 10)
    for bad in (100, -1):
        with pytest.raises(
            ValueError, match=r"overlap must be from 0 to below 100 percent.*Fix:"
        ):
            forms.rst_tooverlappingtiles_tiles(good, 32, 32, bad)


def test_a_failure_in_the_middle_is_NULL_not_part_of_an_array(monkeypatch):
    """UC has no error tile and no half array: a window that cannot be cut makes the whole result NULL."""
    real, calls = tiling.cut_window, []

    def flaky(ds, win):
        calls.append(win)
        if len(calls) == 3:
            raise RuntimeError("read failed")
        return real(ds, win)

    monkeypatch.setattr(tiling, "cut_window", flaky)
    assert forms.rst_retile_tiles(_tile(_raster(64, 48)), 16, 16) is None
    assert len(calls) == 3


# --- what a sub-tile carries ---------------------------------------------------------------------------------------------
def test_a_sub_tile_records_its_window_its_size_and_its_srid():
    rows = forms.rst_retile_tiles(_tile(_raster(64, 48), cellid=77), 32, 32)
    assert [r["window"] for r in rows] == [
        {"col_off": 0, "row_off": 0, "width": 32, "height": 32},
        {"col_off": 32, "row_off": 0, "width": 32, "height": 32},
        {"col_off": 0, "row_off": 32, "width": 32, "height": 16},
        {"col_off": 32, "row_off": 32, "width": 32, "height": 16},
    ]
    assert {r["cellid"] for r in rows} == {77}  # the parent's cell id
    assert {r["crs"] for r in rows} == {"32633"}
    assert rows[2]["metadata"] == {
        "driver": "GTiff",
        "width": "32",
        "height": "16",
        "count": "1",
    }
    assert all(r["path"] is None and r["clip_polygon"] is None for r in rows)


def test_the_one_tile_that_covers_its_whole_input_keeps_the_input_provenance():
    """As the session's does: the piece IS the input, so it keeps the input's window, clip and band marker instead of a window
    that would read as the first piece of its parent."""
    parent = _tile(
        _raster(64, 48),
        window={"col_off": 10, "row_off": 20, "width": 64, "height": 48},
        clip_polygon=b"poly",
        clip_crs="EPSG:4326",
        metadata={"band": "2"},
    )
    (whole,) = forms.rst_retile_tiles(parent, 64, 48)
    assert whole["window"] == {"col_off": 10, "row_off": 20, "width": 64, "height": 48}
    assert (whole["clip_polygon"], whole["clip_crs"]) == (b"poly", "EPSG:4326")
    assert whole["metadata"]["band"] == "2"
    pieces = forms.rst_retile_tiles(parent, 32, 32)
    assert pieces[0]["window"] == {
        "col_off": 0,
        "row_off": 0,
        "width": 32,
        "height": 32,
    }
    assert pieces[0]["clip_polygon"] is None and pieces[0]["metadata"]["band"] == "2"


def test_separated_bands_are_in_band_order_with_their_band_number():
    raster = _raster(20, 10, 3, "int16")
    rows = forms.rst_separatebands_tiles(_tile(raster))
    assert [r["metadata"]["band"] for r in rows] == ["1", "2", "3"]
    with MemoryFile(raster) as mf, mf.open() as src:
        for i, r in enumerate(rows, start=1):
            with MemoryFile(r["raster"]) as o, o.open() as got:
                assert got.count == 1 and np.array_equal(got.read(1), src.read(i))
            assert r["window"] is None


def test_maketiles_cuts_the_session_grid_from_the_encoded_size():
    """The grid follows the tile's own byte length, as the session reads it (1200 x 1200 float32, 5.76 MB, in 1 MB parts)."""
    rows = forms.rst_maketiles_tiles(oracles._build("rg_big", "/tmp"), 1)
    assert len(rows) == 16 and {r["window"]["width"] for r in rows} == {300}
    assert len(forms.rst_maketiles_tiles(oracles._build("rg_big", "/tmp"), 0)) == 1


def test_two_tiles_one_after_the_other_each_get_their_own_windows():
    """A shape that kept the first tile's dataset for the second would give the first tile's pixels twice: the pair differs in
    ONE pixel, and exactly the piece that holds it differs."""
    a_bytes, b_bytes = oracles._edit_pair()
    a = forms.rst_retile_tiles(_tile(a_bytes), 256, 256)
    b = forms.rst_retile_tiles(_tile(b_bytes), 256, 256)

    def sums(rows):
        out = []
        for r in rows:
            with MemoryFile(r["raster"]) as mf, mf.open() as ds:
                out.append(int(ds.read(1).sum()))
        return out

    differ = [x != y for x, y in zip(sums(a), sums(b))]
    assert len(a) == len(b) == 4 and differ.count(True) == 1


# --- the shape of the SQL ------------------------------------------------------------------------------------------------
WHEEL = "file:///Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"


@pytest.mark.parametrize("name,private", sorted(FAMILY.items()))
def test_the_table_function_is_an_inline_over_the_private_array_routine(name, private):
    fn = MAN.functions[name]
    assert fn.levels["L4"] == {"tvf_over": private}
    sql = ddl.function_ddl(fn, "cat", "sch", WHEEL, MAN.uc_dependencies, "0.6.0")
    assert len(sql) == 1
    params = ", ".join(p.name for p in fn.params)
    assert f"RETURN SELECT inline(`cat`.`sch`.{private}({params}))" in sql[0]
    assert (
        "RETURNS TABLE (cellid BIGINT, raster BINARY, path STRING, path_mode STRING, window struct<"
        in sql[0]
    )
    assert sql[0].rstrip().endswith(")") and "LANGUAGE" not in sql[0]
    routine = MAN.private[private]
    text = ddl.private_ddl(routine, "cat", "sch", WHEEL, MAN.uc_dependencies, "0.6.0")
    assert f"CREATE OR REPLACE FUNCTION `cat`.`sch`.{private}(" in text
    assert text.count("RETURNS array<struct<cellid:bigint,raster:binary") == 1
    assert (
        "LANGUAGE PYTHON" in text
        and f"from {routine.handler.split(':')[0]} import {routine.handler.split(':')[1]}"
        in text
    )
    assert [n for n, _ in routine.params] == [p.name for p in fn.params]
    assert routine.deps == ("rasterio", "pyproj", "shapely")


def test_the_session_and_the_table_function_have_the_same_columns():
    cols = [list(MAN.functions[n].columns) for n in FAMILY]
    assert all(c == cols[0] for c in cols)
    assert [n for n, _ in cols[0]] == [
        "cellid", "raster", "path", "path_mode", "window", "clip_polygon", "clip_crs", "crs", "metadata",
    ]  # fmt: skip
    assert tier_b._tile_dict({}, b"", 1, {}).keys() == {n for n, _ in cols[0]}


def test_the_session_overlap_check_is_the_shared_one():
    """The session UDTF and the UC routine read the overlap through `tiling.require_overlap`, so the two say the same."""
    assert tiling.require_overlap(None) is None and tiling.require_overlap(50.7) == 50
    with pytest.raises(ValueError, match="overlap must be a number"):
        tiling.require_overlap(float("nan"))
