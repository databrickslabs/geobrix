"""The workspace rows of gbx_rst_fromfile (oracle_tables/raster_constructor.py, UC_FROMFILE): their `{vol}` fixtures, their
expected values, and the fixture files the gate stages. The rows run on a warehouse; these tests pin what does not need one.
"""

import importlib.util
import sys
from pathlib import Path
from test._cog_fixture import cog_bytes

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.bench import oracles, probes
from databricks.labs.gbx.bench.oracle_tables import raster_constructor as rc
from databricks.labs.gbx.pyrx.core import cog_ranges as cr

REPO = Path(__file__).resolve().parents[4]
UC_ROWS = {c.cid: c for c in rc.UC_FROMFILE}


def _fixtures_module():
    spec = importlib.util.spec_from_file_location(
        "uc_fixtures", REPO / "examples" / "tests" / "uc_fixtures.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_the_rows_are_uc_recipes_of_fromfile_in_the_slice():
    assert set(UC_ROWS) == {
        "uc_fromfile_clip", "uc_fromfile_clip_pixels", "uc_fromfile_clip_ranges", "uc_fromfile_full_pixels",
        "uc_fromfile_resolution", "uc_fromfile_outside_null", "uc_fromfile_geom4326", "uc_fromfile_offset_file",
        "uc_fromfile_striped_refused", "uc_fromfile_not_tiff_refused",
    }  # fmt: skip
    in_slice = {c.cid for c in probes.uc_slice_cases(("RASTER_CONSTRUCTOR",))}
    assert set(UC_ROWS) <= in_slice
    assert all(
        c.fn == "gbx_rst_fromfile" and c.surfaces == ("uc",) and c.sql
        for c in UC_ROWS.values()
    )


def test_vol_and_q_are_substituted_and_an_unset_vol_names_no_real_folder():
    sql = UC_ROWS["uc_fromfile_clip"].sql
    out = oracles.render_recipe(sql, "`c`.`s`.", vol="/Volumes/c/s/v/uc-it/abc/")
    assert "to_file('/Volumes/c/s/v/uc-it/abc/fixtures/cog.tif')" in out
    assert (
        "`c`.`s`.gbx_rst_fromfile(" in out and "{vol}" not in out and "{q}" not in out
    )
    assert oracles.FIXTURES_VOL_UNSET in oracles.render_recipe(sql, "")
    ((cid, stmt),) = oracles.uc_sql(
        [UC_ROWS["uc_fromfile_clip"]], "c", "s", "/tmp", vol="/Volumes/x/y/z"
    )
    assert "/Volumes/x/y/z/fixtures/cog.tif" in stmt


def test_the_pixel_expectations_come_from_the_fixtures_generator():
    d = rc.cog_pixels()
    with MemoryFile(cog_bytes()) as mf, mf.open() as ds:
        assert np.array_equal(ds.read(), d)
    assert d.shape == rc.COG_SHAPE
    want = [float(d[b, 400:1000, 500:1200].mean()) for b in range(3)]
    assert (
        rc._clip_avg() == want
    )  # the clip box is 700 x 600 pixels from column 500, row 400


def test_the_fixture_files_have_the_layout_the_rows_assume(tmp_path):
    sizes = _fixtures_module().build(tmp_path)
    assert set(sizes) == {"cog.tif", "cog_prefixed.tif", "striped.tif", "notiff.bin"}
    cog = (tmp_path / "cog.tif").read_bytes()
    assert (tmp_path / "cog_prefixed.tif").read_bytes()[
        rc.FIXTURE_PREFIX_BYTES :
    ] == cog
    assert len(cog) > cr.HEAD_BYTES  # the window's blocks are fetched by ranges
    with MemoryFile((tmp_path / "striped.tif").read_bytes()) as mf, mf.open() as ds:
        assert ds.profile.get("tiled") is not True
    assert not (tmp_path / "notiff.bin").read_bytes().startswith((b"II", b"MM"))


def test_the_clip_window_needs_more_than_one_range_past_the_head():
    from databricks.labs.gbx.uc import tier_b

    class G:
        def getBytes(self):  # noqa: N802
            import shapely.geometry as sg

            return sg.box(405000, 3990000, 412000, 3996000).wkb

        def getSrid(self):  # noqa: N802
            return 32614

    blob = cog_bytes()
    plan = tier_b.fromfile_plan(blob[: cr.HEAD_BYTES], len(blob), G(), None, None)
    assert len(plan["offs"]) > 1 and plan["offs"][0] >= cr.HEAD_BYTES


def test_a_recipe_list_of_doubles_that_arrives_as_text_is_judged_by_value():
    c = UC_ROWS["uc_fromfile_clip_pixels"]
    want = rc._clip_avg()
    assert (
        oracles.check_uc_results(
            [c], {c.cid: {"value": [repr(v) for v in want]}}, "/tmp"
        )
        == []
    )
    bad = oracles.check_uc_results(
        [c], {c.cid: {"value": ["1.0", "2.0", "3.0"]}}, "/tmp"
    )
    assert len(bad) == 1 and "uc_fromfile_clip_pixels" in bad[0]
