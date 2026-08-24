import pytest
from databricks.labs.gbx import crs_grids
from databricks.labs.gbx.core import proj_grids


@pytest.fixture(autouse=True)
def _clear():
    proj_grids.set_registered_dirs([], replace=True)
    yield
    proj_grids.set_registered_dirs([], replace=True)


def test_records_dirs_and_returns_list(tmp_path):
    d = tmp_path / "grids"
    d.mkdir()
    (d / "OSTN15.gsb").write_bytes(b"\x00")
    out = crs_grids.register_proj_grids(spark=None, dirs=str(d))
    assert out == [str(d)]
    assert proj_grids.get_registered_dirs() == [str(d)]


def test_missing_dir_warns_but_records(recwarn):
    out = crs_grids.register_proj_grids(spark=None, dirs="/nope/not/here")
    assert out == ["/nope/not/here"]
    assert any("not" in str(w.message).lower() for w in recwarn.list)


def test_top_level_reexport():
    import databricks.labs.gbx as gbx
    assert gbx.register_proj_grids is crs_grids.register_proj_grids


def test_replace_true_resets(tmp_path):
    crs_grids.register_proj_grids(spark=None, dirs="/Volumes/a")
    out = crs_grids.register_proj_grids(spark=None, dirs="/Volumes/b", replace=True)
    assert out == ["/Volumes/b"]
