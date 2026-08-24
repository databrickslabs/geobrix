import pytest
from databricks.labs.gbx.core import proj_grids


@pytest.fixture(autouse=True)
def _clear():
    proj_grids.set_registered_dirs([], replace=True)
    yield
    proj_grids.set_registered_dirs([], replace=True)


def test_single_str_is_wrapped():
    assert proj_grids.set_registered_dirs("/Volumes/a/b/grids") == ["/Volumes/a/b/grids"]
    assert proj_grids.get_registered_dirs() == ["/Volumes/a/b/grids"]


def test_accumulates_and_dedupes_preserving_order():
    proj_grids.set_registered_dirs("/Volumes/a")
    proj_grids.set_registered_dirs(["/Volumes/b", "/Volumes/a"])
    assert proj_grids.get_registered_dirs() == ["/Volumes/a", "/Volumes/b"]


def test_replace_resets():
    proj_grids.set_registered_dirs(["/Volumes/a", "/Volumes/b"])
    assert proj_grids.set_registered_dirs("/Volumes/c", replace=True) == ["/Volumes/c"]


def test_replace_empty_clears():
    proj_grids.set_registered_dirs("/Volumes/a")
    assert proj_grids.set_registered_dirs([], replace=True) == []


def test_get_returns_copy_not_alias():
    proj_grids.set_registered_dirs("/Volumes/a")
    got = proj_grids.get_registered_dirs()
    got.append("/Volumes/mutated")
    assert proj_grids.get_registered_dirs() == ["/Volumes/a"]


def test_non_string_sequence_raises_typeerror():
    with pytest.raises(TypeError):
        proj_grids.set_registered_dirs(123)
