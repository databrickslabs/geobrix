import os
import pytest
from databricks.labs.gbx.pyrx import _env


@pytest.fixture
def _isolated_env(monkeypatch):
    monkeypatch.setenv("PROJ_DATA", "/bundled/proj")
    monkeypatch.delenv("PROJ_LIB", raising=False)
    # Neutralize the bundled-data auto-set so we test only the prepend behavior.
    monkeypatch.setattr(_env, "_bundled_proj_data", lambda: "/bundled/proj")
    monkeypatch.setattr(_env, "_bundled_gdal_data", lambda: None)
    yield


def test_explicit_dirs_are_prepended(_isolated_env):
    _env.configure_gdal_env(extra_proj_dirs=["/Volumes/a/grids"])
    assert os.environ["PROJ_DATA"] == "/Volumes/a/grids:/bundled/proj"


def test_multiple_dirs_prepended_in_order(_isolated_env):
    _env.configure_gdal_env(extra_proj_dirs=["/Volumes/a", "/Volumes/b"])
    assert os.environ["PROJ_DATA"] == "/Volumes/a:/Volumes/b:/bundled/proj"


def test_idempotent_no_duplicate_prepend(_isolated_env):
    _env.configure_gdal_env(extra_proj_dirs=["/Volumes/a"])
    _env.configure_gdal_env(extra_proj_dirs=["/Volumes/a"])
    assert os.environ["PROJ_DATA"] == "/Volumes/a:/bundled/proj"


def test_empty_list_is_noop(_isolated_env):
    _env.configure_gdal_env(extra_proj_dirs=[])
    assert os.environ["PROJ_DATA"] == "/bundled/proj"


def test_none_reads_registry(monkeypatch, _isolated_env):
    from databricks.labs.gbx.core import proj_grids
    monkeypatch.setattr(proj_grids, "get_registered_dirs", lambda: ["/Volumes/reg"])
    _env.configure_gdal_env()  # None → read registry
    assert os.environ["PROJ_DATA"] == "/Volumes/reg:/bundled/proj"
