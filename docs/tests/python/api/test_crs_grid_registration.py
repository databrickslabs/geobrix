"""
Tests for the custom PROJ grid-shift registration doc example.

Exercises ``register_custom_grids_example`` against the synthetic NTv2 fixture
and asserts that the known +30 arc-second latitude shift is applied, proving
the grid was actually consulted.
"""

import os

import pytest

pyproj = pytest.importorskip("pyproj")
pytest.importorskip("shapely")
from shapely import from_wkt  # noqa: E402

from databricks.labs.gbx.core import proj_grids  # noqa: E402

try:
    from . import crs_grid_registration as examples
except (ModuleNotFoundError, ImportError):
    import crs_grid_registration as examples  # type: ignore[no-redef]


@pytest.fixture(autouse=True)
def _isolate():
    """Reset process-global CRS state before/after each test.

    Clears the grid-dir registry, the thread-local pyproj Transformer cache,
    and the pyproj PROJ data dir so tests are order-independent.
    """
    from databricks.labs.gbx.core import crs as _corecrs  # noqa: PLC0415

    orig_datadir = pyproj.datadir.get_data_dir()
    pyproj.network.set_network_enabled(False)

    def _reset():
        proj_grids.set_registered_dirs([], replace=True)
        _corecrs._thread_local.__dict__.pop("transformers", None)
        pyproj.datadir.set_data_dir(orig_datadir)

    _reset()
    yield
    _reset()


def _lonlat(result):
    """Extract (lon, lat) from the result of st_transformcrs.

    The SQL/UDF path returns EWKB as ``bytearray``; the medium-preserving
    Python core path returns EWKT as ``str``. Handle both.
    """
    if isinstance(result, (bytes, bytearray)):
        from shapely import from_wkb  # noqa: PLC0415
        g = from_wkb(bytes(result))
        return (g.x, g.y)
    assert isinstance(result, str), (
        f"expected WKT/EWKT string or EWKB bytes, got {type(result).__name__}"
    )
    body = result.split(";", 1)[1] if result.startswith("SRID=") else result
    g = from_wkt(body)
    return (g.x, g.y)


def test_register_custom_grids_fixture_present():
    """Fail fast with a helpful message if the synthetic grid is missing."""
    assert os.path.isfile(os.path.join(examples.GRID_DIR, "synthetic.gsb")), (
        f"synthetic NTv2 fixture missing at {examples.GRID_DIR}/synthetic.gsb; "
        f"regenerate via sample-data/…/proj-grids/gen_synthetic_gsb.py"
    )


def test_register_custom_grids_example(spark):
    """The doc example applies the synthetic grid's +30 arc-second latitude shift."""
    result = examples.register_custom_grids_example(spark)
    assert result is not None, "st_transformcrs returned NULL after registration"

    _lon, lat = _lonlat(result)
    assert lat == pytest.approx(examples.EXPECTED_LAT, abs=1e-6), (
        f"expected +30 arc-sec lat shift to {examples.EXPECTED_LAT:.6f}, got {lat:.6f}; "
        f"the registered grid was not consulted"
    )
