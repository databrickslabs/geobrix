"""
Self-tests for shared fixture helpers in _fixtures.py.

Verifies that each builder returns a DataFrame with a non-null `tile` column
when run in the geobrix-dev container (where both the /Volumes sample-data
mount and the committed src/test/resources fixtures are accessible).

Run with:
    gbx:test:python-docs --path docs/tests/python/api/test_fixtures_helpers.py --skip-build
"""

import pytest
from pathlib import Path


# ---------------------------------------------------------------------------
# Path-existence checks (fast, no Spark)
# ---------------------------------------------------------------------------

def test_multiband_path_exists():
    """The committed multiband GeoTIFF must be present in the repo."""
    from api._fixtures import multiband_path
    p = multiband_path()
    assert p.exists(), f"Committed multiband fixture missing: {p}"


def test_netcdf_path_exists():
    """The committed NetCDF fixture must be present in the repo."""
    from api._fixtures import netcdf_path
    p = netcdf_path()
    assert p.exists(), f"Committed NetCDF fixture missing: {p}"


def test_single_band_path_resolves():
    """The single-band path must point to an existing file (requires /Volumes mount)."""
    from api._fixtures import single_band_path
    p = single_band_path()
    assert Path(p).exists(), (
        f"Single-band fixture not found: {p}\n"
        "Ensure the container was started with the sample-data Volumes mount "
        "(scripts/docker/start_docker_with_volumes.sh)."
    )


def test_dem_path_resolves():
    """The DEM path must point to an existing file (requires /Volumes mount)."""
    from api._fixtures import dem_path
    p = dem_path()
    assert Path(p).exists(), (
        f"DEM fixture not found: {p}\n"
        "Ensure the container was started with the sample-data Volumes mount."
    )


# ---------------------------------------------------------------------------
# Builder tests — require Spark (uses the session-scoped spark fixture)
# ---------------------------------------------------------------------------

def test_single_band_tile_df_light(spark):
    """single_band_tile_df() returns a DataFrame with a non-null tile column."""
    from api._fixtures import single_band_tile_df
    df = single_band_tile_df(spark)
    row = df.select("tile").first()
    assert row is not None, "single_band_tile_df returned no rows"
    assert row["tile"] is not None, "tile column is null in single_band_tile_df"


def test_multiband_tile_df_light(spark):
    """multiband_tile_df() returns a DataFrame with a non-null tile column."""
    from api._fixtures import multiband_tile_df
    df = multiband_tile_df(spark)
    row = df.select("tile").first()
    assert row is not None, "multiband_tile_df returned no rows"
    assert row["tile"] is not None, "tile column is null in multiband_tile_df"


def test_dem_tile_df_light(spark):
    """dem_tile_df() returns a DataFrame with a non-null tile column."""
    from api._fixtures import dem_tile_df
    df = dem_tile_df(spark)
    row = df.select("tile").first()
    assert row is not None, "dem_tile_df returned no rows"
    assert row["tile"] is not None, "tile column is null in dem_tile_df"


def test_netcdf_tile_df_light(spark):
    """netcdf_tile_df() returns a DataFrame with a non-null tile column."""
    from api._fixtures import netcdf_tile_df
    df = netcdf_tile_df(spark)
    row = df.select("tile").first()
    assert row is not None, "netcdf_tile_df returned no rows"
    assert row["tile"] is not None, "tile column is null in netcdf_tile_df"


def test_single_band_tile_df_heavy(spark):
    """single_band_tile_df_heavy() returns a DataFrame with a non-null tile column."""
    from api._fixtures import single_band_tile_df_heavy
    df = single_band_tile_df_heavy(spark)
    row = df.select("tile").first()
    assert row is not None, "single_band_tile_df_heavy returned no rows"
    assert row["tile"] is not None, "tile column is null in single_band_tile_df_heavy"


def test_multiband_tile_df_heavy(spark):
    """multiband_tile_df_heavy() returns a DataFrame with a non-null tile column."""
    from api._fixtures import multiband_tile_df_heavy
    df = multiband_tile_df_heavy(spark)
    row = df.select("tile").first()
    assert row is not None, "multiband_tile_df_heavy returned no rows"
    assert row["tile"] is not None, "tile column is null in multiband_tile_df_heavy"


def test_dem_tile_df_heavy(spark):
    """dem_tile_df_heavy() returns a DataFrame with a non-null tile column."""
    from api._fixtures import dem_tile_df_heavy
    df = dem_tile_df_heavy(spark)
    row = df.select("tile").first()
    assert row is not None, "dem_tile_df_heavy returned no rows"
    assert row["tile"] is not None, "tile column is null in dem_tile_df_heavy"


def test_netcdf_tile_df_heavy(spark):
    """netcdf_tile_df_heavy() returns a DataFrame with a non-null tile column."""
    from api._fixtures import netcdf_tile_df_heavy
    df = netcdf_tile_df_heavy(spark)
    row = df.select("tile").first()
    assert row is not None, "netcdf_tile_df_heavy returned no rows"
    assert row["tile"] is not None, "tile column is null in netcdf_tile_df_heavy"
