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
    """The single-band path must point to the committed fixture file."""
    from api._fixtures import single_band_path

    p = single_band_path()
    assert Path(p).exists(), (
        f"Single-band fixture not found: {p}\n"
        "Regenerate with _fixtures.make_single_band_fixture()."
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


# ---------------------------------------------------------------------------
# VectorX fixture tests — TIN, MVT, CRS, legacy geometry
# ---------------------------------------------------------------------------


def test_elevation_dtm_point_path_exists():
    """The committed sd46_dtm_point.shp mass-points fixture must be present."""
    from api._fixtures import elevation_dtm_point_path

    p = elevation_dtm_point_path()
    assert p.exists(), f"Committed TIN mass-points fixture missing: {p}"


def test_elevation_dtm_breakline_path_exists():
    """The committed sd46_dtm_breakline.shp breaklines fixture must be present."""
    from api._fixtures import elevation_dtm_breakline_path

    p = elevation_dtm_breakline_path()
    assert p.exists(), f"Committed TIN breaklines fixture missing: {p}"


def test_tin_df_non_degenerate(spark):
    """tin_df() returns a single row with pts (4 WKB POINT Z) and empty bl.

    Verifies non-degenerate data: pts has 4 elements, bl is empty,
    and the Z range across the 4 points is non-flat (elevation range = 10 m).
    """
    from api._fixtures import tin_df

    df = tin_df(spark)
    rows = df.collect()
    assert len(rows) == 1, "tin_df should return exactly one row"

    pts = rows[0]["pts"]
    bl = rows[0]["bl"]

    assert pts is not None, "pts column is None"
    assert len(pts) == 4, f"pts should have 4 points, got {len(pts)}"
    for i, pt in enumerate(pts):
        assert pt is not None, f"pts[{i}] is None"
        assert len(bytes(pt)) > 0, f"pts[{i}] is empty bytes"
        # WKB POINT Z is 29 bytes: 1 (byte order) + 4 (type) + 8 (x) + 8 (y) + 8 (z)
        assert len(bytes(pt)) == 29, f"pts[{i}] WKB POINTZ should be 29 bytes, got {len(bytes(pt))}"

    assert bl is not None, "bl column is None"
    assert len(bl) == 0, f"bl should be empty (no breaklines), got {len(bl)} elements"


def test_tin_df_elevations_non_flat(spark):
    """Z values in tin_df() must span a non-trivial elevation range (>= 5 m)."""
    import struct
    from api._fixtures import tin_df

    df = tin_df(spark)
    pts = df.collect()[0]["pts"]

    # Extract Z from each WKB POINT Z:
    # Offset: 1 (LE flag) + 4 (type) + 8 (x) + 8 (y) = 21; then 8 bytes for Z.
    z_values = []
    for pt in pts:
        raw = bytes(pt)
        z = struct.unpack_from("<d", raw, 21)[0]
        z_values.append(z)

    z_range = max(z_values) - min(z_values)
    assert z_range >= 5.0, (
        f"TIN fixture Z range should be >= 5 m for non-flat surface, got {z_range} m. "
        f"Z values: {z_values}"
    )


def test_mvt_features_df_non_degenerate(spark):
    """mvt_features_df() returns 2 rows with z/x/y/geom_wkb/attrs columns."""
    from api._fixtures import mvt_features_df

    df = mvt_features_df(spark)
    rows = df.collect()
    assert len(rows) == 2, f"mvt_features_df should return 2 rows, got {len(rows)}"

    for row in rows:
        assert row["z"] == 0
        assert row["x"] == 0
        assert row["y"] == 0
        assert row["geom_wkb"] is not None, "geom_wkb is None"
        assert len(bytes(row["geom_wkb"])) > 0, "geom_wkb is empty"
        assert row["attrs"] is not None, "attrs struct is None"
        # WKB POINT (2D): 1 + 4 + 8 + 8 = 21 bytes
        assert len(bytes(row["geom_wkb"])) == 21, (
            f"MVT geom_wkb should be 21-byte 2D WKB POINT, got {len(bytes(row['geom_wkb']))}"
        )

    names = {row["attrs"]["name"] for row in rows}
    assert names == {"a", "b"}, f"Expected names 'a' and 'b', got {names}"


def test_geom_ewkt_df_non_degenerate(spark):
    """geom_ewkt_df() returns 1 row with a non-empty EWKT geom string."""
    from api._fixtures import geom_ewkt_df

    df = geom_ewkt_df(spark)
    rows = df.collect()
    assert len(rows) == 1, "geom_ewkt_df should return exactly one row"

    geom = rows[0]["geom"]
    assert geom is not None, "geom column is None"
    assert "POINT" in geom.upper(), f"Expected POINT in EWKT, got: {geom}"
    assert "SRID=" in geom.upper(), f"Expected SRID= in EWKT for CRS examples, got: {geom}"


def test_legacy_geom_df_non_degenerate(spark):
    """legacy_geom_df() returns 1 row with a valid legacy Mosaic geometry struct."""
    from api._fixtures import legacy_geom_df

    df = legacy_geom_df(spark)
    rows = df.collect()
    assert len(rows) == 1, "legacy_geom_df should return exactly one row"

    g = rows[0]["geom_legacy"]
    assert g is not None, "geom_legacy struct is None"
    # typeId=1 is POINT in legacy Mosaic InternalGeometry
    assert g["typeId"] == 1, f"Expected typeId=1 (POINT), got {g['typeId']}"
    assert g["boundaries"] is not None and len(g["boundaries"]) > 0, (
        "boundaries should be non-empty for a POINT"
    )


def test_create_setup_views_vectorx_light(spark):
    """create_setup_views_vectorx_light() creates all four VectorX temp views."""
    from api._fixtures import create_setup_views_vectorx_light

    create_setup_views_vectorx_light(spark)
    for view in ("tin_survey", "mvt_features", "vector_geoms", "legacy_geoms"):
        df = spark.table(view)
        assert df.count() >= 1, f"VectorX light view '{view}' is empty after setup"
