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


# ---------------------------------------------------------------------------
# GridX fixture tests — BNG, Quadbin, Custom
# ---------------------------------------------------------------------------


def test_bng_cells_df_non_degenerate(spark):
    """bng_cells_df() returns 1 row with a valid BNG cell-id string."""
    from api._fixtures import bng_cells_df, _BNG_CELL_ID

    rows = bng_cells_df(spark).collect()
    assert len(rows) == 1, "bng_cells_df should return exactly 1 row"
    cellid = rows[0]["cellid"]
    assert cellid is not None, "cellid is None"
    assert isinstance(cellid, str), f"cellid should be string, got {type(cellid)}"
    assert cellid == _BNG_CELL_ID, f"Expected '{_BNG_CELL_ID}', got '{cellid}'"


def test_bng_cell_pairs_df_non_degenerate(spark):
    """bng_cell_pairs_df() returns 1 row with two distinct adjacent BNG cell-ids."""
    from api._fixtures import bng_cell_pairs_df, _BNG_CELL_ID, _BNG_CELL_ID_2

    rows = bng_cell_pairs_df(spark).collect()
    assert len(rows) == 1, "bng_cell_pairs_df should return exactly 1 row"
    assert rows[0]["cellid1"] == _BNG_CELL_ID, f"cellid1 mismatch"
    assert rows[0]["cellid2"] == _BNG_CELL_ID_2, f"cellid2 mismatch"
    assert rows[0]["cellid1"] != rows[0]["cellid2"], "cellid1 and cellid2 should differ"


def test_bng_coordinates_df_non_degenerate(spark):
    """bng_coordinates_df() returns 1 row with BNG easting, northing, and WKT point."""
    from api._fixtures import bng_coordinates_df, _BNG_EASTING, _BNG_NORTHING, _BNG_POINT_WKT

    rows = bng_coordinates_df(spark).collect()
    assert len(rows) == 1, "bng_coordinates_df should return exactly 1 row"
    assert rows[0]["easting"] == _BNG_EASTING, "easting mismatch"
    assert rows[0]["northing"] == _BNG_NORTHING, "northing mismatch"
    geom = rows[0]["geom"]
    assert geom is not None, "geom is None"
    assert "POINT" in geom.upper(), f"Expected POINT geometry, got: {geom}"
    assert "530000" in geom and "180000" in geom, (
        f"Expected BNG coords (530000, 180000) in point, got: {geom}"
    )


def test_bng_polygons_df_non_degenerate(spark):
    """bng_polygons_df() returns 1 row with a valid BNG polygon in EPSG:27700."""
    from api._fixtures import bng_polygons_df, _BNG_POLYGON_WKT

    rows = bng_polygons_df(spark).collect()
    assert len(rows) == 1, "bng_polygons_df should return exactly 1 row"
    geom = rows[0]["geom"]
    assert geom is not None, "geom is None"
    assert "POLYGON" in geom.upper(), f"Expected POLYGON geometry, got: {geom}"
    # Confirm BNG coords (eastings > 100000, NOT lon/lat near 0)
    assert "529000" in geom, (
        f"Expected BNG easting 529000 in polygon (got WGS84?): {geom}"
    )


def test_bng_chips_df_non_degenerate(spark):
    """bng_chips_df() returns 9 chip structs with valid cellid/core/chip fields.

    The 3km × 3km BNG polygon at resolution 3 (1km cells) produces exactly
    9 chips. The center cell TQ3080 must be core=True (fully interior).
    """
    from api._fixtures import bng_chips_df

    rows = bng_chips_df(spark).collect()
    assert len(rows) == 9, (
        f"bng_chips_df should return 9 chips (3×3 at res=3), got {len(rows)}.\n"
        "Ensure BNG polygon is in EPSG:27700 (eastings/northings), not WGS84."
    )
    for i, row in enumerate(rows):
        chip = row["chip"]
        assert chip is not None, f"chip[{i}] struct is None"
        assert chip["cellid"] is not None, f"chip[{i}].cellid is None"
        assert isinstance(chip["cellid"], str), f"chip[{i}].cellid should be string"
        assert len(chip["cellid"]) >= 4, (
            f"chip[{i}].cellid looks too short: '{chip['cellid']}'"
        )
        assert chip["core"] is not None, f"chip[{i}].core is None"
        assert isinstance(chip["core"], bool), f"chip[{i}].core should be bool"

    # TQ3080 is the core cell (fully inside the polygon) — assert it directly
    cell_ids = {row["chip"]["cellid"] for row in rows}
    assert "TQ3080" in cell_ids, (
        f"Expected TQ3080 among chip cell-ids, got: {sorted(cell_ids)}"
    )
    tq3080_rows = [row for row in rows if row["chip"]["cellid"] == "TQ3080"]
    assert len(tq3080_rows) == 1, f"Expected exactly 1 TQ3080 chip, got {len(tq3080_rows)}"
    assert tq3080_rows[0]["chip"]["core"] is True, (
        "TQ3080 should be a core chip (fully interior to the polygon, core=True)"
    )


def test_bng_chips_df_chip_struct_valid(spark):
    """bng_chips_df() chip structs have the correct STRUCT<cellid, core, chip> schema."""
    from api._fixtures import bng_chips_df

    df = bng_chips_df(spark)
    schema_str = str(df.schema)
    # Check field names present in the chip STRUCT
    assert "cellid" in schema_str, f"'cellid' not found in chip schema: {schema_str}"
    assert "core" in schema_str, f"'core' not found in chip schema: {schema_str}"
    assert "chip" in schema_str, f"'chip' not found in chip schema: {schema_str}"


def test_quadbin_cells_df_non_degenerate(spark):
    """quadbin_cells_df() returns 1 row with a valid quadbin cell LONG."""
    from api._fixtures import quadbin_cells_df, _QUADBIN_CELL_SF_Z10

    rows = quadbin_cells_df(spark).collect()
    assert len(rows) == 1, "quadbin_cells_df should return exactly 1 row"
    cell = rows[0]["cell"]
    assert cell is not None, "cell is None"
    assert isinstance(cell, int), f"cell should be int (LONG), got {type(cell)}"
    assert cell == _QUADBIN_CELL_SF_Z10, (
        f"Expected SF z10 cell {_QUADBIN_CELL_SF_Z10}, got {cell}"
    )


def test_quadbin_cell_pairs_df_non_degenerate(spark):
    """quadbin_cell_pairs_df() returns 1 row with two distinct quadbin cells."""
    from api._fixtures import quadbin_cell_pairs_df

    rows = quadbin_cell_pairs_df(spark).collect()
    assert len(rows) == 1, "quadbin_cell_pairs_df should return exactly 1 row"
    cell1 = rows[0]["cell1"]
    cell2 = rows[0]["cell2"]
    assert cell1 is not None, "cell1 is None"
    assert cell2 is not None, "cell2 is None"
    assert isinstance(cell1, int), f"cell1 should be int (LONG), got {type(cell1)}"
    assert isinstance(cell2, int), f"cell2 should be int (LONG), got {type(cell2)}"
    assert cell1 != cell2, "cell1 and cell2 should be different cells"
    # Both cells should look like valid quadbin IDs (positive large longs)
    assert cell1 > 0, f"cell1 should be positive, got {cell1}"
    assert cell2 > 0, f"cell2 should be positive, got {cell2}"


def test_quadbin_polygons_df_non_degenerate(spark):
    """quadbin_polygons_df() returns 1 row with a WGS84 polygon string."""
    from api._fixtures import quadbin_polygons_df

    rows = quadbin_polygons_df(spark).collect()
    assert len(rows) == 1, "quadbin_polygons_df should return exactly 1 row"
    geom = rows[0]["geom"]
    assert geom is not None, "geom is None"
    assert "POLYGON" in geom.upper(), f"Expected POLYGON, got: {geom}"
    # WGS84 coords are small (±180 lon, ±90 lat)
    assert "-1" in geom and "1" in geom, (
        f"Expected WGS84 coords (near origin) in polygon, got: {geom}"
    )


def test_quadbin_kring_cells_df_non_degenerate(spark):
    """quadbin_kring_cells_df() returns 9 cells (k=1 ring including center)."""
    from api._fixtures import quadbin_kring_cells_df

    rows = quadbin_kring_cells_df(spark).collect()
    assert len(rows) == 9, (
        f"quadbin kring k=1 should return 9 cells (3×3 ring), got {len(rows)}"
    )
    cells = [row["cell"] for row in rows]
    assert all(c is not None for c in cells), "Some cells are None"
    assert all(isinstance(c, int) for c in cells), "All cells should be LONG"
    assert len(set(cells)) == 9, f"Expected 9 distinct cells, got {len(set(cells))}"


def test_custom_grid_df_non_degenerate(spark):
    """custom_grid_df() returns 1 row with grid STRUCT, cell LONG, and point STRING."""
    from api._fixtures import custom_grid_df, _CUSTOM_CELL_ID, _BNG_POINT_WKT

    rows = custom_grid_df(spark).collect()
    assert len(rows) == 1, "custom_grid_df should return exactly 1 row"

    grid = rows[0]["grid"]
    cell = rows[0]["cell"]
    point = rows[0]["point"]

    assert grid is not None, "grid struct is None"
    assert cell is not None, "cell is None"
    assert point is not None, "point is None"

    assert isinstance(cell, int), f"cell should be int (LONG), got {type(cell)}"
    assert cell == _CUSTOM_CELL_ID, (
        f"Expected custom cell {_CUSTOM_CELL_ID}, got {cell}"
    )
    assert "POINT" in point.upper(), f"Expected POINT in point string, got: {point}"
    assert "530000" in point, f"Expected BNG easting 530000 in point: {point}"


def test_custom_grid_df_struct_fields(spark):
    """custom_grid_df() grid struct has the expected fields for a custom grid spec."""
    from api._fixtures import custom_grid_df

    df = custom_grid_df(spark)
    schema_str = str(df.schema)
    # Custom grid struct should contain these field names
    for field in ("bound_x_min", "bound_x_max", "cell_splits", "root_cell_size_x", "srid"):
        assert field in schema_str, (
            f"Expected field '{field}' in custom grid schema, got: {schema_str}"
        )


def test_create_setup_views_gridx_light(spark):
    """create_setup_views_gridx_light() creates all ten GridX temp views."""
    from api._fixtures import create_setup_views_gridx_light

    create_setup_views_gridx_light(spark)
    expected_views = (
        "bng_cells",
        "bng_cell_pairs",
        "bng_points",
        "bng_polygons",
        "bng_chips",
        "quadbin_cells",
        "quadbin_cell_pairs",
        "quadbin_polygons",
        "quadbin_kring_cells",
        "custom_grids",
    )
    for view in expected_views:
        df = spark.table(view)
        assert df.count() >= 1, f"GridX light view '{view}' is empty after setup"
