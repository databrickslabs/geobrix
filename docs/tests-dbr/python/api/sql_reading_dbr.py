"""
SQL examples for reading data with Databricks ST_ functions (DBR only).

For docs/docs/api/sql.mdx#reading-data-with-sql.
Uses st_geomfromwkb, st_area, st_centroid. Validated under docs/tests-dbr/.
"""

SQL_READ_SHAPEFILE = """-- Read shapefile (use your path)
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT * FROM shapefile_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip`;

-- Convert geometry
CREATE OR REPLACE VIEW shapes_geom AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapes;

SELECT
    name,
    st_area(geometry) as area,
    st_centroid(geometry) as center
FROM shapes_geom;"""

SQL_READ_SHAPEFILE_output = """
+----+--------+--------------------+
|name|area    |center              |
+----+--------+--------------------+
|... |12345.67|POINT (...)         |
|... |...     |...                 |
+----+--------+--------------------+
"""
