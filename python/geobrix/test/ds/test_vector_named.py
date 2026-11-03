import json
import os

from databricks.labs.gbx.ds import vector as V


def test_named_drivers_preset():
    assert V.ShapefileGbxDataSource.name() == "shapefile_gbx"
    assert V.GeoJSONGbxDataSource.name() == "geojson_gbx"
    assert V.GpkgGbxDataSource.name() == "gpkg_gbx"
    assert V.FileGdbGbxDataSource.name() == "file_gdb_gbx"
    assert V.ShapefileGbxDataSource._READER._DRIVER == "ESRI Shapefile"
    assert V.GeoJSONGbxDataSource._READER._DRIVER == "GeoJSON"
    assert V.GpkgGbxDataSource._READER._DRIVER == "GPKG"
    assert V.FileGdbGbxDataSource._READER._DRIVER == "OpenFileGDB"


def test_geojson_gbx_reads(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    p = os.path.join(str(tmp_path), "pts.geojson")
    with open(p, "w") as f:
        json.dump(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"k": 1},
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    }
                ],
            },
            f,
        )
    df = spark.read.format("geojson_gbx").load(p)
    assert "geom_0" in df.columns and df.count() == 1
