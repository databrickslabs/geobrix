# Lakeflow pipeline source file for the GeoBrix connector.
#
# Add this file to a serverless pipeline (libraries -> file) whose environment
# installs the three wheels listed in the connector README, then edit the spec.
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector.sources.geobrix import GeoBrixDataSource

# Inject the UC connection's options (sourceName, root_path, allowlist) into reads.
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
spark.dataSource.register(GeoBrixDataSource)

pipeline_spec = {
    "connection_name": "<connection_name>",
    "objects": [
        # A dataset discovered under the connection's root_path (file or folder name).
        {"table": {"source_table": "<dataset>", "destination_table": "<table>",
                   "table_configuration": {"scd_type": "APPEND_ONLY", "format": "geojson"}}},
        # An explicit path (relative to root_path, or absolute inside it) plus a layer.
        {"table": {"source_table": "<name>", "destination_table": "<table>",
                   "table_configuration": {"scd_type": "APPEND_ONLY", "format": "gpkg",
                                           "path": "<dir>/<file>.gpkg", "layer": "<layer>"}}},
    ],
}
ingest(spark, pipeline_spec)
