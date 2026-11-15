# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

QUADBIN_CELL_SCHEMA = StructType(
    [
        StructField("cell", LongType(), False),
        StructField("geom", BinaryType(), True),
    ]
)

# BNG chip struct: cellid is STRING (public surface), chip is plain WKB (no SRID).
# Fields are nullable so the struct can be produced as a pandas_udf return type
# (the Arrow-inferred schema marks all fields nullable) and so a dissolved/empty
# chip can carry NULL.
BNG_CHIP_SCHEMA = StructType(
    [
        StructField("cellid", StringType(), True),
        StructField("core", BooleanType(), True),
        StructField("chip", BinaryType(), True),
    ]
)

# Grid-spec struct produced by gbx_custom_grid, consumed by all gbx_custom_* ops.
# Field names/types match heavy Custom_GridSpec.gridStructType exactly so the SAME
# struct flows into both light and heavy consumers. srid == -1 means no CRS.
CUSTOM_GRID_SCHEMA = StructType(
    [
        StructField("bound_x_min", LongType(), False),
        StructField("bound_x_max", LongType(), False),
        StructField("bound_y_min", LongType(), False),
        StructField("bound_y_max", LongType(), False),
        StructField("cell_splits", IntegerType(), False),
        StructField("root_cell_size_x", IntegerType(), False),
        StructField("root_cell_size_y", IntegerType(), False),
        StructField("srid", IntegerType(), False),
    ]
)
