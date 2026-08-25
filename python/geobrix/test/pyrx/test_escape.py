from test.pyrx.conftest import make_geotiff_bytes

import numpy as np
from pyspark.sql.types import IntegerType, StructField, StructType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
from databricks.labs.gbx.pyrx.functions import rst_apply, tile_to_numpy


def test_tile_to_numpy_bytes_and_struct_agree():
    raw = make_geotiff_bytes(width=4, height=3, count=2)
    arr_bytes = tile_to_numpy(raw)
    assert isinstance(arr_bytes, np.ndarray)
    assert arr_bytes.shape == (2, 3, 4)
    tile = _serde.build_tile(raw, "GTiff", cellid=0)
    arr_struct = tile_to_numpy(tile)
    assert np.array_equal(arr_bytes, arr_struct)


def test_rst_apply_scalar_with_nondefault_returntype(spark):
    raw = make_geotiff_bytes(width=4, height=3, count=1)
    tile = _serde.build_tile(raw, "GTiff", cellid=0)
    schema = StructType([StructField("tile", V2_TILE_SCHEMA, nullable=True)])
    df = spark.createDataFrame([(tile,)], schema)
    out = df.select(
        rst_apply("tile", lambda ds: ds.count, returnType=IntegerType()).alias("nbands")
    ).collect()
    assert out[0]["nbands"] == 1  # ds.count == band count == 1


def test_rst_apply_null_tile_returns_null(spark):
    df = spark.createDataFrame(
        [(None,)],
        StructType([StructField("tile", V2_TILE_SCHEMA, nullable=True)]),
    )
    out = df.select(rst_apply("tile", lambda ds: 1.0).alias("v")).collect()
    assert out[0]["v"] is None
