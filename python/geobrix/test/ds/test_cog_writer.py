import glob
import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, assert_path_schema
from pyspark.sql.types import (
    StringType, StructField, StructType, LongType,
)
from databricks.labs.gbx.pyrx.core import cog as gbxcog


def _write_src(path, w=512, h=512):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=from_origin(0, 60, 0.01, 0.01))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w))


def test_assert_path_schema_requires_path():
    ok = StructType([StructField("path", StringType(), False)])
    assert_path_schema(ok)  # no raise
    bad = StructType([StructField("name", StringType(), False)])
    try:
        assert_path_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_writer_prepares_valid_cog(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([
        StructField("path", StringType(), False),
        StructField("name", StringType(), False),
    ])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    # rows are plain dicts (works like a Spark Row via subscript)
    row = {"path": str(src), "name": "scene.tif"}
    w.write(iter([row]))
    produced = glob.glob(os.path.join(str(out), "*.tif"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1
