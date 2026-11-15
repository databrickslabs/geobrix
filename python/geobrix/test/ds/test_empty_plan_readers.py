"""Every `*_gbx` reader emits nothing for the empty-plan placeholder.

When `partitions()` plans nothing, Spark still calls `read` once with a placeholder: `None` on OSS Spark, a bare
`InputPartition` (no fields) on the Databricks runtime. A reader that reads a field of it fails with AttributeError.
"""

import pytest
from pyspark.sql.datasource import InputPartition

from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader
from databricks.labs.gbx.ds._pmtiles_read import (
    PMtilesArchiveReader,
    PMtilesRasterReader,
)
from databricks.labs.gbx.ds.exif import ExifGbxReader
from databricks.labs.gbx.ds.file import FileGbxReader
from databricks.labs.gbx.ds.lidar import LidarGbxReader
from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
from databricks.labs.gbx.ds.vector import _ShapefileReader
from databricks.labs.gbx.zarr.datasource import ZarrGbxReader


def _read_nothing(reader):
    for placeholder in (None, InputPartition(None)):
        assert list(reader.read(placeholder)) == []


@pytest.mark.parametrize(
    "make",
    [
        lambda o: FileGbxReader(o),
        lambda o: ExifGbxReader(o),
        lambda o: LidarGbxReader(o),
        lambda o: NetcdfRasterReader(o),
        lambda o: NetcdfVectorReader(o),
        lambda o: PMtilesRasterReader(o),
        lambda o: PMtilesArchiveReader(o),
        lambda o: _ShapefileReader(o),
    ],
    ids=[
        "file",
        "exif",
        "lidar",
        "netcdf_raster",
        "netcdf_vector",
        "pmtiles_raster",
        "pmtiles_archive",
        "vector",
    ],
)
def test_the_empty_plan_placeholder_reads_nothing(make, tmp_path):
    _read_nothing(make({"path": str(tmp_path)}))


def test_a_filter_regex_matching_nothing_plans_and_reads_nothing(tmp_path):
    (tmp_path / "a.jpg").write_bytes(b"x")
    reader = ExifGbxReader({"path": str(tmp_path), "filterRegex": r".*\.nomatch$"})
    assert list(reader.partitions()) == []
    _read_nothing(reader)


def test_the_zarr_placeholder_reads_nothing():
    reader = ZarrGbxReader.__new__(
        ZarrGbxReader
    )  # the placeholder is refused before any option is read
    _read_nothing(reader)
