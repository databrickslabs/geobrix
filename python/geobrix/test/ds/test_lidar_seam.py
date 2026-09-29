import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from databricks.labs.gbx.ds._write_lidar import LidarGbxWriter, _reject_v2_options

_XYZ = StructType([StructField(c, DoubleType()) for c in ("x", "y", "z")])


def test_seam_options_graduated():
    # overlap=drop / dedupeExact / voxelSize no longer rejected outright.
    _reject_v2_options({"overlap": "drop"})
    _reject_v2_options({"dedupeExact": "true"})
    _reject_v2_options({"voxelSize": "0.5"})


def test_thinoverlap_and_bad_overlap_still_rejected():
    with pytest.raises(ValueError, match="thinOverlap"):
        _reject_v2_options({"thinOverlap": "0.5"})
    with pytest.raises(ValueError, match="overlap"):
        _reject_v2_options({"overlap": "bogus"})


def test_seam_option_requires_merge_mode():
    # A seam option in parts mode (merge not set) is a clear error.
    with pytest.raises(ValueError, match="requires .*merge"):
        LidarGbxWriter({"path": "/tmp/x", "overlap": "drop"}, _XYZ, overwrite=True)


def test_merge_mode_parses_seam_options():
    w = LidarGbxWriter(
        {
            "path": "/tmp/x",
            "merge": "true",
            "overlap": "drop",
            "dedupeExact": "true",
            "voxelSize": "0.5",
        },
        _XYZ,
        overwrite=True,
    )
    assert w.overlap == "drop" and w.dedupe_exact is True and w.voxel_size == 0.5
