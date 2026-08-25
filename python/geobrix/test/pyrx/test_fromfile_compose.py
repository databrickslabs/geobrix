"""Task 8: rst_fromfile composes the file_gbx core (byte-identical behavior)."""

from unittest.mock import patch


def test_fromfile_impl_uses_file_gbx_to_local_path():
    from databricks.labs.gbx.pyrx import functions as prx

    with patch(
        "databricks.labs.gbx.ds.file_gbx.to_local_path",
        return_value="/nonexistent/x.tif",
    ) as tlp:
        # Virtual branch: header open fails on a bogus path → None, but
        # to_local_path from file_gbx MUST have been the resolver.
        out = prx._fromfile_impl("/Volumes/c/s/v/x.tif", "GTiff", False)
    assert tlp.called
    assert out is None


def test_fromfile_impl_virtual_tile_bytes_identical(tmp_path):
    """Virtual tile still records header dims and points at the local path."""
    from databricks.labs.gbx.pyrx import functions as prx

    from .conftest import make_geotiff_bytes

    p = tmp_path / "x.tif"
    p.write_bytes(make_geotiff_bytes(width=4, height=3, count=1))
    row = prx._fromfile_impl(str(p), "GTiff", False)
    assert row["path"] == str(p)
    assert row["raster"] is None
    assert row["window"]["width"] == 4 and row["window"]["height"] == 3


def test_fromfile_impl_materialized_uses_stage_helper(tmp_path):
    from databricks.labs.gbx.pyrx import functions as prx

    from .conftest import make_geotiff_bytes

    p = tmp_path / "x.tif"
    p.write_bytes(make_geotiff_bytes(width=4, height=3, count=1))
    with patch(
        "databricks.labs.gbx.ds.file_gbx._stage_local_if_needed",
        return_value=(str(p), False),
    ) as stage:
        row = prx._fromfile_impl(str(p), "GTiff", True)
    assert stage.called
    assert row["raster"] is not None  # materialized bytes present
