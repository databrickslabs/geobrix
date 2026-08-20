"""Task 3: generic gbx_file_write composes open_for_write."""

from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.gbx.ds import file_gbx


def test_gbx_file_write_delegates_to_open_for_write_resolving_session():
    df = MagicMock()
    df.sparkSession = "DRIVER_SPARK"
    with patch("databricks.labs.gbx.ds.file_gbx.open_for_write") as ofw:
        file_gbx.gbx_file_write(
            df, "cat.sch.tbl", file_mode="external", layout="cluster", overwrite=True
        )
    ofw.assert_called_once_with(
        "DRIVER_SPARK",
        df,
        "cat.sch.tbl",
        file_mode="external",
        filespace=None,
        layout="cluster",
        overwrite=True,
        file_col="tile_file",
    )


def test_gbx_file_write_explicit_spark_wins():
    df = MagicMock()
    df.sparkSession = "DRIVER_SPARK"
    with patch("databricks.labs.gbx.ds.file_gbx.open_for_write") as ofw:
        file_gbx.gbx_file_write(df, "t", spark="EXPLICIT")
    assert ofw.call_args[0][0] == "EXPLICIT"


def test_gbx_file_write_invalid_layout_raises_before_delegation():
    df = MagicMock()
    df.sparkSession = "DRIVER_SPARK"
    with patch("databricks.labs.gbx.ds.file_gbx.open_for_write") as ofw:
        with pytest.raises(ValueError):
            file_gbx.gbx_file_write(df, "t", layout="zorder")
    ofw.assert_not_called()
