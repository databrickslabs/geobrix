"""
Tests for GDAL CLI integration examples.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import gdal_cli


def test_gdalinfo_cli_command_and_output():
    """CLI doc: command constant (code block) and output constant (shell result)."""
    assert hasattr(gdal_cli, "gdalinfo_cli_command")
    assert hasattr(gdal_cli, "gdalinfo_cli_output")
    cmd = gdal_cli.gdalinfo_cli_command
    out = gdal_cli.gdalinfo_cli_output
    assert "/Volumes/" in cmd and "gdalinfo" in cmd
    assert "Driver:" in out or "Size is" in out


def test_preprocessing_cli_commands_and_output():
    """CLI doc: commands (code block) and per-step output (shell results)."""
    assert hasattr(gdal_cli, "preprocessing_cli_commands")
    assert hasattr(gdal_cli, "preprocessing_cli_output")
    cmd = gdal_cli.preprocessing_cli_commands
    out = gdal_cli.preprocessing_cli_output
    assert "/Volumes/" in cmd and "gdalwarp" in cmd and "gdal_translate" in cmd
    assert "Step 1" in out and "Step 2" in out


def test_gdalwarp_cli_command_and_output():
    """CLI doc: gdalwarp command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdalwarp_cli_command")
    assert hasattr(gdal_cli, "gdalwarp_cli_output")
    cmd = gdal_cli.gdalwarp_cli_command
    out = gdal_cli.gdalwarp_cli_output
    assert "/Volumes/" in cmd and "gdalwarp" in cmd
    assert "Creating output" in out or "done" in out


def test_gdal_translate_cli_command_and_output():
    """CLI doc: gdal_translate command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdal_translate_cli_command")
    assert hasattr(gdal_cli, "gdal_translate_cli_output")
    cmd = gdal_cli.gdal_translate_cli_command
    out = gdal_cli.gdal_translate_cli_output
    assert "/Volumes/" in cmd and "gdal_translate" in cmd
    assert "Input file size" in out or "done" in out


def test_gdal_merge_cli_command_and_output():
    """CLI doc: gdal_merge command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdal_merge_cli_command")
    assert hasattr(gdal_cli, "gdal_merge_cli_output")
    cmd = gdal_cli.gdal_merge_cli_command
    out = gdal_cli.gdal_merge_cli_output
    assert "/Volumes/" in cmd and "gdal_merge" in cmd
    assert "done" in out


def test_gdalbuildvrt_cli_command_and_output():
    """CLI doc: gdalbuildvrt command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdalbuildvrt_cli_command")
    assert hasattr(gdal_cli, "gdalbuildvrt_cli_output")
    cmd = gdal_cli.gdalbuildvrt_cli_command
    out = gdal_cli.gdalbuildvrt_cli_output
    assert "/Volumes/" in cmd and "gdalbuildvrt" in cmd
    assert "done" in out


def test_gdaldem_cli_command_and_output():
    """CLI doc: gdaldem command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdaldem_cli_command")
    assert hasattr(gdal_cli, "gdaldem_cli_output")
    cmd = gdal_cli.gdaldem_cli_command
    out = gdal_cli.gdaldem_cli_output
    assert "/Volumes/" in cmd and "gdaldem" in cmd
    assert "done" in out


def test_gdal_calc_cli_command_and_output():
    """CLI doc: gdal_calc command (code block) and shell output."""
    assert hasattr(gdal_cli, "gdal_calc_cli_command")
    assert hasattr(gdal_cli, "gdal_calc_cli_output")
    cmd = gdal_cli.gdal_calc_cli_command
    out = gdal_cli.gdal_calc_cli_output
    assert "/Volumes/" in cmd and "gdal_calc" in cmd
    assert "done" in out


def test_ogr2ogr_cli_command_and_output():
    """CLI doc: ogr2ogr command (code block) and shell output."""
    assert hasattr(gdal_cli, "ogr2ogr_cli_command")
    assert hasattr(gdal_cli, "ogr2ogr_cli_output")
    cmd = gdal_cli.ogr2ogr_cli_command
    out = gdal_cli.ogr2ogr_cli_output
    assert "/Volumes/" in cmd and "ogr2ogr" in cmd
    assert "done" in out


def test_satellite_preprocessing_cli_commands_and_output():
    """CLI doc: satellite preprocessing script (code block) and per-step output."""
    assert hasattr(gdal_cli, "satellite_preprocessing_cli_commands")
    assert hasattr(gdal_cli, "satellite_preprocessing_cli_output")
    cmd = gdal_cli.satellite_preprocessing_cli_commands
    out = gdal_cli.satellite_preprocessing_cli_output
    assert "/Volumes/" in cmd and "geobrix-examples/nyc/sentinel2" in cmd
    assert "Step 1" in out and "Step 2" in out and "Step 3" in out


def test_gdal_cli_in_spark_udfs():
    """Doc-referenced: GDAL CLI in UDFs (structure check)."""
    assert callable(gdal_cli.gdal_cli_in_spark_udfs)


def test_gdal_cli_in_spark_udfs_output_constant():
    """Doc-referenced: output constant for Example output block."""
    assert hasattr(gdal_cli, "gdal_cli_in_spark_udfs_output")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
