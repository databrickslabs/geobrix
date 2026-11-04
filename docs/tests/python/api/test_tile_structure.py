"""
Tests for Tile Structure Documentation Examples

These are structure tests to validate that the example functions are properly defined.
"""
import pytest
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import tile_structure


def test_sql_cellid_non_tessellated():
    """Test SQL cellid non-tessellated constant."""
    assert isinstance(tile_structure.SQL_CELLID_NON_TESSELLATED, str)
    assert "SELECT" in tile_structure.SQL_CELLID_NON_TESSELLATED


def test_sql_cellid_tessellated():
    """Test SQL cellid tessellated constant."""
    assert isinstance(tile_structure.SQL_CELLID_TESSELLATED, str)
    assert "explode" in tile_structure.SQL_CELLID_TESSELLATED


def test_access_path_and_binary(spark):
    """Test distinguishing virtual from materialized tiles via path and raster (structure check)."""
    assert callable(tile_structure.access_path_and_binary)


def test_tile_path_mode_storage(spark):
    """Test tile_path_mode_storage example is defined (structure check)."""
    assert callable(tile_structure.tile_path_mode_storage)


def test_access_metadata_fields(spark):
    """Test accessing metadata fields (structure check)."""
    assert callable(tile_structure.access_metadata_fields)


def test_accessing_tile_fields_python(spark):
    """The reader the example uses hands back the raster bytes it selects."""
    df = tile_structure.accessing_tile_fields_python(spark)
    rows = df.select("tile.raster", "tile.metadata.driver").collect()
    assert rows
    assert all(r["raster"] is not None and len(r["raster"]) > 0 for r in rows)
    assert {r["driver"] for r in rows} == {"GTiff"}


def test_sql_accessing_tile_fields():
    """Test SQL accessing tile fields constant."""
    assert isinstance(tile_structure.SQL_ACCESSING_TILE_FIELDS, str)
    assert "SELECT" in tile_structure.SQL_ACCESSING_TILE_FIELDS


def test_filtering_by_metadata(spark):
    """Test filtering by metadata (structure check)."""
    assert callable(tile_structure.filtering_by_metadata)


def test_using_tiles_in_custom_udfs(spark):
    """data_size is the byte length of each tile's raster: positive, not the 0 a virtual tile gives."""
    df = tile_structure.using_tiles_in_custom_udfs(spark)
    sizes = [r["data_size"] for r in df.select("data_size").collect()]
    assert sizes
    assert all(n > 0 for n in sizes)


def test_processing_binary_raster_data(spark):
    """mean_value is the band-1 mean of the file, not None."""
    import rasterio

    df = tile_structure.processing_binary_raster_data(spark)
    means = [r["mean_value"] for r in df.select("mean_value").collect()]
    assert len(means) == 1 and means[0] is not None
    with rasterio.open(tile_structure.SAMPLE_NYC_RASTER) as src:
        expected = float(src.read(1).mean())
    assert means[0] == pytest.approx(expected, rel=1e-6)


def test_comparing_fromfile_vs_fromcontent_tiles(spark):
    """Test comparing rst_fromfile vs rst_fromcontent tiles (structure check)."""
    assert callable(tile_structure.comparing_fromfile_vs_fromcontent_tiles)


def test_non_tessellated_tiles(spark):
    """The tiles carry the raster bytes the example selects."""
    df = tile_structure.non_tessellated_tiles(spark)
    rows = df.select("tile.raster").collect()
    assert rows
    assert all(r["raster"] is not None and len(r["raster"]) > 0 for r in rows)


def test_tessellated_tiles(spark):
    """Test tessellated tiles (structure check)."""
    assert callable(tile_structure.tessellated_tiles)


def test_best_practice_access_fields_efficiently(spark):
    """Test best practice for accessing fields (structure check)."""
    assert callable(tile_structure.best_practice_access_fields_efficiently)


def test_best_practice_filter_early_on_metadata(spark):
    """Test best practice for filtering early (structure check)."""
    assert callable(tile_structure.best_practice_filter_early_on_metadata)


def test_best_practice_use_accessor_functions(spark):
    """Test best practice for using accessor functions (structure check)."""
    assert callable(tile_structure.best_practice_use_accessor_functions)


def test_pattern_conditional_processing_based_on_metadata(spark):
    """Test pattern for conditional processing (structure check)."""
    assert callable(tile_structure.pattern_conditional_processing_based_on_metadata)


def test_pattern_joining_tiles_by_cell_id(spark):
    """Test pattern for joining tiles (structure check)."""
    assert callable(tile_structure.pattern_joining_tiles_by_cell_id)


def test_pattern_extract_binary_for_external_processing(spark, tmp_path):
    """The exported parquet holds the raster bytes, and they open as a raster."""
    from rasterio.io import MemoryFile

    exported = tile_structure.pattern_extract_binary_for_external_processing(
        spark, export_path=str(tmp_path / "export")
    )
    rows = exported.collect()
    assert rows
    for r in rows:
        assert r["raster_bytes"] is not None and len(r["raster_bytes"]) > 0
        with MemoryFile(bytes(r["raster_bytes"])) as mem, mem.open() as src:
            assert src.width > 0 and src.height > 0


def test_performance_io_patterns(spark):
    """Test performance I/O patterns (structure check)."""
    assert callable(tile_structure.performance_io_patterns)


def test_troubleshooting_inspect_raster_field(spark):
    """The raster field is non-empty binary, so its length is a real byte count."""
    df = tile_structure.troubleshooting_inspect_raster_field(spark)
    sizes = [r[0] for r in df.select(tile_structure.f.length("tile.raster")).collect()]
    assert sizes
    assert all(n and n > 0 for n in sizes)


def test_troubleshooting_null_cellid(spark):
    """Test troubleshooting null cellid (structure check)."""
    assert callable(tile_structure.troubleshooting_null_cellid)
