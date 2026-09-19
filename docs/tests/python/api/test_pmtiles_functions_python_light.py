"""
Tests for the light (pmtiles) tier per-function pmtiles_agg example.

Ensures pmtiles_agg_python_light_example in pmtiles_functions_python_light.py
executes and produces a valid PMTile v3 binary blob.
"""
import struct
import pytest

try:
    from . import pmtiles_functions_python_light as pmtiles_light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import pmtiles_functions_python_light as pmtiles_light_examples
    except ModuleNotFoundError:
        pmtiles_light_examples = None


def _validate_pmtile(blob, name):
    """Assert that `blob` is a well-formed PMTile v3 archive."""
    assert blob is not None, f"{name}: result is None"
    data = bytes(blob)
    assert data[:7] == b"PMTiles", f"{name}: bad magic: {data[:8]!r}"
    assert data[7] == 3, f"{name}: bad version byte: {data[7]}"
    addressed = struct.unpack_from("<Q", data, 72)[0]
    assert addressed == 9, f"{name}: expected 9 addressed tiles; got {addressed}"


def test_pmtiles_agg_python_light_example(spark):
    """pmtiles_agg (light tier) returns a valid PMTile v3 BINARY blob with 9 tiles."""
    assert pmtiles_light_examples is not None
    result = pmtiles_light_examples.pmtiles_agg_python_light_example(spark)
    _validate_pmtile(result, "pmtiles_agg_python_light")


def test_pmtiles_agg_python_light_example_output_constant_exists():
    """pmtiles_agg_python_light_example_output constant is defined and non-empty."""
    assert pmtiles_light_examples is not None
    assert hasattr(pmtiles_light_examples, "pmtiles_agg_python_light_example_output")
    assert pmtiles_light_examples.pmtiles_agg_python_light_example_output.strip()
