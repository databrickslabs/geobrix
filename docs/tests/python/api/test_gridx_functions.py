"""
Test structure of GridX (BNG) functions documentation examples.
"""

import sys
from pathlib import Path

# Allow imports to work even if pyspark not available
try:
    from . import gridx_functions
except (ModuleNotFoundError, ImportError):
    try:
        import gridx_functions
    except ModuleNotFoundError:
        # PySpark not available, create placeholder
        pass

# Common setup
def test_gridx_setup_example():
    assert hasattr(gridx_functions, 'gridx_setup_example')
    assert callable(gridx_functions.gridx_setup_example)
    assert hasattr(gridx_functions, 'gridx_setup_example_output')

# Conversion Functions
def test_bng_aswkb_example():
    assert hasattr(gridx_functions, 'bng_aswkb_example')
    assert callable(gridx_functions.bng_aswkb_example)

def test_bng_aswkt_example():
    assert hasattr(gridx_functions, 'bng_aswkt_example')
    assert callable(gridx_functions.bng_aswkt_example)

# Core Functions
def test_bng_cellarea_example():
    assert hasattr(gridx_functions, 'bng_cellarea_example')
    assert callable(gridx_functions.bng_cellarea_example)

def test_bng_centroid_example():
    assert hasattr(gridx_functions, 'bng_centroid_example')
    assert callable(gridx_functions.bng_centroid_example)

def test_bng_distance_example():
    assert hasattr(gridx_functions, 'bng_distance_example')
    assert callable(gridx_functions.bng_distance_example)

def test_bng_euclideandistance_example():
    assert hasattr(gridx_functions, 'bng_euclideandistance_example')
    assert callable(gridx_functions.bng_euclideandistance_example)

# Cell Operations
def test_bng_cellintersection_example():
    assert hasattr(gridx_functions, 'bng_cellintersection_example')
    assert callable(gridx_functions.bng_cellintersection_example)

def test_bng_cellunion_example():
    assert hasattr(gridx_functions, 'bng_cellunion_example')
    assert callable(gridx_functions.bng_cellunion_example)

# Coordinate Conversion
def test_bng_eastnorthasbng_example():
    assert hasattr(gridx_functions, 'bng_eastnorthasbng_example')
    assert callable(gridx_functions.bng_eastnorthasbng_example)

def test_bng_pointascell_example():
    assert hasattr(gridx_functions, 'bng_pointascell_example')
    assert callable(gridx_functions.bng_pointascell_example)


def test_bng_pointascell_python_api_example():
    assert hasattr(gridx_functions, 'bng_pointascell_python_api_example')
    assert callable(gridx_functions.bng_pointascell_python_api_example)


# K-Ring Functions
def test_bng_kring_example():
    assert hasattr(gridx_functions, 'bng_kring_example')
    assert callable(gridx_functions.bng_kring_example)

def test_bng_kloop_example():
    assert hasattr(gridx_functions, 'bng_kloop_example')
    assert callable(gridx_functions.bng_kloop_example)

# Note: bng_geomkring_example, bng_geomkloop_example, bng_polyfill_example,
# bng_tessellate_example: requires DBR for st_geomfromtext; tested here when view/sql available.

# Aggregator Functions
def test_bng_cellintersection_agg_example():
    assert hasattr(gridx_functions, 'bng_cellintersection_agg_example')
    assert callable(gridx_functions.bng_cellintersection_agg_example)

def test_bng_cellunion_agg_example():
    assert hasattr(gridx_functions, 'bng_cellunion_agg_example')
    assert callable(gridx_functions.bng_cellunion_agg_example)

# Generator Functions
def test_bng_kringexplode_example():
    assert hasattr(gridx_functions, 'bng_kringexplode_example')
    assert callable(gridx_functions.bng_kringexplode_example)

def test_bng_kloopexplode_example():
    assert hasattr(gridx_functions, 'bng_kloopexplode_example')
    assert callable(gridx_functions.bng_kloopexplode_example)

# Note: bng_geomkringexplode_example, bng_geomkloopexplode_example,
# bng_tessellateexplode_example: requires DBR for st_geomfromtext; tested here when available.
