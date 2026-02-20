"""
Central sample-data path for documentation tests.

Used at runtime so tests read from the minimal bundle (test-data) or from
GBX_SAMPLE_DATA_ROOT when set. Docs can still display the canonical path
/Volumes/main/default/geobrix_samples/geobrix-examples in prose.
"""
import os

_root = os.environ.get("GBX_SAMPLE_DATA_ROOT")
if _root:
    _root = _root.rstrip("/")
    SAMPLE_DATA_BASE = os.path.join(_root, "geobrix-examples")
    SAMPLE_DATA_VOLUME = os.path.basename(_root)  # e.g. test-data or geobrix_samples
else:
    # Default: minimal bundle path so tests pass without full bundle or env
    SAMPLE_DATA_BASE = "/Volumes/main/default/test-data/geobrix-examples"
    SAMPLE_DATA_VOLUME = "test-data"

# Relaxed bounds for minimal bundle (1 borough, 10 taxi zones, 10 subway, 8 parks, 0+ rasters)
MIN_BOROUGHS = 1
MAX_BOROUGHS = 10
MIN_VECTOR_ROWS = 1  # subway/parks minimal bundle has 10 / 8
MIN_RASTER_ROWS = 0  # skip tests when 0; allow when >= 1
MIN_TAXI_ZONES = 1
MAX_TAXI_ZONES = 300
