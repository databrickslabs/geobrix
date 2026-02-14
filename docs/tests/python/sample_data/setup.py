"""
Setup page code examples - Configure Storage (payload only).

Single source of truth for docs/docs/sample-data/setup.mdx.
Tested by: docs/tests/python/sample_data/test_setup.py
"""

SETUP_CONFIGURE_STORAGE = """from pathlib import Path

# Configure your storage location
CATALOG = "main"  # Your catalog name
SCHEMA = "default"  # Your schema name
VOLUME = "geobrix_samples"  # Volume name for sample data

# Base path for all sample data
SAMPLE_DATA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/geobrix-examples"

# Create directory structure
Path(SAMPLE_DATA_PATH).mkdir(parents=True, exist_ok=True)

print(f"✅ Sample data will be stored at: {SAMPLE_DATA_PATH}")"""

SETUP_CONFIGURE_STORAGE_output = """✅ Sample data will be stored at: /Volumes/main/default/geobrix_samples/geobrix-examples"""
