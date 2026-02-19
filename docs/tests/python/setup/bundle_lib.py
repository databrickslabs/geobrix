"""
Re-export GeoBrix sample module for repo scripts and tests.

When the GeoBrix package is installed (pip install -e python/geobrix), this module
delegates to databricks.labs.gbx.sample so essential_bundle.py, complete_bundle.py,
and sample-data/ scripts work without duplication. End users use the package directly.
"""

try:
    from databricks.labs.gbx.sample import (
        get_temp_dir,
        get_volumes_path,
        run_complete_bundle,
        run_essential_bundle,
    )
    from databricks.labs.gbx.sample._bundle import (
        _copy_final_to_volumes,
        _ensure_dir,
        download_to_path,
        download_srtm_to_path,
        srtm_hgt_to_geotiff,
    )
except ImportError as e:
    raise RuntimeError(
        "Install the GeoBrix package to use bundle_lib: pip install -e python/geobrix"
    ) from e

__all__ = [
    "_copy_final_to_volumes",
    "_ensure_dir",
    "download_to_path",
    "download_srtm_to_path",
    "srtm_hgt_to_geotiff",
    "get_temp_dir",
    "get_volumes_path",
    "run_complete_bundle",
    "run_essential_bundle",
]
