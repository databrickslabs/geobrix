"""Re-exports from databricks.labs.gbx.ds.file_gbx for backward compatibility.

The FILE capability detection, column-level FILE injection, and FileRef windowed-read
helpers are now centralized in the unified file_gbx base. This module re-exports them
so existing pyrx code continues to work unchanged.

See databricks.labs.gbx.ds.file_gbx for the canonical implementations.
"""

# Re-export from the unified file_gbx base (ds/file_gbx.py)
from databricks.labs.gbx.ds.file_gbx import (
    _FILE_SUPPORT_CACHE,
    FileRefReadError,
    file_ref_arg,
    file_supported,
    open_windowed_via_fileref,
)

__all__ = [
    "file_supported",
    "file_ref_arg",
    "FileRefReadError",
    "open_windowed_via_fileref",
    "_FILE_SUPPORT_CACHE",
]
