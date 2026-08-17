"""Self-describing TBLPROPERTIES for GeoBrix FILE-column tables.

The writer stamps these so the reader can branch its handling on the on-table
format version (geobrix_writer_version): v1 -> today's logic; a future v2+ ->
new logic, without breaking old-table reads.
"""
from typing import Optional

WRITER_VERSION_KEY = "geobrix_writer_version"
CURRENT_WRITER_VERSION = "1"
LIBRARY_VERSION_KEY = "geobrix.version"
WRITE_STRATEGY_KEY = "geobrix.file.write_strategy"
FILESPACE_KEY = "databricks.filespace-preview"

_FILE_MODES = {"external", "managed"}
_LAYOUTS = {"plain", "order", "cluster"}


def build_props(*, file_mode: str, layout: str, filespace: Optional[str],
                library_version: str) -> dict:
    if file_mode not in _FILE_MODES:
        raise ValueError(f"file_mode must be one of {_FILE_MODES}, got {file_mode!r}")
    if layout not in _LAYOUTS:
        raise ValueError(f"layout must be one of {_LAYOUTS}, got {layout!r}")
    props = {
        WRITER_VERSION_KEY: CURRENT_WRITER_VERSION,
        LIBRARY_VERSION_KEY: library_version,
        WRITE_STRATEGY_KEY: f"{file_mode}:{layout}",
    }
    if file_mode == "managed":
        if not filespace:
            raise ValueError("managed file_mode requires a filespace (/Volumes/...)")
        props[FILESPACE_KEY] = filespace
    return props


def parse_props(props: dict) -> dict:
    version = props.get(WRITER_VERSION_KEY)
    strategy = props.get(WRITE_STRATEGY_KEY, "")
    file_mode, _, layout = strategy.partition(":")
    return {
        "writer_version": version,
        "file_mode": file_mode or None,
        "layout": layout or None,
        "is_geobrix": version is not None,
    }
