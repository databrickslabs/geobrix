"""v2 virtual-tile representation.

A tile is bytes-free ("virtual") when ``raster is None`` — it carries a
``path`` + pixel ``window`` and materializes lazily via ``open_tile``. A tile
with ``raster`` set is materialized (v1-compatible). The struct is the same
across both cases so parity locks once; ``path``/``window``/``clip_polygon``/
``clip_crs``/``crs`` are null for a plain v1 tile.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

Window4 = Tuple[int, int, int, int]  # (col_off, row_off, width, height)

_WINDOW_STRUCT = StructType(
    [
        StructField("col_off", IntegerType(), nullable=False),
        StructField("row_off", IntegerType(), nullable=False),
        StructField("width", IntegerType(), nullable=False),
        StructField("height", IntegerType(), nullable=False),
    ]
)

V2_TILE_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=True),
        StructField("path", StringType(), nullable=True),
        StructField("window", _WINDOW_STRUCT, nullable=True),
        StructField("clip_polygon", BinaryType(), nullable=True),
        StructField("clip_crs", StringType(), nullable=True),
        StructField("crs", StringType(), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


@dataclass
class VirtualTile:
    cellid: int
    raster: Optional[bytes] = None
    path: Optional[str] = None
    window: Optional[Window4] = None
    clip_polygon: Optional[bytes] = None
    clip_crs: Optional[str] = None
    crs: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.raster is None and self.path is None:
            raise ValueError("VirtualTile needs raster bytes or a path")
        if self.raster is None and self.window is None:
            raise ValueError("virtual tile (path, no raster) requires a window")
        if self.window is not None:
            self.window = tuple(int(v) for v in self.window)  # normalize

    def is_virtual(self) -> bool:
        return self.raster is None

    def to_row(self) -> dict:
        win = None
        if self.window is not None:
            c, r, w, h = self.window
            win = {"col_off": c, "row_off": r, "width": w, "height": h}
        return {
            "cellid": int(self.cellid),
            "raster": self.raster,
            "path": self.path,
            "window": win,
            "clip_polygon": self.clip_polygon,
            "clip_crs": self.clip_crs,
            "crs": self.crs,
            "metadata": dict(self.metadata) if self.metadata else {},
        }

    @classmethod
    def from_row(cls, row) -> "VirtualTile":
        d = row.asDict() if hasattr(row, "asDict") else dict(row)
        win = d.get("window")
        if win is not None:
            wd = win.asDict() if hasattr(win, "asDict") else dict(win)
            win = (wd["col_off"], wd["row_off"], wd["width"], wd["height"])
        return cls(
            cellid=d["cellid"],
            raster=d.get("raster"),
            path=d.get("path"),
            window=win,
            clip_polygon=d.get("clip_polygon"),
            clip_crs=d.get("clip_crs"),
            crs=d.get("crs"),
            metadata=dict(d.get("metadata") or {}),
        )
