"""GeoBrix connector: the LakeflowConnect side (discovery, schema, offsets).

Files are read by the shared ``ds.paths`` core, so any supported light reader
(vector, LiDAR, EXIF, raster) works per table. The bulk data path is Arrow-native (see
``datasource.py``); the dict-row methods here (``read_table`` / ``read_partition``)
exist so the framework contract is complete, and reuse the same per-file Arrow reads.
"""

import fnmatch
import os
import re
from typing import Iterator, Optional, Sequence

import pyarrow as pa
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.gbx.ds._listing import list_files
from databricks.labs.gbx.ds.paths import (
    SOURCE_MTIME_COL,
    iter_path_batches,
    path_schema,
    resolve_format,
)

# Short table-option values accepted for `format` (any light reader name also works).
FORMAT_ALIASES = {
    "vector": "vector_gbx",
    "gpkg": "gpkg_gbx",
    "geojson": "geojson_gbx",
    "geojsonl": "geojsonl_gbx",
    "shapefile": "shapefile_gbx",
    "filegdb": "file_gdb_gbx",
    "lidar": "lidar_gbx",
    "exif": "exif_gbx",
    "raster": "raster_gbx",
    "gtiff": "gtiff_gbx",
}

# Discovery: extension -> reader (longest suffix first, so ".gdb.zip" beats ".zip").
# Sidecars such as .shx/.dbf/.prj are never members.
EXT_TO_FORMAT = {
    ".gdb.zip": "file_gdb_gbx",
    ".gdb": "file_gdb_gbx",
    ".gpkg": "gpkg_gbx",
    ".geojsonl": "geojsonl_gbx",
    ".geojsons": "geojsonl_gbx",
    ".geojson": "geojson_gbx",
    ".json": "geojson_gbx",
    ".shp.zip": "shapefile_gbx",
    ".shp": "shapefile_gbx",
    ".shz": "shapefile_gbx",
    ".las": "lidar_gbx",
    ".laz": "lidar_gbx",
    ".jpg": "exif_gbx",
    ".jpeg": "exif_gbx",
    ".tif": "gtiff_gbx",
    ".tiff": "gtiff_gbx",
}
_ALL_EXTS = tuple(sorted(EXT_TO_FORMAT, key=len, reverse=True))

# The generic `raster` reader accepts every raster extension; the generic `vector`
# reader accepts every vector extension (not LiDAR / EXIF / raster).
_RASTER = {"raster_gbx", "gtiff_gbx"}
_NON_VECTOR = {"lidar_gbx", "exif_gbx"} | _RASTER

# Table options handed to the reader unchanged. Everything else on the options
# dict (framework keys, UC connection keys, our own connector options) is either
# mapped explicitly in reader_options() or never reaches the reader.
_PASSTHROUGH_READER_KEYS = ("where", "bbox", "mode")


def _format_of(path: str):
    low = path.lower().rstrip("/")
    for ext in _ALL_EXTS:
        if low.endswith(ext):
            return EXT_TO_FORMAT[ext]
    return None


def format_for(table_options: dict, members: list) -> str:
    """Reader for a table: the `format` option (alias or reader name), else the
    members' extension; a mix of extensions without `format` is an error."""
    fmt = (table_options.get("format") or "").strip().lower()
    if fmt:
        return resolve_format(FORMAT_ALIASES.get(fmt, fmt)).name()
    found = {f for f in (_format_of(m) for m in members) if f}
    if len(found) == 1:
        return found.pop()
    raise ValueError(
        f"Cannot infer a reader for {members[:3]}...: set the `format` table option "
        f"(found {sorted(found) or 'no recognized extensions'})."
    )


_START = {"max_mtime_ns": -1, "at_max": []}


def _strip_ext(name: str) -> str:
    low = name.lower()
    for ext in _ALL_EXTS:
        if low.endswith(ext):
            return name[: -len(ext)]
    return name


def _table_name_for(entry: str) -> str:
    """Sanitized table name for a discovered file or folder."""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", _strip_ext(entry)).strip("_").lower()


def _norm_offset(offset: Optional[dict]) -> dict:
    if not offset:
        return dict(_START)
    return {
        "max_mtime_ns": int(offset.get("max_mtime_ns", -1)),
        "at_max": list(offset.get("at_max", [])),
    }


def _after(path: str, mtime_ns: int, start: dict) -> bool:
    """True when (path, mtime) is strictly after the start offset (exclusive)."""
    if mtime_ns != start["max_mtime_ns"]:
        return mtime_ns > start["max_mtime_ns"]
    return path not in start["at_max"]


def _not_after(path: str, mtime_ns: int, end: dict) -> bool:
    """True when (path, mtime) is at or before the end offset (inclusive).

    A file whose mtime equals the end's max but that is NOT in ``at_max`` landed
    after the end offset was taken; it belongs to the next micro-batch (reading it
    now would read it twice, because the next start offset excludes only at_max).
    """
    if mtime_ns != end["max_mtime_ns"]:
        return mtime_ns < end["max_mtime_ns"]
    return path in end["at_max"]


class GeoBrixLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Geospatial files on Unity Catalog Volumes as append-only Lakeflow tables.

    Connection options: ``root_path`` (optional Volume prefix; enables
    discovery and confines every table path to it).
    Table options: ``path`` (file or folder; relative to ``root_path``),
    ``format``, ``layer``, ``where``, ``bbox``, ``chunk_size``, ``file_glob``, ``mode``.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        root = options.get("root_path")
        # Per-table pins: the reader chosen from all members and the schema of the
        # first file, so every later file is read (and checked) the same way.
        self._schemas: dict[str, StructType] = {}
        self._formats: dict[str, str] = {}
        self.root_path = os.path.normpath(root) if root else None
        if self.root_path and not os.path.isabs(self.root_path):
            raise ValueError(f"root_path must be absolute; got {root!r}")

    # ------------------------------------------------------------- discovery
    def list_tables(self) -> list[str]:
        """Datasets directly under ``root_path``: recognized files and
        folders (a folder is one multi-file dataset). ``[]`` without a root."""
        if not self.root_path:
            return []
        names = []
        for entry in sorted(os.listdir(self.root_path)):
            if entry.startswith((".", "_")):
                continue
            full = os.path.join(self.root_path, entry)
            if os.path.isdir(full) or _format_of(entry):
                name = _table_name_for(entry)
                if name and name not in names:
                    names.append(name)
        return names

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        members = self._members(self._resolve_path(table_name, table_options), table_options)
        if not members:
            raise ValueError(
                f"Table {table_name!r} has no matching files yet; the schema is taken "
                f"from the first file, so stage at least one file before creating the table."
            )
        fmt = format_for(table_options, members)
        schema = path_schema(fmt, self.reader_options(table_options), members[0])
        self._formats[table_name] = fmt
        self._schemas[table_name] = schema
        return schema

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        return {
            "primary_keys": [],
            "cursor_field": SOURCE_MTIME_COL,
            "ingestion_type": "append",
        }

    # ------------------------------------------------------------- offsets
    def is_partitioned(self, table_name: str) -> bool:
        return True

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: Optional[dict] = None,
    ) -> dict:
        stats = self._stat_members(table_name, table_options)
        if not stats:
            return _norm_offset(start_offset)
        max_ns = max(m for _, m in stats)
        return {"max_mtime_ns": max_ns, "at_max": sorted(p for p, m in stats if m == max_ns)}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: Optional[dict] = None,
        end_offset: Optional[dict] = None,
    ) -> Sequence[dict]:
        """One partition per file. Batch (both offsets None) = every file;
        streaming = files in (start, end]."""
        stats = self._stat_members(table_name, table_options)
        if start_offset is None and end_offset is None:
            selected = stats
        else:
            start, end = _norm_offset(start_offset), _norm_offset(end_offset)
            selected = [
                (p, m) for p, m in stats if _after(p, m, start) and _not_after(p, m, end)
            ]
        return [{"path": p, "mtime_ns": m} for p, m in selected]

    # ------------------------------------------------------------- reads
    def iter_member_batches(
        self,
        member: str,
        mtime_ns: int,
        table_options: dict[str, str],
        expected: Optional[StructType] = None,
        table_name: Optional[str] = None,
    ) -> Iterator[pa.RecordBatch]:
        """Arrow batches for one file, with lineage columns appended.

        ``expected`` is the table schema (the data source passes the schema Spark
        planned with); otherwise it is derived from the table's first file and cached.
        """
        if expected is None and table_name is not None:
            expected = self._table_schema(table_name, table_options)
        if table_name is None:
            fmt = format_for(table_options, [member])
        else:
            fmt = self._table_format(table_name, table_options)
            kind = _format_of(member)
            if not table_options.get("format") and kind not in (None, fmt):
                raise ValueError(
                    f"{member}: a {kind} file in a table read with {fmt}; set the "
                    f"`format` table option or split the folder."
                )
        yield from iter_path_batches(
            member,
            int(mtime_ns) // 1_000,
            fmt,
            self.reader_options(table_options),
            expected,
        )

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        for batch in self.iter_member_batches(
            partition["path"],
            int(partition["mtime_ns"]),
            table_options,
            table_name=table_name,
        ):
            yield from batch.to_pylist()

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        end = self.latest_offset(table_name, table_options, start_offset)
        parts = self.get_partitions(table_name, table_options, start_offset or {}, end)

        def rows():
            for p in parts:
                yield from self.read_partition(table_name, p, table_options)

        return rows(), end

    # ------------------------------------------------------------- helpers
    def reader_options(self, table_options: dict[str, str]) -> dict:
        """Map connector table options onto reader options (the named reader sets
        its own driver)."""
        out = {k: v for k, v in table_options.items() if k in _PASSTHROUGH_READER_KEYS}
        if table_options.get("layer"):
            out["layerName"] = table_options["layer"]
        if table_options.get("chunk_size"):
            out["chunkSize"] = str(int(table_options["chunk_size"]))
        return out

    def _table_format(self, table_name: str, table_options: dict[str, str]) -> str:
        if table_name not in self._formats:
            self.get_table_schema(table_name, table_options)
        return self._formats[table_name]

    def _table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        if table_name not in self._schemas:
            self.get_table_schema(table_name, table_options)
        return self._schemas[table_name]

    def _guard(self, path: str) -> str:
        path = os.path.normpath(path)
        if self.root_path and os.path.commonpath([path, self.root_path]) != self.root_path:
            raise ValueError(f"Path {path!r} is outside root_path {self.root_path!r}.")
        return path

    def _resolve_path(self, table_name: str, table_options: dict[str, str]) -> str:
        explicit = table_options.get("path")
        if explicit:
            if not os.path.isabs(explicit):
                if not self.root_path:
                    raise ValueError(
                        f"Relative path {explicit!r} needs a root_path on the connection."
                    )
                explicit = os.path.join(self.root_path, explicit)
            return self._guard(explicit)
        if self.root_path:
            for entry in os.listdir(self.root_path):
                if _table_name_for(entry) == table_name:
                    return self._guard(os.path.join(self.root_path, entry))
        raise ValueError(
            f"Cannot resolve table {table_name!r}: set a 'path' table option, or "
            f"configure root_path so it can be discovered by name."
        )

    def _members(self, path: str, table_options: dict[str, str]) -> list[str]:
        """Data files for a table path: a file (or a .gdb folder) is one member; a
        plain folder is listed recursively, keeping the table format's files (or any
        recognized file when no `format` is set)."""
        if os.path.isfile(path) or path.lower().rstrip("/").endswith(".gdb"):
            return [path]
        if not os.path.isdir(path):
            raise ValueError(f"Table path {path!r} does not exist.")
        want = format_for(table_options, []) if table_options.get("format") else None

        def _wanted(f: str) -> bool:
            kind = _format_of(f)
            if kind is None:
                return False
            if want is None:
                return True
            if want == "vector_gbx":
                return kind not in _NON_VECTOR
            if want == "raster_gbx":
                return kind in _RASTER
            return kind == want

        files = [
            f
            for f in list_files(path, recursive=True, raise_on_empty=False)
            if _wanted(f) and not os.path.basename(f).startswith((".", "_"))
        ]
        glob = table_options.get("file_glob")
        if glob:
            files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), glob)]
        return sorted(files)

    def _stat_members(self, table_name: str, table_options: dict[str, str]) -> list:
        """(path, mtime_ns) per member. A file removed between listing and stat is
        skipped (it no longer exists to ingest); any other error propagates."""
        out = []
        for member in self._members(self._resolve_path(table_name, table_options), table_options):
            try:
                out.append((member, os.stat(member).st_mtime_ns))
            except FileNotFoundError:
                continue
        return out
