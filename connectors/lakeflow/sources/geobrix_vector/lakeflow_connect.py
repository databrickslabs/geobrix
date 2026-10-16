"""GeoBrix vector connector: the LakeflowConnect side (discovery, schema, offsets).

The bulk data path is Arrow-native (see ``datasource.py``); the dict-row methods
here (``read_table`` / ``read_partition``) exist so the framework contract is
complete, and reuse the same per-file Arrow reads.
"""

import fnmatch
import os
import re
from typing import Iterator, Optional, Sequence

import pyarrow as pa
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.gbx.ds._listing import list_files
from databricks.labs.gbx.ds.vector import VectorGbxReader

SOURCE_PATH_COL = "_source_path"
SOURCE_MTIME_COL = "_source_mtime"

# User-facing `format` table option -> OGR driver understood by VectorGbxReader.
FORMAT_TO_DRIVER = {
    "gpkg": "GPKG",
    "geojson": "GeoJSON",
    "geojsonl": "GeoJSONSeq",
    "shapefile": "ESRI Shapefile",
    "filegdb": "OpenFileGDB",
    "vector": "",
}

# Primary data-file extensions per driver (sidecars such as .shx/.dbf/.prj are
# never members). "" = generic: the union of all of them.
_DRIVER_EXTS = {
    "GPKG": (".gpkg",),
    "GeoJSON": (".geojson", ".json"),
    "GeoJSONSeq": (".geojsonl", ".geojsons"),
    "ESRI Shapefile": (".shp", ".shz", ".shp.zip"),
    "OpenFileGDB": (".gdb", ".gdb.zip"),
}
_ALL_EXTS = tuple(sorted({e for exts in _DRIVER_EXTS.values() for e in exts}))

# Table options handed to VectorGbxReader unchanged. Everything else on the
# options dict (framework keys, UC connection keys, our own connector options)
# is either mapped explicitly in reader_options() or never reaches the reader.
_PASSTHROUGH_READER_KEYS = ("where", "bbox")

_START = {"max_mtime_ns": -1, "at_max": []}


def _strip_ext(name: str) -> str:
    low = name.lower()
    for ext in sorted(_ALL_EXTS, key=len, reverse=True):
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


class GeoBrixVectorLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Vector files on Unity Catalog Volumes as append-only Lakeflow tables.

    Connection options: ``root_path`` (optional Volume prefix; enables
    discovery and confines every table path to it).
    Table options: ``path`` (file or folder; relative to ``root_path``),
    ``format``, ``layer``, ``where``, ``bbox``, ``chunk_size``, ``file_glob``.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        root = options.get("root_path")
        self.root_path = os.path.normpath(root) if root else None
        if self.root_path and not os.path.isabs(self.root_path):
            raise ValueError(f"root_path must be absolute; got {root!r}")

    # ------------------------------------------------------------- discovery
    def list_tables(self) -> list[str]:
        """Datasets directly under ``root_path``: recognized vector files and
        folders (a folder is one multi-file dataset). ``[]`` without a root."""
        if not self.root_path:
            return []
        names = []
        for entry in sorted(os.listdir(self.root_path)):
            if entry.startswith((".", "_")):
                continue
            full = os.path.join(self.root_path, entry)
            if os.path.isdir(full) or entry.lower().endswith(_ALL_EXTS):
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
        schema = self._reader_for(members[0], table_options).schema()
        return StructType(
            list(schema.fields)
            + [
                StructField(SOURCE_PATH_COL, StringType(), True),
                StructField(SOURCE_MTIME_COL, TimestampType(), True),
            ]
        )

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
        self, member: str, mtime_ns: int, table_options: dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        """Arrow batches for one file, with lineage columns appended."""
        reader = self._reader_for(member, table_options)
        mtime_us = int(mtime_ns) // 1_000
        ts_type = pa.timestamp("us", tz="UTC")
        for part in reader.partitions():
            for batch in reader.read(part):
                n = batch.num_rows
                batch = batch.append_column(
                    SOURCE_PATH_COL, pa.array([member] * n, type=pa.string())
                )
                yield batch.append_column(SOURCE_MTIME_COL, pa.array([mtime_us] * n, type=ts_type))

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        for batch in self.iter_member_batches(
            partition["path"], int(partition["mtime_ns"]), table_options
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
        """Map connector table options onto VectorGbxReader options."""
        out = {k: v for k, v in table_options.items() if k in _PASSTHROUGH_READER_KEYS}
        fmt = (table_options.get("format") or "vector").lower()
        if fmt not in FORMAT_TO_DRIVER:
            raise ValueError(
                f"Unknown format {fmt!r}; expected one of {sorted(FORMAT_TO_DRIVER)}."
            )
        if FORMAT_TO_DRIVER[fmt]:
            out["driverName"] = FORMAT_TO_DRIVER[fmt]
        if table_options.get("layer"):
            out["layerName"] = table_options["layer"]
        if table_options.get("chunk_size"):
            out["chunkSize"] = str(int(table_options["chunk_size"]))
        return out

    def _reader_for(self, member: str, table_options: dict[str, str]) -> VectorGbxReader:
        return VectorGbxReader({**self.reader_options(table_options), "path": member})

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
        plain folder is listed recursively with the format's extensions."""
        if os.path.isfile(path) or path.lower().rstrip("/").endswith(".gdb"):
            return [path]
        if not os.path.isdir(path):
            raise ValueError(f"Table path {path!r} does not exist.")
        driver = FORMAT_TO_DRIVER[(table_options.get("format") or "vector").lower()]
        exts = _DRIVER_EXTS.get(driver, _ALL_EXTS)
        files = [
            f
            for f in list_files(path, recursive=True, raise_on_empty=False)
            if f.lower().endswith(exts) and not os.path.basename(f).startswith((".", "_"))
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
