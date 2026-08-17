"""Partition-scoped grouped execution for FILE/virtual tiles (light tier).

Amortizes the dominant cost (the source OPEN) by grouping a source raster's
tiles into one partition, contiguous, and reading them from one cached open
resource. Pure Python + mapInPandas -- no .rdd / sc / spark.conf.set.
"""

import os
from collections import OrderedDict
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

GBX_LRU_MAX_BYTES = int(os.environ.get("GBX_LRU_MAX_BYTES", 4 * 1024**3))  # 4 GiB
STREAM_NOMINAL_BYTES = 16 * 1024**2  # resident estimate for an open stream/dataset


def align_partitions(
    df: DataFrame, *, n: int, path_col: str = "tile.path"
) -> DataFrame:
    """Hash-by-path repartition + sort so each source FILE is saturated in one
    partition and contiguous within it. `n` is parallelism-sized by the caller
    (3-5x worker cores on classic; a parallelism target on Serverless) -- never
    n_files, never sc-derived."""
    if n <= 0:
        raise ValueError(f"n must be a positive parallelism target, got {n}")
    col = F.col(path_col)
    return df.repartition(n, col).sortWithinPartitions(col)


class OpenResourceLRU:
    """Per-partition BYTE-BUDGETED LRU of open resources keyed by source uri/path.

    Amortizes the OPEN cost across a source's windows. Instead of a fixed count,
    entries are held up to a byte budget (default 4 GiB) with a max_count handle
    guard, so many small sources stay warm (e.g. ~128 x 32 MiB under 4 GiB) while
    a few huge ones don't blow the budget. Each entry carries a weight: a staged
    local copy weighs its file size (so this budget IS the staged-disk-fill guard,
    and eviction deletes the temp); an open stream/dataset weighs a small nominal
    so the count guard governs. The current (most-recent) entry is never evicted.
    Evicted and remaining resources are always closed (evict + close_all)."""

    def __init__(
        self,
        *,
        max_bytes: int = GBX_LRU_MAX_BYTES,
        max_count: int = 64,
        opener: Callable[[str], Any],
        closer: Callable[[Any], None],
        weigher: Callable[[Any, str], int],
    ):
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if max_count < 1:
            raise ValueError("max_count must be >= 1")
        self.max_bytes = max_bytes
        self.max_count = max_count
        self._opener = opener
        self._closer = closer
        self._weigher = weigher
        self._store: "OrderedDict[str, tuple]" = (
            OrderedDict()
        )  # key -> (resource, weight)
        self.opens = 0
        self.evictions = 0
        self.bytes = 0

    def get(self, key: str) -> Any:
        if key in self._store:
            self._store.move_to_end(key)
            return self._store[key][0]
        res = self._opener(key)
        self.opens += 1
        weight = int(self._weigher(res, key))
        self._store[key] = (res, weight)
        self.bytes += weight
        # evict oldest while over budget, but never the current (most-recent) entry
        while len(self._store) > 1 and (
            self.bytes > self.max_bytes or len(self._store) > self.max_count
        ):
            _, (evicted, w) = self._store.popitem(last=False)
            self.bytes -= w
            self.evictions += 1
            self._closer(evicted)
        return res

    def close_all(self) -> None:
        while self._store:
            _, (res, w) = self._store.popitem(last=False)
            self.bytes -= w
            self._closer(res)


def _make_opener():
    """Capability-adaptive opener factory.  Returns ``(file_ok, opener, closer, weigher)``.

    Runs on the worker; all imports are worker-local (GDAL env configured
    before this is called).

    The LRU is keyed by ``uri`` (the source-path string) — hashable, and the
    identity that amortises opens.

    FILE-capable (Task 9): will cache an open seekable FILE stream per uri
    via ``open_windowed_via_fileref``; the per-tile window read happens inside
    the caller after getting the stream from the LRU.

    Fallback (non-FILE / local Spark): stages the source file locally and
    returns a plain ``rasterio.open`` dataset.  Used only when the caller
    actually invokes ``lru.get(uri)`` for a path-keyed virtual tile; the
    materialized-tile branch bypasses the LRU entirely and never calls this.
    """
    from . import _file_ref

    file_ok = _file_ref.file_supported()

    def opener(uri: str):
        import rasterio

        from .core.preparer import _stage_local_if_needed

        local_path, _ = _stage_local_if_needed(uri)
        return rasterio.open(local_path)

    def closer(src) -> None:
        try:
            src.close()
        except Exception:
            pass

    def weigher(src, key: str) -> int:
        # Nominal weight for an open dataset; count guard governs.
        # Local-stage branch (Task 9 fast-follow) will weigh the staged file size.
        return STREAM_NOMINAL_BYTES

    return file_ok, opener, closer, weigher


def grouped_tile_map(
    df: DataFrame,
    core_fn,
    *,
    return_field: StructField,
    tile_col: str = "tile",
) -> DataFrame:
    """Partition-scoped ``mapInPandas`` executor for light-tier tiles.

    For each tile in the partition, opens the raster source, applies
    ``core_fn(ds) -> value`` on the open ``DatasetReader``, and stores the
    result in a new column ``return_field.name``.

    Tile dispatch:

    - **Materialized tiles** (``raster`` set, ``path`` None): opened per-row via
      ``_open``; the LRU is not consulted — bytes are already inline, nothing
      to amortise.  This is the path exercised by the local unit test.
    - **Virtual tiles** (``path`` set) + FILE-capable (Task 9): LRU caches an
      open seekable stream keyed by ``uri``; ``open_windowed_via_fileref``
      slots in to read each tile window from the cached stream.
    - **Virtual tiles** + fallback (non-FILE): opened per-row via ``_open``;
      no amortisation — each tile's window is unique.

    ``core_fn`` receives an open ``DatasetReader`` and must not hold a
    reference to it after returning.

    Output schema is ``df.schema + [return_field]``.  No cast to
    ``V2_TILE_SCHEMA``.
    """
    out_schema = StructType(list(df.schema.fields) + [return_field])
    out_name = return_field.name

    def _map(pdf_iter):
        from . import _env

        _env.configure_gdal_env()
        file_ok, opener, closer, weigher = _make_opener()
        lru = OpenResourceLRU(opener=opener, closer=closer, weigher=weigher)
        try:
            for pdf in pdf_iter:
                results = []
                for _, row in pdf.iterrows():
                    tile = row[tile_col]
                    # Resolve uri: None for materialized tiles, path string for virtual.
                    try:
                        uri = tile["path"] or None
                    except (KeyError, TypeError):
                        uri = None

                    if uri and file_ok:
                        # FILE fast path (Task 9): get the cached open source from
                        # the LRU, then read the tile's window from it.
                        # Currently returns the full-source dataset; Task 9 will
                        # replace with a windowed read via open_windowed_via_fileref.
                        src = lru.get(uri)
                        results.append(core_fn(src))
                    else:
                        # Fallback: open per-row.  Covers:
                        #   (a) materialized tiles (raster inline, path None)
                        #   (b) virtual tiles when FILE is not supported
                        from .core.open_tile import _open

                        with _open(tile) as ds:
                            results.append(core_fn(ds))
                yield pdf.assign(**{out_name: results})
        finally:
            lru.close_all()

    return df.mapInPandas(_map, schema=out_schema)
