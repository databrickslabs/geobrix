# COG Lane + File-Preparation Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a two-lane raster API — a `file_gbx` path-lister reader (references, no content) and a `cog_gbx` file-preparation writer/reader that masters a COG per file on the Volume — and reverse the 0.4.4 forced-split reader default back to opt-in.

**Architecture:** `file_gbx` lists files as reference rows (path/name/extension/size/mtime, no bytes). Its rows flow into the `cog_gbx` writer, which opens each Volume source per task, converts to a master COG via the existing `analysis.cog_convert` (driver="COG"), and writes it FUSE-safe to the output Volume — pixels never ride in a Spark column (accumulation-proof). COG-creation options move off the gtiff lane onto the cog lane; the raster reader's `splitStrategy` default flips `auto`→`none`.

**Tech Stack:** Python 3.12+, rasterio (bundled GDAL), rio-cogeo (`cog_validate` in tests), PySpark Python DataSource V2, pytest. Light tier — pure Python, no JAR.

## Global Constraints

- **No Spark config in pyrx/ds runtime.** Never `spark.conf.set` / `_jvm` / `.rdd`. Serverless is Spark Connect — `.rdd` does not exist. (memory: pyrx-serverless-no-spark-config)
- **No aliases.** One canonical name per format/option (CLAUDE.md).
- **`_gbx` suffix convention** for format names: `file_gbx`, `cog_gbx` (like `raster_gbx`, `gtiff_gbx`).
- **Real COGs only.** COG output is produced by `analysis.cog_convert` (GDAL `driver="COG"`, ~2.8× decoded RAM, `cog_validate`-clean). Never rio-cogeo `cog_translate`, never `build_overviews` (fails `cog_validate` — wrong IFD order). (memory: cog-encode-memory-driver-cog)
- **FUSE-safe Volume writes.** GTiff/COG writes need random seeks Volume FUSE can't serve — build on local temp, then sequential copy to the Volume. Reads of Volume files stage-to-local first when windowed. (memory: volumes-cleanpath-bare-not-file)
- **Path scheme handling.** `to_spark_uri` for emitted `path` columns; `to_local_path` before any `os.*`/`rasterio.open` on a Volume path. (`ds/_listing.py`)
- **Binding parity enforced.** New registered DataSources go in `_SOURCES`; `gbx:test:bindings` green. No new `gbx_rst_*` SQL functions here (DataSources, not functions). (CLAUDE.md)
- **Doc tests ARE docs.** Real code + real assertions on real sample data under `/Volumes/main/geobrix_samples/geobrix-examples/`; no mocking Spark/GeoBrix/IO; run only in Docker via `gbx:test:*-docs`. (CLAUDE.md)
- **Testing order: maximize fast local first; Serverless is the final gating confirm.** Local unit + Docker doc-tests are necessary but NOT sufficient (0.4.4 passed both while OOMing on Serverless). A Serverless validation task gates "done". (spec §5)
- **Serverless validation mechanics** (spec §5): run via `gbx:test:notebooks-serverless` (jobs.submit + env v5); native `.ipynb` (no jupytext/`# MAGIC`); two-step `%pip` install; **rebuild+stage wheel AFTER last commit and hash-verify staged==local**; persist markers to Volume (NOT stdout — jobs API hides serverless stdout and OOM crashes the worker); judge by markers + child-run error, not harness exit code; no `.rdd`. (memories: whl-change-rebuild-and-stage, bench-wheel-path-divergence, serverless-env-v5, cluster-jar-cache-same-path)
- **No internal/planning vocabulary in `docs/docs/`** (QC judge `internals-leak`; no wave-numbers).
- **Version bump not in this plan.** Continues the 0.4.4 line already bumped; only bump again if the user asks.

---

## File Structure

**Create:**
- `python/geobrix/src/databricks/labs/gbx/ds/file.py` — `FileGbxDataSource`/`FileGbxReader` (`file_gbx`): path-lister, no content.
- `python/geobrix/src/databricks/labs/gbx/ds/cog.py` — `CogGbxDataSource`/`CogGbxReader` (`cog_gbx`): COG-aware reader (subclass of `RasterGbxReader`) + `writer()` factory → `CogGbxWriter`.
- `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py` — `CogGbxWriter`: path-input → per-file master-COG preparation.
- `python/geobrix/test/ds/test_file_datasource.py`
- `python/geobrix/test/ds/test_cog_writer.py`
- `python/geobrix/test/ds/test_cog_reader.py`

**Modify:**
- `python/geobrix/src/databricks/labs/gbx/ds/raster.py` — `splitStrategy` default `auto`→`none`; retire reader `tileFormat`/`cogBlockSize`/`cogOverviewResampling` options (COG-on-split gone from reader). Keep opt-in split (`sizeInMB>0` or explicit `splitStrategy`).
- `python/geobrix/src/databricks/labs/gbx/ds/gtiff.py` — remove `cog`/`cogBlockSize`/`cogOverviewResampling`/`cogCompression` from the gtiff writer factory; gtiff writer writes plain GTiff only.
- `python/geobrix/src/databricks/labs/gbx/ds/register.py` — add `FileGbxDataSource`, `CogGbxDataSource` to `_SOURCES`.
- `python/geobrix/test/pyrx/test_core_budget.py`, `python/geobrix/test/ds/test_raster_*` — update for the default reversal.
- `docs/docs/readers/*`, `docs/docs/writers/*`, `docs/docs/beta-release-notes.mdx`, `docs/sidebars.js`.

---

## Task 1: `file_gbx` reader (path lister)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/file.py`
- Test: `python/geobrix/test/ds/test_file_datasource.py`

**Interfaces:**
- Consumes: `_listing.list_files(path, filter_regex)`, `_listing.to_spark_uri(path)` (exist).
- Produces:
  - `FILE_SCHEMA` = `StructType([StructField("path",StringType(),False), StructField("name",StringType(),False), StructField("extension",StringType(),True), StructField("size",LongType(),False), StructField("modificationTime",TimestampType(),True)])`
  - `FileGbxDataSource.name() -> "file_gbx"`, `FileGbxReader(options)` with `partitions()`/`read()`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_file_datasource.py
import os
from databricks.labs.gbx.ds.file import FileGbxReader, FILE_SCHEMA


def _touch(p, data=b"x"):
    with open(p, "wb") as f:
        f.write(data)


def test_lists_files_with_extension(tmp_path):
    _touch(str(tmp_path / "a.tif"))
    _touch(str(tmp_path / "b.TIFF"))
    _touch(str(tmp_path / "noext"))
    r = FileGbxReader({"path": str(tmp_path)})
    rows = [row for part in r.partitions() for row in r.read(part)]
    by_name = {row[1]: row for row in rows}  # name -> (path,name,ext,size,mtime)
    assert by_name["a.tif"][2] == "tif"
    assert by_name["b.TIFF"][2] == "tiff"      # lowercased
    assert by_name["noext"][2] is None          # null when no extension
    assert all(row[3] >= 1 for row in rows)     # size
    assert by_name["a.tif"][0].endswith("a.tif")  # path


def test_filter_regex(tmp_path):
    _touch(str(tmp_path / "keep.tif"))
    _touch(str(tmp_path / "skip.nc"))
    r = FileGbxReader({"path": str(tmp_path), "filterRegex": r".*\.tif$"})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert [row[1] for row in rows] == ["keep.tif"]


def test_never_reads_content(tmp_path):
    # A non-raster file must list fine — proving no raster/content open.
    _touch(str(tmp_path / "notaraster.tif"), b"not a tiff at all")
    r = FileGbxReader({"path": str(tmp_path)})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert rows[0][1] == "notaraster.tif"  # listed, never opened/decoded
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_file_datasource.py`
Expected: FAIL — `ModuleNotFoundError: ...ds.file`.

- [ ] **Step 3: Implement `file.py`**

```python
"""file_gbx — path-listing DataSource. Emits file REFERENCES, never content
(the deliberate contrast to binaryFile, which drags bytes into memory).
Raster-agnostic: a pure lister; consumers decide what to do with paths."""
from __future__ import annotations

import os
from typing import Dict, Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

from databricks.labs.gbx.ds import _listing

FILE_SCHEMA = StructType([
    StructField("path", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("extension", StringType(), nullable=True),
    StructField("size", LongType(), nullable=False),
    StructField("modificationTime", TimestampType(), nullable=True),
])


class _PathPartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class FileGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("file_gbx requires a 'path' (e.g. .load(path)).")
        self.filter_regex = options.get("filterRegex", ".*")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        return [_PathPartition(f) for f in files]

    def read(self, partition: "_PathPartition") -> Iterator[Tuple]:
        import datetime as _dt

        local = _listing.to_local_path(partition.file_path)
        st = os.stat(local)
        name = os.path.basename(local)
        stem, ext = os.path.splitext(name)
        extension = ext[1:].lower() if ext else None
        source = _listing.to_spark_uri(partition.file_path)
        yield (
            source,
            name,
            extension,
            int(st.st_size),
            _dt.datetime.fromtimestamp(st.st_mtime),
        )


class FileGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "file_gbx"

    def schema(self) -> StructType:
        return FILE_SCHEMA

    def reader(self, schema: StructType) -> DataSourceReader:
        return FileGbxReader(self.options)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_file_datasource.py`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/file.py python/geobrix/test/ds/test_file_datasource.py
git commit -m "feat(ds): file_gbx path-lister reader (references, no content)

Co-authored-by: Isaac"
```

---

## Task 2: `cog_gbx` writer (master-COG preparer)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py`
- Test: `python/geobrix/test/ds/test_cog_writer.py`

**Interfaces:**
- Consumes: `analysis.cog_convert(ds, compression, blocksize, overview_resampling) -> bytes` (exists, driver="COG"); `cog.sniff_header`/`detect_cog` (exists); `_listing.to_local_path` (exists).
- Produces: `CogGbxWriter(path, schema, overwrite, cog_blocksize=512, cog_overview_resampling="AVERAGE", cog_compression="DEFLATE", name_col=None, ext="tif")` — a `DataSourceWriter`. `assert_path_schema(schema)` — validates a `path` column exists.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_cog_writer.py
import glob
import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, assert_path_schema
from pyspark.sql.types import (
    StringType, StructField, StructType, LongType,
)
from databricks.labs.gbx.pyrx.core import cog as gbxcog


def _write_src(path, w=512, h=512):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=from_origin(0, 60, 0.01, 0.01))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w) % 256)


def test_assert_path_schema_requires_path():
    ok = StructType([StructField("path", StringType(), False)])
    assert_path_schema(ok)  # no raise
    bad = StructType([StructField("name", StringType(), False)])
    try:
        assert_path_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_writer_prepares_valid_cog(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([
        StructField("path", StringType(), False),
        StructField("name", StringType(), False),
    ])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    # rows are plain dicts (works like a Spark Row via subscript)
    row = {"path": str(src), "name": "scene.tif"}
    w.write(iter([row]))
    produced = glob.glob(os.path.join(str(out), "*.tif"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `cog_writer.py`**

```python
"""cog_gbx writer — master-COG file preparation.

Accepts PATH-bearing rows (from file_gbx), opens each source file on the
Volume, converts it to ONE master COG (internally tiled + overviews, no split)
via the shared analysis.cog_convert (driver="COG"), and writes it FUSE-safe to
the output Volume. Pixels never ride in a Spark column — accumulation-proof.
"""
from __future__ import annotations

import glob
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _listing


@dataclass
class CogCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_path_schema(schema: StructType) -> None:
    """cog_gbx writer requires a 'path' column (the file_gbx output)."""
    names = [f.name for f in schema.fields]
    if "path" not in names:
        raise ValueError(
            f"cog_gbx writer requires a 'path' column (file_gbx output); got {names}"
        )


class CogGbxWriter(DataSourceWriter):
    def __init__(self, path, schema, overwrite, cog_blocksize=512,
                 cog_overview_resampling="AVERAGE", cog_compression="DEFLATE",
                 name_col=None, ext="tif"):
        assert_path_schema(schema)
        self.out_dir = _listing.to_local_path(path)
        self.overwrite = overwrite
        self.cog_blocksize = int(cog_blocksize)
        self.cog_overview_resampling = cog_overview_resampling
        self.cog_compression = cog_compression
        self.name_col = name_col
        self.ext = ext
        if overwrite and os.path.isdir(self.out_dir):
            for stale in glob.glob(os.path.join(self.out_dir, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert
        import rasterio

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            src = _listing.to_local_path(str(row["path"]))
            # output name: derive from source basename (or nameCol if given)
            if self.name_col and row[self.name_col] is not None:
                base = os.path.basename(str(row[self.name_col]))
            else:
                base = os.path.basename(src)
            stem = os.path.splitext(base)[0]
            out_path = os.path.join(self.out_dir, f"{stem}.{self.ext}")

            # Convert to a master COG. Build to local temp then sequential-copy to
            # the Volume (FUSE-safe). cog_convert handles the driver="COG" encode.
            with rasterio.open(src) as ds:
                cog_bytes = cog_convert(
                    ds, self.cog_compression, self.cog_blocksize,
                    self.cog_overview_resampling,
                )
            fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd)
            try:
                with open(tmp, "wb") as fh:
                    fh.write(cog_bytes)
                shutil.copy(tmp, out_path)  # sequential → FUSE-safe on /Volumes
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written.append(out_path)
        return CogCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, CogCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
```

Note: `cog_convert` already reads the whole array once and encodes via `driver="COG"`; peak is ~2.8× one file's decoded size (one file per task → no accumulation). This is the memory-safe shape validated in the 0.4.4 investigation.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py python/geobrix/test/ds/test_cog_writer.py
git commit -m "feat(ds): cog_gbx writer — per-file master-COG preparation

Co-authored-by: Isaac"
```

---

## Task 3: `cog_gbx` DataSource (reader + writer factory)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/cog.py`
- Test: `python/geobrix/test/ds/test_cog_reader.py`

**Interfaces:**
- Consumes: `RasterGbxReader`/`RasterGbxDataSource` (raster.py); `CogGbxWriter` (Task 2).
- Produces: `CogGbxReader(RasterGbxReader)` (COG-aware; presets nothing that forces split — bbox AOI is the blessed clip path); `CogGbxDataSource.name() -> "cog_gbx"` with `reader()` and `writer()`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_cog_reader.py
from databricks.labs.gbx.ds.cog import CogGbxReader, CogGbxDataSource


def test_cog_reader_defaults_no_split():
    r = CogGbxReader({"path": "/x"})
    # cog lane does not force splitting; strategy resolves to none by default
    assert r.strategy == "none"


def test_cog_reader_honors_bbox():
    r = CogGbxReader({"path": "/x", "bbox": "0,0,1,1"})
    assert r.bbox == (0.0, 0.0, 1.0, 1.0)


def test_datasource_name():
    assert CogGbxDataSource.name() == "cog_gbx"
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_reader.py`
Expected: FAIL — module missing (also `r.strategy` default depends on Task 4's reversal; if run before Task 4, note it).

- [ ] **Step 3: Implement `cog.py`**

```python
"""cog_gbx — the optimized COG lane. Reader is COG-aware (efficient windowed /
overview reads; bbox AOI is the blessed clip path). Writer prepares master COGs.
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql.datasource import DataSourceReader, DataSourceWriter
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds.raster import RasterGbxDataSource, RasterGbxReader


class CogGbxReader(RasterGbxReader):
    def __init__(self, options: Dict[str, str]):
        super().__init__(options)
        self.driver = "GTiff"  # COG opens as GTiff


class CogGbxDataSource(RasterGbxDataSource):
    @classmethod
    def name(cls) -> str:
        return "cog_gbx"

    def reader(self, schema: StructType) -> DataSourceReader:
        return CogGbxReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        from databricks.labs.gbx.ds.cog_writer import CogGbxWriter

        path = self.options.get("path")
        if not path:
            raise ValueError("cog_gbx writer requires an output path (.save(path)).")
        return CogGbxWriter(
            path, schema, overwrite,
            cog_blocksize=int(self.options.get("cogBlockSize", "512")),
            cog_overview_resampling=self.options.get("cogOverviewResampling", "AVERAGE"),
            cog_compression=self.options.get("cogCompression", "DEFLATE"),
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
        )
```

- [ ] **Step 4: Run to verify pass** (after Task 4 lands the `none` default)

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_reader.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/cog.py python/geobrix/test/ds/test_cog_reader.py
git commit -m "feat(ds): cog_gbx DataSource (COG-aware reader + writer factory)

Co-authored-by: Isaac"
```

---

## Task 4: Reverse forced-split default + retire reader COG options

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py:407-417`
- Modify: `python/geobrix/test/pyrx/test_core_budget.py`, `python/geobrix/test/ds/test_raster_datasource.py`, `python/geobrix/test/ds/test_raster_large.py`

**Interfaces:**
- Produces: `RasterGbxReader` default `splitStrategy="none"`; `tileFormat`/`cogBlockSize`/`cogOverviewResampling` removed from the reader's option parsing. Opt-in split preserved via `sizeInMB>0` or explicit `splitStrategy=serverless|classic`.

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/ds/test_raster_datasource.py
from databricks.labs.gbx.ds.raster import RasterGbxReader

def test_reader_default_is_no_split():
    r = RasterGbxReader({"path": "/x"})
    assert r.strategy == "none"      # default reversed from 0.4.4 'auto'

def test_reader_optin_split_still_works():
    r = RasterGbxReader({"path": "/x", "splitStrategy": "serverless"})
    assert r.strategy == "serverless"
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_datasource.py -k "default_is_no_split or optin_split"`
Expected: FAIL — `r.strategy == "serverless"` (current default `auto` → serverless on serverless).

- [ ] **Step 3: Implement the reversal**

In `raster.py:414`, change default `"auto"` → `"none"`:
```python
self.strategy = budget.resolve_strategy(options.get("splitStrategy", "none"))
```
Remove the reader's COG-option parsing (lines 415-417: `self.tile_format`, `self.cog_blocksize`, `self.cog_overview_resampling`). The reader no longer emits COG on split — split tiles are plain GTiff (COG is a writer concern now). Update `_plan_partitions_for_file(...)` call in `partitions()` (raster.py:444-455) to drop `tile_format`/`cog_blocksize`/`cog_overview_resampling` args, and update `_plan_partitions_for_file` + `encode_tile` calls in `read()` to emit GTiff tiles (tile_format="gtiff") for the opt-in split path. (Grep `tile_format=` in raster.py and set to "gtiff" or remove the kwarg so encode_tile defaults to gtiff.)

- [ ] **Step 4: Run + fix regressions**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/`
Expected: PASS. The 0.4.4 tests that asserted `auto`-split-default or COG-on-split (`test_auto_strategy_splits_large_raster_by_default`, `test_auto_tileformat_cog_when_split`) now encode the OLD behavior — update them: default is no-split; opt-in `splitStrategy=serverless` splits into GTiff tiles (not COG). Preserve the split *geometry* contracts; only the default + tile format change. Also `test_core_budget.py` is unaffected (budget values unchanged) but confirm.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/
git commit -m "feat(ds): reverse split default auto->none; COG moves to cog lane

Splitting returns to opt-in (halo mode: prepare a master COG, windowed-read
it). Reader no longer emits COG on split; COG creation is a cog_gbx writer
concern. Undoes the 0.4.4 forced-split default that OOM'd on Serverless.

Co-authored-by: Isaac"
```

---

## Task 5: Strip COG options from the gtiff writer + register both lanes

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/gtiff.py:30-47`
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/register.py:15-39`
- Test: `python/geobrix/test/ds/test_raster_datasource.py` (extend), `python/geobrix/test/ds/test_registration.py` if present

**Interfaces:**
- Consumes: `FileGbxDataSource` (Task 1), `CogGbxDataSource` (Task 3).
- Produces: gtiff writer with no COG options; `file_gbx`/`cog_gbx` registered.

- [ ] **Step 1: Write the failing test**

```python
# add to test_raster_datasource.py
def test_gtiff_writer_has_no_cog_option(tmp_path, spark_or_none=None):
    # The gtiff lane writes plain GTiff; cog options belong to cog_gbx.
    from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
    src = GTiffGbxDataSource()
    src.options = {"path": str(tmp_path)}
    from databricks.labs.gbx.ds.raster import reader_schema
    writer = src.writer(reader_schema(), overwrite=True)
    # RasterGbxWriter no longer configured for COG on the gtiff lane
    assert getattr(writer, "cog", False) is False

# add to a registration test
def test_file_and_cog_registered():
    from databricks.labs.gbx.ds.register import _SOURCES
    names = {s.name() for s in _SOURCES}
    assert "file_gbx" in names and "cog_gbx" in names
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_datasource.py -k "no_cog_option or registered"`
Expected: FAIL — cog options still wired in gtiff writer; sources not registered.

- [ ] **Step 3: Implement**

In `gtiff.py` `writer()`, remove `cog=...`, `cog_blocksize=...`, `cog_overview_resampling=...`, `cog_compression=...` args to `RasterGbxWriter(...)` (leave `force_driver="GTiff"`, `name_col`, `ext`). The `RasterGbxWriter.__init__` COG params keep their defaults (cog=False) so the gtiff lane never COGs. In `register.py`: add imports `from databricks.labs.gbx.ds.file import FileGbxDataSource` and `from databricks.labs.gbx.ds.cog import CogGbxDataSource`, and add both to `_SOURCES`.

- [ ] **Step 4: Run to verify pass + full ds suite + bindings**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/`
Run: `bash scripts/commands/gbx-test-bindings.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/gtiff.py python/geobrix/src/databricks/labs/gbx/ds/register.py python/geobrix/test/
git commit -m "feat(ds): gtiff writer plain-GTiff only; register file_gbx + cog_gbx

Co-authored-by: Isaac"
```

---

## Task 6: Docs — two lanes, halo mode, breaking-change note

**Files:**
- Modify: `docs/docs/beta-release-notes.mdx`, `docs/sidebars.js`
- Create/modify: `docs/docs/readers/file.mdx`, `docs/docs/readers/cog.mdx`, `docs/docs/writers/cog.mdx` (and adjust `readers/raster.mdx`/`gtiff` for the default reversal + moved options)
- Doc-test: `docs/tests/python/` file→cog→bbox round-trip

- [ ] **Step 1: Release notes** — document the breaking change: `splitStrategy` default `auto`→`none`; reader `tileFormat`/`cogBlockSize`/`cogOverviewResampling` removed (COG now via `cog_gbx` writer); new `file_gbx` + `cog_gbx` lanes; halo-mode narrative (prepare master COG → windowed-read; splitting opt-in). No wave-numbers/internal vocab.

- [ ] **Step 2: Lane pages** — `file_gbx` (path lister, references-no-content, schema incl. nullable `extension`); `cog_gbx` reader (COG-aware, bbox clip); `cog_gbx` writer (`df.write.format("cog_gbx").option("cogBlockSize",...).save(...)`, master-COG prep). Wire all new pages into `docs/sidebars.js`.

- [ ] **Step 3: Doc-test** — add a Docker doc-test: `file_gbx`→`cog_gbx` on real sample GeoTIFFs → prepared COGs (assert `cog_validate`/`sniff_header.is_cog`); then `cog_gbx.load(...).option("bbox",...)` clips. Follow the raw-loader import pattern.

- [ ] **Step 4: Run doc-tests (Docker, dispatch as subagent — long-running)**

Run: `bash scripts/commands/gbx-test-python-docs.sh --log cog-lane-docs.log`
Verify: `grep -rn -iE "wave [0-9]+" docs/docs/` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "docs: two-lane raster API, cog_gbx file preparer, halo-mode + breaking note

Co-authored-by: Isaac"
```

---

## Task 7: Serverless validation (GATING — local green is not sufficient)

**Files:**
- Create: `prompts/testing/2026-07-30-cog-lane-serverless-validation.ipynb` (gitignored scratch), plus persisted markers.

**This task gates "done". Do NOT run until Tasks 1-6 are local-green and Docker doc-tests pass.** Follow the exact mechanics that worked in the 0.4.4 investigation (spec §5).

- [ ] **Step 1: Rebuild + stage wheel AFTER the last commit; hash-verify**

Run: `bash scripts/commands/gbx-data-push-wheel.sh` (dispatch/await). Then hash-verify the staged Volume wheel matches the local build AND contains the new symbols:
```python
# local sha256 == staged sha256; staged wheel zip contains ds/file.py, ds/cog.py, ds/cog_writer.py
```
A stale same-path wheel silently invalidates the run — this bit us repeatedly in 0.4.4.

- [ ] **Step 2: Author a native `.ipynb` (no jupytext/`# MAGIC`)**

Cells: two-step `%pip` install (`--force-reinstall --no-deps geobrix` then `geobrix[light]`); register via `from databricks.labs.gbx.ds import register as ds_register; ds_register.register(spark)`; generate a real multi-GiB striped source on the Volume FUSE-safe (build local temp, row-band stream, sequential copy); then:
  1. `file_gbx.load(dir)` → assert reference rows (path/extension), no content.
  2. `.write.format("cog_gbx").option("cogBlockSize","512").save(out)` on a multi-GiB source → assert output COG exists + `cog_validate` (persist a marker BEFORE and AFTER to the Volume).
  3. Many-files case: a dir of moderate rasters → all prepared, no per-worker accumulation OOM.
  4. `cog_gbx.load(out).option("bbox","...")` → clips a prepared master COG.
Persist each stage's outcome as a uniquely-named JSON marker on the Volume (NOT stdout). No `.rdd`.

- [ ] **Step 3: Run on Serverless via jobs.submit harness**

Run: `bash scripts/commands/gbx-test-notebooks-serverless.sh --notebook 'prompts/testing/2026-07-30-cog-lane-serverless-validation.ipynb' --ws-dir '<existing ws dir>' --wheel /Volumes/.../geobrix-<ver>-py3-none-any.whl --extras light --log cog-lane-serverless.log`

- [ ] **Step 4: Judge by persisted markers + child-run error, NOT harness exit code**

Read the Volume markers. PASS criteria: file_gbx lists without content; cog_gbx prepares a valid master COG for a multi-GiB source without OOM; many-files completes; bbox clip works. Record the per-file size that succeeds vs strains (the documented single-file ceiling). If OOM, read the specific child-run error + markers to locate the stage; do NOT guess.

- [ ] **Step 5: Record result**

Append the validated outcome + single-file ceiling to `prompts/testing/2026-07-30-serverless-oom-rootcause.md` (or a new cog-lane results doc); link the summary when reporting. No commit needed (scratch), unless updating a committed doc caveat.

---

## Self-Review

**1. Spec coverage:**
- §1 two-lane architecture → Task 4 (reader default/options), Task 5 (gtiff writer strip + register), Task 3 (cog reader), Task 2 (cog writer). ✓
- §2 file_gbx reader (schema incl. nullable extension, no content) → Task 1. ✓
- §3 cog_gbx writer (path-input, per-file master COG, FUSE-safe, cog options) → Task 2. ✓
- §4 components (file.py, cog.py, cog_writer.py, raster/gtiff/register changes) → Tasks 1-5. ✓
- §5 testing: local unit (Tasks 1-5), Docker doc-test (Task 6), **gating Serverless validation** (Task 7). ✓
- §5 docs (release notes breaking, lane pages, sidebar, halo narrative) → Task 6. ✓
- Roadmap: virtual tiling deferred → not a task (correct). ✓

**2. Placeholder scan:** No TBD/TODO. Task 7 references "<existing ws dir>" and "<ver>" — these are runtime values the executor fills from the staged wheel path / workspace (like the 0.4.4 runs), not plan placeholders; acceptable. Doc pages (Task 6) describe exact content to write.

**3. Type consistency:** `FILE_SCHEMA` fields (path,name,extension,size,modificationTime) consistent Task 1 ↔ Task 2 (writer reads `row["path"]`/`row[name_col]`). `CogGbxWriter` signature (Task 2) matches the `cog.py` factory call (Task 3). `assert_path_schema` used in Task 2 tests + writer. `r.strategy` default `none` asserted in Task 3 + Task 4 (Task 3's cog reader inherits the reversed default from Task 4 — noted in Task 3 Step 2 that it depends on Task 4; sequencing: Task 4 before Task 3's final green, or run Task 3 last). `analysis.cog_convert` signature matches existing. ✓

**Sequencing note:** Task 3's `test_cog_reader_defaults_no_split` depends on Task 4's reversal. Execute Task 4 before finalizing Task 3, OR accept Task 3 Step 2 failing on that one assertion until Task 4 lands. Recommend order: 1 → 2 → 4 → 3 → 5 → 6 → 7.
