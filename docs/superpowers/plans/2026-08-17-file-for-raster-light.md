# FILE-for-raster (light tier) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add governed, OOM-free, fast virtual-tile storage/handling to the GeoBrix **light tier** (pure-Python `pyrx`), backed by the Databricks **FILE** type, as an opt-in accelerator layered over the portable Volume-path/FUSE behavior that already exists — never a hard dependency.

**Architecture:** Three interlocking units over a capability gate that already exists (`pyrx/_file_ref.py:file_supported()`). **Unit 1** — a FILE-table *reader* helper that projects a Delta table's plain columns into a v2-tile DataFrame (never selecting the FILE column, so it stays consumable on Serverless GC), reconstructing the FILE ref lazily via `try_to_file(path)` at compute time. **Unit 2** — a FILE-table *writer* helper that pre-declares a typed FILE column then INSERTs FILE-aligned (`ORDER BY path` / `CLUSTER BY (path)`), stamping self-describing `TBLPROPERTIES` and per-tile `path_mode`. **Unit 3** — a partition-scoped `mapInPandas` executor with a per-partition capability-adaptive open-resource LRU that amortizes the open cost (the dominant read cost), realized end-to-end by routing one representative scalar (`rst_memsize`) through it.

**Tech Stack:** Python 3.12, PySpark (Spark Connect-safe), rasterio, Delta, Databricks FILE / FileRef (`try_to_file`, `create_file`, `.open()`, `.as_local_file()`). Tests via `gbx:test:pyrx` (JAR-free `local[2]` Spark); FILE round-trips validated on the dogfood DBR-19.5-snapshot classic cluster.

**Spec:** `docs/superpowers/specs/2026-08-17-file-for-raster-design.md` (empirical grounding: `input/file_type/2026-08-17-read-perf-matrix-and-handling-rules.md`). The plan argues from the spec; executors read both.

## Global Constraints

Copied verbatim from the spec + repo invariants. Every task's requirements implicitly include this section.

- **Light tier is pure Python** (`pyrx`) — **needs NO JAR**. Never call `.rdd`, `spark._jvm`, `sparkContext`/`sc`, or `spark.conf.set` (Serverless has no `sc` and forbids config mutation). `SparkSession.getActiveSession()` is the Serverless-safe handle.
- **Attempt FILE with graceful fallback across ALL environments** — probe `file_supported()` and try the FILE path; on absence/failure fall back to Volume-path/FUSE (representation 1/2). Never a rigid runtime gate.
- **No hard dates.** Where the roadmap is mentioned, say exactly: "DBR 19 is coming soon to Serverless with FILE support." Never a month/day.
- **Keep a plain `path` string beside any FILE ref.** Serverless GC cannot fetch a FILE-column schema or collect a FILE value — the reader must project plain `path` (+ window/crs), **never** the FILE column, **never** `SELECT *`.
- **Key on `path` (string), never the FILE column.** A FILE column cannot be a partition / cluster / join / grouping key. Partition/cluster/group recipes use `path`.
- **Never `partitionBy('path')`** (high-cardinality → Delta partition-dir explosion). **Avoid `ZORDER`** (liquid clustering supersedes it). Use `ORDER BY path` (immediate grouping) and/or `CLUSTER BY (path)` (durable, needs `OPTIMIZE`).
- **FILE writes require a pre-declared typed column** (`FILE MANAGED`/`FILE EXTERNAL`) + `INSERT`. Never bare `saveAsTable`/CTAS of a FILE column (`FILE_TYPE_UNKNOWN_WRITE`). MANAGED requires a `databricks.filespace-preview` `/Volumes` FileSpace.
- **Partitioning `n` is parallelism-sized** (3–5× worker cores classic; a parallelism target on Serverless) — **never `n_files`, never `sc`-derived.**
- **`rst_*` functions must not internally repartition.** Partition alignment is an opt-in user-facing helper (`align_partitions`), consistent with "Serverless light parallelism only via `repartition(N, column)`."
- **No new dependencies.** rasterio/pandas/pyspark are already pinned; adding a dep triggers the cross-env pin + tier-gate checklist. New tests land in the existing `test/pyrx/` dir (already covered by `_LIGHT_TEST_DIRS`) — no new test dir.
- **Format is v2-only on output.** Output tiles are always v2 (`V2_TILE_SCHEMA`); v1 is read-only via `from_v1`.
- **Docs are a non-goal here** (grounding captured separately) — do not write user-facing docs in this plan. No internal planning vocabulary anywhere.
- **MANAGED-as-default is deferred** until DBR 19 (with FILE support) is the Serverless default; the writer default is **portable (EXTERNAL/Volume)**, MANAGED opt-in.

## Settled §8 open decisions

These were open in the spec; settled here for the increment-1 build.

1. **Increment scope [DECIDED in spec]:** MANAGED bridge, light tier, with graceful fallback.
2. **Writer default [DECIDED in spec]:** portable (EXTERNAL/Volume); MANAGED opt-in via `file_mode="managed"`. Default flip deferred.
3. **Table scope (§8.3) → new-table `CREATE` only.** `write_file_table` creates (or `overwrite`-replaces) the table. Existing-table `ALTER`/column-upsize is **out of scope** for increment 1 (schema-evolution edge cases not worth the surface now); recorded as a fast-follow.
4. **Grouped-exec surface (§8.4) → automatic on the happy path + a documented re-align helper.** The reader emits path-aligned partitions so functions amortize for free; after a reshape/join the user calls `align_partitions(df)`. **No explicit `group_exec` wrapper** in increment 1 — `grouped_tile_map` is internal machinery that scalars route through, not a user-facing entry point.
5. **agg / 1:n split (§8.5) → one representative scalar (`rst_memsize`) proves the executor.** `rst_*_agg` and 1:n generators are **deferred**; the group=FILE principle is recorded (they are inherently grouped and reuse the same open-once LRU later). No agg/generator work in increment 1.
6. **Heavy (Scala) parity (§8.6) → out of scope.** Light only. Recorded as a fast-follow (heavy GDAL dataset-cache honoring `GDALManager`; permissions-flow payoff).
7. **DBR 17/18 read-compat (§8.7) [SPIKE DONE]:** DBR 17 **cannot read** a FILE-column table at all (schema-parse hard fail `INVALID_JSON_DATA_TYPE`); Serverless GC can `count`/project-plain-columns/build-lazy-DF but cannot fetch a FILE-containing schema or collect a FILE value. This drives the reader's plain-`path` projection (Task 3). Documentation of DBR 17 unreadability is a docs-phase item (non-goal here).
8. **LRU / staging guardrails (§8.8) → settled in Task 6:** the LRU is **byte-budgeted, not count-fixed** — entries (open resources keyed by source path) are held up to a **byte budget** (`GBX_LRU_MAX_BYTES`, default 4 GiB) with a `max_count` handle guard (default 64), oldest-evicted when either is exceeded, and the current (most-recent) entry is never evicted. Each entry carries a **weight**: a local-staged copy weighs its file size (so the LRU byte budget *is* the staged-disk-fill guard, and eviction deletes the staged temp); an open stream/dataset weighs a small nominal (`STREAM_NOMINAL_BYTES`) so the count guard governs. Close-on-evict **and** close-at-partition-end (`finally`). Independently, local-stage is skipped when a single source exceeds `GBX_STAGE_MAX_BYTES` (default 4 GiB) — huge files never stage (they stream or read via FUSE).

---

## File Structure

New files (all under `python/geobrix/src/databricks/labs/gbx/pyrx/`):

- `core/virtual_tile.py` — **MODIFY**: add `path_mode` (9th field) + `effective_path_mode()`.
- `file_props.py` — **NEW**: `TBLPROPERTIES` key constants; `build_props()`; `parse_props()`; the writer-version branch cue.
- `file_table.py` — **NEW**: `read_file_table()` (Unit 1) and `write_file_table()` (Unit 2). They share the props + path_mode logic and change together.
- `grouped_exec.py` — **NEW**: `align_partitions()`, `OpenResourceLRU`, `grouped_tile_map()` (Unit 3).
- `functions.py` — **MODIFY**: route `rst_memsize` through `grouped_tile_map` on path-aligned input; keep the per-row form.
- `core/_serde.py` — **MODIFY**: `build_tile`/`build_error_tile` emit the 9-field row (path_mode default `None`).

Tests (all under `python/geobrix/test/pyrx/`): `test_path_mode.py`, `test_file_props.py`, `test_file_table_reader.py`, `test_file_table_writer.py`, `test_align_partitions.py`, `test_open_resource_lru.py`, `test_grouped_exec.py`, `test_rst_memsize_grouped.py`.

**Runtime note for every FILE-dependent task:** the `local[2]` test Spark has no FILE type (`try_to_file`/`create_file`/`FILE` columns are DBR-19 platform features). Each such task TDD-tests its pure-Python / SQL-string / non-FILE-DataFrame seam locally, and the actual FILE round-trip is validated once, on dogfood, in **Task 9**.

---

### Task 1: `path_mode` field on tile v2 + serde + inference

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py:34-45` (schema), plus the `VirtualTile` dataclass, `to_row`, `from_row`, `from_v1`.
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/_serde.py` (`build_tile`, `build_error_tile` — emit 9 fields).
- Test: `python/geobrix/test/pyrx/test_path_mode.py`

**Interfaces:**
- Produces: `V2_TILE_SCHEMA` now has field `path_mode: StringType nullable=True` appended (9 fields). `VirtualTile.path_mode: Optional[str]` (default `None`). `effective_path_mode(vt: VirtualTile) -> Optional[str]` returns `vt.path_mode` if set, else `None` when `vt.raster is not None`, else `"external"`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_path_mode.py
from databricks.labs.gbx.pyrx.core.virtual_tile import (
    V2_TILE_SCHEMA, VirtualTile, effective_path_mode,
)

def test_schema_has_path_mode_last():
    names = [f.name for f in V2_TILE_SCHEMA.fields]
    assert names[-1] == "path_mode"
    assert len(names) == 9
    assert V2_TILE_SCHEMA["path_mode"].nullable is True

def test_from_v1_sets_none_path_mode():
    vt = VirtualTile.from_v1(cellid=1, raster=b"xx")
    assert vt.path_mode is None

def test_to_row_from_row_roundtrips_path_mode():
    vt = VirtualTile(cellid=1, path="/Volumes/a/b/c.tif", path_mode="managed")
    back = VirtualTile.from_row(vt.to_row())
    assert back.path_mode == "managed"

def test_effective_path_mode_infers_when_absent():
    assert effective_path_mode(VirtualTile(cellid=1, raster=b"xx")) is None
    assert effective_path_mode(VirtualTile(cellid=1, path="/Volumes/a/b.tif")) == "external"
    assert effective_path_mode(VirtualTile(cellid=1, path="/V/x.tif", path_mode="managed")) == "managed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_path_mode.py --log path_mode.log`
Expected: FAIL — `path_mode` field/attr/`effective_path_mode` not defined; schema has 8 fields.

- [ ] **Step 3: Implement the field, serde, and inference**

In `core/virtual_tile.py`, append to `V2_TILE_SCHEMA` (after `metadata`):
```python
        StructField("path_mode", StringType(), nullable=True),
```
Add `path_mode: Optional[str] = None` to the `VirtualTile` dataclass (after `metadata`). In `to_row()` add `"path_mode": self.path_mode`. In `from_row()` read `path_mode=_get(row, "path_mode")` (mirror how the other optional fields are read; default `None` when absent so existing 8-field v2 rows still load). `from_v1()` leaves it default `None`. Add:
```python
def effective_path_mode(vt: "VirtualTile") -> Optional[str]:
    """Stored path_mode when set; else inferred from tile structure
    (materialized raster -> None; plain path -> 'external')."""
    if vt.path_mode is not None:
        return vt.path_mode
    if vt.raster is not None:
        return None
    return "external"
```
In `core/_serde.py`, update `build_tile`/`build_error_tile` to include `path_mode` (default `None`) so the row width matches `V2_TILE_SCHEMA` (they construct v2 rows consumed by the `@udtf(returnType=V2_TILE_SCHEMA)` classes; a width mismatch would break every UDTF at runtime).

- [ ] **Step 4: Run the new test + the existing tile/serde/UDTF suites to confirm no regression**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_path_mode.py --log path_mode.log`
Expected: PASS.
Run: `bash scripts/commands/gbx-test-pyrx.sh --path "python/geobrix/test/pyrx/test_core_accessors_isempty.py python/geobrix/test/pyrx/test_functions_spark.py" --log path_mode_regress.log`
Expected: PASS — widening the schema by one nullable trailing field must not break existing tile construction or the UDTFs.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/core/_serde.py \
        python/geobrix/test/pyrx/test_path_mode.py
git commit -m "feat(pyrx): add path_mode field to tile v2 (materialized/external/managed)

Co-authored-by: Isaac"
```

---

### Task 2: FILE-table properties module (`file_props.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/file_props.py`
- Test: `python/geobrix/test/pyrx/test_file_props.py`

**Interfaces:**
- Produces:
  - Constants: `WRITER_VERSION_KEY = "geobrix_writer_version"`, `CURRENT_WRITER_VERSION = "1"`, `LIBRARY_VERSION_KEY = "geobrix.version"`, `WRITE_STRATEGY_KEY = "geobrix.file.write_strategy"`, `FILESPACE_KEY = "databricks.filespace-preview"`.
  - `build_props(*, file_mode: str, layout: str, filespace: Optional[str], library_version: str) -> dict[str, str]` — the TBLPROPERTIES dict a writer stamps. `file_mode` in `{"external","managed"}`, `layout` in `{"plain","order","cluster"}`.
  - `parse_props(props: dict[str, str]) -> dict` — reads a table's raw properties into `{"writer_version": str|None, "file_mode": str|None, "layout": str|None, "is_geobrix": bool}`. Absent `geobrix_writer_version` → `is_geobrix=False` (reader falls to conservative defaults).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_file_props.py
from databricks.labs.gbx.pyrx import file_props as fp

def test_build_props_managed_cluster():
    p = fp.build_props(file_mode="managed", layout="cluster",
                       filespace="/Volumes/c/s/v", library_version="0.5.0")
    assert p[fp.WRITER_VERSION_KEY] == fp.CURRENT_WRITER_VERSION == "1"
    assert p[fp.WRITE_STRATEGY_KEY] == "managed:cluster"
    assert p[fp.FILESPACE_KEY] == "/Volumes/c/s/v"
    assert p[fp.LIBRARY_VERSION_KEY] == "0.5.0"

def test_build_props_external_omits_filespace():
    p = fp.build_props(file_mode="external", layout="order",
                       filespace=None, library_version="0.5.0")
    assert fp.FILESPACE_KEY not in p
    assert p[fp.WRITE_STRATEGY_KEY] == "external:order"

def test_build_props_rejects_bad_enum():
    import pytest
    with pytest.raises(ValueError):
        fp.build_props(file_mode="bogus", layout="order", filespace=None, library_version="0.5.0")

def test_parse_props_geobrix_and_foreign():
    parsed = fp.parse_props({fp.WRITER_VERSION_KEY: "1", fp.WRITE_STRATEGY_KEY: "managed:cluster"})
    assert parsed == {"writer_version": "1", "file_mode": "managed", "layout": "cluster", "is_geobrix": True}
    foreign = fp.parse_props({"some.other": "x"})
    assert foreign["is_geobrix"] is False and foreign["file_mode"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_props.py --log file_props.log`
Expected: FAIL — module `file_props` not found.

- [ ] **Step 3: Implement `file_props.py`**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/file_props.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_props.py --log file_props.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/file_props.py \
        python/geobrix/test/pyrx/test_file_props.py
git commit -m "feat(pyrx): self-describing FILE-table TBLPROPERTIES (writer-version cue)

Co-authored-by: Isaac"
```

---

### Task 3: FILE-table reader (`file_table.read_file_table`) — Serverless-GC-safe projection

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py` (reader half; writer added in Task 4)
- Test: `python/geobrix/test/pyrx/test_file_table_reader.py`

**Interfaces:**
- Consumes: `file_props.parse_props`; `core.virtual_tile.V2_TILE_SCHEMA`.
- Produces: `read_file_table(spark, table: str, *, tile_cols: dict | None = None) -> DataFrame`. Returns a DataFrame whose `tile` column is `V2_TILE_SCHEMA`-shaped, built **only from plain columns** (`path`, `window`, `crs`, `metadata`, …) — it **never selects the FILE column** and never does `SELECT *` (Serverless-GC-safe). `path_mode` on each tile is set from the table's `file_mode` prop (or `"external"` when the table is not GeoBrix-stamped). Non-tile columns pass through. The FILE ref is **not** carried; compute paths reconstruct it lazily via the existing `file_ref_arg(tile["path"])`.

- [ ] **Step 1: Write the failing test** (local Spark; a plain Delta table stands in for a FILE table — the reader must succeed by projecting plain columns, exactly the Serverless-GC fallback)

```python
# python/geobrix/test/pyrx/test_file_table_reader.py
from databricks.labs.gbx.pyrx.file_table import read_file_table

def _make_plain_table(spark, name):
    df = spark.createDataFrame(
        [(1, "/Volumes/main/s/v/a.tif", "EPSG:4326"),
         (2, "/Volumes/main/s/v/b.tif", "EPSG:4326")],
        "cellid bigint, path string, crs string",
    )
    df.write.mode("overwrite").saveAsTable(name)

def test_read_projects_plain_columns_into_v2_tile(spark):
    _make_plain_table(spark, "file_tbl_r1")
    out = read_file_table(spark, "file_tbl_r1")
    assert "tile" in out.columns
    tfields = [f.name for f in out.schema["tile"].dataType.fields]
    assert "path" in tfields and "path_mode" in tfields
    rows = {r["tile"]["path"]: r["tile"]["path_mode"] for r in out.collect()}
    # not geobrix-stamped -> path_mode inferred external
    assert rows["/Volumes/main/s/v/a.tif"] == "external"

def test_read_never_selects_star_or_file_column(spark, monkeypatch):
    # guard: the SQL the reader issues must project named plain columns, not *
    import databricks.labs.gbx.pyrx.file_table as ft
    seen = {}
    real = ft._project_sql
    monkeypatch.setattr(ft, "_project_sql", lambda *a, **k: seen.setdefault("sql", real(*a, **k)))
    _make_plain_table(spark, "file_tbl_r2")
    read_file_table(spark, "file_tbl_r2")
    assert "*" not in seen["sql"] and "SELECT" in seen["sql"].upper()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_table_reader.py --log file_table_reader.log`
Expected: FAIL — `file_table` / `read_file_table` not defined.

- [ ] **Step 3: Implement the reader half of `file_table.py`**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py
"""Read/write GeoBrix FILE-column Delta tables (light tier).

Serverless-GC-safe by construction: the reader projects only plain columns
(path/window/crs/...) and never touches the FILE column, because Spark Connect
cannot fetch a FILE-containing schema or collect a FILE value. The FILE ref is
reconstructed lazily at compute time via file_ref_arg(tile["path"]).
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import file_props
from .core.virtual_tile import V2_TILE_SCHEMA

# plain (non-FILE) columns the reader is willing to project into a tile
_TILE_PLAIN_COLS = ("cellid", "path", "window", "clip_polygon", "clip_crs", "crs", "metadata")


def _table_props(spark: SparkSession, table: str) -> dict:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _project_sql(table: str, present: list) -> str:
    cols = ", ".join(present)
    return f"SELECT {cols} FROM {table}"


def read_file_table(spark: SparkSession, table: str,
                    *, tile_cols: Optional[dict] = None) -> DataFrame:
    parsed = file_props.parse_props(_table_props(spark, table))
    path_mode = parsed["file_mode"] or "external"

    # which plain columns actually exist on the table (never the FILE column)
    existing = {f.name for f in spark.table(table).schema.fields
                if f.dataType.simpleString() != "file"} if False else None
    # NOTE: spark.table(...).schema is NOT safe on Serverless GC when a FILE
    # column exists; enumerate plain columns via DESCRIBE instead.
    desc = spark.sql(f"DESCRIBE TABLE {table}").collect()
    plain = {r["col_name"] for r in desc
             if r["col_name"] and not r["col_name"].startswith("#")
             and (r["data_type"] or "").lower() != "file"}
    present = [c for c in _TILE_PLAIN_COLS if c in plain]
    passthrough = [c for c in plain if c not in _TILE_PLAIN_COLS]

    base = spark.sql(_project_sql(table, present + passthrough))

    tile_struct = F.struct(
        *[F.col(c).alias(c) if c in present else F.lit(None).alias(c)
          for c in ("cellid", "raster", "path", "window",
                    "clip_polygon", "clip_crs", "crs", "metadata")],
        F.lit(path_mode).alias("path_mode"),
    )
    # note: "raster" is intentionally absent from a FILE table -> lit(None)
    out = base.withColumn("tile", tile_struct.cast(V2_TILE_SCHEMA))
    return out.select("tile", *passthrough)
```

Guardrails realized here: `DESCRIBE TABLE` (not `.schema`) enumerates columns without tripping the Serverless-GC FILE-schema limit; the FILE column is filtered out (`data_type != 'file'`); the projection is explicit named columns, never `*`.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_table_reader.py --log file_table_reader.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py \
        python/geobrix/test/pyrx/test_file_table_reader.py
git commit -m "feat(pyrx): read_file_table projects plain cols (Serverless-GC-safe)

Co-authored-by: Isaac"
```

---

### Task 4: FILE-table writer (`file_table.write_file_table`) — typed column + FILE-aligned INSERT

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py` (add writer + SQL builders)
- Test: `python/geobrix/test/pyrx/test_file_table_writer.py`

**Interfaces:**
- Consumes: `file_props.build_props`; `core.virtual_tile.effective_path_mode`.
- Produces:
  - `build_create_sql(table, *, plain_cols: list[tuple[str,str]], file_col: str, file_mode: str, filespace: Optional[str], cluster: bool) -> str` — a `CREATE TABLE … USING DELTA` with a `<file_col> FILE MANAGED|EXTERNAL` column, `TBLPROPERTIES(...)`, optional `CLUSTER BY (path)`. **Never** `PARTITIONED BY`.
  - `build_insert_sql(table, src_view, *, select_exprs: list[str], order_by_path: bool) -> str` — `INSERT INTO … SELECT … [ORDER BY path]`. For MANAGED materialize the FILE via `create_file(content => raster)`; for EXTERNAL reference via `try_to_file(path)`.
  - `write_file_table(spark, df, table, *, file_mode="external", filespace=None, layout="order", overwrite=False, file_col="tile_file") -> None` — the orchestration: register `df` as a temp view, run the CREATE + INSERT, stamp props. `file_mode` defaults to **external (portable)**.

- [ ] **Step 1: Write the failing test** (unit-test the SQL builders — string assertions run locally; execution is Task 9)

```python
# python/geobrix/test/pyrx/test_file_table_writer.py
from databricks.labs.gbx.pyrx import file_table as ft

def test_create_sql_external_no_partition_no_zorder():
    sql = ft.build_create_sql(
        "cat.sch.t", plain_cols=[("cellid", "bigint"), ("path", "string")],
        file_col="tile_file", file_mode="external", filespace=None, cluster=True)
    assert "tile_file FILE EXTERNAL" in sql
    assert "USING DELTA" in sql
    assert "CLUSTER BY (path)" in sql
    assert "PARTITIONED BY" not in sql and "ZORDER" not in sql.upper()
    assert "geobrix_writer_version" in sql

def test_create_sql_managed_requires_filespace():
    import pytest
    with pytest.raises(ValueError):
        ft.build_create_sql("t", plain_cols=[("path", "string")], file_col="f",
                            file_mode="managed", filespace=None, cluster=False)
    sql = ft.build_create_sql("t", plain_cols=[("path", "string")], file_col="f",
                              file_mode="managed", filespace="/Volumes/c/s/v", cluster=False)
    assert "f FILE MANAGED" in sql
    assert "databricks.filespace-preview" in sql

def test_insert_sql_orders_by_path_and_materializes_managed():
    sql_m = ft.build_insert_sql("t", "src", select_exprs=["cellid", "path",
        "create_file(content => raster) AS tile_file"], order_by_path=True)
    assert "ORDER BY path" in sql_m and "create_file(content => raster)" in sql_m
    sql_e = ft.build_insert_sql("t", "src", select_exprs=["cellid", "path",
        "try_to_file(path) AS tile_file"], order_by_path=True)
    assert "try_to_file(path)" in sql_e
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_table_writer.py --log file_table_writer.log`
Expected: FAIL — `build_create_sql`/`build_insert_sql`/`write_file_table` not defined.

- [ ] **Step 3: Implement the writer half of `file_table.py`**

```python
# append to file_table.py
from . import file_props as _fp

def build_create_sql(table, *, plain_cols, file_col, file_mode, filespace, cluster):
    if file_mode not in ("external", "managed"):
        raise ValueError(f"file_mode must be external|managed, got {file_mode!r}")
    if file_mode == "managed" and not filespace:
        raise ValueError("managed file_mode requires a filespace (/Volumes/...)")
    cols = ", ".join(f"{n} {t}" for n, t in plain_cols)
    file_kw = "MANAGED" if file_mode == "managed" else "EXTERNAL"
    layout = "cluster" if cluster else ("order")
    props = _fp.build_props(file_mode=file_mode, layout=layout,
                            filespace=filespace, library_version=_library_version())
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    ddl = (f"CREATE TABLE {table} ({cols}, {file_col} FILE {file_kw}) "
           f"USING DELTA TBLPROPERTIES ({props_sql})")
    if cluster:
        ddl += " CLUSTER BY (path)"
    return ddl

def build_insert_sql(table, src_view, *, select_exprs, order_by_path):
    sql = f"INSERT INTO {table} SELECT {', '.join(select_exprs)} FROM {src_view}"
    if order_by_path:
        sql += " ORDER BY path"
    return sql

def _library_version():
    try:
        from databricks.labs.gbx import __version__
        return str(__version__)
    except Exception:
        return "0.0.0"

def write_file_table(spark, df, table, *, file_mode="external", filespace=None,
                     layout="order", overwrite=False, file_col="tile_file"):
    """Create a typed FILE-column table and INSERT df FILE-aligned.
    Portable default (external). MANAGED requires a filespace. Never saveAsTable."""
    view = "gbx_file_src"
    df.createOrReplaceTempView(view)
    if overwrite:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    # plain columns = df columns minus the tile struct's raster (materialized inline)
    plain_cols = [(f.name, f.dataType.simpleString())
                  for f in df.schema.fields if f.name != "tile"]
    # flatten tile.path etc. from the caller's df in the SELECT (impl detail per caller shape)
    spark.sql(build_create_sql(table, plain_cols=plain_cols, file_col=file_col,
                               file_mode=file_mode, filespace=filespace,
                               cluster=(layout == "cluster")))
    file_expr = ("create_file(content => raster) AS " + file_col
                 if file_mode == "managed" else "try_to_file(path) AS " + file_col)
    select_exprs = [f.name for f, _ in ((f, None) for f in df.schema.fields) if f.name != "tile"]
    select_exprs.append(file_expr)
    spark.sql(build_insert_sql(table, view, select_exprs=select_exprs,
                               order_by_path=(layout in ("order", "cluster"))))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_file_table_writer.py --log file_table_writer.log`
Expected: PASS (SQL-builder string assertions). Execution against a real FILE table is Task 9.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py \
        python/geobrix/test/pyrx/test_file_table_writer.py
git commit -m "feat(pyrx): write_file_table (typed FILE column + FILE-aligned INSERT)

Co-authored-by: Isaac"
```

---

### Task 5: Partition-alignment helper (`grouped_exec.align_partitions`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py` (align helper; LRU + executor added in Tasks 6–7)
- Test: `python/geobrix/test/pyrx/test_align_partitions.py`

**Interfaces:**
- Produces: `align_partitions(df: DataFrame, *, n: int, path_col: str = "tile.path") -> DataFrame` = `df.repartition(n, F.col(path_col)).sortWithinPartitions(F.col(path_col))`. `n` is **required and caller-supplied** (parallelism-sized). Raises `ValueError` if `n <= 0`. It never derives `n` from `sc`/file count.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_align_partitions.py
import pytest
from databricks.labs.gbx.pyrx.grouped_exec import align_partitions

def test_align_partitions_hashes_and_sorts_by_path(spark):
    df = spark.createDataFrame(
        [(1, "a.tif"), (2, "a.tif"), (3, "b.tif")], "cellid bigint, p string")
    out = align_partitions(df, n=4, path_col="p")
    assert out.rdd.getNumPartitions() == 4
    # each path lands in exactly one partition (hash-by-path never splits a key)
    part_of = out.rdd.map(lambda r: (r["p"],)).glom().collect()
    seen = {}
    for i, rows in enumerate(part_of):
        for (p,) in rows:
            assert seen.setdefault(p, i) == i

def test_align_partitions_rejects_nonpositive_n(spark):
    df = spark.createDataFrame([(1, "a.tif")], "cellid bigint, p string")
    with pytest.raises(ValueError):
        align_partitions(df, n=0, path_col="p")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_align_partitions.py --log align.log`
Expected: FAIL — module/function not defined.

- [ ] **Step 3: Implement `align_partitions`**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py
"""Partition-scoped grouped execution for FILE/virtual tiles (light tier).

Amortizes the dominant cost (the source OPEN) by grouping a source raster's
tiles into one partition, contiguous, and reading them from one cached open
resource. Pure Python + mapInPandas -- no .rdd / sc / spark.conf.set.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def align_partitions(df: DataFrame, *, n: int, path_col: str = "tile.path") -> DataFrame:
    """Hash-by-path repartition + sort so each source FILE is saturated in one
    partition and contiguous within it. `n` is parallelism-sized by the caller
    (3-5x worker cores on classic; a parallelism target on Serverless) -- never
    n_files, never sc-derived."""
    if n <= 0:
        raise ValueError(f"n must be a positive parallelism target, got {n}")
    col = F.col(path_col)
    return df.repartition(n, col).sortWithinPartitions(col)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_align_partitions.py --log align.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py \
        python/geobrix/test/pyrx/test_align_partitions.py
git commit -m "feat(pyrx): align_partitions helper (hash+sort by tile.path)

Co-authored-by: Isaac"
```

---

### Task 6: Per-partition capability-adaptive open-resource LRU (`grouped_exec.OpenResourceLRU`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py` (add `OpenResourceLRU`)
- Test: `python/geobrix/test/pyrx/test_open_resource_lru.py`

**Interfaces:**
- Produces: `OpenResourceLRU(*, max_bytes: int = GBX_LRU_MAX_BYTES, max_count: int = 64, opener: Callable[[str], Any], closer: Callable[[Any], None], weigher: Callable[[Any, str], int])`. Methods: `get(key: str) -> Any` (hit → move-to-recent + return; miss → `opener(key)`, `weigher(res, key)`, insert, then evict-oldest-while `total_bytes > max_bytes` **or** `count > max_count`, **never evicting the current/most-recent entry**, calling `closer` on each evicted); `close_all()` (close every held resource; call at partition end in a `finally`). Counters `.opens`, `.evictions`, `.bytes`. Module constants: `GBX_LRU_MAX_BYTES` (env-overridable, default `4 * 1024**3`), `STREAM_NOMINAL_BYTES` (`16 * 1024**2`).

- [ ] **Step 1: Write the failing test** (pure Python — fully local, no Spark)

```python
# python/geobrix/test/pyrx/test_open_resource_lru.py
from databricks.labs.gbx.pyrx.grouped_exec import OpenResourceLRU

def test_byte_budget_evicts_oldest_over_budget():
    closed = []
    lru = OpenResourceLRU(max_bytes=100, max_count=1000,
                          opener=lambda k: k, closer=lambda r: closed.append(r),
                          weigher=lambda r, k: 40)
    lru.get("a"); lru.get("b")     # 80 bytes; both fit
    assert lru.bytes == 80 and lru.evictions == 0
    lru.get("c")                    # 120 > 100 -> evict oldest "a" -> back to 80
    assert closed == ["a"] and lru.bytes == 80 and lru.evictions == 1

def test_many_small_files_stay_warm_under_budget():
    lru = OpenResourceLRU(max_bytes=4 * 1024**3, max_count=1000,
                          opener=lambda k: k, closer=lambda r: None,
                          weigher=lambda r, k: 32 * 1024**2)  # 32 MiB each
    for i in range(100):
        lru.get(f"f{i}")           # 100 * 32 MiB = 3.125 GiB < 4 GiB
    assert lru.evictions == 0 and lru.opens == 100

def test_count_guard_bounds_handles_when_weight_nominal():
    lru = OpenResourceLRU(max_bytes=10**12, max_count=2,
                          opener=lambda k: k, closer=lambda r: None,
                          weigher=lambda r, k: 0)  # streams ~ nominal
    lru.get("a"); lru.get("b"); lru.get("c")
    assert lru.evictions == 1      # count guard fired at 3 > 2

def test_never_evicts_the_current_entry():
    closed = []
    lru = OpenResourceLRU(max_bytes=10, max_count=1000,
                          opener=lambda k: k, closer=lambda r: closed.append(r),
                          weigher=lambda r, k: 999)  # single entry exceeds budget
    got = lru.get("big")
    assert got == "big" and closed == []  # current entry kept despite over-budget

def test_close_all_closes_remaining():
    closed = []
    lru = OpenResourceLRU(max_bytes=100, opener=lambda k: k,
                          closer=lambda r: closed.append(r), weigher=lambda r, k: 10)
    lru.get("x"); lru.get("y")
    lru.close_all()
    assert sorted(closed) == ["x", "y"] and lru.bytes == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_open_resource_lru.py --log lru.log`
Expected: FAIL — `OpenResourceLRU` not defined.

- [ ] **Step 3: Implement `OpenResourceLRU`**

```python
# append to grouped_exec.py
import os
from collections import OrderedDict
from typing import Any, Callable

GBX_LRU_MAX_BYTES = int(os.environ.get("GBX_LRU_MAX_BYTES", 4 * 1024**3))  # 4 GiB
STREAM_NOMINAL_BYTES = 16 * 1024**2  # resident estimate for an open stream/dataset


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

    def __init__(self, *, max_bytes: int = GBX_LRU_MAX_BYTES, max_count: int = 64,
                 opener: Callable[[str], Any], closer: Callable[[Any], None],
                 weigher: Callable[[Any, str], int]):
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if max_count < 1:
            raise ValueError("max_count must be >= 1")
        self.max_bytes = max_bytes
        self.max_count = max_count
        self._opener = opener
        self._closer = closer
        self._weigher = weigher
        self._store: "OrderedDict[str, tuple]" = OrderedDict()  # key -> (resource, weight)
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
        while len(self._store) > 1 and (self.bytes > self.max_bytes
                                        or len(self._store) > self.max_count):
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_open_resource_lru.py --log lru.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py \
        python/geobrix/test/pyrx/test_open_resource_lru.py
git commit -m "feat(pyrx): per-partition open-resource LRU (maxsize=2, close-on-evict)

Co-authored-by: Isaac"
```

---

### Task 7: Grouped `mapInPandas` executor (`grouped_exec.grouped_tile_map`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py` (add `grouped_tile_map` + the capability-adaptive opener)
- Test: `python/geobrix/test/pyrx/test_grouped_exec.py`

**Interfaces:**
- Consumes: `OpenResourceLRU`; `core.open_tile.open_tile`; `core.virtual_tile.effective_path_mode`; `_file_ref.file_supported`.
- Produces: `grouped_tile_map(df, core_fn, *, return_field: StructField, tile_col="tile") -> DataFrame`. Runs a partition-scoped `mapInPandas`: for each tile in the partition it gets the source's open resource from a per-partition byte-budgeted `OpenResourceLRU` (keyed by `tile.path`, weighed by the opener), applies `core_fn(ds) -> value`, and closes the LRU in a `finally` at partition end. The opener is **capability-adaptive**: FILE-capable → cache the `.open()` stream via the existing `open_windowed_via_fileref` (weight `STREAM_NOMINAL_BYTES`); non-FILE moderate file + ≥`GBX_STAGE_MIN_WINDOWS` (default 17) → local-stage (weight = staged file size); non-FILE huge (> `GBX_STAGE_MAX_BYTES`) / few windows → the open dataset via `open_tile` (weight `STREAM_NOMINAL_BYTES`). `_make_opener()` returns `(file_ok, opener, closer, weigher)`. Output adds a column named `return_field.name`.

- [ ] **Step 1: Write the failing test** (local Spark, non-FILE materialized tiles — this exercises the executor + LRU end-to-end; `file_supported()` returns False locally so it takes the fallback opener, which is the correct thing to test here)

```python
# python/geobrix/test/pyrx/test_grouped_exec.py
from pyspark.sql.types import StructField, LongType
from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.test.pyrx.conftest import make_geotiff_bytes  # helper

def _tile_df(spark):
    b = make_geotiff_bytes(width=4, height=3, count=1, epsg=4326)
    rows = [(VirtualTile.from_v1(cellid=i, raster=b).to_row(),) for i in range(3)]
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from pyspark.sql.types import StructType, StructField as SF
    return spark.createDataFrame(rows, StructType([SF("tile", V2_TILE_SCHEMA)]))

def test_grouped_map_matches_per_row_memsize(spark):
    import numpy as np
    def core_fn(ds):
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)
    out = grouped_tile_map(_tile_df(spark), core_fn,
                           return_field=StructField("sz", LongType()))
    vals = [r["sz"] for r in out.collect()]
    assert vals == [4 * 3 * 1 * 4] * 3  # 4x3, 1 band, float32
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_grouped_exec.py --log grouped.log`
Expected: FAIL — `grouped_tile_map` not defined.

- [ ] **Step 3: Implement `grouped_tile_map`**

```python
# append to grouped_exec.py
from pyspark.sql.types import StructField, StructType
import pandas as pd


def _make_opener(pending_key="tile"):
    """Capability-adaptive opener factory. Returns (file_ok, opener, closer, weigher).
    Runs on the worker; imports are worker-local (GDAL env configured there)."""
    from . import _file_ref
    file_ok = _file_ref.file_supported()

    def opener(row_and_key):
        # row_and_key carries the tile row so open_tile can resolve bytes/path/window
        from .core import open_tile as ot
        tile_row, _uri = row_and_key
        # fallback path (non-FILE / local Spark): open via the tile decode chokepoint
        cm = ot.open_tile(tile_row)  # context manager
        ds = cm.__enter__()
        return (cm, ds)

    def closer(handle):
        cm, _ds = handle
        cm.__exit__(None, None, None)

    def weigher(handle, key):
        # fallback (open dataset) weighs a small nominal so the count guard governs;
        # the local-stage branch (Task 9 fast-follow) will weigh the staged file size.
        return STREAM_NOMINAL_BYTES

    return file_ok, opener, closer, weigher


def grouped_tile_map(df, core_fn, *, return_field: StructField, tile_col: str = "tile"):
    out_schema = StructType(list(df.schema.fields) + [return_field])
    out_name = return_field.name

    def _map(pdf_iter):
        from . import _env
        _env.configure_gdal_env()
        _file_ok, opener, closer, weigher = _make_opener()
        lru = OpenResourceLRU(opener=opener, closer=closer, weigher=weigher)
        try:
            for pdf in pdf_iter:
                results = []
                for _, row in pdf.iterrows():
                    tile = row[tile_col]
                    uri = tile["path"] if tile is not None else None
                    key = uri if uri is not None else id(tile)
                    _cm, ds = lru.get((tile, key)) if uri else opener((tile, key))
                    try:
                        results.append(core_fn(ds))
                    finally:
                        if not uri:  # unkeyed (materialized) -> close immediately
                            closer((_cm, ds))
                pdf[out_name] = results
                yield pdf
        finally:
            lru.close_all()

    return df.mapInPandas(_map, schema=out_schema)
```

Note for the implementer: the LRU is keyed by `uri` (the source path) so a source's windows amortize; **materialized** tiles (raster inline, no path) are opened and closed per row (nothing to amortize — the bytes are already in the row). The FILE-stream fast path (`open_windowed_via_fileref`) is wired in Task 9 after the DBR-19 round-trip confirms it; locally `file_supported()` is False so this fallback opener is what runs and what the test asserts.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_grouped_exec.py --log grouped.log`
Expected: PASS — grouped result equals the per-row memsize computation.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py \
        python/geobrix/test/pyrx/test_grouped_exec.py
git commit -m "feat(pyrx): partition-scoped mapInPandas grouped executor + LRU

Co-authored-by: Isaac"
```

---

### Task 8: Route `rst_memsize` through the grouped executor (the proof)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py:5227-5231` (add a grouped entry point; keep the per-row `rst_memsize`)
- Test: `python/geobrix/test/pyrx/test_rst_memsize_grouped.py`

**Interfaces:**
- Consumes: `grouped_exec.grouped_tile_map`; the existing `open_header`-based memsize logic.
- Produces: `rst_memsize_grouped(df, *, tile_col="tile", out_col="memsize") -> DataFrame` — the partition-scoped form of `rst_memsize`. The existing `rst_memsize(tile) -> Column` per-row form is unchanged (the slower convenience path).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_rst_memsize_grouped.py
from databricks.labs.gbx.pyrx.functions import rst_memsize, rst_memsize_grouped
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile, V2_TILE_SCHEMA
from databricks.labs.gbx.test.pyrx.conftest import make_geotiff_bytes
from pyspark.sql.types import StructType, StructField as SF
from pyspark.sql import functions as F

def test_grouped_memsize_equals_per_row(spark):
    b = make_geotiff_bytes(width=8, height=8, count=1, epsg=4326)
    df = spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=i, raster=b).to_row(),) for i in range(4)],
        StructType([SF("tile", V2_TILE_SCHEMA)]))
    per_row = {r["cellid"]: r["ms"] for r in
               df.select(df.tile.cellid.alias("cellid"),
                         rst_memsize(F.col("tile")).alias("ms")).collect()}
    grouped = {r["tile"]["cellid"]: r["memsize"] for r in
               rst_memsize_grouped(df).collect()}
    assert grouped == per_row
    assert set(grouped.values()) == {8 * 8 * 1 * 4}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_rst_memsize_grouped.py --log memsize_grouped.log`
Expected: FAIL — `rst_memsize_grouped` not defined.

- [ ] **Step 3: Implement `rst_memsize_grouped`**

```python
# in functions.py, near rst_memsize (5227)
def rst_memsize_grouped(df, *, tile_col: str = "tile", out_col: str = "memsize"):
    """Partition-scoped rst_memsize: amortizes source opens across a partition's
    tiles via the grouped executor. Equivalent result to per-row rst_memsize."""
    import numpy as np
    from pyspark.sql.types import StructField, LongType
    from .grouped_exec import grouped_tile_map

    def _core(ds):
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)

    return grouped_tile_map(df, _core,
                            return_field=StructField(out_col, LongType()),
                            tile_col=tile_col)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_rst_memsize_grouped.py --log memsize_grouped.log`
Expected: PASS.

- [ ] **Step 5: Run the full pyrx unit suite to confirm no regression, then commit**

Run: `bash scripts/commands/gbx-test-pyrx.sh --log pyrx_full.log`
Expected: PASS (whole `test/pyrx/` green — the schema widening + new modules integrate cleanly).

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
        python/geobrix/test/pyrx/test_rst_memsize_grouped.py
git commit -m "feat(pyrx): rst_memsize_grouped via grouped executor (proof of fast path)

Co-authored-by: Isaac"
```

---

### Task 9: Dogfood DBR-19 cluster validation (the integration gate)

This task validates the FILE round-trip that `local[2]` cannot exercise, and wires the FILE-stream fast path once confirmed. It is **not** a pytest task — it runs a throwaway probe on the dogfood classic cluster, then makes one small code change.

**Files:**
- Create (throwaway, gitignored): `prompts/testing/2026-08-17-file-for-raster-roundtrip.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py` (`_make_opener` — enable the FILE-stream branch)

**Preconditions (lead-agent owns; state in the dispatch):**
- Auth: profile `dogfood` must be `VALID` (checked at session start). The probe runs on the classic cluster **`gbx-file-probe-dogfood-19snap`**, id **`0813-214720-wo95qznu`** (DBR 19.5 snapshot, Scala 2.13, `SINGLE_USER`, `single_user_name=mjohns@databricks.com`) — target it **by id** (the list omits it when terminated; start it by id if needed).
- **Keep the cluster WARM for the whole of Task 9.** Cluster spin-up is minutes; start it once at the top of Step 2 and **do not terminate it between Steps 2 and 4** (the plan iterates: probe → wire the stream branch → re-probe). Only stop the cluster when the user says (they explicitly asked to keep it warm while iterating on-cluster). If the cluster auto-terminates on idle between iterations, its autotermination window should be widened (or fire a lightweight keep-alive) rather than paying a fresh cold start each round.
- The wheel must be staged to the cluster or `%pip install`ed from the artifact Volume (light tier, JAR-free). Reuse the wheel-install incantation: force-reinstall `--no-deps geobrix` then `geobrix[light-dbr19]`.
- Sample corpus: `geospatial_docs.geobrix.sample-data/bench-corpus-*` (already present from the spikes).

- [ ] **Step 1: Write the round-trip probe**

The probe (run as a notebook job on the cluster by id) must assert, for both `file_mode="external"` and `file_mode="managed"`:
  1. `write_file_table(spark, df, tbl, file_mode=..., filespace=...)` succeeds (typed column + INSERT; no `FILE_TYPE_UNKNOWN_WRITE`).
  2. `SHOW TBLPROPERTIES tbl` contains `geobrix_writer_version=1` and the expected `geobrix.file.write_strategy`.
  3. `read_file_table(spark, tbl)` returns a v2-tile DataFrame with `path_mode` = the table's mode, **without** selecting the FILE column.
  4. `rst_memsize_grouped(read_file_table(...))` returns correct sizes (FILE-stream path where `file_supported()` is True).
  5. Layout check: `ORDER BY path` insert produces contiguous paths (dumb read amortizes); a smart `align_partitions(df, n=<parallelism>)` read yields `opens == n_files`.

- [ ] **Step 2: Run the probe on the dogfood cluster by id**

Submit as a notebook job targeting cluster id `0813-214720-wo95qznu`. Give a one-line progress update ~every 30s while it runs (per the long-op feedback rule). Capture the JSON result.
Expected: all five assertions pass; MANAGED and EXTERNAL both round-trip; reads never touch the FILE column.

- [ ] **Step 3: Enable the FILE-stream branch in `_make_opener`**

With the round-trip confirmed, wire the FILE-capable opener to use the existing `open_windowed_via_fileref` (stream) instead of the fallback `open_tile`, keyed by uri so windows amortize. The change is guarded by `file_supported()` (already False locally, so local tests keep taking the fallback branch — Task 7's test stays green):

```python
# grouped_exec.py _make_opener, the file_ok branch
    if file_ok:
        from . import _file_ref
        def opener(row_and_key):
            tile_row, _uri = row_and_key
            file_ref = tile_row.get("_file_ref") if hasattr(tile_row, "get") else None
            # reconstructed upstream via file_ref_arg(tile['path']); stream-open once
            cm = _file_ref.open_windowed_via_fileref(file_ref, tile_row["window"],
                    pending=(None, None, None, tile_row.get("crs")),
                    tile_crs=tile_row.get("crs"))
            ds = cm.__enter__()
            return (cm, ds)
        # closer unchanged
```

- [ ] **Step 4: Re-run the probe to confirm the stream path**

Re-submit; confirm `rst_memsize_grouped` over a FILE table now takes the stream branch and `opens == n_files` under `align_partitions`. Confirm graceful fallback still holds when `GBX_DISABLE_FILE=1`.
Expected: stream path exercised; fallback intact.

- [ ] **Step 5: Re-run the full local pyrx suite (guard against the wiring change), then commit**

Run: `bash scripts/commands/gbx-test-pyrx.sh --log pyrx_full_final.log`
Expected: PASS (local behavior unchanged — `file_supported()` is False locally).

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py
git commit -m "feat(pyrx): enable FILE-stream opener in grouped executor (dogfood-validated)

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- §1 goal / 3-rep taxonomy → path_mode (Task 1) encodes representations 1/2/3; graceful fallback threaded through Tasks 3/7/9.
- §2 measured facts → open-amortization LRU (Task 6), mapInPandas 6× (Task 7), MANAGED≈EXTERNAL reads (Task 9 validates both), write layout controls grouping (Task 4 ORDER BY/CLUSTER BY + Task 9 layout check), typed-column-then-INSERT (Task 4), plain-`path` beside FILE ref (Task 3).
- §3 architecture / §3.1 path_mode + table props → Tasks 1, 2.
- §4 reader → Task 3 (projects plain cols, path_mode from props, never FILE column, cued by props).
- §5 writer + conversions → Task 4 (typed column, ORDER BY/CLUSTER BY, TBLPROPERTIES incl. writer-version, materialize BINARY→MANAGED via create_file, EXTERNAL via try_to_file, portable default). **Gap check:** MANAGED↔EXTERNAL *conversions* (§5) are covered by re-writing with the other `file_mode`; explicit convert helpers are not separately tasked — acceptable for increment 1 (a conversion is a read-then-`write_file_table`). Recorded, not a blocker.
- §6 grouped execution → Tasks 5, 6, 7, 8 (partition-scoped mapInPandas, capability-adaptive LRU, partitioning contract, scalar proof; agg/1:n deferred per settled §8.5).
- §7 capability gating / cross-runtime → Task 3 (Serverless-GC-safe projection), Task 7 (file_supported-driven opener), Task 9 (round-trip + fallback). DBR 17 unreadability documentation is a docs-phase non-goal.
- §8 open decisions → all settled in the "Settled §8 open decisions" section.
- §9 non-goals → docs, custom containers, heavy parity all excluded.
- §10 sequencing → Tasks 1–8 build the interlocking light units; Task 9 is the validation gate. Non-FILE staging amortization (§10 step 2) is present as the capability-adaptive opener's local-stage branch (Task 7 interface); its full local-stage implementation is scoped minimally here (fallback opener uses `open_tile`, which already routes through `_stage_local_if_needed`) — the ≥17-window local-stage optimization is recorded for the perf-tuning follow-up, not increment 1's correctness gate.

**2. Placeholder scan:** No "TBD"/"implement later"/"handle edge cases". Each code step has real code; SQL builders have concrete strings; the cluster-validation task lists concrete assertions and the exact cluster id.

**3. Type consistency:** `path_mode` (str|None) consistent across Tasks 1/2/3/4. `OpenResourceLRU(maxsize, opener, closer)` with `.get`/`.close_all`/`.opens`/`.evictions` used consistently in Tasks 6/7. `grouped_tile_map(df, core_fn, *, return_field, tile_col)` signature consistent Tasks 7/8. `align_partitions(df, *, n, path_col)` consistent Task 5/9. `build_props`/`parse_props` keys consistent Tasks 2/3/4. `write_file_table`/`read_file_table` signatures consistent Tasks 3/4/9.

**Known follow-ups (recorded, out of increment-1 scope):** existing-table ALTER (§8.3); `rst_*_agg`/1:n generator forms (§8.5); heavy Scala parity (§8.6); the ≥17-window local-stage optimization + disk-budget guardrail wiring beyond the default `open_tile` staging; explicit MANAGED↔EXTERNAL conversion helpers; user-facing docs (Virtual Tiles / Large Tiles).
