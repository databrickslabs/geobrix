# FILE-for-Raster (Light Tier) — Increment 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the light-tier FILE fast path beyond the header-only `rst_memsize_grouped` to pixel-reading ops (`rst_clip` family), 1:n generators, and `rst_*_agg` aggregators, closing the cross-tier tile-schema gap and the header-vs-pixel executor asymmetry (M2), with a memory-aware large-file strategy.

**Architecture:** The partition-scoped grouped executor (`grouped_tile_map` + per-partition `OpenResourceLRU` in `pyrx/grouped_exec.py`) gains a `view` contract so `core_fn` can request a real windowed pixel read (via existing `_window_dataset_bytes`) instead of the header-only `_WindowHeaderView`. Pixel ops get `df→df` `_grouped` variants (mirroring `rst_memsize_grouped`); generators stay `@udtf` but become FILE-aware via a `file_ref` arg; aggregators open FILE tiles as EXTERNAL (FUSE), dropping the `try_to_file` injection Spark 4 rejects in `.agg()`. The heavy Scala v2 tile gains `path_mode` (NULL for materialized) to match the 9-field light schema.

**Tech Stack:** Python 3.12 (`pyrx`, pure-Python, JAR-less), pytest, rasterio; Scala 2.13 / Spark 4 (heavy tile schema) built + tested in the `geobrix-dev` Docker container; DBR-19 dogfood classic cluster `0813-214720-wo95qznu` for FILE round-trip validation.

**Spec:** `docs/superpowers/specs/2026-08-18-file-for-raster-increment2-design.md`

## Global Constraints

- Light tier is pure Python and JAR-less: **no `spark.conf.set` / `_jvm` / `.rdd` in `pyrx`** (bench-only `repartition` excepted); Serverless parallelism only via `repartition(N, column)`.
- **SQL binds positionally**; an extra wrapper arg beyond `builder()` arity is silently dropped.
- **FILE is DBR-19-only** → absent from `local[2]` test Spark. Each FILE-dependent seam is TDD-tested at its pure boundary locally (`gbx:test:pyrx`); the FILE round-trip is validated once on dogfood cluster `0813-214720-wo95qznu` (Task 9).
- **No hard dates and no internal planning vocabulary** (`wave`/`increment`) in user-facing docs (`docs/docs/**`).
- **Package-source changes run the affected unit suites** (`gbx:test:pyrx` on the touched `python/geobrix/test/pyrx/**`), not only doc-tests.
- **Graceful fallback is mandatory**: every FILE fast path degrades to the existing materialized/FUSE path on any failure. A FILE miss is a perf regression, never a correctness one.
- Run tests via the `gbx:*` palette (`gbx:test:pyrx`, `gbx:test:scala`), never ad-hoc `pytest`/`mvn`/`docker`. If a `gbx:*` command is broken, fix the command.
- Do not push; commits are held on `beta/0.5.0`. Commit messages end with `Co-authored-by: Isaac`.

---

## File Structure

**Modified (Python, `python/geobrix/src/databricks/labs/gbx/pyrx/`):**
- `grouped_exec.py` — add `view` param to `grouped_tile_map`; fix `OpenResourceLRU` weigher + size-adaptive open + temp-leak in `_make_opener` (Tasks 3, 4).
- `core/preparer.py` — add `GBX_STAGE_MAX_BYTES` guard to `_stage_local_if_needed` (Task 4).
- `file_table.py` — reader-2b managed-uri resolution in `read_file_table` (Task 2).
- `functions.py` — `rst_clip_grouped` + other pixel-op `_grouped` variants (Tasks 5, 6); FILE-aware generator UDTFs (Task 7); aggregators-as-external (Task 8).

**Modified (Scala, `src/main/scala/com/databricks/labs/gbx/rasterx/util/`):**
- `RST_ExpressionUtil.scala` — add `path_mode` 9th field to `v2TileType` (Task 1).
- `RasterSerializationUtil.scala` — v2 field-count 8→9, NULL `path_mode` for materialized (Task 1).

**Tests (`python/geobrix/test/pyrx/`):** `test_core_virtual_tile.py`, `test_grouped_exec.py`, `test_open_resource_lru.py`, `test_file_table_reader.py`, `test_rst_clip_grouped.py` (new), `test_core_agg.py`, `test_core_tiling.py`; Scala test under `src/test/scala/.../rasterx/util/`.

**Docs (Task 10):** `docs/docs/**/performance.mdx`, the Tile Structure page, bench harness.

---

## Task 1: Heavy-tier `path_mode` (close the 9-vs-8 tile-schema gap)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala:101-110`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` (v2 field-count + materialized population)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtilTest.scala` (or the existing serialization test suite)

**Interfaces:**
- Produces: heavy `v2TileType` with 9 fields ending in `StructField("path_mode", StringType, nullable = true)`, matching light `V2_TILE_SCHEMA` (`pyrx/core/virtual_tile.py:34-46`). Materialized heavy tiles set `path_mode = null`.

- [ ] **Step 1: Write the failing Scala test** — assert the v2 tile schema has 9 fields ending in `path_mode`.

```scala
test("v2TileType carries path_mode as the 9th field") {
  val fields = RST_ExpressionUtil.v2TileType.fields
  assert(fields.length == 9, s"expected 9 fields, got ${fields.length}")
  assert(fields.last.name == "path_mode")
  assert(fields.last.dataType == StringType)
  assert(fields.last.nullable)
}
```

- [ ] **Step 2: Run it to confirm it fails** — `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtilTest'` → FAIL (`expected 9 fields, got 8`).

- [ ] **Step 3: Add the field** to `RST_ExpressionUtil.scala` `v2TileType`:

```scala
    StructField("metadata", MapType(StringType, StringType), nullable = true),
    StructField("path_mode", StringType, nullable = true)))
```

- [ ] **Step 4: Update `RasterSerializationUtil.scala`** — change the v2 field-count expectation (the `8 for v2` reference near line 129 → `9 for v2`), and where a materialized v2 tile Row is constructed, append a trailing `null` for `path_mode` so the Row arity matches the 9-field schema. Grep the file for the row builder(s) and add the trailing `null` to each v2 construction.

- [ ] **Step 5: Write the cross-tier round-trip test** (Scala) — build a materialized v2 tile Row, wrap in a DataFrame with `v2TileType`, and assert `path_mode` reads back as `null`:

```scala
test("materialized v2 tile sets path_mode = null") {
  val df = /* build 1-row DF with v2TileType from a materialized tile */
  val pm = df.select(col("tile.path_mode")).head().get(0)
  assert(pm == null)
}
```

- [ ] **Step 6: Run the Scala suite** — `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.*'` → PASS. (Requires a container build; dispatch as a Task subagent, this is minutes-long.)

- [ ] **Step 7: Light-side schema-equality assertion** — add to `python/geobrix/test/pyrx/test_core_virtual_tile.py` a test that the 9 field names/order match the documented heavy contract (guards future drift):

```python
def test_v2_schema_field_order_matches_heavy_contract():
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    names = [f.name for f in V2_TILE_SCHEMA.fields]
    assert names == ["cellid","raster","path","window","clip_polygon",
                     "clip_crs","crs","metadata","path_mode"]
```

- [ ] **Step 8: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_core_virtual_tile.py` → PASS.

- [ ] **Step 9: Commit** — `git add` the two Scala files + the two tests; `git commit -m "fix(rasterx): add path_mode 9th field to heavy v2 tile schema"`.

---

## Task 2: Reader-2b — capability-gated managed-uri resolution

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/file_table.py:69-123` (`read_file_table`)
- Test: `python/geobrix/test/pyrx/test_file_table_reader.py`

**Interfaces:**
- Consumes: `file_supported(spark)` (`pyrx/_file_ref.py:41`); `effective_path_mode` semantics (`pyrx/core/virtual_tile.py:117`).
- Produces: for a **managed** table on a FILE-capable session, `read_file_table` sets `tile.path = <file_col>.uri` with the `dbfs:` scheme stripped and `path_mode = "managed"`; otherwise unchanged (plain `path` column, `path_mode` from the table property). New pure helper `_strip_dbfs_scheme(uri: str) -> str`.

- [ ] **Step 1: Write the failing unit test** for the pure scheme-stripper:

```python
def test_strip_dbfs_scheme():
    from databricks.labs.gbx.pyrx.file_table import _strip_dbfs_scheme
    assert _strip_dbfs_scheme("dbfs:/Volumes/c/s/v/f.tif") == "/Volumes/c/s/v/f.tif"
    assert _strip_dbfs_scheme("/Volumes/c/s/v/f.tif") == "/Volumes/c/s/v/f.tif"
    assert _strip_dbfs_scheme(None) is None
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_file_table_reader.py::test_strip_dbfs_scheme` → FAIL (`cannot import _strip_dbfs_scheme`).

- [ ] **Step 3: Implement the helper** in `file_table.py`:

```python
def _strip_dbfs_scheme(uri):
    """Return a FUSE-openable /Volumes path from a FileRef .uri (dbfs:/Volumes/...)."""
    if uri is None:
        return None
    return uri[len("dbfs:"):] if uri.startswith("dbfs:") else uri
```

- [ ] **Step 4: Run** the test → PASS.

- [ ] **Step 5: Wire capability-gated managed resolution into `read_file_table`.** After determining `file_mode` from the table properties, when `file_mode == "managed"` **and** `file_supported(spark)` is True, project the FILE column's `.uri` subfield and derive `path` from it; otherwise keep the current plain-column projection. Use `F.expr(f"regexp_replace({file_col}.uri, '^dbfs:', '')")` for the SQL-side strip so it stays a distributable column (never `.collect()` the FileRef). Set `path_mode = F.lit("managed")`. The non-managed / non-capable branch is unchanged (Serverless-GC-safe: the FILE column is only referenced when `file_supported`).

- [ ] **Step 6: Write a unit test** that, with `file_supported` monkeypatched True and a fake managed-table schema, `read_file_table` builds `tile.path` from the stripped uri and `path_mode == "managed"`; and with `file_supported` False, it never references the FILE column (assert the projected SQL does not name `<file_col>.uri`). Drive this against a `local[2]` session with a stubbed table (no real FILE type — assert on the built plan/columns, not execution).

- [ ] **Step 7: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_file_table_reader.py` → PASS.

- [ ] **Step 8: Tier-general note (no code move).** Confirm by inspection that `file_supported` and `file_ref_arg` in `_file_ref.py` have no raster-specific coupling (they operate on a `path` string / generic tile Column). Add a module docstring line marking them the shared FILE-capability surface for future `pyvx` reuse. (SQL `<file_col>.uri` subfield projection on classic DBR-19 is verified on-cluster in Task 9; if it fails there, the managed branch falls back to the plain `path` column with `path_mode="managed"` — documented in Task 9.)

- [ ] **Step 9: Commit** — `git commit -m "feat(pyrx): reader-2b managed-uri resolution in read_file_table"`.

---

## Task 3: Executor view contract + M2 (header vs pixel view)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py:182-351` (`grouped_tile_map`)
- Test: `python/geobrix/test/pyrx/test_grouped_exec.py`

**Interfaces:**
- Consumes: `_window_dataset_bytes(src, window, pending=(None,None,None,None)) -> bytes` (`pyrx/core/open_tile.py:92`); `_WindowHeaderView` (`open_tile.py:430`).
- Produces: `grouped_tile_map(df, core_fn, *, return_field, tile_col="tile", view="header")`. `view="header"` → `core_fn` receives `_WindowHeaderView` (today's behavior). `view="pixels"` → on the FILE fast path, the window is materialized from the cached source via `_window_dataset_bytes` and opened as a real `rasterio` `DatasetReader` handed to `core_fn`; the fallback path already hands a real dataset.

- [ ] **Step 1: Write the failing test** — a `view="pixels"` core_fn can `read()` pixels; a `view="header"` core_fn gets a view whose `.read()` raises. Use a materialized-tile DataFrame (fallback path, no FILE) so it runs on `local[2]`:

```python
def test_grouped_tile_map_pixel_view_allows_read(materialized_tile_df):
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map
    from pyspark.sql.types import StructField, LongType
    def core(ds):
        return int(ds.read(1).sum())  # reads pixels
    out = grouped_tile_map(materialized_tile_df, core,
                           return_field=StructField("s", LongType()), view="pixels")
    assert out.select("s").head()[0] is not None
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_grouped_exec.py::test_grouped_tile_map_pixel_view_allows_read` → FAIL (`grouped_tile_map() got an unexpected keyword argument 'view'`).

- [ ] **Step 3: Add the `view` param and pixel-view branch.** In `grouped_tile_map`, thread `view` into the per-partition function. On the FILE fast path, when `view == "pixels"`: build the tile's `Window` and `pending` tuple (bands/nodata/srid/crs from the tile struct, as the scalar path does), call `_window_dataset_bytes(cached_src, window, pending)`, open the bytes with `rasterio.open(rasterio.io.MemoryFile(b))` inside the per-tile `ExitStack`, and pass that `DatasetReader` to `core_fn`. When `view == "header"`, keep `_WindowHeaderView`. The fallback path is unchanged (already a real dataset). Preserve per-tile degrade-to-None on error.

- [ ] **Step 4: Run** the test → PASS.

- [ ] **Step 5: Regression test** — assert `view="header"` still gives a header view and that `rst_memsize_grouped` (which uses the default) is unaffected:

```python
def test_grouped_tile_map_header_view_forbids_read(materialized_tile_df):
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map
    from pyspark.sql.types import StructField, LongType
    def core(view):
        try: view.read(1); return -1
        except Exception: return int(view.width)  # header attrs only
    out = grouped_tile_map(materialized_tile_df, core,
                           return_field=StructField("w", LongType()))  # default view="header"
    assert out.select("w").head()[0] > 0
```

- [ ] **Step 6: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_grouped_exec.py` → PASS.

- [ ] **Step 7: Commit** — `git commit -m "feat(pyrx): header-vs-pixel view contract in grouped_tile_map (M2)"`.

---

## Task 4: Memory-aware LRU + staging (size-adaptive, budget-real, leak-free)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/grouped_exec.py:55-179` (`OpenResourceLRU`, `_make_opener`)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py:278-323` (`_stage_local_if_needed`)
- Test: `python/geobrix/test/pyrx/test_open_resource_lru.py`

**Interfaces:**
- Consumes: `FileRef.size` (bytes); `_stage_local_if_needed(path) -> (local_path, is_temp)`.
- Produces: `OpenResourceLRU` weighs entries by **actual byte size**; `max_count` default lowered and size-adaptive; `_make_opener` chooses stream vs FUSE by size and **deletes staged temp files on eviction**; `_stage_local_if_needed` honors `GBX_STAGE_MAX_BYTES` (default 4 GiB) — files over the cap are never staged (caller degrades). New env: `GBX_STREAM_MAX_BYTES` (default 268435456 = 256 MiB), `GBX_STAGE_MAX_BYTES` (default 4 GiB).

- [ ] **Step 1: Write the failing weigher test** — a stream entry weighs its real file size, not the 16 MiB nominal:

```python
def test_lru_weighs_by_real_size():
    from databricks.labs.gbx.pyrx.grouped_exec import OpenResourceLRU
    closed = []
    lru = OpenResourceLRU(max_bytes=100, max_count=8,
                          opener=lambda k: {"k": k},
                          closer=lambda s: closed.append(s),
                          weigher=lambda s, k: 60)  # 60 bytes each
    lru.get("a"); lru.get("b")   # 120 > 100 → oldest ("a") evicted
    assert closed == [{"k": "a"}]
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_open_resource_lru.py::test_lru_weighs_by_real_size` → this already passes if the LRU honors the weigher; the real bug is `_make_opener`'s weigher returning `STREAM_NOMINAL_BYTES`. Add the failing test against `_make_opener`'s weigher:

```python
def test_make_opener_weigher_uses_fileref_size():
    from databricks.labs.gbx.pyrx.grouped_exec import _make_opener
    fr_holder, opener, closer, weigher = _make_opener()
    class FakeSrc: pass
    src = FakeSrc()
    fr_holder[0] = type("FR", (), {"size": 500_000_000})()  # 500 MB FileRef
    assert weigher(src, "uri") == 500_000_000   # not STREAM_NOMINAL_BYTES
```

- [ ] **Step 3: Fix `_make_opener`.** The weigher returns the real byte size: prefer the current `FileRef.size` (via `fr_holder[0].size`) for a stream entry, else `os.path.getsize(staged_path)` for a staged entry. Make the opener **size-adaptive**: if the FileRef size ≤ `GBX_STREAM_MAX_BYTES` and the source is not a striped layout, use `FileRef.open()` stream; otherwise use `FileRef.as_local_file()` (lazy FUSE). Track whether the opener staged a temp path; the closer must `os.remove` it (fixing the leak). Default `max_count` for the LRU lowered to a size-adaptive small value (e.g. 4).

- [ ] **Step 4: Run** the two tests → PASS.

- [ ] **Step 5: Write the stage-guard failing test:**

```python
def test_stage_local_respects_max_bytes(monkeypatch, tmp_path):
    import os
    from databricks.labs.gbx.pyrx.core import preparer
    big = tmp_path / "big.tif"; big.write_bytes(b"x" * 1024)
    monkeypatch.setenv("GBX_STAGE_MAX_BYTES", "512")   # cap below file size
    monkeypatch.setattr(preparer, "_is_fuse_path", lambda p: True)   # force stage path
    monkeypatch.setattr(preparer, "_probe_direct_access", lambda p: False)
    with pytest.raises(preparer.StageTooLargeError):
        preparer._stage_local_if_needed(str(big))
```

- [ ] **Step 6: Add the guard** to `_stage_local_if_needed`: before copying, if the source size exceeds `int(os.environ.get("GBX_STAGE_MAX_BYTES", 4 * 1024**3))`, raise a new `StageTooLargeError` (subclass of `Exception`) so the grouped-exec/scalar caller degrades gracefully (its existing `try/except` returns None / falls to header). Export `StageTooLargeError`.

- [ ] **Step 7: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_open_resource_lru.py python/geobrix/test/pyrx/test_grouped_exec.py` → PASS.

- [ ] **Step 8: Commit** — `git commit -m "fix(pyrx): memory-aware LRU weigher + size-adaptive open + stage guard"`.

---

## Task 5: `rst_clip_grouped` — first pixel op through the executor

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (add `rst_clip_grouped` near `rst_memsize_grouped:5234`)
- Test: `python/geobrix/test/pyrx/test_rst_clip_grouped.py` (new)

**Interfaces:**
- Consumes: `grouped_tile_map(..., view="pixels")` (Task 3); `edit.clip_to_geom(ds, geom, all_touched, geom_crs)` and `_serde.build_tile` (used by `_uf_clip:1773`).
- Produces: `rst_clip_grouped(df, geom_wkb, all_touched=False, clip_crs=None, *, tile_col="tile", out_col="tile") -> DataFrame` — a df→df clip that opens each input via the FILE fast path (pixel view) and amortizes opens per partition.

- [ ] **Step 1: Write the failing test** — on materialized tiles (fallback path, `local[2]`), `rst_clip_grouped` matches the scalar `rst_clip` result:

```python
def test_rst_clip_grouped_matches_scalar(materialized_tile_df, london_geom_wkb):
    from databricks.labs.gbx.pyrx.functions import rst_clip_grouped
    out = rst_clip_grouped(materialized_tile_df, london_geom_wkb, all_touched=False)
    row = out.select("tile").head()
    assert row is not None and row[0] is not None   # produced a clipped tile
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_rst_clip_grouped.py` → FAIL (`cannot import rst_clip_grouped`).

- [ ] **Step 3: Implement `rst_clip_grouped`:**

```python
def rst_clip_grouped(df, geom_wkb, all_touched=False, clip_crs=None,
                     *, tile_col="tile", out_col="tile"):
    """Partition-scoped rst_clip via the grouped executor (FILE pixel fast path)."""
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    g = _parse_geom_bytes(geom_wkb)   # driver-side constant geom
    def _core(ds):
        new_bytes = edit.clip_to_geom(ds, g, bool(all_touched), geom_crs=clip_crs)
        return None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", 0)
    return grouped_tile_map(df, _core,
                            return_field=StructField(out_col, V2_TILE_SCHEMA),
                            tile_col=tile_col, view="pixels")
```

(If `edit.clip_to_geom` needs the geom parsed per-call, parse inside `_core`; keep the driver-side broadcast if the parse is picklable.)

- [ ] **Step 4: Run** the test → PASS.

- [ ] **Step 5: Add a fallback-parity test** — clip result pixels equal the scalar `_uf_clip` output on the same materialized tile (assert array equality via `rasterio` read of both output tiles).

- [ ] **Step 6: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_rst_clip_grouped.py` → PASS.

- [ ] **Step 7: Commit** — `git commit -m "feat(pyrx): rst_clip_grouped pixel op via grouped executor"`.

---

## Task 6: Remaining pixel-op `_grouped` variants (batch)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- Test: `python/geobrix/test/pyrx/test_rst_clip_grouped.py` (extend) + the relevant existing op tests.

**Interfaces:**
- Produces: `_grouped` df→df variants for the pixel-reading scalar tile→tile ops, each using `grouped_tile_map(view="pixels")` with the op's existing pixel kernel.

- [ ] **Step 1: Enumerate the pixel-op set.** Run `grep -n "ot._open(tile" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` to list every scalar UDF that opens the input and reads pixels (the `rst_clip` shape). Record the resulting function names in the task ledger — that is the exact batch.

- [ ] **Step 2: Write one failing test per new `_grouped` variant** asserting fallback-parity with its scalar form on a materialized tile (same shape as Task 5 Step 5). Put them in `test_rst_clip_grouped.py` (or the op's existing test file).

- [ ] **Step 3: Run** the new tests → FAIL (imports missing).

- [ ] **Step 4: Add each `_grouped` variant** with the identical wiring template (only the `_core` kernel body differs — reuse the scalar op's existing pixel kernel):

```python
def rst_<op>_grouped(df, <op_args>, *, tile_col="tile", out_col="tile"):
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    def _core(ds):
        new_bytes = <existing scalar kernel using ds>
        return None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", 0)
    return grouped_tile_map(df, _core,
                            return_field=StructField(out_col, V2_TILE_SCHEMA),
                            tile_col=tile_col, view="pixels")
```

- [ ] **Step 5: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/` on the touched files → PASS.

- [ ] **Step 6: Commit** — `git commit -m "feat(pyrx): _grouped variants for the pixel-op family"`.

---

## Task 7: FILE-aware 1:n generators

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (the generator UDTFs near lines 4107-4466: `rst_retile`, `rst_tooverlappingtiles`, `rst_maketiles`, `rst_h3_tessellate`, `rst_bng_tessellate`, `rst_quadbin_tessellate`, `rst_separatebands`)
- Test: `python/geobrix/test/pyrx/test_core_tiling.py`, `test_core_tessellate.py`

**Interfaces:**
- Consumes: `file_ref_arg(tile_col, spark)` (`_file_ref.py:115`); `ot._open(tile, file_ref=...)`.
- Produces: each generator UDTF gains a leading `file_ref` `eval` arg; its wrapper passes `file_ref_arg(tc)`; the UDTF opens the input via `ot._open(tile, file_ref=file_ref)` (FILE stream fast path, FUSE fallback). Behavior with FILE unavailable is byte-identical to today.

- [ ] **Step 1: Write the failing test** — with `file_supported` False (local), `rst_retile` yields the same sub-tiles as today (FILE arg is `None` → FUSE open):

```python
def test_rst_retile_file_unavailable_unchanged(materialized_tile_df):
    from databricks.labs.gbx.pyrx.functions import rst_retile
    out = materialized_tile_df.select(rst_retile("tile", 256, 256).alias("t"))
    assert out.count() > 0
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_core_tiling.py::test_rst_retile_file_unavailable_unchanged` → confirm current behavior is preserved (baseline PASS; this test guards the refactor).

- [ ] **Step 3: Add the `file_ref` arg** to each generator UDTF `eval` (leading param) and open the input via `ot._open(tile, file_ref=file_ref)` instead of the bare path. Update each wrapper to pass `file_ref_arg(_col(tile))` as the first UDTF argument. UDTFs are not aggregates, so `try_to_file` as an arg is permitted (unlike Task 8).

- [ ] **Step 4: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_core_tiling.py python/geobrix/test/pyrx/test_core_tessellate.py` → PASS (all generators unchanged under `file_supported=False`).

- [ ] **Step 5: Commit** — `git commit -m "feat(pyrx): FILE-aware 1:n generator UDTFs"`.

Note (documented follow-up, not this task): open-amortization across `eval` calls via a UDTF-instance-scoped `OpenResourceLRU` is a further optimization; per-`eval` `.open()` is acceptable because generators read the whole input tile.

---

## Task 8: Aggregators handle FILE tiles as EXTERNAL

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py:6839-6875` (`rst_merge_agg`, `rst_combineavg_agg`)
- Test: `python/geobrix/test/pyrx/test_core_agg.py`

**Interfaces:**
- Produces: `rst_merge_agg` / `rst_combineavg_agg` no longer inject `file_ref_arg` (`try_to_file`) into the aggregate; they open input tiles via the FUSE path (`tile.path`, external). For a managed table, `tile.path` is the managed FUSE path (Task 2 reader-2b), so managed tiles are read as external — matching the spec ("aggregators do not treat paths as managed").

- [ ] **Step 1: Write the failing test** — `rst_merge_agg` produces a Column with no `try_to_file` in its expression tree (so Spark 4 won't reject it in `.agg()`):

```python
def test_merge_agg_has_no_try_to_file():
    from databricks.labs.gbx.pyrx.functions import rst_merge_agg
    expr = rst_merge_agg("tile")._jc.toString()
    assert "try_to_file" not in expr.lower()
```

- [ ] **Step 2: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_core_agg.py::test_merge_agg_has_no_try_to_file` → FAIL (current code injects `file_ref_arg`).

- [ ] **Step 3: Drop the file-ref branch.** Change `rst_merge_agg` and `rst_combineavg_agg` to always use the FUSE-opening UDF (the current non-file `_merge_agg_udf` / `_combineavg_agg_udf`, which open via `tile.path`), removing the `if file_supported(): ... file_ref_arg(tc)` branch:

```python
def rst_merge_agg(tile: ColLike) -> Column:
    tc = _col(tile)
    return _as_tile_udf(_merge_agg_udf(tc))   # opens via FUSE (tile.path); external

def rst_combineavg_agg(tile: ColLike) -> Column:
    tc = _col(tile)
    return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(tc))
```

- [ ] **Step 4: Run** the test → PASS.

- [ ] **Step 5: Parity test** — `rst_merge_agg` over a materialized-tile group still produces a correct mosaic (assert the merged tile is non-null and covers the union envelope), confirming no regression from dropping the file branch.

- [ ] **Step 6: Run** `gbx:test:pyrx --path python/geobrix/test/pyrx/test_core_agg.py` → PASS.

- [ ] **Step 7: Commit** — `git commit -m "fix(pyrx): aggregators open FILE tiles as external (drop try_to_file in agg)"`.

---

## Task 9: On-cluster FILE validation (integration gate)

**Files:**
- Create: `prompts/testing/2026-08-18-increment2-oncluster-validation.py` (throwaway harness, gitignored)

**Interfaces:**
- Consumes: everything above; dogfood cluster `0813-214720-wo95qznu` (start it warm first; do NOT terminate mid-phase).

- [ ] **Step 1: Write the validation harness** (notebook-job on the warm cluster, mirroring `prompts/testing/2026-08-18-fileref-openmode-managed-vs-external-probe.py`). It must, over BOTH a real MANAGED and a real EXTERNAL FILE table (build small ones from a corpus COG via inline DDL + `create_file` / `try_to_file`):
  - **Reader-2b:** `read_file_table` on the managed table yields `tile.path` = the stripped managed uri and `path_mode == "managed"`; verify SQL `<file_col>.uri` subfield projection works on classic DBR-19. **If it does not**, record the fallback (plain `path` column, `path_mode="managed"`, opened as external) and note it for the spec.
  - **Pixel op:** `rst_clip_grouped` over the FILE table returns pixel-correct clips (compare against a scalar `rst_clip` baseline), exercising the FILE pixel fast path AND the graceful fallback (force `GBX_DISABLE_FILE=1` in a second pass — identical result).
  - **Generators:** `rst_retile` over the FILE table yields correct sub-tiles via the FILE stream.
  - **Aggregators:** `rst_merge_agg` over a grouped FILE table succeeds inside `.agg()` (no `AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`) and mosaics correctly.
  - **Stability:** a large (>`GBX_STREAM_MAX_BYTES`) source uses FUSE (not stream) and does not blow RSS; a source over `GBX_STAGE_MAX_BYTES` degrades gracefully.
- [ ] **Step 2: Ensure the wheel is current on-cluster.** Because these modules are unpushed, rebuild + stage the increment-2 wheel to the Volume first (`gbx:data:push-wheel` or the build+copy path) so the harness installs code that includes Tasks 2-8. Verify the installed wheel imports `rst_clip_grouped`.
- [ ] **Step 3: Run the harness** (background) and read the JSON verdict. Every check must pass or record a documented fallback.
- [ ] **Step 4: If reader-2b SQL uri-projection failed on-cluster**, apply the documented fallback in `file_table.py` (managed → plain `path`, `path_mode="managed"`, external open) and re-run the reader-2b check.
- [ ] **Step 5: Commit** any code fix from Step 4; record the validation result in the SDD ledger and the `file-for-raster-plan-ready` memory.

---

## Task 10: Documentation + re-benchmark

**Files:**
- Modify: `docs/docs/**/performance.mdx` (new section); the Tile Structure page (diagram); `docs/sidebars.js` only if a new page is added.
- Bench: the existing bench harness (`[[bench-spark-path-single-iter]]`, spark-path = 0 warmup / 1 measured).

- [ ] **Step 1: Re-benchmark virtual tiles with and without FILE** on the warm cluster (bench harness), capturing open-amortized read times for materialized vs virtual-EXTERNAL vs virtual-MANAGED at representative sizes. Save results under a gitignored bench-results path.
- [ ] **Step 2: Write the Performance-page section** — "how GeoBrix drives performance via virtual tiles + the FILE type": open-cost-dominated reads, per-partition open-amortization via the grouped executor, MANAGED ≈ EXTERNAL for read memory (both slurp on stream / both lazy on FUSE), the size-adaptive stream-vs-FUSE trade, path-aligned write layout (ORDER BY / CLUSTER BY path). Classify by SHAPE/family per existing conventions. **No internal vocabulary; no hard dates** ("DBR 19 is coming soon to Serverless with FILE support"). Backtick any bare `STRUCT<…>` in prose.
- [ ] **Step 3: Update the Tile Structure page diagram** to show the 9th field `path_mode` and its values (materialized→null / external / managed).
- [ ] **Step 4: Guard checks** — `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` prints nothing; run `gbx:docs:build` yourself and confirm no MDX/JSX crash; if a new page was added, wire it into `docs/sidebars.js`.
- [ ] **Step 5: Commit** — `git commit -m "docs(file): virtual-tiles + FILE performance section and tile-structure diagram"`.

---

## Self-Review

**Spec coverage:** Task 1 = spec §3 (heavy path_mode); Task 2 = §4 (reader-2b + tier-general note); Task 3 = §6.2 (M2 view contract); Task 4 = §6.3 (memory-aware stability, incl. `/vsicurl` correctly NOT pursued); Tasks 5-6 = §6.1 pixel ops; Task 7 = §6.1 generators; Task 8 = §5 (aggregators-as-external); Task 9 = §10 (on-cluster validation, incl. the reader-2b SQL-uri-projection open question); Task 10 = §7 (docs + re-bench). Spec §8 non-goals (existing-table ALTER, MANAGED↔EXTERNAL convert, heavy virtual ops, Serverless-GC FILE, follow-ups F1/F2) are intentionally excluded — no task, by design.

**Placeholder scan:** Task 6 uses an explicit enumeration step (a concrete grep) + a single shared wiring template for same-shape batch work (SDD-endorsed batching), not a "similar to Task N" hand-wave. Tasks 9-10 are validation/docs with concrete, checkable deliverables rather than TDD code, which is appropriate for their nature.

**Type consistency:** `V2_TILE_SCHEMA` (9 fields) is the produce of Task 1 (heavy) and the schema used by Tasks 5-7; `grouped_tile_map(..., view=)` is produced in Task 3 and consumed in Tasks 5-6; `file_ref_arg` is consumed in Task 7 and deliberately dropped in Task 8; `_strip_dbfs_scheme` / `path_mode="managed"` from Task 2 feed the aggregator external-read assumption in Task 8. Names are consistent across tasks.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-18-file-for-raster-increment2.md`. Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task, task review between tasks, broad review at the end.
2. **Inline Execution** — execute in this session with checkpoints.

Which approach?
