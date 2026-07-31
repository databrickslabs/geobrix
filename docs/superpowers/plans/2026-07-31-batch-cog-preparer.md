# Batch COG Preparer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a driver-orchestrated batch COG preparer (`prepare_cogs` over dir/file/list with live progress + summary) and an opt-in `cog_gbx` `driverMode` that routes conversion to the driver, escaping the ~1 GB per-PySpark-UDF cap that OOMs multi-GiB sources.

**Architecture:** Extend the existing Spark-free `pyrx/core/preparer.py` — `prepare_cog` gains an optional `out_name`; a new `_resolve_sources` normalizes dir/file/list input into a flat deduped file list; a new `prepare_cogs` loops over files (staging `/Volumes`→local, converting, cleaning up), prints live per-file progress, returns a summary dict. Then wire `cogSubdataset`/`cogSkipIfExists` into the default `cog_gbx` `write()` path, and add `driverMode` (write() gathers path refs on executors; commit() runs `prepare_cogs` on the driver).

**Tech Stack:** Python 3.12, rasterio (no `osgeo.gdal`), PySpark Python DataSource V2, Databricks Serverless (env v5), Docker dev container for local tests.

## Global Constraints

- **Exploratory / non-wired:** NO catalog registration, NO entries in `registered_functions.txt` / `function-info.json` / `functions.py` bindings / Scala. Keeps binding-parity + QC green.
- **Core file:** `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py` (Spark-free; must not import from the `ds` layer at module import — `ds → core` is the dependency direction, so any `ds._listing` use inside `preparer.py` is a lazy in-function import).
- **rasterio only** — never `osgeo.gdal`.
- **Public batch entry:** `prepare_cogs(sources, out_dir, blocksize=512, resampling="AVERAGE", compression="DEFLATE", subdataset=None, skip_if_exists=True, recursive=True, extensions=DEFAULT_RASTER_EXTS, verbose=True) -> dict`.
- **`sources` polymorphism:** a `str`/PathLike (dir OR file), or an iterable freely mixing dirs and files. Resolve each into files, flatten, de-dup preserving first-seen order. Missing path → non-fatal `error:not-found` record.
- **`DEFAULT_RASTER_EXTS = (".tif", ".tiff", ".cog", ".nc", ".h5", ".hdf")`** (case-insensitive). Directory listing filters by these; an explicitly-named file bypasses the filter. `extensions=None` disables filtering.
- **Output naming:** original source basename + `.cog` (never strip extension): `scene.tif` → `scene.tif.cog`. `prepare_cogs` passes the ORIGINAL basename via `prepare_cog(..., out_name=...)` even when the staged input is a temp file.
- **Staging (load-bearing):** GDAL cannot open a `/Volumes` (FUSE) striped TIFF directly. `prepare_cogs` stages each `/Volumes`/`dbfs:` source to a local temp (sequential `shutil.copyfileobj`, 8 MiB chunks), converts, deletes the temp — one file's footprint at a time. A plain local path is passed through without a redundant copy.
- **Per-file error isolation:** conversion failure → `(None, "error:<reason>")`, batch continues. Resolution failure (missing path) → `error:not-found` record, batch continues. Only a truly unusable call (e.g. `out_dir` uncreatable) may raise.
- **Return summary dict keys:** `total, ok, skipped, error, out_dir, peak_rss_mib, elapsed_s, results` where each `results` item is `{index, source, output_path, status, peak_rss_mib, elapsed_s}`.
- **Live print (verbose=True):** one line per file `[i/total] status source [→ out_name] (N left, Xs, peak M MiB)` with `flush=True`; a final `done: A ok, B skipped, C error of T (peak M MiB, Xs total)`. `verbose=False` suppresses all prints.
- **`cog_gbx` new options:** `cogSubdataset` (general, both paths), `cogSkipIfExists` (general, both paths, default `true`), `driverMode` (default `false`), `driverModeVerbose` (driverMode-only, default `true`). `recursive`/`extensions` NOT exposed on the writer (input is a pre-resolved DataFrame). Option-string booleans parsed case-insensitively (`"true"`/`"false"`).
- **`driverMode` mechanism:** `write(iterator)` on executors — if `driverMode`, gather `path` strings into `CogCommitMessage(paths=...)` and do NO conversion; else convert per-row (existing). `commit(messages)` on the driver — if `driverMode`, flatten all paths and call `prepare_cogs(paths, out_dir, ...)`.
- **Testing:** local-first in Docker via `bash scripts/commands/gbx-test-python.sh --path <path>`; Serverless is the final gate. Every commit message ends with a trailer line: `Co-authored-by: Isaac`.

## File Structure

- **Modify** `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py` — add `out_name` to `prepare_cog`; add `DEFAULT_RASTER_EXTS`, `_resolve_sources`, `prepare_cogs`.
- **Modify** `python/geobrix/test/pyrx/test_preparer.py` — append tests for `out_name`, `_resolve_sources`, `prepare_cogs`.
- **Modify** `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py` — thread `cog_subdataset`/`cog_skip_if_exists` into per-row `write()`; add `driver_mode`/`driver_mode_verbose`; branch `write()`/`commit()` on `driver_mode`.
- **Modify** `python/geobrix/src/databricks/labs/gbx/ds/cog.py` — parse the new options and pass to `CogGbxWriter`.
- **Modify** `python/geobrix/test/ds/test_cog_writer.py` — append tests for the new options + driverMode.
- **Create** `prompts/testing/2026-07-31-batch-cog-preparer-serverless.ipynb` — throwaway Serverless gate (untracked, gitignored `prompts/`).

**Execution order:** 1 → 2 → 3 → 4 → 5 → 6 (Task 4 default-path options before Task 5 driverMode; Task 5 needs `prepare_cogs` from Task 3).

---

### Task 1: `prepare_cog` gains optional `out_name`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`
- Test: `python/geobrix/test/pyrx/test_preparer.py`

**Interfaces:**
- Produces: `prepare_cog(path, out_dir, blocksize=512, resampling="AVERAGE", compression="DEFLATE", subdataset=None, skip_if_exists=True, out_name=None) -> (output_path: str|None, status: str)`. When `out_name` is given, output is `<out_dir>/<out_name>.cog`; when `None`, derives from `os.path.basename(path)` (existing behavior).

- [ ] **Step 1: Write the failing tests** (append to `test_preparer.py`)

```python
def test_prepare_cog_out_name_overrides_basename(tmp_path):
    src = tmp_path / "in" / "staged_tmp12345.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    # out_name mimics prepare_cogs passing the ORIGINAL basename while the input
    # path is a staged temp.
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, out_name="scene_original.tif"
    )
    assert status == "ok"
    assert out_path == str(out / "scene_original.tif.cog")
    assert os.path.exists(out_path)


def test_prepare_cog_out_name_none_uses_basename(tmp_path):
    src = tmp_path / "in" / "plain.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "plain.tif.cog")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t1.log`
Expected: FAIL (`prepare_cog() got an unexpected keyword argument 'out_name'`).

- [ ] **Step 3: Implement** — modify `prepare_cog` signature and the name line.

Change the signature to add `out_name: Optional[str] = None` (last param). Replace the name-derivation line:

```python
    # was: name = cog_output_name(os.path.basename(path))
    base = out_name if out_name is not None else os.path.basename(path)
    name = cog_output_name(base)
    out_path = os.path.join(out_dir, name)
```

Everything else in `prepare_cog` is unchanged. Update the docstring to note `out_name` overrides the basename used for the `.cog` output (for callers that stage to a temp but want the original name).

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t1.log`
Expected: PASS (existing tests + 2 new).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py python/geobrix/test/pyrx/test_preparer.py
git commit -m "feat(pyrx): prepare_cog gains optional out_name (original-source naming)"
```

---

### Task 2: `_resolve_sources` — normalize dir/file/list into a flat deduped file list

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`
- Test: `python/geobrix/test/pyrx/test_preparer.py`

**Interfaces:**
- Produces:
  - `DEFAULT_RASTER_EXTS = (".tif", ".tiff", ".cog", ".nc", ".h5", ".hdf")`
  - `_resolve_sources(sources, recursive=True, extensions=DEFAULT_RASTER_EXTS) -> list[tuple[str, Optional[str]]]` — returns a list of `(path, error)` pairs in first-seen order: `error=None` for a resolved existing file; `error="not-found"` for a listed path that does not exist. De-duped by path.

- [ ] **Step 1: Write the failing tests** (append)

```python
from databricks.labs.gbx.pyrx.core.preparer import _resolve_sources, DEFAULT_RASTER_EXTS


def _touch_tif(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    _write_src(str(p))


def test_resolve_single_file(tmp_path):
    f = tmp_path / "a.tif"
    _touch_tif(f)
    assert _resolve_sources(str(f)) == [(str(f), None)]


def test_resolve_dir_lists_rasters_recursive(tmp_path):
    _touch_tif(tmp_path / "a.tif")
    _touch_tif(tmp_path / "sub" / "b.tif")
    (tmp_path / "note.json").write_text("{}")  # non-raster excluded
    got = sorted(p for p, e in _resolve_sources(str(tmp_path)))
    assert got == sorted([str(tmp_path / "a.tif"), str(tmp_path / "sub" / "b.tif")])


def test_resolve_dir_non_recursive(tmp_path):
    _touch_tif(tmp_path / "a.tif")
    _touch_tif(tmp_path / "sub" / "b.tif")
    got = sorted(p for p, e in _resolve_sources(str(tmp_path), recursive=False))
    assert got == [str(tmp_path / "a.tif")]  # sub/ not descended


def test_resolve_list_mixes_files_and_dirs_dedup(tmp_path):
    _touch_tif(tmp_path / "d" / "a.tif")
    f = tmp_path / "d" / "a.tif"  # same file, also named explicitly
    _touch_tif(tmp_path / "standalone.tif")
    resolved = _resolve_sources([str(tmp_path / "d"), str(f), str(tmp_path / "standalone.tif")])
    paths = [p for p, e in resolved]
    # a.tif appears once (dir + explicit), plus standalone.tif
    assert paths.count(str(f)) == 1
    assert str(tmp_path / "standalone.tif") in paths


def test_resolve_missing_path_is_not_found(tmp_path):
    resolved = _resolve_sources(str(tmp_path / "nope.tif"))
    assert resolved == [(str(tmp_path / "nope.tif"), "not-found")]


def test_resolve_explicit_file_bypasses_extension_filter(tmp_path):
    weird = tmp_path / "data.bin"       # not in DEFAULT_RASTER_EXTS
    _touch_tif(weird)                    # but it IS a real GeoTIFF on disk
    # named explicitly → included despite extension
    assert _resolve_sources(str(weird)) == [(str(weird), None)]
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t2.log`
Expected: FAIL (`_resolve_sources` / `DEFAULT_RASTER_EXTS` undefined).

- [ ] **Step 3: Implement** (add to `preparer.py`)

```python
import os
from typing import List, Optional, Tuple  # ensure List/Tuple imported

DEFAULT_RASTER_EXTS = (".tif", ".tiff", ".cog", ".nc", ".h5", ".hdf")


def _has_ext(path: str, extensions) -> bool:
    if not extensions:
        return True
    return os.path.splitext(path)[1].lower() in {e.lower() for e in extensions}


def _resolve_sources(
    sources,
    recursive: bool = True,
    extensions=DEFAULT_RASTER_EXTS,
) -> List[Tuple[str, Optional[str]]]:
    """Normalize dir | file | iterable-of-both into a flat deduped [(path, error)].

    error=None for a resolved existing file; error="not-found" for a listed path
    that does not exist. A directory is listed (recursive by default) and
    extension-filtered; an explicitly-named file bypasses the filter. Scheme-
    qualified inputs (dbfs:/..., file:/...) are stripped via ds._listing.to_local_path.
    """
    from databricks.labs.gbx.ds._listing import to_local_path

    # Normalize to a list of items. A lone str/PathLike is one item.
    if isinstance(sources, (str, os.PathLike)):
        items = [sources]
    else:
        items = list(sources)

    out: List[Tuple[str, Optional[str]]] = []
    seen = set()

    def _add(path: str, err: Optional[str]) -> None:
        if path in seen:
            return
        seen.add(path)
        out.append((path, err))

    for item in items:
        local = to_local_path(str(item))
        if os.path.isfile(local):
            _add(local, None)  # explicit file — no extension filter
        elif os.path.isdir(local):
            if recursive:
                for root, _dirs, names in os.walk(local):
                    for name in sorted(names):
                        full = os.path.join(root, name)
                        if _has_ext(full, extensions):
                            _add(full, None)
            else:
                for name in sorted(os.listdir(local)):
                    full = os.path.join(local, name)
                    if os.path.isfile(full) and _has_ext(full, extensions):
                        _add(full, None)
        else:
            _add(local, "not-found")

    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t2.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py python/geobrix/test/pyrx/test_preparer.py
git commit -m "feat(pyrx): _resolve_sources normalizes dir/file/list to flat deduped files"
```

---

### Task 3: `prepare_cogs` — batch loop with staging, progress, summary

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`
- Test: `python/geobrix/test/pyrx/test_preparer.py`

**Interfaces:**
- Consumes: `_resolve_sources` (Task 2), `prepare_cog` with `out_name` (Task 1), `_peak_rss_mib` (existing).
- Produces: `prepare_cogs(sources, out_dir, blocksize=512, resampling="AVERAGE", compression="DEFLATE", subdataset=None, skip_if_exists=True, recursive=True, extensions=DEFAULT_RASTER_EXTS, verbose=True) -> Dict[str, object]` with keys `total, ok, skipped, error, out_dir, peak_rss_mib, elapsed_s, results` (each result: `{index, source, output_path, status, peak_rss_mib, elapsed_s}`).

- [ ] **Step 1: Write the failing tests** (append)

```python
from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs


def test_prepare_cogs_dir_summary_and_valid_cogs(tmp_path):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    _touch_tif(d / "b.tif")
    out = tmp_path / "out"
    summary = prepare_cogs(str(d), str(out), blocksize=256, verbose=False)
    assert summary["total"] == 2
    assert summary["ok"] == 2 and summary["skipped"] == 0 and summary["error"] == 0
    assert summary["out_dir"] == str(out)
    assert isinstance(summary["peak_rss_mib"], float)
    assert isinstance(summary["elapsed_s"], float)
    names = sorted(os.path.basename(r["output_path"]) for r in summary["results"])
    assert names == ["a.tif.cog", "b.tif.cog"]
    for r in summary["results"]:
        assert r["status"] == "ok"
        with open(r["output_path"], "rb") as fh:
            assert gbxcog.sniff_header(fh.read()).is_cog is True


def test_prepare_cogs_list_mixed_with_error_and_skip(tmp_path):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    out.mkdir()
    # Pre-create b's output so it is skipped.
    _touch_tif(tmp_path / "b.tif")
    (out / "b.tif.cog").write_bytes(b"sentinel")
    missing = str(tmp_path / "ghost.tif")
    summary = prepare_cogs(
        [str(d), str(tmp_path / "b.tif"), missing],
        str(out), blocksize=256, verbose=False,
    )
    by_status = {}
    for r in summary["results"]:
        by_status.setdefault(r["status"].split(":", 1)[0], []).append(r)
    assert summary["ok"] == 1        # a.tif
    assert summary["skipped"] == 1   # b.tif (pre-existing output)
    assert summary["error"] == 1     # ghost.tif not-found
    assert summary["total"] == 3
    # not-found surfaced as an error record with null output_path
    nf = [r for r in summary["results"] if r["status"] == "error:not-found"]
    assert len(nf) == 1 and nf[0]["output_path"] is None


def test_prepare_cogs_verbose_prints_progress(tmp_path, capsys):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    prepare_cogs(str(d), str(out), blocksize=256, verbose=True)
    captured = capsys.readouterr().out
    assert "[1/1]" in captured
    assert "done:" in captured


def test_prepare_cogs_verbose_false_silent(tmp_path, capsys):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    prepare_cogs(str(d), str(out), blocksize=256, verbose=False)
    assert capsys.readouterr().out == ""
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t3.log`
Expected: FAIL (`prepare_cogs` undefined).

- [ ] **Step 3: Implement** (add to `preparer.py`)

```python
import time


def _stage_local_if_needed(path: str) -> Tuple[str, bool]:
    """Return (local_path, is_temp). If path is already a plain local file, pass
    it through (is_temp=False). Otherwise (or always, for FUSE safety) copy it to
    a local temp via sequential copyfileobj and return (temp, True).

    GDAL cannot open a /Volumes FUSE striped TIFF directly; staging to local disk
    first is required. Heuristic: stage anything under /Volumes or /dbfs.
    """
    needs_stage = path.startswith("/Volumes") or path.startswith("/dbfs")
    if not needs_stage:
        return path, False
    fd, tmp = tempfile.mkstemp(suffix=os.path.splitext(path)[1] or ".tif")
    os.close(fd)
    with open(path, "rb") as _src, open(tmp, "wb") as _dst:
        shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
    return tmp, True


def prepare_cogs(
    sources,
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,
    skip_if_exists: bool = True,
    recursive: bool = True,
    extensions=DEFAULT_RASTER_EXTS,
    verbose: bool = True,
) -> Dict[str, object]:
    """Prepare one master COG per source, driver-side, with live progress + summary.

    ``sources`` may be a directory, a single file, or an iterable freely mixing
    dirs and files. See _resolve_sources for resolution rules. Returns a summary
    dict (keys: total, ok, skipped, error, out_dir, peak_rss_mib, elapsed_s, results).
    """
    os.makedirs(out_dir, exist_ok=True)
    resolved = _resolve_sources(sources, recursive=recursive, extensions=extensions)
    total = len(resolved)
    results: List[Dict[str, object]] = []
    counts = {"ok": 0, "skipped": 0, "error": 0}
    peak = 0.0
    t0 = time.time()

    for i, (src, err) in enumerate(resolved, start=1):
        f_t0 = time.time()
        if err == "not-found":
            status, out_path = "error:not-found", None
        else:
            original_base = os.path.basename(src)
            local_src, is_temp = _stage_local_if_needed(src)
            try:
                out_path, status = prepare_cog(
                    local_src, out_dir,
                    blocksize=blocksize, resampling=resampling,
                    compression=compression, subdataset=subdataset,
                    skip_if_exists=skip_if_exists, out_name=original_base,
                )
            finally:
                if is_temp and os.path.exists(local_src):
                    os.remove(local_src)
        f_elapsed = round(time.time() - f_t0, 2)
        rss = _peak_rss_mib()
        peak = max(peak, rss)
        key = status.split(":", 1)[0]
        counts[key] = counts.get(key, 0) + 1
        results.append({
            "index": i, "source": src, "output_path": out_path,
            "status": status, "peak_rss_mib": rss, "elapsed_s": f_elapsed,
        })
        if verbose:
            left = total - i
            arrow = f" -> {os.path.basename(out_path)}" if out_path else ""
            print(
                f"[{i}/{total}] {key:<7} {os.path.basename(src)}{arrow} "
                f"({left} left, {f_elapsed}s, peak {rss:.0f} MiB)",
                flush=True,
            )

    elapsed = round(time.time() - t0, 2)
    if verbose:
        print(
            f"done: {counts['ok']} ok, {counts['skipped']} skipped, "
            f"{counts['error']} error of {total} (peak {peak:.0f} MiB, {elapsed}s total)",
            flush=True,
        )
    return {
        "total": total,
        "ok": counts["ok"], "skipped": counts["skipped"], "error": counts["error"],
        "out_dir": out_dir, "peak_rss_mib": peak, "elapsed_s": elapsed,
        "results": results,
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log batch-t3.log`
Expected: PASS (all preparer tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py python/geobrix/test/pyrx/test_preparer.py
git commit -m "feat(pyrx): prepare_cogs batch preparer (dir/file/list, staging, progress, summary)"
```

---

### Task 4: Wire `cogSubdataset` + `cogSkipIfExists` into the default `cog_gbx` write() path

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/cog.py`
- Test: `python/geobrix/test/ds/test_cog_writer.py`

**Interfaces:**
- Consumes: `prepare_cog` core signature (with `subdataset`, `skip_if_exists` — already present) — but the writer's per-row path calls `cog_convert_file` directly today, so this task adds the skip/subdataset handling inline in `write()`.
- Produces: `CogGbxWriter.__init__` gains `cog_subdataset=None`, `cog_skip_if_exists=True`; `write()` honors both. `CogGbxDataSource.writer()` parses `cogSubdataset` and `cogSkipIfExists` options.

- [ ] **Step 1: Write the failing tests** (append to `test_cog_writer.py`)

```python
def test_writer_skip_if_exists_default_skips(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tif").write_bytes(b"sentinel")  # ext default "tif" → <stem>.tif
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=False, cog_blocksize=256,
                     cog_skip_if_exists=True)
    w.write(iter([{"path": str(src)}]))
    # untouched sentinel — skipped, not reconverted
    assert (out / "scene.tif").read_bytes() == b"sentinel"


def test_writer_skip_if_exists_false_reconverts(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tif").write_bytes(b"sentinel")
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=False, cog_blocksize=256,
                     cog_skip_if_exists=False)
    w.write(iter([{"path": str(src)}]))
    with open(out / "scene.tif", "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True  # real COG now
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py --log batch-t4.log`
Expected: FAIL (`cog_skip_if_exists` unexpected kwarg).

- [ ] **Step 3: Implement**

In `cog_writer.py` `CogGbxWriter.__init__`, add params `cog_subdataset=None`, `cog_skip_if_exists=True`; store as `self.cog_subdataset`, `self.cog_skip_if_exists`. In `write()`, inside the per-row loop, before conversion:

```python
            # Skip when the output already exists (idempotent resume).
            if self.cog_skip_if_exists and os.path.exists(out_path):
                written.append(out_path)
                continue
            # Build a NetCDF subdataset URI when requested.
            conv_src = src_volume
            if self.cog_subdataset:
                conv_src = f'NETCDF:"{src_volume}":{self.cog_subdataset}'
```

Then pass `conv_src` (not `src_volume`) to `cog_convert_file`.

In `cog.py` `writer()`, add to the `CogGbxWriter(...)` call:

```python
            cog_subdataset=self.options.get("cogSubdataset"),
            cog_skip_if_exists=self.options.get("cogSkipIfExists", "true").lower() == "true",
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py --log batch-t4.log`
Expected: PASS (existing writer tests + 2 new).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py python/geobrix/src/databricks/labs/gbx/ds/cog.py python/geobrix/test/ds/test_cog_writer.py
git commit -m "feat(ds): cog_gbx cogSubdataset + cogSkipIfExists options (both write paths)"
```

---

### Task 5: `cog_gbx` `driverMode` — write() gathers refs, commit() runs prepare_cogs on driver

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/cog.py`
- Test: `python/geobrix/test/ds/test_cog_writer.py`

**Interfaces:**
- Consumes: `prepare_cogs` (Task 3); `cog_subdataset`/`cog_skip_if_exists` (Task 4).
- Produces: `CogGbxWriter.__init__` gains `driver_mode=False`, `driver_mode_verbose=True`. `write()` — if `driver_mode`: collect row `path` strings, do NO conversion, return `CogCommitMessage(paths=[<source paths>])`. `commit()` — if `driver_mode`: flatten all messages' paths, call `prepare_cogs(paths, out_dir, ...)`. `CogGbxDataSource.writer()` parses `driverMode` + `driverModeVerbose`.

- [ ] **Step 1: Write the failing tests** (append)

```python
def test_driver_mode_write_gathers_paths_no_conversion(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256,
                     driver_mode=True)
    msg = w.write(iter([{"path": str(src)}]))
    # write() gathered the source path, produced NO .cog on the worker path
    assert list(msg.paths) == [str(src)]
    assert not glob.glob(os.path.join(str(out), "*"))


def test_driver_mode_commit_prepares_cogs(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256,
                     driver_mode=True, driver_mode_verbose=False)
    msg = w.write(iter([{"path": str(src)}]))
    w.commit([msg])
    produced = glob.glob(os.path.join(str(out), "*.cog"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True
    assert os.path.basename(produced[0]) == "scene.tif.cog"  # original-source naming


def test_default_mode_unchanged(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)  # driver_mode default False
    w.write(iter([{"path": str(src)}]))
    # default path converts in write() → <stem>.tif exists
    assert glob.glob(os.path.join(str(out), "*.tif"))
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py --log batch-t5.log`
Expected: FAIL (`driver_mode` unexpected kwarg).

- [ ] **Step 3: Implement**

In `CogGbxWriter.__init__`: add `driver_mode=False`, `driver_mode_verbose=True`; store on self.

In `write()`, branch at the top:

```python
    def write(self, iterator):
        if self.driver_mode:
            # Gather source path strings only — NO conversion on the executor
            # (cap-safe: no GDAL, no pixels). Conversion happens on the driver in commit().
            paths = [str(row["path"]) for row in iterator]
            return CogCommitMessage(paths=paths)
        # ... existing per-partition conversion path unchanged ...
```

In `commit()`, branch:

```python
    def commit(self, messages):
        if self.driver_mode:
            from databricks.labs.gbx.ds._listing import to_local_path
            from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs

            all_paths = []
            for m in messages:
                if isinstance(m, CogCommitMessage):
                    all_paths.extend(to_local_path(p) for p in m.paths)
            prepare_cogs(
                all_paths, self.out_dir,
                blocksize=self.cog_blocksize,
                resampling=self.cog_overview_resampling,
                compression=self.cog_compression,
                subdataset=self.cog_subdataset,
                skip_if_exists=self.cog_skip_if_exists,
                verbose=self.driver_mode_verbose,
            )
        return None
```

(Note: `commit()` currently returns `None` unconditionally; keep that for the default path. Output naming in driverMode is `<basename>.cog` via `prepare_cogs`, which differs from the default path's `<stem>.tif` — this is intentional per spec: driverMode is the prepare_cogs lane.)

In `cog.py` `writer()`, add:

```python
            driver_mode=self.options.get("driverMode", "false").lower() == "true",
            driver_mode_verbose=self.options.get("driverModeVerbose", "true").lower() == "true",
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_cog_writer.py --log batch-t5.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py python/geobrix/src/databricks/labs/gbx/ds/cog.py python/geobrix/test/ds/test_cog_writer.py
git commit -m "feat(ds): cog_gbx driverMode — commit() runs prepare_cogs on the driver"
```

---

### Task 6: Serverless gate notebook (throwaway, driver-side batch + writer driverMode)

**Files:**
- Create: `prompts/testing/2026-07-31-batch-cog-preparer-serverless.ipynb`

**Interfaces:**
- Consumes: `prepare_cogs`, `cog_gbx` writer `driverMode`, `file_gbx`, `run_notebooks_serverless.py`.
- Produces: (no code interface) a Serverless run proving multi-file driver-side batch prep AND writer driverMode both work on the real corpus.

> Deliverable is a runnable experiment; no local pytest gate. `prompts/` is gitignored — do NOT `git add` the notebook. Verification is (a) local dry-run of the core logic, (b) the Serverless run (final gate, run by the controller).

- [ ] **Step 1: Author the notebook** — native `.ipynb` JSON (no `# MAGIC`/jupytext mixing). Cells:

Cell 1 (markdown): purpose — "Batch prepare_cogs + cog_gbx driverMode on Serverless (driver-orchestrated, escapes the ~1GB UDF cap). Scaling-ladder knobs SIZE_PX / N_COPIES."

Cell 2a (code) — SCALING KNOBS + generate `N_COPIES` synthetic striped GeoTIFFs (each local-temp → copyfile to the Volume, one at a time; clear dirs first):

```python
import glob
import os
import shutil
import tempfile
import numpy as np
import rasterio
from rasterio.transform import from_origin

CORPUS = "/Volumes/geospatial_docs/geobrix/sample-data/large-raster/corpus"
OUT_DIR = "/Volumes/geospatial_docs/geobrix/sample-data/large-raster/out"
OUT_DIR2 = "/Volumes/geospatial_docs/geobrix/sample-data/large-raster/out_writer"

# ── SCALING LADDER KNOBS (set per rung) ─────────────────────────────────────
# A: 20000/1 (~1.5GiB)  B: 20000/10  C: 51500/1 (~10GiB)  D: 51500/5
SIZE_PX = 20000
N_COPIES = 1
ROW_CHUNK = 256  # windowed writes; ~SIZE_PX*256*4 bytes per chunk (never full array)

for _d in (CORPUS, OUT_DIR, OUT_DIR2):
    os.makedirs(_d, exist_ok=True)
    for _f in glob.glob(os.path.join(_d, "*")):
        try: os.remove(_f)
        except OSError: pass

profile = dict(driver="GTiff", width=SIZE_PX, height=SIZE_PX, count=1,
               dtype="float32", crs="EPSG:4326",
               transform=from_origin(-180.0, 90.0, 360.0 / SIZE_PX, 180.0 / SIZE_PX),
               tiled=False)  # striped — worst case

for k in range(N_COPIES):
    dst = os.path.join(CORPUS, f"synthetic_{SIZE_PX}px_{k:02d}.tif")
    _fd, local = tempfile.mkstemp(suffix=".tif"); os.close(_fd)
    with rasterio.open(local, "w", **profile) as ds:
        row = 0
        while row < SIZE_PX:
            h = min(ROW_CHUNK, SIZE_PX - row)
            ds.write((np.random.rand(1, h, SIZE_PX) * 1000).astype("float32"),
                     window=rasterio.windows.Window(0, row, SIZE_PX, h))
            row += h
    shutil.copyfile(local, dst)  # sequential → FUSE-safe
    os.remove(local)
    print(f"generated {dst} ({os.path.getsize(dst)/(1024**3):.2f} GiB)", flush=True)
```

Cell 2b (code) — batch over the corpus dir via `prepare_cogs`, capture summary:

```python
import json
from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs

summary = prepare_cogs(CORPUS, OUT_DIR, blocksize=512, verbose=True)
print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))
```

Cell 3 (code) — writer driverMode over the same corpus via `file_gbx`:

```python
from databricks.labs.gbx.ds.register import register
register(spark)

# OUT_DIR2 defined + cleared in Cell 2a.
files = spark.read.format("file_gbx").load(CORPUS)
(files.write.format("cog_gbx")
   .option("driverMode", "true")
   .option("cogSkipIfExists", "true")
   .mode("overwrite")
   .save(OUT_DIR2))
print("driverMode writer done")
```

Cell 4 (code) — validate + exit JSON:

```python
import glob
from databricks.labs.gbx.pyrx.core import cog as gbxcog

def _validate(d):
    out = []
    for p in glob.glob(d + "/*.cog"):
        with open(p, "rb") as fh:
            info = gbxcog.sniff_header(fh.read(1 << 20))
        out.append({"path": p, "is_cog": bool(info.is_cog), "overviews": int(info.overview_levels)})
    return out

result = {
    "batch_summary": {k: v for k, v in summary.items() if k != "results"},
    "batch_validated": _validate(OUT_DIR),
    "writer_validated": _validate(OUT_DIR2),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
```

- [ ] **Step 2: Local dry-run of the core** (Docker; validates prepare_cogs, not Spark)

Run:
```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 -c \"
import tempfile, os, numpy as np, rasterio
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs
d = tempfile.mkdtemp(); cor = os.path.join(d,'cor'); os.makedirs(cor)
p = dict(driver='GTiff',width=512,height=512,count=1,dtype='uint8',crs='EPSG:4326',transform=from_origin(0,60,0.01,0.01))
for n in ('a.tif','b.tif'):
    with rasterio.open(os.path.join(cor,n),'w',**p) as ds: ds.write(np.zeros((1,512,512),'uint8'))
s = prepare_cogs(cor, os.path.join(d,'out'), blocksize=256, verbose=True)
print('SUMMARY', s['total'], s['ok'], s['error'])
\""
```
Expected: two `[i/2] ok` lines + `done:` line + `SUMMARY 2 2 0`.

- [ ] **Step 3: Serverless run (FINAL GATE — controller runs it) — SCALING LADDER**

Prerequisites: wheel rebuilt from latest commit + staged (hash-verified staged==local, upload to `/Volumes/geospatial_docs/geobrix/sample-data/`); runner via a Python ≥3.11 interpreter (tomllib).

Run the ladder in order, **stop-on-first-fail** (each rung is its own run + its own recorded result). The notebook takes a `SIZE_PX` (single-file pixel side) and `N_COPIES` (how many corpus files) knob at the top of its generator cell; the generator writes `N_COPIES` distinct synthetic striped GeoTIFFs of `SIZE_PX²` into `CORPUS` (each generated to a driver-local temp then copyfile'd to the Volume — FUSE-safe, one at a time so gen never accumulates). Size math: float32, 1 band → `SIZE_PX²·4` bytes. `20000²·4 ≈ 1.49 GiB`; `51500²·4 ≈ 9.88 GiB ≈ 10 GiB`.

| Rung | SIZE_PX | N_COPIES | Approx per-file | Total corpus | Purpose |
|---|---|---|---|---|---|
| A | 20000 | 1 | 1.49 GiB | ~1.5 GiB | baseline (already proven driver-side); re-confirm via prepare_cogs + writer driverMode |
| B | 20000 | 10 | 1.49 GiB | ~14.9 GiB | 10× the baseline — batch loop over many large files, one-at-a-time footprint holds |
| C | 51500 | 1 | 9.88 GiB | ~9.9 GiB | single 10 GiB striped source — driver-memory headroom for GDAL's overview transient at 10 GiB |
| D | 51500 | 5 | 9.88 GiB | ~49.4 GiB | 5× 10 GiB — sustained batch at extreme scale |

For each rung, set `SIZE_PX`/`N_COPIES` in the notebook (Step 1 makes them top-of-cell knobs), then:
```bash
python notebooks/tests/run_notebooks_serverless.py \
  --notebook prompts/testing/2026-07-31-batch-cog-preparer-serverless.ipynb \
  --extras light
```
Expected PASS per rung: run SUCCEEDS; `notebook_output` `batch_summary` has `ok == N_COPIES`, `error == 0`; `batch_validated` + `writer_validated` list `is_cog=true` COGs; recorded `peak_rss_mib` stays within driver memory.

**Rung interpretation (all Serverless-preserving — the goal is to find the real driver-side ceiling, if any):**
- A/B PASS → driver-side batch prep is solid at 1.5 GiB scale and count.
- C is the key unknown: a 10 GiB striped source's GDAL overview-build transient may exceed even driver memory. If C OOMs, that bounds the single-file ceiling (record the peak RSS at failure) → pivot to the spec's bounded/shallow-overview fallback for >N GiB single files. B/D failing but A/C passing would instead implicate cumulative/temp-space effects (record disk + RSS).
- D PASS → sustained extreme-scale batch works; D fail after C pass → a batch-cumulative limit (temp cleanup, disk), not per-file memory.

**Corpus note:** rungs generate large data on the Volume. Between rungs, the notebook clears `CORPUS`/`OUT_DIR`/`OUT_DIR2` first (Task-6 cell already clears before generating) so a smaller prior rung's files don't inflate a later count. Rung D generates ~49 GiB transiently — confirm Volume capacity before running D.

- [ ] **Step 4: Record each rung's result** — after EACH rung, append the rung letter + PASS/FAIL + peak RSS + run id to `prompts/testing/2026-07-30-serverless-oom-rootcause.md`, and after the ladder completes update the `serverless-cog-preparer-scalar-udf` memory with the driver-side ceiling finding (the largest single-file size that PASSED, and where/if it broke). No code commit (notebook untracked).

---

## Self-Review

**1. Spec coverage:**
- `prepare_cogs(sources, ...)` dir/file/list → Task 2 (`_resolve_sources`) + Task 3. ✅
- `prepare_cog` `out_name` (original-source naming) → Task 1. ✅
- Staging one-file-at-a-time → Task 3 `_stage_local_if_needed`. ✅
- Live progress + summary dict (all keys) → Task 3. ✅
- Two-tier error isolation (not-found + conversion) → Task 3 tests. ✅
- `cogSubdataset` + `cogSkipIfExists` general (both paths) → Task 4 (default write) + Task 5 (driverMode via prepare_cogs). ✅
- `driverMode` (default false) + `driverModeVerbose` → Task 5. ✅
- `recursive`/`extensions` omitted on writer → not exposed in Task 4/5 cog.py parsing. ✅
- Non-wired → no registration touched in any task. ✅
- Serverless gate (both function + writer) → Task 6. ✅
- DEFAULT_RASTER_EXTS value → Task 2. ✅

**2. Placeholder scan:** No TBD/TODO in code steps. `<...>` only in run-time Volume paths (Task 6 prereqs name them explicitly). Clean.

**3. Type consistency:** `prepare_cog(..., out_name=None)` defined Task 1, consumed Task 3. `_resolve_sources -> list[(path, error)]` defined Task 2, consumed Task 3. `prepare_cogs(...) -> dict` keys consistent Task 3 ↔ Task 6. `CogGbxWriter` params `cog_subdataset`/`cog_skip_if_exists` (Task 4) + `driver_mode`/`driver_mode_verbose` (Task 5) match `cog.py` option parsing. `CogCommitMessage(paths=...)` already exists in cog_writer.py (used by Task 5). ✅
