# Serverless COG Preparer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an exploratory (non-wired) scalar-UDF COG preparer over the existing streaming core, plus a throwaway Serverless experiment that answers whether a generic Python-worker UDF can COG-convert a multi-GiB source where the `cog_gbx` DS-V2 writer OOM'd.

**Architecture:** A Spark-free core function `prepare_cog` (naming, subdataset URI, skip-if-exists, calls the existing `cog_convert_file`, per-call error isolation) is wrapped by an RSS-instrumented function `prepare_cog_measured` returning a driver-collectable dict. A throwaway notebook defines a scalar UDF around `prepare_cog_measured` and runs it distributed against the real VIIRS corpus on Serverless, collecting results on the driver.

**Tech Stack:** Python 3.12, rasterio (no `osgeo.gdal`), PySpark scalar UDF, Databricks Serverless (env v5), Docker dev container for local tests.

## Global Constraints

- **Exploratory / non-wired:** NO catalog registration, NO entries in `docs/tests-function-info/registered_functions.txt`, `function-info.json`, Python `functions.py` bindings, or Scala. Keeps binding-parity + QC green. Nothing ships from this pass.
- **Core lives at:** `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py` (Spark-free, mirrors the other `core/*.py` modules).
- **rasterio only** — never import `osgeo.gdal` (matches `cog_convert_file`).
- **Output naming:** full source basename **+ `.cog`** appended, never strip the source extension: `myfile1.tiff` → `myfile1.tiff.cog`.
- **`skip_if_exists=True` default:** name-only existence check (does `<out_dir>/<name>.cog` exist), NOT a validity check. `=False` = rebuild everything ("force write over all data").
- **Source scope:** any GDAL-readable single dataset; optional `subdataset` param builds a NetCDF subdataset URI; swath/warp-required sources excluded (out of scope).
- **Return contract:** `(output_path: str|None, peak_rss_mib: float|None, status: str)` where `status` ∈ `"ok"`, `"skipped"`, `"error:<reason>"`. Per-call failures return an `"error:"` status and do NOT raise (job survives). OOM is uncatchable and surfaces as a failed Spark task, not a status.
- **Reuse the existing core:** `cog_convert_file(src_path, dst_path, compression="DEFLATE", blocksize=512, overview_resampling="AVERAGE") -> None` in `pyrx/core/analysis.py`. Do not reimplement conversion.
- **FUSE-safe output write:** local temp COG → `shutil.copyfile` to `out_dir` (bytes-only, no chmod).
- **Testing:** local-first in Docker via `bash scripts/commands/gbx-test-python.sh --path <path>`; the Serverless experiment is the final gate (local green is necessary but not sufficient).

---

## File Structure

- **Create** `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py` — the Spark-free core: `cog_output_name`, `_subdataset_uri`, `prepare_cog`, `prepare_cog_measured`.
- **Create** `python/geobrix/test/pyrx/test_preparer.py` — local unit tests for the core.
- **Create** `prompts/testing/2026-07-31-serverless-cog-preparer-experiment.ipynb` — throwaway Serverless experiment notebook (gitignored under `prompts/`; not a `gbx:test:*` suite member).

---

### Task 1: Core `prepare_cog` (naming, subdataset, skip, convert, error isolation)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`
- Test: `python/geobrix/test/pyrx/test_preparer.py`

**Interfaces:**
- Consumes: `cog_convert_file(src_path, dst_path, compression, blocksize, overview_resampling) -> None` from `databricks.labs.gbx.pyrx.core.analysis`.
- Produces:
  - `cog_output_name(source_basename: str) -> str` — appends `.cog`.
  - `_subdataset_uri(path: str, subdataset: str | None) -> str` — NetCDF subdataset URI or the bare path.
  - `prepare_cog(path, out_dir, blocksize=512, resampling="AVERAGE", compression="DEFLATE", subdataset=None, skip_if_exists=True) -> tuple[str | None, str]` returning `(output_path, status)`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/pyrx/test_preparer.py
import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import cog as gbxcog
from databricks.labs.gbx.pyrx.core.preparer import (
    cog_output_name,
    _subdataset_uri,
    prepare_cog,
)


def _write_src(path, w=512, h=512):
    profile = dict(
        driver="GTiff", width=w, height=h, count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w))


def test_cog_output_name_appends_cog():
    assert cog_output_name("myfile1.tiff") == "myfile1.tiff.cog"
    assert cog_output_name("scene.tif") == "scene.tif.cog"
    assert cog_output_name("no_ext") == "no_ext.cog"


def test_subdataset_uri_bare_path_when_none():
    assert _subdataset_uri("/data/x.tif", None) == "/data/x.tif"


def test_subdataset_uri_builds_netcdf_uri():
    assert _subdataset_uri("/data/x.nc", "temp") == 'NETCDF:"/data/x.nc":temp'


def test_prepare_cog_produces_valid_cog_named_dot_cog(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "scene.tiff.cog")
    assert os.path.exists(out_path)
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1


def test_prepare_cog_skips_when_exists_default(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    # Pre-create the target so skip_if_exists (default True) short-circuits.
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel-not-a-real-cog")
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "skipped"
    assert out_path == str(target)
    # Untouched — still the sentinel bytes (no reconvert).
    assert target.read_bytes() == b"sentinel-not-a-real-cog"


def test_prepare_cog_force_rebuild_when_skip_false(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel")
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, skip_if_exists=False
    )
    assert status == "ok"
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True  # real COG now, sentinel overwritten


def test_prepare_cog_error_isolation_returns_status(tmp_path):
    out = tmp_path / "out"
    # Nonexistent source → convert fails; must return ('error:...') not raise.
    out_path, status = prepare_cog(str(tmp_path / "missing.tif"), str(out))
    assert out_path is None
    assert status.startswith("error:")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log preparer-t1.log`
Expected: FAIL (ImportError — `preparer` module / functions not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py
"""Exploratory COG file-preparation core (Spark-free).

Wraps the streaming ``cog_convert_file`` with output-naming, optional NetCDF
subdataset URIs, skip-if-exists idempotency, and per-call error isolation.
Callable directly from the driver (front-door B) and from a scalar UDF
(front-door A, defined in the throwaway experiment notebook). NON-WIRED: not
registered as a gbx_* function.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Optional, Tuple


def cog_output_name(source_basename: str) -> str:
    """Full source basename + ``.cog`` (extension preserved): x.tiff -> x.tiff.cog."""
    return f"{source_basename}.cog"


def _subdataset_uri(path: str, subdataset: Optional[str]) -> str:
    """Build a NetCDF subdataset URI when a subdataset is named, else the bare path.

    NetCDF is the primary multi-subdataset case in geobrix. HDF/GRIB users pass a
    complete GDAL subdataset URI as ``path`` with ``subdataset=None``.
    """
    if subdataset is None or str(subdataset).strip() == "":
        return path
    return f'NETCDF:"{path}":{subdataset}'


def prepare_cog(
    path: str,
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,
    skip_if_exists: bool = True,
) -> Tuple[Optional[str], str]:
    """Prepare ONE master COG from ``path`` into ``out_dir`` as ``<basename>.cog``.

    Returns ``(output_path, status)``:
      * ``("<out>/<name>.cog", "ok")``       — converted successfully
      * ``("<out>/<name>.cog", "skipped")``  — already existed, skip_if_exists=True
      * ``(None, "error:<reason>")``          — convert failed (does NOT raise)

    OOM is uncatchable and will kill the task rather than return "error:".
    """
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

    name = cog_output_name(os.path.basename(path))
    out_path = os.path.join(out_dir, name)

    if skip_if_exists and os.path.exists(out_path):
        return out_path, "skipped"

    src = _subdataset_uri(path, subdataset)
    try:
        os.makedirs(out_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(suffix=".cog")
        os.close(fd)
        try:
            cog_convert_file(
                src, tmp,
                compression=compression,
                blocksize=blocksize,
                overview_resampling=resampling,
            )
            shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        return out_path, "ok"
    except Exception as exc:  # noqa: BLE001 — per-row isolation is the contract
        return None, f"error:{type(exc).__name__}: {exc}"[:300]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log preparer-t1.log`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py python/geobrix/test/pyrx/test_preparer.py
git commit -m "feat(pyrx): exploratory prepare_cog core (naming, subdataset, skip, error isolation)"
```

---

### Task 2: RSS-instrumented `prepare_cog_measured` (driver-collectable struct)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`
- Test: `python/geobrix/test/pyrx/test_preparer.py`

**Interfaces:**
- Consumes: `prepare_cog(...) -> (output_path, status)` from Task 1.
- Produces: `prepare_cog_measured(path, out_dir, blocksize=512, resampling="AVERAGE", compression="DEFLATE", subdataset=None, skip_if_exists=True) -> dict` with keys `output_path: str|None`, `peak_rss_mib: float|None`, `status: str`. This is the exact per-row payload the scalar UDF returns (struct fields = dict keys).

- [ ] **Step 1: Write the failing tests**

```python
# append to python/geobrix/test/pyrx/test_preparer.py
from databricks.labs.gbx.pyrx.core.preparer import prepare_cog_measured


def test_prepare_cog_measured_ok_has_rss(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    r = prepare_cog_measured(str(src), str(out), blocksize=256)
    assert r["status"] == "ok"
    assert r["output_path"] == str(out / "scene.tiff.cog")
    assert isinstance(r["peak_rss_mib"], float) and r["peak_rss_mib"] > 0


def test_prepare_cog_measured_error_passthrough(tmp_path):
    out = tmp_path / "out"
    r = prepare_cog_measured(str(tmp_path / "missing.tif"), str(out))
    assert r["output_path"] is None
    assert r["status"].startswith("error:")
    # RSS is still reported (measured around the attempt).
    assert isinstance(r["peak_rss_mib"], float)


def test_prepare_cog_measured_skipped_status(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tiff.cog").write_bytes(b"sentinel")
    r = prepare_cog_measured(str(src), str(out))
    assert r["status"] == "skipped"
    assert r["output_path"] == str(out / "scene.tiff.cog")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log preparer-t2.log`
Expected: FAIL (`prepare_cog_measured` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# append to python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py
import sys
import resource
from typing import Dict


def _peak_rss_mib() -> float:
    """Process high-water RSS in MiB (darwin reports bytes, linux KiB)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def prepare_cog_measured(
    path: str,
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,
    skip_if_exists: bool = True,
) -> Dict[str, object]:
    """prepare_cog + peak-RSS capture, returning a driver-collectable dict.

    Keys: output_path (str|None), peak_rss_mib (float), status (str). This is the
    exact per-row payload the scalar UDF returns; RSS is captured on the DRIVER
    side by collecting this value (worker markers are unreliable on Serverless).
    """
    out_path, status = prepare_cog(
        path, out_dir,
        blocksize=blocksize,
        resampling=resampling,
        compression=compression,
        subdataset=subdataset,
        skip_if_exists=skip_if_exists,
    )
    return {
        "output_path": out_path,
        "peak_rss_mib": _peak_rss_mib(),
        "status": status,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_preparer.py --log preparer-t2.log`
Expected: PASS (9 tests total).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py python/geobrix/test/pyrx/test_preparer.py
git commit -m "feat(pyrx): prepare_cog_measured — driver-collectable RSS/status struct"
```

---

### Task 3: Throwaway Serverless experiment notebook

**Files:**
- Create: `prompts/testing/2026-07-31-serverless-cog-preparer-experiment.ipynb`

**Interfaces:**
- Consumes: `prepare_cog_measured(...)` from Task 2; `file_gbx` reader (`format("file_gbx")`); `run_notebooks_serverless.py` harness.
- Produces: (no code interface) a Serverless run whose driver-collected output records, per source file, `(output_path, peak_rss_mib, status)` and whether the ~1.5 GiB source cleared the ceiling.

> This task's deliverable is a runnable experiment, not a unit test. It has no local `pytest` gate — its verification is (a) the notebook's cells execute logically against a small local file when dry-run, and (b) the Serverless run (final gate, run separately with the user-provided corpus path). Because `prompts/` is gitignored, the notebook is committed only via an explicit `git add -f` if the user wants it tracked; default is to leave it local.

- [ ] **Step 1: Author the experiment notebook**

Create `prompts/testing/2026-07-31-serverless-cog-preparer-experiment.ipynb` as a **native `.ipynb`** (do NOT mix `# MAGIC` jupytext markers with ipynb JSON — write clean notebook JSON directly). Cells:

Cell 1 (markdown): title + purpose — "Does a scalar UDF clear the DS-V2 write ceiling for multi-GiB COG on Serverless?"

Cell 2 (code) — imports + register + define the scalar UDF wrapping `prepare_cog_measured`:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx.core.preparer import prepare_cog_measured

register(spark)

# NON-WIRED: UDF defined inline here, NOT registered in the gbx_* catalog.
_PREP_SCHEMA = StructType([
    StructField("output_path", StringType(), True),
    StructField("peak_rss_mib", DoubleType(), True),
    StructField("status", StringType(), True),
])

OUT_DIR = "/Volumes/<catalog>/<schema>/<volume>/cog-preparer-experiment"  # set at run time

@F.udf(_PREP_SCHEMA)
def prepare_cog_udf(path):
    return prepare_cog_measured(path, OUT_DIR, blocksize=512)
```

Cell 3 (markdown): "Set CORPUS to the VIIRS ~1.5 GiB striped GeoTIFF Volume dir (user provides at run time)."

Cell 4 (code) — list corpus with `file_gbx`, run the UDF distributed, collect on the driver:

```python
CORPUS = "/Volumes/<catalog>/<schema>/<volume>/<viirs-dir>"  # set at run time

files = spark.read.format("file_gbx").load(CORPUS)
prepared = files.withColumn("r", prepare_cog_udf(F.col("path")))
# .collect() forces distributed execution and returns results to the DRIVER
# (reliable capture — no worker markers).
rows = prepared.select("path", "r.*").collect()
for row in rows:
    print(row["path"], "->", row["status"], f"{row['peak_rss_mib']:.0f} MiB", row["output_path"])
```

Cell 5 (code) — validate any produced COG on the driver:

```python
import glob
from databricks.labs.gbx.pyrx.core import cog as gbxcog

produced = glob.glob(OUT_DIR + "/*.cog")
print(f"produced {len(produced)} .cog files")
for p in produced:
    with open(p, "rb") as fh:
        info = gbxcog.sniff_header(fh.read(1 << 20))  # header only
    print(p, "is_cog=", info.is_cog, "overviews=", info.overview_levels)
```

- [ ] **Step 2: Local dry-run of the notebook's UDF logic**

Do NOT run the notebook on Serverless yet. Verify the UDF body logic against a small local file, in Docker:

Run:
```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 -c \"
import tempfile, os, numpy as np, rasterio
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx.core.preparer import prepare_cog_measured
d = tempfile.mkdtemp(); src = os.path.join(d,'s.tiff')
p = dict(driver='GTiff',width=512,height=512,count=1,dtype='uint8',crs='EPSG:4326',transform=from_origin(0,60,0.01,0.01))
import numpy as np
with rasterio.open(src,'w',**p) as ds: ds.write(np.zeros((1,512,512),'uint8'))
out = os.path.join(d,'out')
print(prepare_cog_measured(src, out, blocksize=256))
\""
```
Expected: prints a dict with `status='ok'`, a `.cog` path, and a positive `peak_rss_mib`.

- [ ] **Step 3: Serverless run (FINAL GATE — run with user-provided corpus path)**

Prerequisites (confirm with the user before running): (a) wheel rebuilt from the latest commit and staged to the Volume with hash verified staged==local; (b) the VIIRS corpus Volume path; (c) a writable `OUT_DIR` Volume path. Fill `CORPUS` and `OUT_DIR` in the notebook, then:

Run:
```bash
python notebooks/tests/run_notebooks_serverless.py \
  --notebook prompts/testing/2026-07-31-serverless-cog-preparer-experiment.ipynb \
  --extras light
```
Expected outcomes:
- **PASS:** run SUCCEEDS; driver output shows `status="ok"` for the ~1.5 GiB source, a valid `.cog` (is_cog=True), and a recorded `peak_rss_mib`. → distributed Serverless COG prep works; no classic needed.
- **FAIL:** run fails with `UDF_PYSPARK_ERROR.OOM` (task killed, no `"error:"` status). → the generic-UDF sandbox shares the DS-V2 ceiling; pivot to the spec's fallbacks (driver `prepare_cog`, then bounded/shallow overviews, then decimated master).

- [ ] **Step 4: Record the result**

Append the outcome (PASS/FAIL, peak RSS, run id/url) to `prompts/testing/2026-07-30-serverless-oom-rootcause.md`, and update the memory `serverless-cog-preparer-scalar-udf` with the finding. No code commit unless the notebook is being tracked (`git add -f`).

---

## Self-Review

**1. Spec coverage:**
- Scalar UDF front-door (subject) → Tasks 1+2 (core) + Task 3 (UDF in notebook). ✅
- Shared core reuse of `cog_convert_file` → Task 1 consumes it. ✅
- Format-agnostic + `subdataset` param → Task 1 `_subdataset_uri`. ✅ (swath exclusion is a non-goal, no code.)
- `<name>.cog` naming → Task 1 `cog_output_name`. ✅
- `skip_if_exists` default-True name-only / `=False` rebuild → Task 1 tests. ✅
- Return struct `(output_path, peak_rss_mib, status)` + per-row error isolation + OOM-as-run-state → Task 2 + Task 3 pass/fail. ✅
- Driver-collected RSS via `.collect()` → Task 3 Cell 4. ✅
- FUSE-safe output write → Task 1 `shutil.copyfile`. ✅
- Non-wired (no registration/bindings) → Global Constraints + Task 3 inline UDF. ✅
- Local-first then Serverless gate → Task 1/2 Docker pytest, Task 3 Step 3. ✅
- Front-door B (driver `prepare_cog`) → `prepare_cog` IS the driver-callable helper (Task 1); no separate task needed since it's the same function. ✅
- Fallbacks → spec non-goals / documented next-steps; not built this pass (correct — gated on FAIL). ✅

**2. Placeholder scan:** `<catalog>/<schema>/<volume>` and `CORPUS`/`OUT_DIR` are run-time values the user supplies at the Serverless gate (Task 3 explicitly calls this out), not plan placeholders — acceptable. No TBD/TODO in code steps.

**3. Type consistency:** `prepare_cog` returns `(output_path, status)` tuple in Task 1 and is consumed as such in Task 2; `prepare_cog_measured` returns dict with keys `output_path/peak_rss_mib/status` matching the Task 3 struct fields exactly. `cog_output_name` used consistently. ✅
