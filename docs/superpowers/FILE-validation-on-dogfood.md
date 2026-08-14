# FILE Validation Runbook — DBR 19 Dedicated Cluster (Dogfood)

**Purpose:** Manually validate FILE-path read behavior on a FILE-enabled classic DBR 19.x
dedicated single-user cluster. Runs once per release as a post-release smoke test.
**Not a CI gate.** CI covers only the fallback path (FILE absent).

---

## CRITICAL: Sentinel Prerequisite

**Before doing anything else, confirm the sentinel file is accessible on the cluster.**

`file_supported()` works by running an end-to-end roundtrip that mints a `try_to_file` expression
in the Spark plan and consumes it in a UDF. The sentinel path is hardcoded in
`python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py` as:

```
/Volumes/main/geobrix_samples/geobrix-examples/london/LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF
```

Exact sentinel logic from `_check_file_support()`:

```python
sentinel_path = (
    "/Volumes/main/geobrix_samples/geobrix-examples/london/"
    "LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
)
df_with_fref = spark.sql(f"SELECT try_to_file('{sentinel_path}') AS fref")

@F.udf("string")
def _consume_fref(fref):
    with fref.open() as f:
        byte_read = f.read(1)
    return "success" if byte_read else "empty"

result_df = df_with_fref.select(_consume_fref(F.col("fref")))
result = result_df.collect()[0][0]
# Returns True only if result == "success"
```

**If this sentinel file does not exist or is not readable on the validation cluster,
`file_supported()` will return `False` and the FILE path stays dormant for the entire
session. The whole validation will be inconclusive.**

Step 0 of this runbook is to confirm the sentinel exists and is readable. If it does not
exist, either:
- Stage the eo-series sample bundle to the cluster's Volume (see Prerequisites), or
- Update the sentinel path in `_file_ref.py` to a path that does exist, and rebuild the wheel.

---

## Prerequisites

1. **Access to dogfood cluster** — internal Databricks workspace with `fileReferenceCreationMode=MANAGED`
   in spark_conf (this is set on dogfood by default; classic dedicated single-user only).
   **Serverless is NOT a valid target** — the Spark Connect client in serverless-GC does not
   yet surface the FILE type to the Python layer; `file_supported()` returns False there.

2. **GeoBrix wheel built and staged** to the cluster's accessible Volume. For example:
   ```
   /Volumes/main/geobrix_samples/geobrix-current.whl
   ```
   Adapt this path to wherever the release wheel was staged for this release cycle.

3. **Sample raster data (eo-series)** available at the sentinel path and for the pixel
   equality test. The essential bundle suffices:
   ```
   gbx:data:download --bundle essential
   gbx:data:push-wheel --profile oauth-fe
   ```
   Confirm the sentinel file exists on the cluster:
   ```python
   import os
   sentinel = (
       "/Volumes/main/geobrix_samples/geobrix-examples/london/"
       "LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
   )
   assert os.path.exists(sentinel), f"Sentinel not found: {sentinel}"
   print("Sentinel OK")
   ```

---

## Steps

### Step 1 — Provision the Cluster

| Setting | Value |
|---------|-------|
| Cluster name | `geobrix-file-validation` (or any ad-hoc name) |
| Runtime | Databricks Runtime 19.x, latest stable |
| Mode | **Single-user dedicated** (required for FILE) — NOT serverless |
| Workers | 2–4 workers (small; validation only) |
| Spark conf | `spark.databricks.fileReferenceCreationMode MANAGED` |

Install the wheel in a notebook cell or as a cluster library:

```python
%pip install /Volumes/main/geobrix_samples/geobrix-current.whl
# Adapt path to wherever the release wheel is staged.
dbutils.library.restartPython()
```

### Step 2 — Run the Validation Notebook

Create an ad-hoc notebook on the provisioned cluster and run these cells in order.

---

**Cell 0 — Confirm sentinel file exists**

```python
import os

sentinel_path = (
    "/Volumes/main/geobrix_samples/geobrix-examples/london/"
    "LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
)
assert os.path.exists(sentinel_path), (
    f"SENTINEL NOT FOUND: {sentinel_path}\n"
    "file_supported() will return False. Stage the sample bundle before continuing.\n"
    "Run: gbx:data:download --bundle essential && gbx:data:push-wheel --profile oauth-fe"
)
print(f"Sentinel OK: {sentinel_path}")
```

---

**Cell 1 — Feature-detect confirmation**

```python
from databricks.labs.gbx.pyrx._file_ref import file_supported

result = file_supported()
# file_supported() takes NO arguments — it calls SparkSession.getActiveSession() internally.
# Signature: file_supported() -> bool

print(f"FILE support detected: {result}")

if not result:
    raise AssertionError(
        "FILE not detected. Possible causes:\n"
        "  1. GBX_DISABLE_FILE=1 is set in the environment\n"
        "  2. Cluster is NOT classic dedicated single-user with fileReferenceCreationMode=MANAGED\n"
        "  3. Sentinel file not readable (run Cell 0 first)\n"
        "  4. SparkSession not active\n"
        "Do NOT continue — the FILE path is dormant and the test is inconclusive."
    )

print("SUCCESS: FILE support confirmed. Continuing.")
```

---

**Cell 2 — Load eo-series tile and create virtual tiles (FILE path active)**

```python
from databricks.labs.gbx import pyrx

sample_path = (
    "/Volumes/main/geobrix_samples/geobrix-examples/london/"
    "LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
)
# Adapt sample_path to a raster file that exists on the validation cluster.
# The sentinel file (used above) is also a valid choice.

df = pyrx.rst_fromfile(f"'{sample_path}'", "GTiff")
df_pd = df.toPandas()

assert len(df_pd) >= 1, f"Expected at least 1 tile, got {len(df_pd)}"
print(f"Virtual tiles created: {len(df_pd)} rows")
print(df_pd[["source", "tile"]].head())
```

---

**Cell 3 — Pixel equality: FILE path vs fallback path**

This cell exercises `open_tile` directly with both a real FileRef (FILE path) and
`file_ref=None` (fallback path), then asserts pixel equality.

**Note:** FileRef is obtained by collecting from a Spark SQL result — there is no
`FileRef(path)` constructor. The FILE column is minted in the Spark plan via
`try_to_file` and arrives at the Python driver as a FileRef object via `.collect()`.

```python
from databricks.labs.gbx.pyrx.core.open_tile import open_tile
from databricks.labs.gbx.pyrx._serde import _deserialize_tile
import numpy as np

# Get the first tile struct from the DataFrame.
tile_row = df_pd.iloc[0]["tile"]

# Deserialize to VirtualTile (internal representation used by open_tile).
# open_tile accepts a dict-like tile row; adapt as needed for the internal API.
tile = _deserialize_tile(tile_row)

# -- FALLBACK PATH (file_ref=None; plain /Volumes path read) --
with open_tile(tile, file_ref=None) as ds_fallback:
    w = tile.window  # (col_off, row_off, width, height)
    from rasterio.windows import Window
    pixels_fallback = ds_fallback.read(
        1,
        window=Window(w[0], w[1], min(w[2], 100), min(w[3], 100))
    )
print(f"Fallback path — shape: {pixels_fallback.shape}, dtype: {pixels_fallback.dtype}")

# -- FILE PATH (real FileRef minted via try_to_file in the Spark plan) --
fref_row = spark.sql(
    f"SELECT try_to_file('{sample_path}') AS fref"
).collect()[0]
real_fref = fref_row.fref

with open_tile(tile, file_ref=real_fref) as ds_file:
    pixels_file = ds_file.read(
        1,
        window=Window(w[0], w[1], min(w[2], 100), min(w[3], 100))
    )
print(f"FILE path — shape: {pixels_file.shape}, dtype: {pixels_file.dtype}")

# -- PIXEL EQUALITY ASSERTION --
assert pixels_fallback.shape == pixels_file.shape, (
    f"Shape mismatch: fallback={pixels_fallback.shape}, file={pixels_file.shape}"
)
assert np.array_equal(pixels_fallback, pixels_file), (
    "Pixel data differs between FILE path and fallback path.\n"
    f"Fallback sample: {pixels_fallback[:3, :3]}\n"
    f"FILE sample:     {pixels_file[:3, :3]}"
)
print("SUCCESS: FILE path and fallback path produce identical pixels.")
```

---

**Cell 4 — Rewired accessor smoke test (rst_height, rst_metadata, rst_numbands)**

These functions go through `file_ref_arg()` in the binding layer and exercise the
full FILE-wired code path when `file_supported()` is True.

```python
from pyspark.sql import functions as F

df_tiles = pyrx.rst_fromfile(f"'{sample_path}'", "GTiff")

# rst_height: returns the pixel height of each tile.
df_heights = df_tiles.select(
    pyrx.rst_height(F.col("tile")).alias("height")
)
height_val = df_heights.collect()[0]["height"]
assert height_val is not None and height_val > 0, (
    f"Expected a positive height, got: {height_val}"
)
print(f"rst_height: {height_val}")

# rst_metadata: returns driver-level metadata as a map.
df_meta = df_tiles.select(
    pyrx.rst_metadata(F.col("tile")).alias("metadata")
)
metadata_val = df_meta.collect()[0]["metadata"]
assert metadata_val is not None, "rst_metadata returned None"
print(f"rst_metadata keys: {list(metadata_val.keys())}")

# rst_numbands: returns band count.
df_bands = df_tiles.select(
    pyrx.rst_numbands(F.col("tile")).alias("numbands")
)
numbands_val = df_bands.collect()[0]["numbands"]
assert numbands_val is not None and numbands_val >= 1, (
    f"Expected band count >= 1, got: {numbands_val}"
)
print(f"rst_numbands: {numbands_val}")

print("SUCCESS: FILE-wired accessors (rst_height, rst_metadata, rst_numbands) returned valid data.")
```

---

**Cell 5 — Verify env override disables FILE**

Confirms `GBX_DISABLE_FILE=1` suppresses FILE even when the cluster supports it.

```python
import os
from databricks.labs.gbx.pyrx import _file_ref as _fr

# Save and set override.
_original = os.environ.get("GBX_DISABLE_FILE")
os.environ["GBX_DISABLE_FILE"] = "1"

# Clear the memoization cache so the next call re-evaluates.
_fr._FILE_SUPPORT_CACHE.clear()

result_overridden = _fr.file_supported()
assert not result_overridden, "Expected False when GBX_DISABLE_FILE=1"
print("GBX_DISABLE_FILE=1 correctly suppresses FILE detection.")

# Restore env and cache.
if _original is None:
    del os.environ["GBX_DISABLE_FILE"]
else:
    os.environ["GBX_DISABLE_FILE"] = _original
_fr._FILE_SUPPORT_CACHE.clear()
# Re-warm the cache so subsequent cells see FILE=True again.
_ = _fr.file_supported()
print("Cache restored. Validation complete.")
```

---

### Step 3 — Validation Checklist

Check off each item before signing off:

- [ ] Cluster provisioned with DBR 19.x, classic dedicated single-user,
      `fileReferenceCreationMode=MANAGED` in spark_conf.
- [ ] GeoBrix wheel installed without errors (`%pip install ... && dbutils.library.restartPython()`).
- [ ] **Cell 0:** Sentinel file exists and is readable at the exact hardcoded path.
- [ ] **Cell 1:** `file_supported()` returns `True`. (If False, STOP — the rest is inconclusive.)
- [ ] **Cell 2:** `rst_fromfile` produces at least 1 virtual tile row.
- [ ] **Cell 3:** FILE path and fallback path produce pixel-identical results.
- [ ] **Cell 4:** `rst_height`, `rst_metadata`, `rst_numbands` return valid non-null data.
- [ ] **Cell 5:** `GBX_DISABLE_FILE=1` env override correctly suppresses FILE detection.

### Step 4 — Signoff

If all checklist items pass:

- The FILE byte-range read path is validated end-to-end on a FILE-enabled DBR 19.x cluster.
- The fallback path (FILE absent) is unchanged and still tested in CI.
- No user-facing API change; all behavior is backward-compatible.
- Note in the release log: "FILE validation on dogfood passed for release X.Y.Z."

Known asymmetry: CI tests validate only the **fallback path** (FILE absent). The
FILE-specific optimization (byte-range reads from FileRef) is exercised only in this
manual runbook, because CI clusters do not have `fileReferenceCreationMode=MANAGED`.

### Step 5 — Teardown

1. Terminate the validation cluster when done (do not leave it running idle).
2. Archive or export the notebook to `prompts/validation/` if you want a local reference
   (that directory is gitignored; the runbook itself is version-controlled here).
3. No code changes are required unless a cell fails — if it does, open an issue and record
   the failure details before terminating the cluster.

---

## Reference: sentinel path and feature-detect mechanics

Source: `python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py`

| Detail | Value |
|--------|-------|
| Sentinel file path | `/Volumes/main/geobrix_samples/geobrix-examples/london/LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF` |
| Feature-detect function | `file_supported()` — no arguments; uses `SparkSession.getActiveSession()` |
| Memoization | Per SparkSession (`id(spark)` → bool cache); runs roundtrip at most once |
| Env override | `GBX_DISABLE_FILE=1` → immediately returns False (no Spark touched) |
| Roundtrip | `spark.sql("SELECT try_to_file('<sentinel>') AS fref")` + UDF consuming `fref.open().read(1)` |
| FileRef constructor | **Does not exist** — FileRef is only minted via `try_to_file` in the Spark plan |
| Valid cluster target | Classic dedicated single-user DBR 19.x with `fileReferenceCreationMode=MANAGED` |
| Invalid targets | Serverless (FILE type not surfaced by Spark Connect client layer) |
