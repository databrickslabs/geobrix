# Light `netcdf_gbx` writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Serverless-safe light `netcdf_gbx` WRITER — the symmetric inverse of the light `netcdf_gbx` reader — with a raster mode (grid tiles → CF grid NetCDF, one `.nc` per row) and a vector mode (points → CF Discrete Sampling Geometry NetCDF, one `.nc` per partition), plus a light-only writer throughput benchmark and a reader `_FillValue` cross-tier parity defense.

**Architecture:** A `writer(schema, overwrite)` method on `NetcdfGbxDataSource` dispatches on the `mode` option (same as the reader) to one of two `DataSourceWriter`s in a new `ds/_write_netcdf.py`. Both use the DataSource V2 `write(iterator)` per-partition path (no pandas_udf), encode with `netCDF4.Dataset` to a worker-local temp file, then `shutil.copyfile` to the scheme-stripped `/Volumes` path. Raster inverts `_netcdf.grid_transform_crs`/`array_2d`; vector inverts `point_arrays`.

**Tech Stack:** Python 3.12 / PySpark DataSource V2 / `netCDF4` (encode) / `rasterio` (decode tiles) / `shapely` (point WKB) / `numpy`. Tests: pytest (local Spark) in Docker; cluster jobs.submit for the writer bench (human-gated).

## Global Constraints

- **Serverless-safe:** NO `spark.conf.set`, `_jvm`, `.rdd`, `cache`, `persist` in the writer module. The DataSource V2 `write(iterator)` path is per-partition Python; no Spark-config mutation.
- **FUSE write discipline** (`volumes-cleanpath-bare-not-file`): strip the path scheme with `_listing.to_local_path(path)` in the constructor; `netCDF4.Dataset` writes to a worker-local `tempfile` (it needs random-access construction — writing directly to a Volume FUSE path can corrupt); move to the Volume with `shutil.copyfile` (NOT `shutil.copy`/`copy2` — they `chmod` and FUSE rejects it; NOT `os.rename` — unreliable on FUSE).
- **No aliases** — one canonical writer per format; `mode` option selects raster/vector (mirrors the reader).
- **Mirror the existing writer** (`ds/writer.py` `RasterGbxWriter`): `write(iterator)` per partition, `commit` no-op, `abort` deletes written paths; `overwrite` globs+removes stale `*.nc` under the scheme-stripped path.
- **Round-trip is the primary test gate:** write → re-read with the reader → assert values/CRS/transform/nodata (raster) and lon/lat/attrs (vector) match.
- **No CI plumbing change:** `netcdf4` is already in `requirements-pyrx-ci.in` + `pyproject.toml [light]`; `test/ds/` is already in `_LIGHT_TEST_DIRS` and both CI action lists. Do NOT add dependency/test-dir entries.
- **Docker:** Python tests run via `bash scripts/commands/gbx-test-python.sh --path <path> --log <name>.log` in the `geobrix-dev` container. RUN TESTS SYNCHRONOUSLY to completion (Python tests are seconds); do NOT background-monitor and return early. Lint via `gbx-lint-python.sh --fix` then verify Docker `--check` (host/Docker black can differ — Docker is authoritative).
- **Commit mechanics** (a repo pre-commit guard false-positives on piping + leading-dash tokens): write each commit message to a temp file and `git commit -F /tmp/<name>.txt`; do NOT pipe the commit through `tail`/`2>&1`; do NOT put leading-dash tokens in the message body. End every message with the `Co-authored-by: Isaac` trailer. Do NOT push. Do NOT switch git accounts.
- **Wheel:** after the light package change, rebuild + restage the wheel (`whl-change-rebuild-and-stage`) — done in the bench task (Task 6) before the cluster run.

---

### Task 1: Raster writer — `NetcdfRasterGbxWriter` + `NetcdfGbxDataSource.writer` dispatch

Grid tile → CF grid NetCDF, one `.nc` per row. Inverts the raster reader.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` (add `writer()` dispatch)
- Test: `python/geobrix/test/ds/test_netcdf_writer.py`

**Interfaces:**
- Consumes: `ds.writer.assert_write_schema(schema)` (exact `(source, tile)`); `ds._listing.to_local_path(path)`; `rasterio.io.MemoryFile`; `netCDF4.Dataset`. Reader inverse target: `_netcdf.grid_transform_crs` (Affine: `ulx=min(lons)-px/2`, `uly=max(lats)+py/2`) and `_netcdf.array_2d` (north-up).
- Produces: `NetcdfRasterGbxWriter(options: dict, schema: StructType, overwrite: bool)` with `write(iterator) -> NetcdfCommitMessage(paths=[...])`, `commit`, `abort`. `NetcdfGbxDataSource.writer(schema, overwrite)` returns it for `mode="raster"`. Output var name = parsed from `source` selector `NETCDF:"…":var`, else `varNameCol` option, else `"data"`. Filename = `nameCol` (basename), else var name, else content-hash+uuid.

- [ ] **Step 1: Write the failing raster round-trip test**

`python/geobrix/test/ds/test_netcdf_writer.py`. Build a known grid, read it via `netcdf_gbx`, write via `netcdf_gbx`, re-read, compare. Reuse the `_write_regular_grid` helper pattern from `test_netcdf_datasource.py`.

```python
import numpy as np
import pytest
from netCDF4 import Dataset
from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource


def _write_regular_grid(path, var="ch4"):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3); ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",)); lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",)); lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]; lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable(var, "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_raster_write_roundtrip(spark, tmp_path):
    src = tmp_path / "in.nc"; _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(src))          # (source, tile), 1 grid var
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    # re-read the written .nc
    re = spark.read.format("netcdf_gbx").load(str(outdir)).collect()
    assert len(re) == 1
    from rasterio.io import MemoryFile
    with MemoryFile(bytes(re[0]["tile"]["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1); epsg = ds.crs.to_epsg()
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)
    assert epsg == 4326
```

- [ ] **Step 2: Run to verify failure**

Run (synchronous): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k roundtrip --log netcdf-writer.log`
Expected: FAIL — `netcdf_gbx` has no writer (`writer()` not implemented / unsupported save).

- [ ] **Step 3: Implement `NetcdfRasterGbxWriter` in `_write_netcdf.py`**

```python
"""netcdf_gbx writer (DataSource V2). Inverse of the netcdf_gbx reader.

Raster mode: (source, tile) grid tiles -> CF grid NetCDF, one .nc per row.
Serverless-safe: write(iterator) + netCDF4 encode to worker-local temp then
shutil.copyfile to the FUSE path. No spark.conf/_jvm/.rdd.
"""
from __future__ import annotations

import glob
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


@dataclass
class NetcdfCommitMessage(WriterCommitMessage):
    paths: List[str]


def _var_from_source(source: Optional[str]) -> Optional[str]:
    # source is NETCDF:"/path/file.nc":var  -> return the trailing var, else None.
    if source and source.startswith("NETCDF:") and ":" in source:
        return source.rsplit(":", 1)[-1] or None
    return None


class NetcdfRasterGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds.writer import assert_write_schema
        from databricks.labs.gbx.ds._listing import to_local_path

        assert_write_schema(schema)  # exact (source, tile)
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.var_name_col = options.get("varNameCol")
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.nc")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        from rasterio.io import MemoryFile
        from netCDF4 import Dataset
        import numpy as np

        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            source = row["source"]
            raster_bytes = bytes(row["tile"]["raster"])
            with MemoryFile(raster_bytes) as mf, mf.open() as ds:
                arr = ds.read(1)
                transform = ds.transform
                epsg = ds.crs.to_epsg() if ds.crs else None
                nodata = ds.nodata
            h, w = arr.shape[-2], arr.shape[-1]
            # pixel-center 1-D coords; array_2d is north-up so transform.e < 0 -> lat descending
            lon = np.array([transform.c + transform.a * (i + 0.5) for i in range(w)])
            lat = np.array([transform.f + transform.e * (j + 0.5) for j in range(h)])
            # variable name: varNameCol override -> source selector -> "data"
            var = None
            if self.var_name_col and row[self.var_name_col]:
                var = os.path.basename(str(row[self.var_name_col]))
            var = var or _var_from_source(source) or "data"
            # filename
            if self.name_col and row[self.name_col]:
                stem = os.path.basename(str(row[self.name_col]))
            else:
                stem = var if _var_from_source(source) else uuid.uuid4().hex[:12]

            tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
            tmp.close()
            try:
                nc = Dataset(tmp.name, "w")
                try:
                    nc.createDimension("lat", h)
                    nc.createDimension("lon", w)
                    vlat = nc.createVariable("lat", "f8", ("lat",))
                    vlat.standard_name = "latitude"; vlat.units = "degrees_north"; vlat[:] = lat
                    vlon = nc.createVariable("lon", "f8", ("lon",))
                    vlon.standard_name = "longitude"; vlon.units = "degrees_east"; vlon[:] = lon
                    kw = {} if nodata is None else {"fill_value": nodata}
                    dv = nc.createVariable(var, arr.dtype.str[1:], ("lat", "lon"), **kw)
                    if epsg and epsg != 4326:
                        crs = nc.createVariable("crs", "i4")
                        crs.grid_mapping_name = "latitude_longitude"
                        crs.spatial_epsg = int(epsg)
                        dv.grid_mapping = "crs"
                    dv[:] = arr
                finally:
                    nc.close()
                out = os.path.join(self.path, f"{stem}.nc")
                shutil.copyfile(tmp.name, out)  # FUSE-safe (no chmod, no rename)
                written.append(out)
            finally:
                os.unlink(tmp.name)
        return NetcdfCommitMessage(paths=written)

    def commit(self, messages):
        return None

    def abort(self, messages):
        for msg in messages:
            if isinstance(msg, NetcdfCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
```

- [ ] **Step 4: Wire `writer()` dispatch in `netcdf.py`**

Add to `NetcdfGbxDataSource`:

```python
    def writer(self, schema, overwrite: bool):
        mode = self._mode()
        if mode == "raster":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfRasterGbxWriter
            if not self.options.get("path"):
                raise ValueError("netcdf_gbx writer requires an output path (.save(path)).")
            return NetcdfRasterGbxWriter(self.options, schema, overwrite)
        if mode == "vector":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfVectorGbxWriter
            if not self.options.get("path"):
                raise ValueError("netcdf_gbx writer requires an output path (.save(path)).")
            return NetcdfVectorGbxWriter(self.options, schema, overwrite)
        raise ValueError(f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector').")
```

(Task 1 only exercises the raster branch; the vector import resolves in Task 2. If PySpark eagerly imports at `writer()` call time for vector, that's fine — Task 1 tests only call raster.)

- [ ] **Step 5: Run raster round-trip to green**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k roundtrip --log netcdf-writer.log`
Expected: PASS.

- [ ] **Step 6: Add raster edge-case tests + run**

Add: `test_raster_write_overwrite_clears_stale`, `test_raster_write_nameCol`, `test_raster_write_non4326_crs` (write a grid with EPSG:27700 source CRS → re-read → CRS preserved), `test_raster_write_nodata_preserved`. Run the raster subset to green.

- [ ] **Step 7: Lint + commit**

Run: `bash scripts/commands/gbx-lint-python.sh --fix` then Docker `--check` (confirm the new files clean; ignore the pre-existing `test_vector_raster_bridge.py`).

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py \
        python/geobrix/src/databricks/labs/gbx/ds/netcdf.py \
        python/geobrix/test/ds/test_netcdf_writer.py
git commit -F /tmp/task1-msg.txt   # "feat(netcdf): light netcdf_gbx raster writer (grid tile -> CF grid .nc)"
```

---

### Task 2: Vector writer — `NetcdfVectorGbxWriter` (points → CF-DSG, one `.nc` per partition)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` (add `NetcdfVectorGbxWriter`)
- Test: `python/geobrix/test/ds/test_netcdf_writer.py`

**Interfaces:**
- Consumes: the dynamic vector schema (attribute columns + `geom_0` WKB + `geom_0_srid` + `geom_0_srid_proj`); `shapely.from_wkb`; `netCDF4.Dataset`; `_netcdf.np_to_spark` (for type mapping reference — writer maps the other way, Spark col type → numpy).
- Produces: `NetcdfVectorGbxWriter(options, schema, overwrite)` — `write(iterator)` collects the partition's rows and writes ONE CF-DSG `.nc` (`featureType="point"`, `obs` dim, `latitude`/`longitude` coord vars, one data var per attribute with `coordinates="latitude longitude"`). Empty partition → empty commit message (no file). Filename = `nameCol` (first row) else `part-<uuid8>.nc`.

- [ ] **Step 1: Write the failing vector round-trip test**

```python
def test_vector_write_roundtrip(spark, tmp_path):
    import shapely
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
    # build a points DataFrame matching the vector reader's output schema
    from pyspark.sql.types import (StructType, StructField, FloatType, IntegerType,
                                   BinaryType, StringType)
    schema = StructType([
        StructField("ch4", FloatType(), True),
        StructField("qa_value", IntegerType(), True),
        StructField("geom_0", BinaryType(), True),
        StructField("geom_0_srid", StringType(), True),
        StructField("geom_0_srid_proj", StringType(), True),
    ])
    pts = [(float(i), i % 2,
            bytes(shapely.to_wkb(shapely.Point(10.0 + i * 0.1, 50.0 + i * 0.1))),
            "4326", "EPSG:4326") for i in range(5)]
    df = spark.createDataFrame(pts, schema)
    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "vout"
    (df.write.format("netcdf_gbx").option("mode", "vector")
       .mode("overwrite").save(str(outdir)))
    re = (spark.read.format("netcdf_gbx").option("mode", "vector")
          .option("variables", "ch4,qa_value").load(str(outdir)).orderBy("ch4").collect())
    assert len(re) == 5
    pt0 = shapely.from_wkb(bytes(re[0]["geom_0"]))
    assert pt0.x == pytest.approx(10.0) and pt0.y == pytest.approx(50.0)
    assert re[1]["qa_value"] == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k vector_write --log netcdf-writer.log`
Expected: FAIL — `NetcdfVectorGbxWriter` not defined (ImportError in the vector `writer()` branch).

- [ ] **Step 3: Implement `NetcdfVectorGbxWriter`**

```python
class NetcdfVectorGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path
        names = [f.name for f in schema.fields]
        if "geom_0" not in names or "geom_0_srid" not in names:
            raise ValueError(
                "netcdf_gbx vector writer requires the vector schema "
                "(attributes + geom_0 + geom_0_srid[+ geom_0_srid_proj]); got "
                f"{names}")
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.attr_cols = [n for n in names if not n.startswith("geom_0")]
        self.dtypes = {f.name: f.dataType for f in schema.fields}
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.nc")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import shapely
        import numpy as np
        from netCDF4 import Dataset
        from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType

        os.makedirs(self.path, exist_ok=True)
        lons: list = []; lats: list = []
        attrs = {c: [] for c in self.attr_cols}
        srids = set()
        name = None
        for row in iterator:
            if name is None and self.name_col and row[self.name_col]:
                name = os.path.basename(str(row[self.name_col]))
            pt = shapely.from_wkb(bytes(row["geom_0"]))
            lons.append(pt.x); lats.append(pt.y)
            if row["geom_0_srid"] is not None:
                srids.add(str(row["geom_0_srid"]))
            for c in self.attr_cols:
                attrs[c].append(row[c])
        if not lons:
            return NetcdfCommitMessage(paths=[])   # empty partition -> no file

        def _np_dtype(col):
            dt = self.dtypes[col]
            if isinstance(dt, (IntegerType,)): return "i4"
            if isinstance(dt, (LongType,)): return "i8"
            if isinstance(dt, (FloatType,)): return "f4"
            if isinstance(dt, (DoubleType,)): return "f8"
            return "f8"

        stem = name or f"part-{uuid.uuid4().hex[:8]}"
        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False); tmp.close()
        try:
            nc = Dataset(tmp.name, "w")
            try:
                nc.featureType = "point"
                n = len(lons)
                nc.createDimension("obs", n)
                vlat = nc.createVariable("latitude", "f8", ("obs",))
                vlat.standard_name = "latitude"; vlat.units = "degrees_north"; vlat[:] = np.array(lats)
                vlon = nc.createVariable("longitude", "f8", ("obs",))
                vlon.standard_name = "longitude"; vlon.units = "degrees_east"; vlon[:] = np.array(lons)
                if len(srids) == 1 and next(iter(srids)) not in ("4326", None):
                    crs = nc.createVariable("crs", "i4"); crs.spatial_epsg = int(next(iter(srids)))
                for c in self.attr_cols:
                    dv = nc.createVariable(c, _np_dtype(c), ("obs",))
                    dv.coordinates = "latitude longitude"
                    dv[:] = np.array(attrs[c])
            finally:
                nc.close()
            out = os.path.join(self.path, f"{stem}.nc")
            shutil.copyfile(tmp.name, out)
        finally:
            os.unlink(tmp.name)
        return NetcdfCommitMessage(paths=[out])

    def commit(self, messages):
        return None

    def abort(self, messages):
        for msg in messages:
            if isinstance(msg, NetcdfCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
```

- [ ] **Step 4: Run vector round-trip to green**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k vector_write --log netcdf-writer.log`
Expected: PASS.

- [ ] **Step 5: Add vector edge cases + full-suite run**

Add: `test_vector_write_featuretype_and_obs` (open output with plain `netCDF4`, assert `featureType=="point"` + `obs` dim size), `test_vector_write_empty_partition_no_file`, `test_vector_write_nameCol`. Run the WHOLE `test_netcdf_writer.py` to green.

- [ ] **Step 6: Serverless-safety guard + lint + commit**

Run: `grep -nE "spark\\.conf\\.set|_jvm|\\.rdd|\\.cache\\(|\\.persist\\(" python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` — expect nothing. Lint (`--fix` then Docker `--check`).

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py \
        python/geobrix/test/ds/test_netcdf_writer.py
git commit -F /tmp/task2-msg.txt   # "feat(netcdf): light netcdf_gbx vector writer (points -> CF-DSG .nc)"
```

---

### Task 3: Reader `_FillValue` cross-tier parity defense

Extend the reader parity test so the "equivalent physical values" claim is verified on a fill cell; if heavy diverges, fix heavy to map the unscaled fill → NaN.

**Files:**
- Modify: `python/geobrix/test/ds/test_netcdf_cross_tier.py`
- Possibly modify: `src/main/scala/.../rasterx/operations/WindowedExtract.scala` (only if heavy diverges — see Step 3)

**Interfaces:** consumes both registered readers (needs the heavy JAR — this task's cluster run is folded into Task 6, but the fixture change + local-light assertions land here).

- [ ] **Step 1: Add a `_FillValue` cell to the scaled-grid fixture**

In `test_netcdf_cross_tier.py`'s `_write_scaled_grid`, set at least one pixel to the raw `_FillValue` (e.g. `v[0, 0] = -32768`). Add an assertion in `test_netcdf_gdal_applies_scale_matches_light` that the fill cell matches across tiers (light → NaN via mask_and_scale). Use `np.testing.assert_allclose(..., equal_nan=True)` so NaN==NaN passes and NaN!=number fails.

- [ ] **Step 2: Run in Docker (needs heavy JAR)**

This requires the JAR with Tasks 1+2 is NOT needed (this is a reader test) but DOES need the heavy `netcdf_gdal` JAR present. Run in the writer-bench cluster window (Task 6) OR locally if a JAR-backed session is available:
Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_cross_tier.py --with-integration --log netcdf-fillcell.log`
Expected: either PASS (heavy already maps fill→NaN — claim defended) or FAIL showing light=NaN vs heavy=decoded-sentinel.

- [ ] **Step 3: If it FAILS — fix heavy to map unscaled fill → NaN**

In the `applyScale` path (`WindowedExtract.fallback` with `-unscale`), ensure the source `_FillValue`/nodata is carried so decoded fill cells become NaN (matching light). The `gdal_translate -unscale` path preserves nodata; verify the output tile's nodata is set and the reader surfaces it as NaN. If GDAL's `-unscale` does not map the fill, add `-a_nodata` handling or post-process the decoded tile's nodata. Re-run Step 2 to green. (If this proves out-of-scope/complex, STOP and report — do NOT silently soften the doc claim; escalate the decision.)

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/ds/test_netcdf_cross_tier.py \
        [src/main/scala/.../WindowedExtract.scala if changed]
git commit -F /tmp/task3-msg.txt   # "test(netcdf): defend cross-tier parity on a _FillValue cell"
```

---

### Task 4: Writer benchmark harness (raster + vector legs)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (`--benchmark-netcdf-writer`/`--netcdf-writer-only` cell)
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (if a `stage`/helper tweak is needed; `run_format_write` already exists)
- Modify: `notebooks/tests/push_and_run_bench_on_cluster.py` (parse the new flags, thread to the cell)
- Test: `python/geobrix/test/bench/` smoke tests (imports, skip-clean when corpus empty)

**Interfaces:**
- Consumes: `readers.run_format_write(spark, input_path, out_path, run_id, warmup, measured, read_fmt=, write_fmt=, mode="overwrite", options=)` (existing). Produces: two writer-bench ResultRows (light-only — no heavy NetCDF writer): raster (`read_fmt="netcdf_gbx"` grid over `{CORPUS}/netcdf` → `write_fmt="netcdf_gbx"` to `{CORPUS}/netcdf-out`) and vector (`read_fmt="netcdf_gbx"` mode=vector over `{CORPUS}/netcdf-swath` → `write_fmt="netcdf_gbx"` mode=vector to `{CORPUS}/netcdf-swath-out`).

- [ ] **Step 1: Write failing smoke test for the flag**

In `python/geobrix/test/bench/`, assert the new `--netcdf-writer-only` flag parses and the cell is emitted when set (mirror the existing netcdf/pmtiles flag tests).

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/bench/ -k netcdf_writer --log bench-netcdf-writer.log`
Expected: FAIL — flag/cell not present.

- [ ] **Step 3: Add the writer-bench flag + cell**

In `cluster.py`, add `BENCHMARK_NETCDF_WRITER`/`NETCDF_WRITER_ONLY` (mirror `_CELL_NETCDF`) and a `_CELL_NETCDF_WRITER` string cell with two legs calling `run_format_write` (raster + vector), `mode="overwrite"`, light-only (comment: heavy has no netcdf writer). Guard: skip clean if the input corpus is empty; log row/granule counts. Add the flags to `push_and_run_bench_on_cluster.py` arg parsing + the reader-only exclusion set (so a writer-only run doesn't require the function-corpus scaffold — reuse the lazy `corpus.json` guard).

- [ ] **Step 4: Run smoke tests to green + commit**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/bench/ --log bench-netcdf-writer.log`
Expected: PASS.

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/cluster.py \
        python/geobrix/src/databricks/labs/gbx/bench/readers.py \
        notebooks/tests/push_and_run_bench_on_cluster.py \
        python/geobrix/test/bench/
git commit -F /tmp/task4-msg.txt   # "bench(netcdf): light netcdf_gbx writer throughput legs (raster + vector)"
```

---

### Task 5: Docs

**Files:**
- Modify: `docs/docs/readers/netcdf.mdx` (document the writer: both modes, options `mode`/`nameCol`/`varNameCol`, round-trip, vector one-file-per-partition, scale/offset + non-EPSG-CRS limitations)
- Modify: `docs/docs/api/benchmarking.mdx` (the writer-bench legs)
- Modify: `docs/docs/beta-release-notes.mdx` (new `netcdf_gbx` writer)

- [ ] **Step 1: Write the docs**

Document the writer accurately: `df.write.format("netcdf_gbx").save(path)` (raster default), `.option("mode","vector")` for DSG points. Note the two documented limitations (writes decoded physical values, no integer re-packing; non-EPSG CRS stored as WKT attr). USER-FACING VOICE — no internal vocabulary (no wave/subagent/phase/task/spec refs). Run `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` → nothing new.

- [ ] **Step 2: Commit**

```bash
git add docs/docs/readers/netcdf.mdx docs/docs/api/benchmarking.mdx docs/docs/beta-release-notes.mdx
git commit -F /tmp/task5-msg.txt   # "docs(netcdf): document the light netcdf_gbx writer + writer bench"
```

---

### Task 6: Wheel rebuild + at-scale writer bench (human-gated)

**Files:** none (build + cluster run).

- [ ] **Step 1: Rebuild + restage the wheel/JAR**

The light change (Tasks 1-2) requires a wheel rebuild; Task 3 may touch the JAR. Dispatch: `set -a; source notebooks/tests/databricks_cluster_config.env; set +a` then `bash scripts/commands/gbx-data-push-wheel.sh` (rebuilds JAR + tests.jar + wheel, uploads); sync the wheel to `sample-data/` (per `bench-wheel-path-divergence`).

- [ ] **Step 2: (Re)start cluster + run the writer bench**

Start `0519-143423-0jwqt79u`, poll RUNNING + libs INSTALLED. The NASA-NEX raster corpus (`{CORPUS}/netcdf`) + S5P swath corpus (`{CORPUS}/netcdf-swath`) are already staged from the reader-bench cycle. Run `bash scripts/commands/gbx-bench-cluster.sh --netcdf-writer-only --row-counts 1000`. Confirm both writer legs converge; give the run's `summary.md` link. Record the raster + vector writer throughput in the ledger.

- [ ] **Step 3: Run the `_FillValue` reader parity test on the warm cluster**

With the cluster up + fresh JAR, run the Task-3 test with `--with-integration` to verify (or, if it fails, confirm the Task-3 heavy fix landed and re-verify). Record the parity result.

- [ ] **Step 4: Stop the cluster**

`databricks clusters delete 0519-143423-0jwqt79u --profile oauth-fe` once captured (`stop-clusters-you-start`).

---

## Sequencing note

Tasks 1→2 build the writer (Python; 1 wheel covers both). Task 3 (reader parity fixture) is independent test work (its cluster verification folds into Task 6). Task 4 (bench harness) is Python plumbing. Task 5 is docs. Task 6 is the human-gated wheel-rebuild + at-scale writer bench + the on-cluster `_FillValue` verification. Tasks 1–5 are local; only Task 6 needs the cluster.

## Loose ends to surface at "done" (per report-loose-ends-after-spec-execution)

- Scale/offset packing (writer emits decoded physical values, no integer re-compression) — documented limitation, possible follow-up.
- Non-EPSG CRS fidelity (WKT-in-attr fallback) — documented limitation.
- The `_FillValue` heavy fix (Task 3 Step 3) only materializes if the on-cluster test shows divergence — report which way it went.
- Wheel rebuild+restage is required after Tasks 1-2 (done in Task 6).
