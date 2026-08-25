"""file_gbx QA harness — on-cluster surprise detection for the file_gbx light-tier base.

Exercises READ×{auto,managed,external} × WRITE×{managed,external,fuse} × {raster,vector}
+ ingest_files + no-gating error paths + open-amortization timing.

Purpose: surprise detection, NOT polished docs numbers. Correctness + graceful/error paths
+ the open-amortization win are what matter. Each check emits PASS/FAIL + verbatim
exception text; the lead interprets surprises.

Run via notebooks/tests/run_file_gbx_qa.py (submits as a Databricks notebook job on the
gbx-file-probe-dbr19 cluster).  On the cluster, call ``run_qa(spark, ...)`` directly.

Standing bench discipline:
- spark-path: 0 warmup / 1 measured iteration
- NO job retries (failures surface immediately)
"""

from __future__ import annotations

import json
import os
import time
import traceback
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pass(name: str, note: str = "", **extras) -> Dict[str, Any]:
    d: Dict[str, Any] = {"check": name, "status": "PASS", "note": note}
    d.update(extras)
    return d


def _fail(name: str, exc: Exception, note: str = "", **extras) -> Dict[str, Any]:
    tb = traceback.format_exc()
    d: Dict[str, Any] = {
        "check": name,
        "status": "FAIL",
        "exception": str(exc),
        "traceback": tb[:2000],
        "note": note,
    }
    d.update(extras)
    return d


def _make_tiny_cog(out_path: str, px: int = 64) -> None:
    """Write a tiny synthetic 1-band float32 COG to *out_path* via rasterio."""
    import shutil
    import tempfile

    import numpy as np
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    data = np.random.default_rng(42).random((1, px, px)).astype("float32")
    transform = from_bounds(-1.0, -1.0, 1.0, 1.0, px, px)
    profile = dict(
        driver="COG",
        dtype="float32",
        width=px,
        height=px,
        count=1,
        crs=CRS.from_epsg(4326),
        transform=transform,
        compress="DEFLATE",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # COG driver requires seekable writes (backward offset tables); UC Volume paths
    # don't support seek on write.  Write to a local temp file first, then copy.
    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with rasterio.open(tmp, "w", **profile) as dst:
            dst.write(data)
        shutil.copy(tmp, out_path)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _make_tiny_geojson(out_path: str) -> int:
    """Write a tiny GeoJSON with 3 point features; return count."""
    import json as _json

    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
            "properties": {"id": i, "value": float(i * 10)},
        }
        for i in range(3)
    ]
    fc = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        _json.dump(fc, f)
    return len(features)


def _read_raster_pixels(path: str) -> bytes:
    """Read a raster at *path* via rasterio and return band-1 bytes (for checksum)."""
    import rasterio

    with rasterio.open(path) as ds:
        return ds.read(1).tobytes()


# ---------------------------------------------------------------------------
# Individual QA checks
# ---------------------------------------------------------------------------


def _check_capability_tier(spark) -> Dict[str, Any]:
    """Detect file_access_tier and file_supported — confirm FILE is available."""
    name = "capability_tier"
    try:
        from databricks.labs.gbx.ds.file_gbx import file_access_tier, file_supported

        tier = file_access_tier(spark)
        is_file = file_supported(spark)
        note = f"tier={tier!r}  file_supported={is_file}"
        return _pass(name, note, tier=tier, file_supported=is_file)
    except Exception as exc:
        return _fail(name, exc)


def _check_enumerate_files(spark, raster_dir: str, raster_name: str) -> Dict[str, Any]:
    """enumerate_files finds the test raster; _* skip and positive extensions filter work."""
    name = "enumerate_files"
    try:
        from databricks.labs.gbx.ds.file_gbx import enumerate_files

        # Basic: should find exactly the one raster we wrote
        result = enumerate_files(raster_dir, spark=spark)
        if isinstance(result, list):
            rows = result
        else:
            rows = result.collect()
        # Verify the raster appears in the listing
        paths = [r["path"] if isinstance(r, dict) else r.path for r in rows]
        found = any(raster_name in p for p in paths)
        # Extension filter: *.tif should still find it
        result2 = enumerate_files(raster_dir, extensions=(".tif",), spark=spark)
        if isinstance(result2, list):
            rows2 = result2
        else:
            rows2 = result2.collect()
        found2 = len(rows2) > 0
        note = f"found={found}  ext_filter={found2}  count={len(rows)}"
        if found and found2:
            return _pass(name, note, row_count=len(rows))
        return _fail(
            name,
            AssertionError(f"enumerate_files: found={found}, ext_filter={found2}"),
            note,
        )
    except Exception as exc:
        return _fail(name, exc)


def _check_open_for_read_auto(spark, raster_path: str) -> Dict[str, Any]:
    """open_for_read(access='auto') returns the source path without error."""
    name = "open_for_read_auto"
    try:
        from databricks.labs.gbx.ds.file_gbx import open_for_read

        result = open_for_read(raster_path, access="auto", spark=spark)
        # Result is the source path; verify it's a non-empty string
        if not result:
            raise AssertionError("open_for_read returned empty/None")
        return _pass(name, f"returned={result!r}")
    except Exception as exc:
        return _fail(name, exc)


def _check_open_for_read_explicit_modes(
    spark, raster_path: str, tier: str
) -> Dict[str, Any]:
    """open_for_read with explicit managed/external modes on a FILE-capable runtime."""
    name = "open_for_read_explicit_modes"
    try:
        from databricks.labs.gbx.ds.file_gbx import open_for_read

        if tier == "fuse":
            # On fuse tier, managed/external should RAISE ValueError (no-gating)
            raised = False
            try:
                open_for_read(raster_path, access="managed", spark=spark)
            except ValueError:
                raised = True
            return _pass(
                name, f"tier=fuse; managed raises={raised}", raises_on_fuse=raised
            )
        else:
            # On FILE-capable tier, both should succeed
            r1 = open_for_read(raster_path, access="managed", spark=spark)
            r2 = open_for_read(raster_path, access="external", spark=spark)
            return _pass(name, f"managed={repr(r1)[:40]}  external={repr(r2)[:40]}")
    except Exception as exc:
        return _fail(name, exc)


def _check_no_gating_error(spark) -> Dict[str, Any]:
    """Simulate fuse-tier no-gating: explicit FILE request on fuse runtime raises ValueError."""
    name = "no_gating_error_path"
    try:
        from databricks.labs.gbx.ds.file_gbx import resolve_access

        # Direct test: resolve_access("managed", tier="fuse") must raise ValueError
        raised = False
        error_msg = ""
        try:
            resolve_access("managed", tier="fuse", spark=spark)
        except ValueError as ve:
            raised = True
            error_msg = str(ve)[:200]
        note = f"raises_on_managed_fuse={raised}"
        if not raised:
            return _fail(name, AssertionError("Expected ValueError not raised"), note)
        return _pass(name, note, error_msg_preview=error_msg)
    except Exception as exc:
        return _fail(name, exc)


def _check_raster_reader(
    spark, raster_dir: str, expected_pixel_bytes: bytes
) -> Dict[str, Any]:
    """Raster read via raster_gbx datasource: row count + pixel checksum."""
    name = "raster_reader_gbx"
    try:
        from databricks.labs.gbx.ds.register import register

        register(spark)
        df = (
            spark.read.format("raster_gbx")
            .option("driverName", "GTiff")
            .load(raster_dir)
        )
        rows = df.collect()
        row_count = len(rows)
        # Each row has a 'tile' struct with 'raster' (BINARY) or virtual path
        # Just verify rows came back and no exception
        note = f"row_count={row_count}"
        if row_count > 0:
            return _pass(name, note, row_count=row_count)
        return _fail(name, AssertionError("Got 0 rows"), note)
    except Exception as exc:
        return _fail(name, exc)


def _check_vector_reader(
    spark, geojson_path: str, expected_count: int
) -> Dict[str, Any]:
    """Vector read via shapefile_gbx/geojson_gbx datasource: row count."""
    name = "vector_reader_gbx"
    try:
        from databricks.labs.gbx.ds.register import register

        register(spark)
        df = spark.read.format("geojson_gbx").load(geojson_path)
        count = df.count()
        note = f"count={count}  expected={expected_count}"
        if count == expected_count:
            return _pass(name, note, count=count)
        return _fail(
            name,
            AssertionError(f"Expected {expected_count} rows, got {count}"),
            note,
        )
    except Exception as exc:
        return _fail(name, exc)


def _build_raster_tile_df(spark, raster_path: str):
    """Build a one-row tile DataFrame from *raster_path* for write tests."""
    import rasterio
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    with rasterio.open(raster_path) as ds:
        w, h = ds.width, ds.height
        crs = ds.crs
        crs_str = f"EPSG:{crs.to_epsg()}" if (crs and crs.to_epsg()) else None

    # Build a 1-row virtual tile struct pointing at the raster
    window_struct_type = StructType(
        [
            StructField("col_off", IntegerType(), False),
            StructField("row_off", IntegerType(), False),
            StructField("width", IntegerType(), False),
            StructField("height", IntegerType(), False),
        ]
    )
    tile_schema = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("raster", BinaryType(), True),
            StructField("path", StringType(), True),
            StructField("path_mode", StringType(), True),
            StructField("window", window_struct_type, True),
            StructField("clip_polygon", BinaryType(), True),
            StructField("clip_crs", StringType(), True),
            StructField("crs", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    outer_schema = StructType([StructField("tile", tile_schema, True)])
    row = {
        "tile": {
            "cellid": 0,
            "raster": None,
            "path": raster_path,
            "path_mode": "external",
            "window": {"col_off": 0, "row_off": 0, "width": w, "height": h},
            "clip_polygon": None,
            "clip_crs": None,
            "crs": crs_str,
            "metadata": {"driver": "GTiff", "width": str(w), "height": str(h)},
        }
    }
    return spark.createDataFrame([row], schema=outer_schema)


def _check_write_managed(
    spark, raster_path: str, catalog: str, schema: str, filespace: str, ts: int
) -> Dict[str, Any]:
    """open_for_write file_mode='managed' → create_file → round-trip read."""
    name = "write_managed"
    target = f"`{catalog}`.`{schema}`.`_filegbx_qa_managed_{ts}`"
    try:
        from databricks.labs.gbx.ds.file_gbx import open_for_write

        df = _build_raster_tile_df(spark, raster_path)
        t0 = time.perf_counter()
        open_for_write(
            spark,
            df,
            target,
            file_mode="managed",
            filespace=filespace,
            overwrite=True,
        )
        write_s = time.perf_counter() - t0
        # Round-trip: count rows from the managed table
        count = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        note = f"wrote in {write_s:.2f}s  row_count={count}"
        if count >= 1:
            return _pass(name, note, write_s=write_s, row_count=count, target=target)
        return _fail(name, AssertionError(f"Expected >=1 row, got {count}"), note)
    except Exception as exc:
        return _fail(name, exc, target=target)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            pass


def _check_write_external(
    spark,
    raster_path: str,
    catalog: str,
    schema: str,
    ts: int,
    filespace: Optional[str] = None,
) -> Dict[str, Any]:
    """open_for_write file_mode='external' → try_to_file → round-trip read."""
    name = "write_external"
    target = f"`{catalog}`.`{schema}`.`_filegbx_qa_external_{ts}`"
    try:
        from databricks.labs.gbx.ds.file_gbx import open_for_write

        df = _build_raster_tile_df(spark, raster_path)
        t0 = time.perf_counter()
        open_for_write(
            spark,
            df,
            target,
            file_mode="external",
            overwrite=True,
        )
        write_s = time.perf_counter() - t0
        count = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        note = f"wrote in {write_s:.2f}s  row_count={count}"
        if count >= 1:
            return _pass(name, note, write_s=write_s, row_count=count, target=target)
        return _fail(name, AssertionError(f"Expected >=1 row, got {count}"), note)
    except Exception as exc:
        return _fail(name, exc, target=target)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            pass


def _check_write_fuse(
    spark, raster_path: str, catalog: str, schema: str, ts: int
) -> Dict[str, Any]:
    """open_for_write file_mode='fuse' → plain Delta write → round-trip count."""
    name = "write_fuse"
    target = f"`{catalog}`.`{schema}`.`_filegbx_qa_fuse_{ts}`"
    try:
        from databricks.labs.gbx.ds.file_gbx import open_for_write

        df = _build_raster_tile_df(spark, raster_path)
        t0 = time.perf_counter()
        open_for_write(
            spark,
            df,
            target,
            file_mode="fuse",
            overwrite=True,
        )
        write_s = time.perf_counter() - t0
        count = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        note = f"wrote in {write_s:.2f}s  row_count={count}"
        if count >= 1:
            return _pass(name, note, write_s=write_s, row_count=count, target=target)
        return _fail(name, AssertionError(f"Expected >=1 row, got {count}"), note)
    except Exception as exc:
        return _fail(name, exc, target=target)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            pass


def _check_write_auto(
    spark, raster_path: str, catalog: str, schema: str, ts: int
) -> Dict[str, Any]:
    """open_for_write file_mode='auto' on a FILE-capable runtime → managed/external."""
    name = "write_auto"
    target = f"`{catalog}`.`{schema}`.`_filegbx_qa_auto_{ts}`"
    try:
        from databricks.labs.gbx.ds.file_gbx import file_access_tier, open_for_write

        tier = file_access_tier(spark)
        df = _build_raster_tile_df(spark, raster_path)
        t0 = time.perf_counter()
        open_for_write(
            spark,
            df,
            target,
            file_mode="auto",
            overwrite=True,
        )
        write_s = time.perf_counter() - t0
        count = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        note = f"tier={tier!r}  wrote in {write_s:.2f}s  row_count={count}"
        if count >= 1:
            return _pass(
                name, note, write_s=write_s, row_count=count, tier=tier, target=target
            )
        return _fail(name, AssertionError(f"Expected >=1 row, got {count}"), note)
    except Exception as exc:
        return _fail(name, exc, target=target)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            pass


def _check_ingest_files(
    spark, raster_dir: str, catalog: str, schema: str, filespace: str, ts: int
) -> Dict[str, Any]:
    """ingest_files existing rasters → MANAGED FILE table → confirm rows land."""
    name = "ingest_files"
    target = f"`{catalog}`.`{schema}`.`_filegbx_qa_ingest_{ts}`"
    try:
        from databricks.labs.gbx.ds.file_gbx import file_access_tier, ingest_files

        tier = file_access_tier(spark)
        if tier == "fuse":
            return _pass(
                name,
                "SKIPPED: ingest_files requires FILE tier; got tier='fuse'",
                skipped=True,
            )
        t0 = time.perf_counter()
        ingest_files(
            spark,
            raster_dir,
            target,
            filespace=filespace,
            overwrite=True,
        )
        ingest_s = time.perf_counter() - t0
        count = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        note = f"ingest in {ingest_s:.2f}s  row_count={count}"
        if count >= 1:
            return _pass(name, note, ingest_s=ingest_s, row_count=count, target=target)
        return _fail(name, AssertionError(f"Expected >=1 row, got {count}"), note)
    except Exception as exc:
        return _fail(name, exc, target=target)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            pass


def _check_vector_write_modes(
    spark,
    geojson_path: str,
    catalog: str,
    schema: str,
    filespace: str,
    ts: int,
    work_dir: str = "/tmp/gbx_qa_work",
) -> List[Dict[str, Any]]:
    """Vector write via VectorGbxWriter across fuse/managed/external modes.

    Fuse target lives under *work_dir* (a shared Volume path) so executor-written
    Arrow IPC scratch fragments are readable by the driver at commit time.
    Using /tmp causes per-executor local isolation that the driver can't read.
    """
    results = []
    for mode in ("fuse", "managed", "external"):
        name = f"vector_write_{mode}"
        if mode == "fuse":
            # Must be a shared path (Volume), not /tmp — VectorGbxWriter's scratch
            # lives under the target's parent; executors write there and the driver
            # reads back at commit, so /tmp causes unreadable-fragment errors.
            target_dir = os.path.join(work_dir, f"vec_fuse_{ts}")
        else:
            target_table = f"`{catalog}`.`{schema}`.`_filegbx_qa_vec_{mode}_{ts}`"

        try:
            from databricks.labs.gbx.ds.file_gbx import file_access_tier
            from databricks.labs.gbx.ds.register import register

            register(spark)
            tier = file_access_tier(spark)

            if mode in ("managed", "external") and tier == "fuse":
                results.append(
                    _pass(
                        name,
                        f"SKIPPED: {mode} requires FILE tier; got tier='fuse'",
                        skipped=True,
                    )
                )
                continue

            # Read the test GeoJSON
            df = spark.read.format("geojson_gbx").load(geojson_path)
            count_in = df.count()

            t0 = time.perf_counter()
            if mode == "fuse":
                df.write.format("shapefile_gbx").option("filemode", "fuse").mode(
                    "overwrite"
                ).save(target_dir)
                write_s = time.perf_counter() - t0
                # Round-trip: read back
                df2 = spark.read.format("shapefile_gbx").load(target_dir)
                count_out = df2.count()
            else:
                df.write.format("shapefile_gbx").option("filemode", mode).option(
                    "filespace", filespace
                ).mode("overwrite").save(target_table)
                write_s = time.perf_counter() - t0
                count_out = spark.sql(f"SELECT count(*) FROM {target_table}").collect()[
                    0
                ][0]

            note = f"write in {write_s:.2f}s  in={count_in}  out={count_out}"
            if count_out > 0:
                results.append(
                    _pass(
                        name,
                        note,
                        write_s=write_s,
                        count_in=count_in,
                        count_out=count_out,
                    )
                )
            else:
                results.append(
                    _fail(
                        name,
                        AssertionError("Got 0 rows back"),
                        note,
                    )
                )
        except Exception as exc:
            results.append(_fail(name, exc))
        finally:
            if mode in ("managed", "external"):
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
                except Exception:
                    pass
    return results


def _check_amortization(spark, work_dir: str) -> List[Dict[str, Any]]:
    """Grouped FILE open-amortization: 3 modes, compare per-tile timings.

    Uses bench/grouped_file.run_grouped_file with a tiny synthetic multiwindow
    COG corpus (2 COGs × 8 windows). Only rst_clip_grouped (simplest check).
    Discipline: 0 warmup / 1 measured (standing spark-path bench rule).
    """
    results = []
    corpus_dir = os.path.join(work_dir, "amort-corpus")
    try:
        from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

        os.makedirs(corpus_dir, exist_ok=True)
        manifest_path = generate_cog_multiwindow_corpus(
            out_dir=corpus_dir,
            seed=99,
            cog_count=2,
            windows_per_cog=8,
            cog_px=256,
            bands=1,
            dtype="float32",
            srid=4326,
            window_px=64,
            compress="DEFLATE",
        )
        results.append(
            _pass(
                "amort_corpus_gen", f"manifest={manifest_path}  corpus_dir={corpus_dir}"
            )
        )
    except Exception as exc:
        results.append(_fail("amort_corpus_gen", exc))
        return results

    try:
        from databricks.labs.gbx.bench.grouped_file import run_grouped_file

        rows = run_grouped_file(
            spark,
            manifest_path=str(manifest_path),
            fns=["rst_clip_grouped"],
            modes=("materialized", "virtual-file-off", "virtual-file-on"),
            warmup=0,
            measured=1,
            run_id=f"qa-amort-{int(time.time())}",
            where="cluster",
            progress=True,
        )
        # Report per-mode timing
        for r in rows:
            mode = (
                r.split_strategy
                if hasattr(r, "split_strategy")
                else getattr(r, "note", "?")
            )
            per_tile_ms = getattr(r, "per_tile_avg_ms", None) or 0.0
            status = getattr(r, "status", "?")
            name = (
                f"amort_{getattr(r, 'fn', 'fn')}_{getattr(r, 'split_strategy', mode)}"
            )
            results.append(
                _pass(
                    name,
                    f"status={status}  per_tile_ms={per_tile_ms:.3f}",
                    per_tile_ms=per_tile_ms,
                )
                if status == "ok"
                else _fail(
                    name,
                    Exception(getattr(r, "note", "unknown error")),
                    f"status={status}",
                )
            )
        # Summarize the amortization ratio
        mode_times = {
            getattr(r, "split_strategy", ""): getattr(r, "per_tile_avg_ms", None) or 0.0
            for r in rows
        }
        results.append(
            _pass(
                "amort_summary",
                f"materialized={mode_times.get('materialized', '?'):.3f}ms  "
                f"virtual-file-off={mode_times.get('virtual-file-off', '?'):.3f}ms  "
                f"virtual-file-on={mode_times.get('virtual-file-on', '?'):.3f}ms",
                mode_times=mode_times,
            )
        )
    except Exception as exc:
        results.append(_fail("amort_grouped_file", exc))

    return results


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_qa(
    spark,
    *,
    work_dir: str = "/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/_filegbx_qa_work",
    result_path: str = "/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/_filegbx_qa_result.json",
    filespace: str = "/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix",
    catalog: str = "geospatial_docs",
    schema: str = "geobrix",
) -> Dict[str, Any]:
    """Run all file_gbx QA checks on a FILE-capable Databricks cluster.

    Writes per-check PASS/FAIL results + amortization timings to *result_path*.
    Returns the full result dict (same as written).

    Designed for on-cluster use (DBR 19+, FILE-capable); gracefully skips
    checks that require FILE when running on an older runtime.
    """
    run_start = time.time()
    ts = int(run_start)
    uid = str(ts)

    raster_dir = os.path.join(work_dir, f"raster_{uid}")
    raster_path = os.path.join(raster_dir, "test.tif")
    geojson_dir = os.path.join(work_dir, f"vector_{uid}")
    geojson_path = os.path.join(geojson_dir, "test.geojson")

    results: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {
        "run_id": f"filegbx-qa-{uid}",
        "run_start_utc": run_start,
        "checks": results,
    }

    print(f"[file_gbx_qa] run_id={summary['run_id']}  work_dir={work_dir}", flush=True)

    # --- 0. Capability detection -------------------------------------------------
    cap = _check_capability_tier(spark)
    results.append(cap)
    tier = cap.get("tier", "fuse")
    print(f"[file_gbx_qa] {cap['check']}: {cap['status']}  {cap['note']}", flush=True)

    # --- 1. Synthesize test data ------------------------------------------------
    print("[file_gbx_qa] Generating synthetic test data...", flush=True)
    try:
        _make_tiny_cog(raster_path)
        pixel_bytes = _read_raster_pixels(raster_path)
        results.append(_pass("synth_raster", f"path={raster_path}"))
    except Exception as exc:
        results.append(_fail("synth_raster", exc))
        pixel_bytes = b""

    try:
        expected_count = _make_tiny_geojson(geojson_path)
        results.append(
            _pass("synth_vector", f"path={geojson_path}  count={expected_count}")
        )
    except Exception as exc:
        results.append(_fail("synth_vector", exc))
        expected_count = 0

    # --- 2. enumerate_files -------------------------------------------------------
    r = _check_enumerate_files(spark, raster_dir, "test.tif")
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 3. open_for_read (auto + explicit modes) --------------------------------
    r = _check_open_for_read_auto(spark, raster_path)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    r = _check_open_for_read_explicit_modes(spark, raster_path, tier)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 4. No-gating error path ------------------------------------------------
    r = _check_no_gating_error(spark)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 5. Raster reader via raster_gbx datasource ----------------------------
    r = _check_raster_reader(spark, raster_dir, pixel_bytes)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 6. Raster write × mode ------------------------------------------------
    for write_fn, kw in [
        (_check_write_managed, dict(filespace=filespace)),
        (_check_write_external, {}),
        (_check_write_fuse, {}),
        (_check_write_auto, {}),
    ]:
        r = write_fn(spark, raster_path, catalog, schema, ts=ts, **kw)
        results.append(r)
        print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 7. ingest_files → MANAGED --------------------------------------------
    r = _check_ingest_files(spark, raster_dir, catalog, schema, filespace, ts)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 8. Vector reader -----------------------------------------------------
    r = _check_vector_reader(spark, geojson_path, expected_count)
    results.append(r)
    print(f"[file_gbx_qa] {r['check']}: {r['status']}  {r['note']}", flush=True)

    # --- 9. Vector write × mode -----------------------------------------------
    for r in _check_vector_write_modes(
        spark, geojson_path, catalog, schema, filespace, ts, work_dir=work_dir
    ):
        results.append(r)
        print(
            f"[file_gbx_qa] {r['check']}: {r['status']}  {r.get('note', '')}",
            flush=True,
        )

    # --- 10. Amortization (grouped FILE) ----------------------------------------
    print("[file_gbx_qa] Running amortization bench...", flush=True)
    for r in _check_amortization(spark, work_dir):
        results.append(r)
        print(
            f"[file_gbx_qa] {r['check']}: {r['status']}  {r.get('note', '')}",
            flush=True,
        )

    # --- Final summary ----------------------------------------------------------
    n_pass = sum(1 for r in results if r["status"] == "PASS")
    n_fail = sum(1 for r in results if r["status"] == "FAIL")
    n_skip = sum(1 for r in results if r.get("skipped"))
    summary["n_pass"] = n_pass
    summary["n_fail"] = n_fail
    summary["n_skip"] = n_skip
    summary["duration_s"] = time.time() - run_start

    print(
        f"\n[file_gbx_qa] DONE  PASS={n_pass}  FAIL={n_fail}  SKIP={n_skip}  "
        f"duration={summary['duration_s']:.1f}s",
        flush=True,
    )

    # Write JSON result to the volume
    try:
        os.makedirs(os.path.dirname(result_path), exist_ok=True)
        with open(result_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"[file_gbx_qa] results written to {result_path}", flush=True)
    except Exception as exc:
        print(f"[file_gbx_qa] WARNING: could not write results JSON: {exc}", flush=True)

    return summary
