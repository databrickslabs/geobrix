#!/usr/bin/env python3
"""Cluster validation for the heavy netcdf_gdal reader's executor-side subdataset
enumeration path.

Why this exists: NetCDF_Batch enumerates subdatasets inside an executor-side UDF
that calls GDALManager.init + NodeFileManager.readRemote to stage the remote .nc
locally before opening it. Local Docker tests run in a single JVM (driver == executor),
so they never exercise the multi-executor path where an executor JVM must init
NodeFileManager and stage the file itself. This submits a one-off job to an existing
cluster that reads a .nc staged on a /Volumes path (remote -> forces readRemote on
executors) via netcdf_gdal, and asserts it enumerates the expected grid variables.

Auth/config from notebooks/tests/databricks_cluster_config.env (DATABRICKS_CONFIG_PROFILE,
CLUSTER_ID). The .nc must already be staged on a Volume (see NC_VOLUME_DIR below).

Usage:
    python notebooks/tests/netcdf_gdal_cluster_validate.py
    (reads CLUSTER_ID + DATABRICKS_CONFIG_PROFILE from env; --no-wait to not block)
"""
import io
import json
import os
import sys
from pathlib import Path

# The Volume dir holding the coral .nc (bare /Volumes path -> remote -> executor readRemote).
NC_VOLUME_DIR = os.environ.get(
    "GBX_NETCDF_VALIDATE_DIR",
    "/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/netcdf-validate",
)
# The coral fixture has exactly two CF grid variables.
EXPECTED_VARS = {"bleaching_alert_area", "mask"}


def _notebook_json(nc_dir: str, expected_vars) -> bytes:
    """A minimal notebook: read the .nc dir via netcdf_gdal and self-assert.

    Repartitions to > 1 so the enumeration UDF is forced onto executors, exercising
    the executor-side GDALManager.init + NodeFileManager.readRemote path.
    """
    src = f'''
import json
from pyspark.sql import functions as F

nc_dir = {nc_dir!r}
expected = set({sorted(expected_vars)!r})

# Force the enumeration UDF onto executors: the input file listing runs on the
# driver, but NetCDF_Batch's enumUDF (readRemote + GDALManager.init + gdal.Open of
# the subdataset selector) runs wherever the UDF lands. A multi-executor cluster
# with a /Volumes (remote) path exercises the executor staging path that local
# single-JVM Docker cannot.
df = spark.read.format("netcdf_gdal").option("filterRegex", r".*\\.nc$").load(nc_dir)

rows = df.select("source").collect()
sources = [r["source"] for r in rows]
# source = NETCDF:"<path>":<var>  -> recover the trailing variable name.
got_vars = sorted({{s.rsplit(":", 1)[-1] for s in sources}})

# Also confirm the tiles actually materialize (read one tile's bytes length) so we
# know the executor didn't just enumerate but also opened + tiled the subdataset.
tile_bytes = df.select(F.length(F.col("tile.raster")).alias("n")).collect()
tile_lens = [r["n"] for r in tile_bytes]

result = {{
    "nc_dir": nc_dir,
    "row_count": len(rows),
    "got_vars": got_vars,
    "expected_vars": sorted(expected),
    "vars_match": set(got_vars) == expected,
    "all_sources_are_selectors": all(s.startswith("NETCDF:") for s in sources),
    "min_tile_bytes": min(tile_lens) if tile_lens else 0,
    "num_executors_seen": spark.sparkContext.defaultParallelism,
}}

print("NETCDF_GDAL_CLUSTER_VALIDATE_RESULT " + json.dumps(result))

assert result["vars_match"], f"variable set mismatch: got {{got_vars}}, expected {{sorted(expected)}}"
assert result["all_sources_are_selectors"], f"source not a NETCDF: selector: {{sources}}"
assert result["min_tile_bytes"] > 0, "a tile materialized with zero raster bytes"
print("PASS: netcdf_gdal executor-side enumeration + tiling validated on cluster")
'''
    cell = {"cell_type": "code", "source": src.strip("\n").splitlines(keepends=True), "metadata": {}, "outputs": [], "execution_count": None}
    nb = {
        "cells": [cell],
        "metadata": {"language_info": {"name": "python"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(nb).encode("utf-8")


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.service.workspace import ImportFormat

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    cluster_id = os.environ.get("CLUSTER_ID")
    if not cluster_id:
        print("Set CLUSTER_ID (and DATABRICKS_CONFIG_PROFILE) in env.", file=sys.stderr)
        return 2
    do_wait = "--no-wait" not in sys.argv

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    runner_dir = os.environ.get(
        "GBX_RUNNER_DIR",
        "/Workspace/Users/mjohns@databricks.com/GeoBrix/gbx-beta-dbr-v0.2.0/sample-data/tests",
    )
    notebook_path = "/" + f"{runner_dir}/netcdf_gdal_cluster_validate".strip().removeprefix("/Workspace").lstrip("/")
    try:
        w.workspace.mkdirs(str(Path(notebook_path).parent))
    except Exception:
        pass

    print(f"Uploading validation notebook to {notebook_path}...")
    w.workspace.upload(
        notebook_path,
        io.BytesIO(_notebook_json(NC_VOLUME_DIR, EXPECTED_VARS)),
        format=ImportFormat.JUPYTER,
        overwrite=True,
    )

    print(f"Submitting netcdf_gdal validation on cluster {cluster_id}...")
    waiter = w.jobs.submit(
        run_name="geobrix-netcdf-gdal-validate",
        timeout_seconds=1800,
        tasks=[
            jobs.SubmitTask(
                task_key="netcdf_gdal_validate",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path, source=jobs.Source.WORKSPACE),
            )
        ],
    )
    try:
        print(f"Run URL: {waiter.run_page_url}")
    except Exception:
        print("Run submitted.")
    if not do_wait:
        print("Run submitted (--no-wait). Check the Databricks UI.")
        return 0

    print("Waiting for run to finish...")
    run = waiter.result()
    state = run.state
    lc = state.life_cycle_state.value if state and state.life_cycle_state and hasattr(state.life_cycle_state, "value") else str(state.life_cycle_state if state else None)
    rs = (state.result_state.value if state and state.result_state and hasattr(state.result_state, "value") else str(state.result_state)) if state and state.result_state else "UNKNOWN"
    print(f"life_cycle={lc} result_state={rs}")
    return 0 if rs == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
