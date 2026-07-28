#!/usr/bin/env python3
"""Stage the existing S5P granules into a {CORPUS}/netcdf dir the reader-bench reads.

The vapor-eyes lakeflow pipeline already landed 409 real Sentinel-5P L2 CH4 granules
(~18 GB, anonymous Planetary Computer download, no token) at SRC. The NetCDF reader-bench
cell reads `{CORPUS}/netcdf`. Rather than re-download, copy the granules server-side (fast,
intra-Volume) into DEST so `GBX_BENCH_CORPUS`=<parent of DEST/netcdf> resolves. Optionally
limit to N granules for a bounded run.
"""
import io
import json
import os
import sys
from pathlib import Path

SRC = os.environ.get("GBX_S5P_SRC", "/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/s5p")
DEST = os.environ.get("GBX_S5P_DEST", "/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/netcdf")
LIMIT = int(os.environ.get("GBX_S5P_LIMIT", "0"))  # 0 = all


def _nb() -> bytes:
    src = f'''
import os, shutil, json
src = {SRC!r}
dest = {DEST!r}
limit = {LIMIT}
os.makedirs(dest, exist_ok=True)
files = sorted(f for f in os.listdir(src) if f.endswith(".nc"))
if limit > 0:
    files = files[:limit]
copied = 0
skipped = 0
for f in files:
    s = os.path.join(src, f); d = os.path.join(dest, f)
    if os.path.exists(d) and os.path.getsize(d) == os.path.getsize(s):
        skipped += 1; continue
    shutil.copy(s, d)  # sequential copy (Volume-safe), matches CLAUDE.md volume I/O guidance
    copied += 1
present = sorted(x for x in os.listdir(dest) if x.endswith(".nc"))
out = {{"src": src, "dest": dest, "requested": len(files), "copied": copied,
       "skipped_same": skipped, "dest_nc_count": len(present)}}
print("S5P_STAGE_RESULT " + json.dumps(out))
dbutils.notebook.exit(json.dumps(out))
'''
    cell = {"cell_type": "code", "source": src.strip("\n").splitlines(keepends=True), "metadata": {}, "outputs": [], "execution_count": None}
    return json.dumps({"cells": [cell], "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}).encode()


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.service.workspace import ImportFormat

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    cluster_id = os.environ.get("CLUSTER_ID")
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    runner_dir = os.environ.get("GBX_RUNNER_DIR", "/Workspace/Users/mjohns@databricks.com/GeoBrix/gbx-beta-dbr-v0.2.0/sample-data/tests")
    nb_path = "/" + f"{runner_dir}/_stage_s5p_netcdf_corpus".strip().removeprefix("/Workspace").lstrip("/")
    try:
        w.workspace.mkdirs(str(Path(nb_path).parent))
    except Exception:
        pass
    w.workspace.upload(nb_path, io.BytesIO(_nb()), format=ImportFormat.JUPYTER, overwrite=True)
    print(f"Submitting S5P corpus stage on cluster {cluster_id} (src={SRC} dest={DEST} limit={LIMIT})...")
    waiter = w.jobs.submit(
        run_name="geobrix-stage-s5p-netcdf",
        timeout_seconds=3600,
        tasks=[jobs.SubmitTask(task_key="stage_s5p", existing_cluster_id=cluster_id,
                               notebook_task=jobs.NotebookTask(notebook_path=nb_path, source=jobs.Source.WORKSPACE))],
    )
    run = waiter.result()
    o = w.jobs.get_run_output(run.tasks[0].run_id if run.tasks else run.run_id)
    if getattr(o, "notebook_output", None) and o.notebook_output.result:
        print("RESULT:", o.notebook_output.result)
    if o.error:
        print("ERROR:", o.error[:500])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
