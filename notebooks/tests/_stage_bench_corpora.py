#!/usr/bin/env python3
"""Stage bounded NetCDF bench corpora on-cluster: NASA-NEX regular grids (raster leg)
into {CORPUS}/netcdf, and an S5P swath subset (vector leg) into {CORPUS}/netcdf-swath.

Raster: stage_nasanex_corpus pulls one item per CMIP6 model for the given scenario/year
(~33 x 245MB global grids); we then TRIM to RASTER_KEEP granules for a bounded,
fast-converging bench read. Vector: copy an S5P subset from the already-staged pool.
"""
import io
import json
import os
from pathlib import Path

CORPUS = os.environ.get("GBX_BENCH_CORPUS", "/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus")
S5P_SRC = "/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/s5p"
RASTER_KEEP = int(os.environ.get("GBX_RASTER_KEEP", "15"))
VECTOR_KEEP = int(os.environ.get("GBX_VECTOR_KEEP", "15"))


def _nb() -> bytes:
    # Cell 1: install the geobrix wheel (not pre-installed on the bench cluster) so
    # databricks.labs.gbx.bench.readers (+ NasaNexDownloader) import. Two-step per the
    # wheel-notebook-install pattern; restartPython MUST end its own cell.
    pip = (
        "%pip install --force-reinstall --no-deps "
        "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.2-py3-none-any.whl\n"
        "%pip install "
        "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.2-py3-none-any.whl[light]\n"
        "dbutils.library.restartPython()"
    )
    src = f'''
import os, json, shutil
from databricks.labs.gbx.bench import readers as _rd

corpus = {CORPUS!r}
raster_dir = corpus + "/netcdf"
swath_dir = corpus + "/netcdf-swath"
s5p_src = {S5P_SRC!r}
raster_keep = {RASTER_KEEP}
vector_keep = {VECTOR_KEEP}
out = {{}}

# --- RASTER: NASA-NEX regular grids (one tas item per model, historical 2014) ---
# The raster dir MUST hold NASA-NEX regular grids, not S5P swaths. A prior run left
# S5P_* swath files here (which read as 0 rows in raster mode), and a bare file-count
# guard then skipped the real NASA-NEX download. Purge any non-NASA-NEX (.e.g S5P_*)
# files first, and only count NASA-NEX grids toward the "already staged" guard.
os.makedirs(raster_dir, exist_ok=True)
for f in list(os.listdir(raster_dir)):
    if f.endswith(".nc") and f.startswith("S5P_"):
        os.remove(os.path.join(raster_dir, f))
grids = sorted(f for f in os.listdir(raster_dir) if f.endswith(".nc") and not f.startswith("S5P_"))
if len(grids) >= raster_keep:
    out["raster_staged"] = "already present: %d" % len(grids)
else:
    try:
        _rd.stage_nasanex_corpus(spark, raster_dir, temporal="2014-01-01/2014-01-02", variables=("tas",))
    except Exception as e:
        out["raster_stage_err"] = f"{{type(e).__name__}}: {{e}}"
    files = sorted(f for f in os.listdir(raster_dir) if f.endswith(".nc") and not f.startswith("S5P_"))
    # trim to raster_keep for a bounded bench read
    for f in files[raster_keep:]:
        os.remove(os.path.join(raster_dir, f))
    out["raster_staged"] = len([f for f in os.listdir(raster_dir) if f.endswith(".nc")])
out["raster_sample_names"] = sorted(f for f in os.listdir(raster_dir) if f.endswith(".nc"))[:3]

# --- VECTOR: S5P swath subset ---
os.makedirs(swath_dir, exist_ok=True)
present = sorted(f for f in os.listdir(swath_dir) if f.endswith(".nc"))
if len(present) >= vector_keep:
    out["swath_staged"] = "already present: %d" % len(present)
else:
    src_files = sorted(f for f in os.listdir(s5p_src) if f.endswith(".nc"))[:vector_keep]
    for f in src_files:
        d = os.path.join(swath_dir, f)
        if not os.path.exists(d):
            shutil.copy(os.path.join(s5p_src, f), d)
    out["swath_staged"] = len([f for f in os.listdir(swath_dir) if f.endswith(".nc")])

# sizes
def _mb(d):
    return round(sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if f.endswith(".nc"))/1e6, 0)
out["raster_MB"] = _mb(raster_dir)
out["swath_MB"] = _mb(swath_dir)
print("STAGE_CORPORA " + json.dumps(out))
dbutils.notebook.exit(json.dumps(out))
'''
    pip_cell = {"cell_type": "code", "source": pip.splitlines(keepends=True), "metadata": {}, "outputs": [], "execution_count": None}
    cell = {"cell_type": "code", "source": src.strip("\n").splitlines(keepends=True), "metadata": {}, "outputs": [], "execution_count": None}
    return json.dumps({"cells": [pip_cell, cell], "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}).encode()


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.service.workspace import ImportFormat
    w = WorkspaceClient(profile=os.environ["DATABRICKS_CONFIG_PROFILE"])
    cid = os.environ["CLUSTER_ID"]
    runner = os.environ.get("GBX_RUNNER_DIR", "/Workspace/Users/mjohns@databricks.com/GeoBrix/gbx-beta-dbr-v0.2.0/sample-data/tests")
    p = "/" + f"{runner}/_stage_bench_corpora".strip().removeprefix("/Workspace").lstrip("/")
    try:
        w.workspace.mkdirs(str(Path(p).parent))
    except Exception:
        pass
    w.workspace.upload(p, io.BytesIO(_nb()), format=ImportFormat.JUPYTER, overwrite=True)
    print(f"Submitting corpus staging on {cid} (raster_keep={RASTER_KEEP}, vector_keep={VECTOR_KEEP})...")
    r = w.jobs.submit(run_name="geobrix-stage-bench-corpora", timeout_seconds=3600,
                      tasks=[jobs.SubmitTask(task_key="stage", existing_cluster_id=cid,
                                             notebook_task=jobs.NotebookTask(notebook_path=p, source=jobs.Source.WORKSPACE))]).result()
    o = w.jobs.get_run_output(r.tasks[0].run_id)
    print("RESULT:", o.notebook_output.result if getattr(o, "notebook_output", None) else (o.error or "")[:600])
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
