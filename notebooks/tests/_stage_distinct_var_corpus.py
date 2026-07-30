#!/usr/bin/env python3
"""Stage a distinct-variable single-grid NetCDF raster corpus for the writer bench's
raster singleFile/merge legs.

The default NASA-NEX raster corpus at {CORPUS}/netcdf holds 33 granules of the SAME
variable (tas, one per CMIP6 model) — which the raster singleFile/merge writer
correctly REJECTS (duplicate variable name across same-grid tiles is the mosaic case,
redirected to gbx_rst_merge_agg). To measure raster parts-vs-single-vs-merge we need
several DISTINCT variables that share ONE grid: one CMIP6 model, one date, N climate
variables (each a separate .nc on the identical 0.25-degree grid). We download a set of
variables for a narrow window, then TRIM to a single item_id's files so every file
shares one grid.
"""
import io
import json
import os
from pathlib import Path

CORPUS = os.environ.get("GBX_BENCH_CORPUS", "/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus")
# GDDP-CMIP6 asset names ARE variable ids; these are all regular 0.25-deg global grids.
VARIABLES = os.environ.get("GBX_DISTINCT_VARS", "tas,pr,hurs,huss,sfcWind,rlds").split(",")


def _nb() -> bytes:
    pip = (
        "%pip install --force-reinstall --no-deps "
        "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.4-py3-none-any.whl\n"
        "%pip install "
        "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.4-py3-none-any.whl[light]\n"
        "%pip install pystac-client planetary-computer\n"
        "dbutils.library.restartPython()"
    )
    src = f'''
import os, json
from collections import defaultdict
from pyspark.sql import functions as F
from databricks.labs.gbx.sample.nasanex import NasaNexDownloader

corpus = {CORPUS!r}
out = corpus + "/netcdf-distinct"
variables = {VARIABLES!r}
MIN_VARS = 3  # singleFile/merge need >=3 DISTINCT data vars sharing one grid
res = {{}}
os.makedirs(out, exist_ok=True)

def _item_of(fn):
    return fn[:-3].rsplit("_", 1)[0]  # "{{item_id}}_{{asset}}.nc" -> item_id

_IGNORE_VARS = ("lat", "lon", "time", "time_bnds", "lat_bnds", "lon_bnds", "crs")

def _inspect_on_disk():
    """Return (files, {{file: {{'vars','latlon'}}}}, error-or-None).

    Proves the staged .nc set is a DISTINCT-variable SINGLE-grid corpus:
    >=MIN_VARS files, each a single distinct data var, all on the SAME lat/lon grid.
    """
    import netCDF4
    files = sorted(x for x in os.listdir(out) if x.endswith(".nc"))
    shapes, grids, allvars = {{}}, set(), []
    for f in files:
        try:
            ds = netCDF4.Dataset(os.path.join(out, f))
            dv = [v for v in ds.variables if v not in _IGNORE_VARS]
            dims = {{d: len(ds.dimensions[d]) for d in ("lat", "lon") if d in ds.dimensions}}
            shapes[f] = {{"vars": dv, "latlon": dims}}
            grids.add(tuple(sorted(dims.items())))
            allvars.extend(dv)
            ds.close()
        except Exception as e:
            shapes[f] = "ERR: %s" % e
    err = None
    ok_files = [f for f in files if isinstance(shapes[f], dict) and shapes[f]["latlon"]]
    if len(ok_files) < MIN_VARS:
        err = "only %d readable grid .nc on disk (<%d) -> singleFile/merge cannot fold" % (len(ok_files), MIN_VARS)
    elif len(grids) > 1:
        err = "staged files span >1 grid %s -> not a single-grid corpus" % sorted(grids)
    elif len(set(allvars)) < MIN_VARS:
        err = "fewer than %d DISTINCT data vars (%s) -> not a multi-variable corpus" % (MIN_VARS, sorted(set(allvars)))
    return files, shapes, err

# Idempotency: a single-item distinct-var set (>=MIN_VARS vars, one grid) already staged?
existing = sorted(f for f in os.listdir(out) if f.endswith(".nc"))
by_item = defaultdict(list)
for f in existing:
    by_item[_item_of(f)].append(f)
ready = {{it: fs for it, fs in by_item.items() if len(fs) >= MIN_VARS}}
if ready:
    res["distinct_staged"] = "already present: %d file(s) across item(s) %s" % (len(existing), list(ready)[:2])
else:
    # Drive client.download DIRECTLY against a SINGLE item's N variables so every
    # .nc shares ONE grid with a DISTINCT variable name. (NasaNexDownloader.download
    # over all 32 models x 6 vars = ~192 huge global grids is the wrong scale.)
    dl = NasaNexDownloader()
    client = dl._get_stac_client()
    bbox = (-180.0, -60.0, 180.0, 85.0)
    raw = client.search(dl._aoi_dataframe(bbox), geojson_col="geojson",
                        collections=[dl.collection], datetime="2014-01-01/2014-01-02")
    # pick ONE item_id (first alphabetically) that carries all requested variables
    avail = raw.filter(F.col("asset_name").isin(variables))
    counts = (avail.groupBy("item_id").agg(F.countDistinct("asset_name").alias("n"))
              .filter(F.col("n") >= MIN_VARS).orderBy("item_id"))
    chosen = counts.limit(1).collect()
    if not chosen:
        res["error"] = "no item_id carries >=%d of %s" % (MIN_VARS, variables)
    else:
        item = chosen[0]["item_id"]
        granules = avail.filter(F.col("item_id") == item).select("item_id", "asset_name", "href").distinct()
        res["chosen_item"] = item
        res["granule_count"] = granules.count()
        # client.download returns a LAZY Spark DataFrame -- it must be MATERIALIZED
        # (an action) or NOTHING downloads. .collect() below is that action; the
        # manifest carries is_out_file_valid per asset so we can prove files landed.
        man = client.download(granules, out, name="{{item_id}}_{{asset_name}}.nc",
                              validate=False, partitions=len(variables)).collect()
        res["manifest"] = [{{k: str(r[k])[:140] for k in r.asDict()}} for r in man]
        res["download_valid"] = sum(1 for r in man if r["is_out_file_valid"])
        res["download_total"] = len(man)
        res["distinct_item"] = item
        res["distinct_staged"] = sorted(os.listdir(out))
        if res["download_valid"] == 0:
            res["error"] = ("download produced 0 valid files (all %d assets failed) -- "
                            "check PC signing / href expiry / network" % len(man))

# Verify on disk (idempotency + fresh-download paths both land here) and FAIL LOUD.
files, shapes, verify_err = _inspect_on_disk()
res["shapes"] = shapes
res["distinct_MB"] = round(sum(os.path.getsize(os.path.join(out, f)) for f in files)/1e6, 1)
if verify_err and "error" not in res:
    res["error"] = "verification failed: " + verify_err
print("STAGE_DISTINCT " + json.dumps(res, default=str))
if res.get("error"):
    # dbutils.notebook.exit() ALWAYS reports run success; raise so a bad/empty corpus
    # surfaces as a FAILED job run instead of a green run with distinct_staged=[].
    raise RuntimeError("distinct-var corpus staging FAILED: " + json.dumps(res, default=str))
dbutils.notebook.exit(json.dumps(res, default=str))
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
    p = "/" + f"{runner}/_stage_distinct_var_corpus".strip().removeprefix("/Workspace").lstrip("/")
    try:
        w.workspace.mkdirs(str(Path(p).parent))
    except Exception:
        pass
    w.workspace.upload(p, io.BytesIO(_nb()), format=ImportFormat.JUPYTER, overwrite=True)
    print(f"Submitting distinct-var corpus staging on {cid} (vars={VARIABLES})...")
    waiter = w.jobs.submit(run_name="geobrix-stage-distinct-var-corpus", timeout_seconds=3600,
                           tasks=[jobs.SubmitTask(task_key="stage", existing_cluster_id=cid,
                                                  notebook_task=jobs.NotebookTask(notebook_path=p, source=jobs.Source.WORKSPACE))])
    try:
        r = waiter.result()
        run_id = r.tasks[0].run_id
    except Exception as e:
        # A FAILED run (the notebook raised on an empty/invalid corpus) lands here.
        # Recover the run_id from the waiter so we can print the notebook's error tail.
        run_id = getattr(waiter, "run_id", None)
        if run_id is not None:
            try:
                run = w.jobs.get_run(run_id)
                run_id = run.tasks[0].run_id
            except Exception:
                pass
        print(f"STAGING RUN FAILED: {e}")
        if run_id is not None:
            o = w.jobs.get_run_output(run_id)
            print("ERROR:", (o.error or "")[:1200])
        return 1
    o = w.jobs.get_run_output(run_id)
    result = o.notebook_output.result if getattr(o, "notebook_output", None) else (o.error or "")[:1200]
    print("RESULT:", result)
    # Guard: even a "successful" run must carry a non-empty distinct_staged.
    try:
        parsed = json.loads(result) if result else {}
        if parsed.get("error") or not parsed.get("distinct_staged"):
            print("STAGING PRODUCED NO CORPUS -- distinct_staged empty or error set.")
            return 1
    except (ValueError, TypeError):
        pass
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
