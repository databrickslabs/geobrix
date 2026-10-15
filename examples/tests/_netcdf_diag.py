#!/usr/bin/env python3
"""One-off diagnostic: surface WHY netcdf_gdal enumeration returns [] on the cluster.

Submits a notebook that (1) reads the same .nc via light netcdf_gbx (does the file/path
work at all?), (2) reads via heavy netcdf_gdal (reproduce the empty result), and (3)
directly invokes the JVM enumeration path with the swallowing try/catch bypassed so the
real executor-side exception surfaces. Prints a JSON blob; does not assert (diagnostic).
"""
import io
import json
import os
import sys
from pathlib import Path

NC_DIR = os.environ.get(
    "GBX_NETCDF_VALIDATE_DIR",
    "/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/netcdf-validate",
)
NC_FILE = NC_DIR + "/ct5km_baa-max-7d_v3.1_20220101.nc"


def _nb() -> bytes:
    src = f'''
import json, traceback
nc_dir = {NC_DIR!r}
nc_file = {NC_FILE!r}
out = {{"nc_dir": nc_dir}}

# (0) can the driver even see the file, and can GDAL open it directly on the DRIVER?
import os
try:
    out["driver_isfile"] = os.path.isfile(nc_file)
    out["driver_listdir"] = os.listdir(nc_dir)[:5]
except Exception as e:
    out["driver_fs_err"] = f"{{type(e).__name__}}: {{e}}"

# (1) light netcdf_gbx over the same /Volumes dir (registers a python DataSource)
try:
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
    spark.dataSource.register(NetcdfGbxDataSource)
    ldf = spark.read.format("netcdf_gbx").option("filterRegex", r".*\\.nc$").load(nc_dir)
    lrows = ldf.collect()
    out["light_row_count"] = len(lrows)
    out["light_sources"] = sorted({{r["source"].rsplit(":",1)[-1] for r in lrows}})
except Exception as e:
    out["light_err"] = f"{{type(e).__name__}}: {{e}}"
    out["light_trace"] = traceback.format_exc()[-1500:]

# (2) heavy netcdf_gdal (reproduce the empty result)
try:
    hdf = spark.read.format("netcdf_gdal").option("filterRegex", r".*\\.nc$").load(nc_dir)
    hrows = hdf.select("source").collect()
    out["heavy_row_count"] = len(hrows)
    out["heavy_sources"] = [r["source"] for r in hrows][:5]
except Exception as e:
    out["heavy_err"] = f"{{type(e).__name__}}: {{e}}"
    out["heavy_trace"] = traceback.format_exc()[-1500:]

# (3) DRIVER-side JVM probe: open the .nc and list SUBDATASETS via the same GDAL the
#     reader uses. If this works on the driver but the reader returns [], the failure is
#     executor-side (NodeFileManager/readRemote/GDALManager init on the executor JVM).
try:
    sc = spark._jvm
    RD = sc.com.databricks.labs.gbx.rasterx.gdal.RasterDriver
    RA = sc.com.databricks.labs.gbx.rasterx.operations.RasterAccessors
    GM = sc.com.databricks.labs.gbx.rasterx.gdal.GDALManager
    EC = sc.com.databricks.labs.gbx.expressions.ExpressionConfig
    NFM = sc.com.databricks.labs.gbx.util.NodeFileManager
    _gdal = sc.org.gdal.gdal.gdal
    cfg = EC.apply(spark._jsparkSession)
    GM.init(cfg)
    NFM.init(cfg.hConf())
    local = NFM.readRemote(nc_file)
    out["driver_readRemote_local"] = str(local)
    # netCDF driver registered on the driver after GDALManager.init?
    out["driver_netcdf_drv"] = str(_gdal.GetDriverByName("netCDF"))
    # open the .nc directly (bypass RasterDriver's scala Map arg entirely)
    dsx = _gdal.Open(str(local))
    if dsx is None:
        out["driver_open_nc"] = "NULL: " + str(_gdal.GetLastErrorMsg())
    else:
        subs = dsx.GetMetadata_Dict("SUBDATASETS")
        out["driver_subdatasets_size"] = subs.size()
        out["driver_subdatasets"] = str(subs.toString())[:800]
        dsx.delete()
except Exception as e:
    out["driver_jvm_err"] = f"{{type(e).__name__}}: {{e}}"
    out["driver_jvm_trace"] = traceback.format_exc()[-2000:]

# (4) EXECUTOR-side probe: run the enum steps inside a Spark task with NO swallowing
#     catch, so the real executor exception surfaces (this is what NetCDF_Batch hides).
def _probe(_iter):
    import traceback as _tb
    try:
        from pyspark import TaskContext
        # Access the JVM helpers from the executor is not possible via py4j; instead
        # exercise the same failure surface: does NodeFileManager.hconf exist here?
        # We can only run python here, so re-derive: stage the file with the same
        # Hadoop path the JVM would use is not reachable from pyspark python. So we
        # instead report the executor's view of whether the .nc is locally openable
        # by GDAL through rasterio (proves the file is fine; isolates JVM staging).
        import os as _os
        info = {{"exec_isfile": _os.path.isfile(nc_file)}}
        try:
            import rasterio as _rio
            with _rio.open(nc_file) as _r:
                info["exec_rasterio_subdatasets"] = len(_r.subdatasets)
        except Exception as _e:
            info["exec_rasterio_err"] = f"{{type(_e).__name__}}: {{_e}}"
        return [json.dumps(info)]
    except Exception:
        return [json.dumps({{"exec_probe_err": _tb.format_exc()[-800:]}})]

try:
    probe = spark.sparkContext.parallelize([1], 1).mapPartitions(_probe).collect()
    out["executor_probe"] = probe
except Exception as e:
    out["executor_probe_err"] = f"{{type(e).__name__}}: {{e}}"

out["defaultParallelism"] = spark.sparkContext.defaultParallelism
print("NETCDF_DIAG_RESULT " + json.dumps(out, default=str))
dbutils.notebook.exit(json.dumps(out, default=str))
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
    nb_path = "/" + f"{runner_dir}/_netcdf_diag".strip().removeprefix("/Workspace").lstrip("/")
    try:
        w.workspace.mkdirs(str(Path(nb_path).parent))
    except Exception:
        pass
    w.workspace.upload(nb_path, io.BytesIO(_nb()), format=ImportFormat.JUPYTER, overwrite=True)
    print(f"Submitting netcdf diag on cluster {cluster_id}...")
    waiter = w.jobs.submit(
        run_name="geobrix-netcdf-diag",
        timeout_seconds=1800,
        tasks=[jobs.SubmitTask(task_key="netcdf_diag", existing_cluster_id=cluster_id,
                               notebook_task=jobs.NotebookTask(notebook_path=nb_path, source=jobs.Source.WORKSPACE))],
    )
    run = waiter.result()
    rid = run.run_id
    o = w.jobs.get_run_output(run.tasks[0].run_id if run.tasks else rid)
    print("=== notebook_output ===")
    if getattr(o, "notebook_output", None) and o.notebook_output.result:
        print(o.notebook_output.result)
    if o.error:
        print("=== error ===\n", o.error)
    if o.error_trace:
        print("=== trace ===\n", o.error_trace[-2000:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
