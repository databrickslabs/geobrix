#!/usr/bin/env python3
"""Serverless (v5, ARM) lightweight-API install smoke.

Submits a one-off **serverless** notebook run that (1) %pip-installs the
GeoBrix wheel with a parameterizable spec, (2) restarts Python, (3) probes
Spark-Connect health + key package versions + the pyrx import, and exits a
JSON verdict. Lets us iterate on the Serverless "halo" install without a
cluster.

Workspace config (host, profile, Volume coordinates) is read from the gitignored
notebooks/tests/databricks_cluster_config.env. Auth mints a bearer token from the
configured CLI profile (the SDK's profile refresh is flaky on some networks) and
uses WorkspaceClient(host, token). With no --spec, the Volume-staged [light_env6] wheel
path is derived from GBX_BUNDLE_VOLUME_*.

Usage (run under .venv-pyrx which has databricks-sdk):
    .venv-pyrx/bin/python notebooks/tests/serverless_light_smoke.py \
        --spec 'geobrix[light_env6] @ file:///Volumes/<catalog>/<schema>/<volume>/geobrix-<version>-py3-none-any.whl' \
        --env-version 5
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, workspace

# All workspace-specific values (profile, Volume coordinates) come from the
# gitignored notebooks/tests/databricks_cluster_config.env so nothing internal is
# baked into this committed file. Same env file the bench launcher uses.
#
# IMPORTANT: the profile is captured into PROFILE and passed explicitly to
# WorkspaceClient(profile=...), but DATABRICKS_CONFIG_PROFILE is deliberately kept
# OUT of os.environ. When that var is present in the environment, the CLI/SDK
# databricks-cli auth takes a refresh path that fails ("refresh token is invalid")
# on this setup, whereas the explicit profile= arg with a clean env mints fine.
_CFG = {}
_ENV_FILE = Path(__file__).resolve().parent / "databricks_cluster_config.env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            _CFG[_k.strip()] = _v.strip().strip("'\"")
            if _k.strip() != "DATABRICKS_CONFIG_PROFILE" and not os.environ.get(_k.strip()):
                os.environ[_k.strip()] = _CFG[_k.strip()]

PROFILE = _CFG.get("DATABRICKS_CONFIG_PROFILE", os.environ.get("DATABRICKS_CONFIG_PROFILE", ""))
os.environ.pop("DATABRICKS_CONFIG_PROFILE", None)  # keep it out of the env
NB_NAME = "geobrix_serverless_light_smoke"


def _default_spec() -> str:
    """Volume-staged [light_env6] wheel path, derived from GBX_BUNDLE_VOLUME_* config."""
    catalog = os.environ.get("GBX_BUNDLE_VOLUME_CATALOG", "main")
    schema = os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA", "default")
    volume = os.environ.get("GBX_BUNDLE_VOLUME_NAME", "geobrix_samples")
    ver = os.environ.get("GBX_VERSION", "0.4.0")
    return (
        f"geobrix[light_env6] @ file:///Volumes/{catalog}/{schema}/{volume}/"
        f"geobrix-{ver}-py3-none-any.whl"
    )


DEFAULT_SPEC = _default_spec()


def _host_from_cfg() -> str:
    h = os.environ.get("DATABRICKS_HOST")
    if h:
        return h.rstrip("/")
    cfg = Path.home() / ".databrickscfg"
    if PROFILE and cfg.exists():
        in_p = False
        for line in cfg.read_text().splitlines():
            s = line.strip()
            if s.startswith("[") and s.endswith("]"):
                in_p = (s == f"[{PROFILE}]")
            elif in_p and s.lower().startswith("host"):
                return s.split("=", 1)[1].strip().rstrip("/")
    return ""


def _mint_token() -> str:
    """Bearer token via the CLI's cached access token (no SDK rotating-refresh)."""
    pre = os.environ.get("DATABRICKS_TOKEN")
    if pre:
        return pre
    if not PROFILE:
        return ""
    clean = {k: v for k, v in os.environ.items() if k != "DATABRICKS_CONFIG_PROFILE"}
    out = subprocess.run(
        ["databricks", "auth", "token", "-p", PROFILE],
        capture_output=True, text=True, timeout=60, env=clean,
    ).stdout
    return json.loads(out)["access_token"] if out.strip().startswith("{") else ""


def _client() -> WorkspaceClient:
    """Authenticate with a CLI-minted bearer token (reliable here — the SDK's
    profile path rotates the single-use refresh token on each client and breaks
    across repeated runs). Falls back to profile/default chain if no token."""
    host, tok = _host_from_cfg(), _mint_token()
    if host and tok:
        return WorkspaceClient(host=host, token=tok)
    return WorkspaceClient(profile=PROFILE) if PROFILE else WorkspaceClient()


def notebook_source(spec: str) -> str:
    # Databricks SOURCE-format Python notebook. %pip + restartPython, then probe.
    probe = '''
import json, importlib.metadata as md, warnings, traceback
r = {}
# 1) Spark-Connect health AFTER the install (protobuf bump can break Connect)
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_range_count"] = int(spark.range(5).count())
    r["spark_ok"] = True
except Exception as e:
    r["spark_ok"] = False
    r["spark_error"] = repr(e)
# 2) Key package versions (confirm what the install upgraded)
for p in ["protobuf","idna","numpy","pandas","pyarrow","grpcio","ipython",
          "rasterio","shapely","rio-tiler","mapbox-vector-tile","pyogrio",
          "pyproj","scikit-image","xarray-spatial","h3","quadbin","scipy","numexpr"]:
    try:
        r["ver_"+p] = md.version(p)
    except Exception:
        r["ver_"+p] = "MISSING"
# 3) Import + register the lightweight raster API and run a tiny op
# 2b) MVT encode via the exact pyvx 2.x API (default_options + typed props)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode(
        {"name": "t", "features": [
            {"geometry": Point(10, 10), "properties": {"id": 1, "name": "x", "v": 1.5}}]},
        default_options={"extents": 4096, "y_coord_down": True})
    r["mvt_encode_bytes"] = len(b)
    r["mvt_ok"] = len(b) > 0
except Exception as e:
    r["mvt_ok"] = False
    r["mvt_error"] = repr(e)
# 3) Import + register the lightweight raster API and run a tiny op
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_import"] = "ok"
    # a trivially small light op end-to-end via SQL would need data; just
    # confirm a registered function resolves in the catalog.
    try:
        funcs = [f.name for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")]
        r["gbx_rst_registered_count"] = len(funcs)
    except Exception as e:
        r["catalog_error"] = repr(e)
except Exception as e:
    r["pyrx_import"] = "ERROR"
    r["pyrx_error"] = repr(e)
    r["pyrx_trace"] = traceback.format_exc()[-1500:]
dbutils.notebook.exit(json.dumps(r))
'''
    return (
        "# Databricks notebook source\n"
        f"# MAGIC %pip install {spec}\n"
        "\n# COMMAND ----------\n"
        "dbutils.library.restartPython()\n"
        "\n# COMMAND ----------\n"
        + probe
    )


def diagnose_source(spec: str, dry_run: bool = True) -> str:
    # No %pip magic — run pip via subprocess to capture the FULL output (the
    # resolver/install error) under the v5 immutable constraints. With
    # dry_run=False this performs the REAL install so install-time failures
    # (uninstall-protected core packages, etc.) surface in the captured output.
    dr = '"--dry-run", ' if dry_run else ""
    body = f'''
import json, subprocess, sys
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
r = {{}}
reqs = {spec!r}.split(" ||| ")
import tempfile, os
rf = os.path.join(tempfile.gettempdir(), "gbx_reqs.txt")
open(rf, "w").write("\\n".join(reqs) + "\\n")
args = [sys.executable, "-m", "pip", "install", {dr}"--no-input",
        "-c", CONSTRAINTS, "-r", rf]
p = subprocess.run(args, capture_output=True, text=True)
r["rc"] = p.returncode
r["stdout"] = p.stdout[-7000:]
r["stderr"] = p.stderr[-7000:]
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def isolate_register_source() -> str:
    # Deps come from the job Environment spec. Wrap udf/udtf.register to log
    # which exact registration raises (the SUM:DOUBLE parse error).
    body = '''
import json, traceback
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from databricks.labs.gbx.pyrx import functions as F
log = []
orig_udf = spark.udf.register
orig_udtf = spark.udtf.register
def wrap_udf(name, f=None, *a, **k):
    try:
        return orig_udf(name, f, *a, **k)
    except Exception as e:
        log.append({"kind":"udf","name":name,"err":repr(e)[:400]}); raise
def wrap_udtf(name, cls=None, *a, **k):
    try:
        return orig_udtf(name, cls, *a, **k)
    except Exception as e:
        log.append({"kind":"udtf","name":name,"err":repr(e)[:400]}); raise
try:
    spark.udf.register = wrap_udf
    spark.udtf.register = wrap_udtf
    patched = True
except Exception as e:
    patched = False
r = {"patched": patched}
try:
    F.register(spark)
    r["register"] = "ok"
except Exception as e:
    r["register"] = "ERROR"
    r["error"] = repr(e)[:400]
    r["first_failed_call"] = log[-1] if log else "n/a (failure not in a wrapped register call)"
r["all_failures"] = log
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def probe_only_source() -> str:
    # No install in the notebook — deps come from the job Environment spec
    # (provisioned before the notebook runs; ephemeral env on the path first).
    body = '''
import json, traceback, importlib.metadata as md
r = {}
for pkg in ["protobuf","idna","typing_extensions","mapbox-vector-tile","rasterio",
            "rio-tiler","shapely","numpy","pyarrow"]:
    try: r["ver_"+pkg] = md.version(pkg)
    except Exception: r["ver_"+pkg] = "MISSING"
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_ok"] = True; r["spark_range_count"] = int(spark.range(5).count())
except Exception as e:
    r["spark_ok"] = False; r["spark_error"] = repr(e)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode({"name":"t","features":[{"geometry":Point(10,10),"properties":{"id":1,"name":"x","v":1.5}}]},
                   default_options={"extents":4096,"y_coord_down":True})
    r["mvt_ok"] = len(b) > 0; r["mvt_bytes"] = len(b)
except Exception as e:
    r["mvt_ok"] = False; r["mvt_error"] = repr(e)
# pyrx: import + register (the SQL names)
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_register"] = "ok"
except Exception as e:
    r["pyrx_register"] = "ERROR"; r["pyrx_register_error"] = repr(e)[:400]
# pyrx: REAL execution on Serverless workers (the user-facing proof) — build a
# tiny in-memory GeoTIFF and read its width back through the Column API.
try:
    import numpy as np
    from pyspark.sql.functions import lit
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx import functions as rx
    with MemoryFile() as mf:
        with mf.open(driver="GTiff", width=4, height=4, count=1, dtype="uint8") as ds:
            ds.write(np.arange(16, dtype="uint8").reshape(4, 4), 1)
        content = mf.read()
    df = spark.createDataFrame([(content,)], "content binary")
    # rst_fromcontent(content, driver) — the GDAL driver name is required.
    w_ = df.select(
        rx.rst_width(rx.rst_fromcontent("content", lit("GTiff"))).alias("w")
    ).collect()
    r["pyrx_exec_width"] = int(w_[0]["w"]) if w_ else None
    r["pyrx_exec_ok"] = (r.get("pyrx_exec_width") == 4)
except Exception as e:
    r["pyrx_exec_ok"] = False; r["pyrx_exec_error"] = repr(e)[:400]
# pyvx register
try:
    from databricks.labs.gbx.pyvx import functions as vx
    vx.register(spark); r["pyvx_register"] = "ok"
except Exception as e:
    r["pyvx_register"] = "ERROR"; r["pyvx_error"] = repr(e)[:400]
# Characterize the catalog.listFunctions() SUM:DOUBLE separately (NOT a gbx
# call path users need — it's a Serverless/UC introspection quirk).
try:
    n = len([f for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")])
    r["listfunctions_ok"] = True; r["gbx_rst_count"] = n
except Exception as e:
    r["listfunctions_ok"] = False; r["listfunctions_error"] = repr(e)[:200]
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def funcval_source(spec: str) -> str:
    # Real subprocess install (bypasses the strict job-%pip magic) THEN exercise
    # the API in the same session: confirms the fixed wheel installs and the
    # lightweight API works (Spark-Connect health, versions, MVT encode, pyrx).
    body = f'''
import json, subprocess, sys, importlib, traceback
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
r = {{}}
p = subprocess.run([sys.executable,"-m","pip","install","--no-input","-c",CONSTRAINTS,{spec!r}],
                   capture_output=True, text=True)
r["install_rc"] = p.returncode
r["install_conflict_report"] = ("dependency conflicts" in (p.stdout+p.stderr))
r["install_tail"] = (p.stdout+p.stderr)[-1200:]
importlib.invalidate_caches()
import importlib.metadata as md
for pkg in ["protobuf","idna","typing_extensions","mapbox-vector-tile","rasterio","shapely","numpy","pyarrow"]:
    try: r["ver_"+pkg] = md.version(pkg)
    except Exception: r["ver_"+pkg] = "MISSING"
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_ok"] = True; r["spark_range_count"] = int(spark.range(5).count())
except Exception as e:
    r["spark_ok"] = False; r["spark_error"] = repr(e)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode({{"name":"t","features":[{{"geometry":Point(10,10),"properties":{{"id":1,"name":"x","v":1.5}}}}]}},
                   default_options={{"extents":4096,"y_coord_down":True}})
    r["mvt_ok"] = len(b) > 0; r["mvt_bytes"] = len(b)
except Exception as e:
    r["mvt_ok"] = False; r["mvt_error"] = repr(e)
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_import"] = "ok"
    r["gbx_rst_count"] = len([f.name for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")])
except Exception as e:
    r["pyrx_import"] = "ERROR"; r["pyrx_error"] = repr(e); r["pyrx_trace"] = traceback.format_exc()[-1200:]
try:
    from databricks.labs.gbx.pyvx import functions as vx
    vx.register(spark)
    r["pyvx_register"] = "ok"
except Exception as e:
    r["pyvx_register"] = "ERROR"; r["pyvx_error"] = repr(e)
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def probe_mvt_source() -> str:
    body = '''
import json, subprocess, sys, re
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
out = {}
idx = subprocess.run([sys.executable,"-m","pip","index","versions","mapbox-vector-tile"],
                     capture_output=True, text=True)
out["index"] = (idx.stdout + idx.stderr)[-1500:]
res = {}
for ver in ["2.0.0","2.0.1","2.1.0","2.2.0"]:
    p = subprocess.run(
        [sys.executable,"-m","pip","install","--dry-run","--no-input","-c",CONSTRAINTS,
         "mapbox-vector-tile=="+ver],
        capture_output=True, text=True)
    txt = p.stdout + p.stderr
    prot = re.search(r"protobuf-([0-9][0-9.]*)", txt)
    res[ver] = {
        "rc": p.returncode,
        "protobuf_would_install": prot.group(1) if prot else "none (base 5.29.4 kept)",
        "conflict": "ERROR" in txt and "dependency conflicts" in txt,
        "tail": txt[-500:],
    }
out["results"] = res
dbutils.notebook.exit(json.dumps(out))
'''
    return "# Databricks notebook source\n" + body


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", default=DEFAULT_SPEC, help="pip install target")
    ap.add_argument("--env-version", default="5")
    ap.add_argument("--run-name", default="geobrix-serverless-light-smoke")
    ap.add_argument("--diagnose", action="store_true",
                    help="pip --dry-run resolver-conflict capture (no install)")
    ap.add_argument("--real-install", action="store_true",
                    help="with --diagnose: REAL subprocess install, captured output")
    ap.add_argument("--probe-mvt", action="store_true",
                    help="probe mapbox-vector-tile 2.x versions for protobuf requirement")
    ap.add_argument("--func-validate", action="store_true",
                    help="real subprocess install + exercise the API in-session")
    ap.add_argument("--env-deps", action="store_true",
                    help="install via the job Environment spec deps (not %pip); probe-only notebook")
    ap.add_argument("--isolate-register", action="store_true",
                    help="env-deps + wrap register to pinpoint the failing UDF registration")
    args = ap.parse_args()

    w = _client()
    if args.isolate_register:
        src = isolate_register_source()
        args.env_deps = True  # provision deps via the Environment spec
    elif args.env_deps:
        src = probe_only_source()
    elif args.func_validate:
        src = funcval_source(args.spec)
    elif args.probe_mvt:
        src = probe_mvt_source()
    elif args.diagnose:
        src = diagnose_source(args.spec, dry_run=not args.real_install)
    else:
        src = notebook_source(args.spec)
    nb_path = f"/Users/{w.current_user.me().user_name}/{NB_NAME}"
    print(f"Install spec: {args.spec}")
    print(f"Uploading smoke notebook to {nb_path} ...")
    import base64
    w.workspace.import_(
        path=nb_path, format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=base64.b64encode(src.encode()).decode(), overwrite=True,
    )

    print(f"Submitting serverless run (env v{args.env_version}) ...")
    run = w.jobs.submit(
        run_name=args.run_name,
        tasks=[jobs.SubmitTask(
            task_key="smoke",
            notebook_task=jobs.NotebookTask(notebook_path=nb_path),
            environment_key="light_env",
        )],
        environments=[jobs.JobEnvironment(
            environment_key="light_env",
            spec=compute.Environment(
                environment_version=args.env_version,
                dependencies=[args.spec] if args.env_deps else None,
            ),
        )],
    )
    run_id = run.bind()["run_id"] if hasattr(run, "bind") else run.run_id
    # run is a Wait; get the run_id
    try:
        run_id = run.run_id
    except Exception:
        pass
    print(f"Run submitted: run_id={run_id}")
    # poll
    final = None
    for i in range(60):
        r = w.jobs.get_run(run_id)
        life = str(r.state.life_cycle_state) if r.state else "?"
        res = str(r.state.result_state) if (r.state and r.state.result_state) else ""
        print(f"poll {i}: life={life} result={res}", flush=True)
        if "TERMINATED" in life or "INTERNAL_ERROR" in life or "SKIPPED" in life:
            final = r
            break
        time.sleep(20)
    if not final:
        print("POLL_TIMEOUT")
        return 1
    # fetch task output
    task_run_id = final.tasks[0].run_id
    out = w.jobs.get_run_output(task_run_id)
    print("=== run result_state:", final.state.result_state, "===")
    if final.state and final.state.state_message:
        print("state_message:", final.state.state_message[:500])
    if getattr(out, "notebook_output", None) and out.notebook_output.result:
        print("=== notebook exit JSON ===")
        try:
            print(json.dumps(json.loads(out.notebook_output.result), indent=2))
        except Exception:
            print(out.notebook_output.result)
    if getattr(out, "error", None):
        print("=== ERROR ===\n", out.error)
    if getattr(out, "error_trace", None):
        print("=== ERROR TRACE (tail) ===\n", out.error_trace[-2000:])
    print("run_page_url:", final.run_page_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
