# Databricks notebook source
# MAGIC %md
# MAGIC # vapor-eyes — shared config (`%run ./config_nb`)
# MAGIC Permian/Delaware-Basin methane cascade on the GeoBrix lightweight tier.
# MAGIC Sets catalog/schema + the Volume ETL tree + toggles + downloaders, mirroring
# MAGIC the eo-series / helios `config_nb`.

# COMMAND ----------

# -- GeoBrix lightweight tier (option-1, default). Two-step install so a freshly
#    rebuilt same-version wheel's bytes replace the cached install (step 1
#    --force-reinstall --no-deps) and the extras still resolve (step 2). Then a
#    %restart_python (next cell) loads the fresh bytes.
# MAGIC %pip install --quiet --disable-pip-version-check --force-reinstall --no-deps "geobrix @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
# MAGIC %pip install --quiet "geobrix[light,stac,vizx] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
# MAGIC %pip install --quiet rich

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os

from pyspark.databricks.sql import functions as DBF  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401
from pyspark.sql.types import *  # noqa: F401,F403

# GeoBrix light tier (option-1 default). option-2 (heavyweight) commented.
from databricks.labs.gbx.pyrx import functions as rx

# from databricks.labs.gbx.rasterx import functions as rx  # option-2: heavyweight
rx.register(spark)  # noqa: F821

from databricks.labs.gbx.ds.register import register  # noqa: E402

register(spark)  # noqa: F821  gtiff_gbx / geojson_gbx / netcdf_gbx / ...

# COMMAND ----------

# ============================================================
# USER SETTINGS — edit these (everything below is wired off them)
# ============================================================

# Unity Catalog: a Volume named 'data' must exist under catalog/schema.
catalog_name = "geospatial_docs"
schema_name = "vapor_eyes"

# Toggles (overridable per-notebook right after %run ./config_nb):
FULL_AOI = False           # False -> SMALL demo AOI; True -> full Delaware Basin
FORCE_REBUILD = False       # True -> re-download / re-create tables (skip-guards off)
INTERACTIVE_PLOTS = False   # False -> static maps (GitHub-friendly); True -> MapLibre

# Delaware Basin AOI (coverage-verified). SMALL is a 15-plume EMIT super-emitter
# cluster; FULL is the wider basin. (minx, miny, maxx, maxy; EPSG:4326)
SMALL_BBOX = (-103.90, 31.65, -103.40, 32.15)
FULL_BBOX = (-104.4, 31.3, -103.0, 32.7)
AOI_BBOX = FULL_BBOX if FULL_AOI else SMALL_BBOX

# Datetime window (anchored near an EMIT overpass over the SMALL cluster).
DATE_WINDOW = "2024-08-01/2024-09-30"

# EMIT / NASA Earthdata token — a Unity Catalog secret (catalog.schema.name),
# read via the secret() SQL function. NB03 (EMIT) only; NB01/02/04/05 don't need it.
EARTHDATA_UC_SECRET = "geospatial_docs.vapor_eyes.earthdata_token"

# COMMAND ----------

# Earthdata token -> env (guarded; NB03 prints clear guidance if absent). Read the
# UC secret via the secret() SQL function; the value is never printed.
_cat, _sch, _name = EARTHDATA_UC_SECRET.split(".")
_tok = None
for _q in (
    f"SELECT secret('{_cat}.{_sch}.{_name}')",          # single dotted arg
    f"SELECT secret('{_cat}.{_sch}', '{_name}')",       # scope, key form
):
    try:
        _tok = spark.sql(_q).collect()[0][0]  # noqa: F821
        if _tok:
            break
    except Exception:
        continue
if _tok:
    os.environ["EARTHDATA_TOKEN"] = _tok
    print(f"... EARTHDATA_TOKEN loaded from UC secret {EARTHDATA_UC_SECRET} (len {len(_tok)})")
else:
    print(
        f"... EARTHDATA_TOKEN NOT set. NB03 (EMIT) needs UC secret "
        f"{EARTHDATA_UC_SECRET}; NB01/02/04/05 run fine without it."
    )
del _tok

# COMMAND ----------

# Spark-conf tuning, guarded for Serverless (no-ops there; AQE handles it).
def set_conf_safe(key, value):
    try:
        spark.conf.set(key, value)  # noqa: F821
        return True
    except Exception as e:
        print(f"... skipping spark.conf.set({key}) [Serverless?]: {type(e).__name__}")
        return False


set_conf_safe("spark.sql.adaptive.coalescePartitions.enabled", "false")
set_conf_safe("spark.sql.shuffle.partitions", 512)

# COMMAND ----------

# Apply catalog/schema from USER SETTINGS.
spark.sql(f"USE CATALOG {catalog_name}")  # noqa: F821
spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")  # noqa: F821
spark.sql(f"USE DATABASE {schema_name}")  # noqa: F821
print(f"... catalog: '{catalog_name}' (USE)")
print(f"... schema:  '{schema_name}' (CREATE / USE)")

# COMMAND ----------

# Volume ETL tree (Volume 'data' must exist).
ETL_DIR = f"/Volumes/{catalog_name}/{schema_name}/data"
VAPOR_EYES_DIR = f"{ETL_DIR}/vapor-eyes"
S5P_DIR = f"{VAPOR_EYES_DIR}/s5p"
S2_DIR = f"{VAPOR_EYES_DIR}/sentinel2"
EMIT_DIR = f"{VAPOR_EYES_DIR}/emit"
WELLS_DIR = f"{VAPOR_EYES_DIR}/wells"
TILES_DIR = f"{VAPOR_EYES_DIR}/tiles"
for _d in (S5P_DIR, S2_DIR, EMIT_DIR, WELLS_DIR, TILES_DIR):
    dbutils.fs.mkdirs(_d)  # noqa: F821
print(f"... VAPOR_EYES_DIR: '{VAPOR_EYES_DIR}' (MKDIRS s5p/ sentinel2/ emit/ wells/ tiles/)")
print(f"... AOI_BBOX ({'FULL' if FULL_AOI else 'SMALL'}): {AOI_BBOX}")

# COMMAND ----------

# Idempotent managed-Delta materializer (Serverless-safe cache() stand-in).
def finalize_delta(df, tbl_name, do_display=True):
    if FORCE_REBUILD:
        spark.sql(f"DROP TABLE IF EXISTS {tbl_name}")  # noqa: F821
    elif spark.catalog.tableExists(tbl_name):  # noqa: F821
        if [f.name for f in spark.table(tbl_name).schema] != [  # noqa: F821
            f.name for f in df.schema
        ]:
            print(f"... schema changed for {tbl_name} -> rewriting (was stale)")
            spark.sql(f"DROP TABLE IF EXISTS {tbl_name}")  # noqa: F821
    if not spark.catalog.tableExists(tbl_name):  # noqa: F821
        df.write.mode("overwrite").saveAsTable(tbl_name)
        print(f"... wrote table {tbl_name} ({spark.table(tbl_name).count():,} rows)")  # noqa: F821
    else:
        print(f"... table {tbl_name} exists (skip; FORCE_REBUILD=False)")
    out = spark.table(tbl_name)  # noqa: F821
    if do_display:
        out.printSchema()
    return out

# COMMAND ----------

# Downloaders + STAC client + vizx helpers.
from databricks.labs.gbx.sample import (  # noqa: E402
    EmitDownloader,
    TropomiDownloader,
    WellsDownloader,
)
from databricks.labs.gbx.stac import StacClient  # noqa: E402

tropomi = TropomiDownloader()
emit = EmitDownloader()
wells = WellsDownloader()
stac_client = StacClient()

from databricks.labs.gbx.vizx import (  # noqa: E402,F401
    plot_pmtiles,
    plot_raster,
)

print("... downloaders: tropomi, emit, wells | stac_client | vizx: plot_raster, plot_pmtiles")
