# Databricks notebook source
# Run as a one-time job on Serverless ENVIRONMENT VERSION 5 (Python 3.12): asserts the
# [stac] extra installs + imports on v5 (catches rio-tiler-9.3.0-style breakage early).
import json
res = {"py": __import__("sys").version.split()[0]}
try:
    import importlib.metadata as md
    import pystac_client, planetary_computer  # noqa: F401
    res["pystac_client"] = md.version("pystac-client")
    res["planetary_computer"] = md.version("planetary-computer")
    from databricks.labs.gbx.stac import StacClient
    res["client_catalog"] = StacClient().catalog  # construct only, no network
    res["stac_import"] = "ok"
except Exception as e:
    import traceback
    res["error"] = repr(e); res["tb"] = traceback.format_exc()[-1200:]
dbutils.notebook.exit(json.dumps(res))
