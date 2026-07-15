"""Task 1 of the vapor-eyes-lf job: date-parameterized downloader driver.

Runs the GeoBrix sample downloaders idempotently (their own skip-guards avoid
re-downloading valid staged files) and lands raw files into the pipeline's own
Volume subtree. Emits NO Delta tables — the pipeline's Auto Loader bronze layer
inventories the staged files. This file is NOT part of the pipeline and must not
import pyspark.pipelines."""
import argparse
import os

# Dual-context import. Under pytest (tests/conftest.py puts `lakeflow/` on
# sys.path) `land` is a namespace package, so `from land._dates` resolves. As a
# job `spark_python_task` the file is exec()'d with its own dir `land/` on
# sys.path[0] and no `__file__`, so `import land` finds land.py itself (not a
# package) — fall back to the flat sibling import `from _dates`.
try:
    from land._dates import asof_window
except ImportError:  # pragma: no cover - exercised only in the job runtime
    from _dates import asof_window


def _subtree(catalog, schema, volume):
    root = f"/Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf"
    return {
        "root": root, "s5p": f"{root}/s5p", "s2": f"{root}/sentinel2",
        "emit": f"{root}/emit", "wells": f"{root}/wells", "cm": f"{root}/cm",
        "context": f"{root}/context",
    }


def run_land(spark, sources, *, catalog, schema, volume, date_window,
             s5p_temporal, bbox=(-104.5, 30.8, -101.0, 33.0),
             cloud_max=20,
             earthdata_secret="geospatial_docs.vapor_eyes.earthdata_token",
             cm_secret="geospatial_docs.vapor_eyes.carbon_mapper_token",
             cm_window="2024-01-01/2026-07-14",
             s5p_windows=None, emit_windows=None):
    """Land raw files for the requested `sources`.

    S5P and EMIT each accept MULTIPLE temporal windows so a single land run can
    build a multi-year record: `s5p_windows` / `emit_windows` are lists of
    `YYYY-MM-DD/YYYY-MM-DD` ranges. Each window is downloaded in turn into the
    same Volume subtree — the downloaders and Auto Loader are idempotent/append,
    so windows simply accumulate granules. When a list is not supplied the code
    falls back to the single-window params (`s5p_temporal` for S5P, `date_window`
    for EMIT), preserving back-compat. S2 always uses the single `date_window`."""
    from databricks.labs.gbx.sample import (
        EmitDownloader, TropomiDownloader, WellsDownloader)
    dirs = _subtree(catalog, schema, volume)
    for d in dirs.values():
        _mkdir(spark, d)
    # Normalize to window lists; single-window params are the back-compat fallback.
    s5p_wins = list(s5p_windows) if s5p_windows else [s5p_temporal]
    emit_wins = list(emit_windows) if emit_windows else [date_window]
    staged = {}
    if "s5p" in sources:
        total = 0
        for win in s5p_wins:
            print(f"... s5p window {win}")
            df = TropomiDownloader().download(bbox, dirs["s5p"], temporal=win, spark=spark)
            rows = df.select("out_file_path", "out_file_sz", "is_out_file_valid").collect()
            for r in rows:
                print(f"... s5p granule: valid={r['is_out_file_valid']} "
                      f"sz={r['out_file_sz']} path={r['out_file_path']}")
            total += len(rows)
        staged["s5p"] = total
        _list_dir(dirs["s5p"], "s5p")
    if "s2" in sources:
        from databricks.labs.gbx.stac import StacClient
        staged["s2"] = _land_s2(
            spark, StacClient(), dirs["s2"], date_window,
            catalog=catalog, schema=schema, cloud_max=cloud_max)
        _list_dir(dirs["s2"], "s2")
    if "emit" in sources:
        # EMIT (NASA LP DAAC) needs an Earthdata bearer token. Read it from the
        # UC secret and export EARTHDATA_TOKEN so the downloader's HTTP client
        # picks it up. Guarded: if the secret is unreadable we log and continue —
        # S5P/wells do not need it, and EMIT then fails loudly on its own.
        token = _read_earthdata_token(spark, earthdata_secret)
        if token:
            os.environ["EARTHDATA_TOKEN"] = token
            print(f"... EARTHDATA_TOKEN set from secret '{earthdata_secret}' "
                  f"({len(token)} chars)")
        else:
            print(f"... WARNING: no Earthdata token from '{earthdata_secret}'; "
                  f"EMIT download may fail (S5P/wells unaffected)")
        total = 0
        for win in emit_wins:
            print(f"... emit window {win}")
            df = EmitDownloader().download(bbox, dirs["emit"], temporal=win, spark=spark)
            total += df.count()
        staged["emit"] = total
        _list_dir(dirs["emit"], "emit")
    if "wells" in sources:
        df = WellsDownloader().download(bbox, dirs["wells"], spark=spark)
        staged["wells"] = int(df.first()["feature_count"])
        _list_dir(dirs["wells"], "wells")
    if "cm" in sources:
        # Carbon Mapper rated plumes (BYOT free token via UC secret). Runtime fetch
        # only -- the collected items land as JSONL on the Volume (NEVER committed to
        # git). Guarded like EMIT: without a token we log and skip (CM is additive).
        token = _read_secret(spark, cm_secret)
        if token:
            staged["cm"] = _land_cm(dirs["cm"], bbox, cm_window, token)
            _list_dir(dirs["cm"], "cm")
        else:
            print(f"... WARNING: no Carbon Mapper token from '{cm_secret}'; "
                  f"skipping CM source (other sources unaffected)")
            staged["cm"] = 0
    if "context" in sources:
        staged["context"] = _land_context(dirs["context"])
        _list_dir(dirs["context"], "context")
    print(f"... landed: {staged}")
    return staged


_EIA_PLAYS_URL = (
    "https://hub.arcgis.com/api/download/v1/items/"
    "3f001fba00dc4add8dbd00542d61e4da/geojson?redirect=true&layers=0"
)
_TIGER_COUNTIES_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"
)


def _http_get_to_file(url, dst, timeout=180):
    """Stream an HTTP GET to a local/Volume file path. Raises on non-200."""
    import requests
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    with open(dst, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                fh.write(chunk)


def _land_context(context_dir):
    """Download the two static Permian context geometry sources into
    context_dir/{plays,counties}. Returns the number of files landed.

    Guarded: any download failure logs a WARNING and is skipped — context is
    additive, so a failure must not abort the (already-valuable) core demo."""
    landed = 0
    plays_dir = os.path.join(context_dir, "plays")
    counties_dir = os.path.join(context_dir, "counties")
    os.makedirs(plays_dir, exist_ok=True)
    os.makedirs(counties_dir, exist_ok=True)
    try:
        dst = os.path.join(plays_dir, "plays.geojson")
        _http_get_to_file(_EIA_PLAYS_URL, dst)
        print(f"... context: EIA plays -> {dst}")
        landed += 1
    except Exception as e:  # noqa: BLE001 - guarded, additive source
        print(f"... WARNING: EIA plays download failed ({e}); skipping")
    try:
        dst = os.path.join(counties_dir, "cb_2024_us_county_500k.zip")
        _http_get_to_file(_TIGER_COUNTIES_URL, dst)
        print(f"... context: TIGER counties -> {dst}")
        landed += 1
    except Exception as e:  # noqa: BLE001 - guarded, additive source
        print(f"... WARNING: TIGER counties download failed ({e}); skipping")
    return landed


def _land_cm(cm_dir, bbox, cm_window, token):
    """Fetch Carbon Mapper annotated CH4 plumes for the AOI + window and write them
    as JSONL to the Volume (one JSON object per line). Runtime-only; never committed.

    API contract (verified via live probe):
      GET https://api.carbonmapper.org/api/v1/catalog/plumes/annotated
      - Bearer token auth.
      - bbox as FOUR REPEATED params (min_lon,min_lat,max_lon,max_lat); the comma
        form 422s.
      - datetime=<start>/<end> (RFC3339); plume_gas=CH4; limit=1000; offset paginates.
      - Response JSON has a flat `items[]`; each item is flat (no nested properties).
    Pagination loops until a page returns fewer than `limit` items. Each item's nested
    `geometry_json` (a GeoJSON geometry object) is coerced to a JSON STRING so the
    bronze Auto Loader reads a stable string column that silver feeds straight to
    st_geomfromgeojson (no per-file struct-schema drift)."""
    import json
    import requests

    minx, miny, maxx, maxy = bbox
    start, end = cm_window.split("/")
    url = "https://api.carbonmapper.org/api/v1/catalog/plumes/annotated"
    headers = {"Authorization": f"Bearer {token}"}
    limit = 1000
    offset = 0
    items = []
    while True:
        params = [
            ("bbox", minx), ("bbox", miny), ("bbox", maxx), ("bbox", maxy),
            ("datetime", f"{start}/{end}"), ("plume_gas", "CH4"),
            ("limit", limit), ("offset", offset),
        ]
        resp = requests.get(url, headers=headers, params=params, timeout=120)
        resp.raise_for_status()
        page = resp.json().get("items", []) or []
        items.extend(page)
        print(f"... cm page offset={offset}: {len(page)} items (total {len(items)})")
        if len(page) < limit:
            break
        offset += limit

    os.makedirs(cm_dir, exist_ok=True)
    win_tag = cm_window.replace("/", "_")
    out_path = f"{cm_dir}/cm_plumes_{win_tag}.jsonl"
    with open(out_path, "w") as fh:
        for it in items:
            rec = dict(it)
            # Coerce the nested GeoJSON geometry to a JSON string for stable ingest.
            if rec.get("geometry_json") is not None and not isinstance(
                rec["geometry_json"], str
            ):
                rec["geometry_json"] = json.dumps(rec["geometry_json"])
            fh.write(json.dumps(rec) + "\n")
    print(f"... cm wrote {len(items)} plume records -> {out_path}")
    return len(items)


def _get_dbutils(spark):
    """Return a dbutils handle usable from a serverless spark_python_task.
    `pyspark.dbutils.DBUtils(spark)` is the in-cluster path; fall back to the
    SDK's RemoteDbUtils if that import is unavailable."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:  # pragma: no cover - depends on runtime
        from databricks.sdk.dbutils import RemoteDbUtils
        return RemoteDbUtils()


def _read_earthdata_token(spark, secret_ref):
    """Back-compat alias: read the Earthdata token from a UC secret."""
    return _read_secret(spark, secret_ref)


def _read_secret(spark, secret_ref):
    """Read a UC secret by dotted ref.

    A 3-part ref (`catalog.schema.key`) uses the 3-arg UC-secret overload
    `dbutils.secrets.get(catalog, schema, key)` (matches the notebook series); a
    2-part ref falls back to the classic `dbutils.secrets.get(scope, key)`. Returns
    None (never raises) on any failure so the caller can degrade gracefully."""
    parts = secret_ref.split(".")
    try:
        dbutils = _get_dbutils(spark)
    except Exception as e:
        print(f"... dbutils unavailable for secret read: {type(e).__name__}: {e}")
        return None
    try:
        if len(parts) == 3:
            return dbutils.secrets.get(parts[0], parts[1], parts[2])
        if len(parts) == 2:
            return dbutils.secrets.get(parts[0], parts[1])
        print(f"... unexpected secret ref '{secret_ref}' (want catalog.schema.key)")
        return None
    except Exception as e:
        print(f"... secret read failed for '{secret_ref}': {type(e).__name__}: {e}")
        return None


def _list_dir(path, label):
    """Driver-side listing of a staged Volume dir — confirms files persisted
    (the download UDF writes on executors; a swallowed executor-side write leaves
    a nonzero row count but an empty dir)."""
    try:
        import os
        entries = os.listdir(path)
        print(f"... {label} dir {path}: {len(entries)} file(s): {entries[:10]}")
    except Exception as e:
        print(f"... {label} dir listing failed {path}: {type(e).__name__}: {e}")


def _mkdir(spark, path):
    try:
        import os
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        print(f"... mkdir skipped {path}: {type(e).__name__}")


def _land_s2(spark, stac, out_dir, date_window, *, catalog, schema, cloud_max=20):
    """Stage Sentinel-2 B11/B12 SWIR COGs windowed to the top S5P hotspot cell.

    Ports NB02 CELL 6-8: reads the pipeline's `s5p_hotspots` silver (produced by a
    prior pipeline run — the land task runs before the pipeline within a job, so this
    consumes the latest available hotspots; on the very first run the table is
    missing/empty and S2 is skipped, then populates on the next daily run). Picks the
    top cell by ch4_max for the latest observation_date, derives its H3-boundary bbox
    (+3 km pad), searches sentinel-2-l2a over that AOI for the date_window, keeps the
    least-cloudy item's B11/B12 assets, and downloads them windowed to the cell bbox.

    Returns the number of staged band assets (0 if skipped / no cloud-free scene)."""
    from pyspark.sql import functions as F

    tbl = f"{catalog}.{schema}.s5p_hotspots"
    try:
        exists = spark.catalog.tableExists(tbl)
    except Exception as e:
        print(f"... s2 skipped: tableExists({tbl}) failed: {type(e).__name__}: {e}")
        return 0
    if not exists:
        print(f"... s2 skipped: {tbl} does not exist yet (first-run ordering)")
        return 0

    hs = spark.table(tbl)
    latest = hs.agg(F.max("observation_date").alias("d")).first()["d"]
    if latest is None:
        print(f"... s2 skipped: {tbl} is empty (no hotspots yet)")
        return 0
    top = (hs.filter(F.col("observation_date") == F.lit(latest))
             .orderBy(F.desc("ch4_max")).first())
    if top is None:
        print(f"... s2 skipped: no hotspot rows for {latest}")
        return 0
    top_cellid = top["h3_cellid"]

    import h3
    # h3 cellid stored as bigint -> unsigned hex string for the h3 python client.
    _h = format(int(top_cellid) & 0xFFFFFFFFFFFFFFFF, "015x")
    _boundary = h3.cell_to_boundary(_h)  # list[(lat, lon)]
    _lats = [p[0] for p in _boundary]
    _lons = [p[1] for p in _boundary]
    pad = 0.03  # ~3 km pad around the cell for scene context
    s2_bbox = (min(_lons) - pad, min(_lats) - pad,
               max(_lons) + pad, max(_lats) + pad)
    print(f"... s2 target hotspot cell {_h} (date {latest}); AOI bbox {s2_bbox}")

    aoi = spark.createDataFrame(
        [(f'{{"type":"Polygon","coordinates":[[[{s2_bbox[0]},{s2_bbox[1]}],'
          f'[{s2_bbox[2]},{s2_bbox[1]}],[{s2_bbox[2]},{s2_bbox[3]}],'
          f'[{s2_bbox[0]},{s2_bbox[3]}],[{s2_bbox[0]},{s2_bbox[1]}]]]}}',)],
        ["geojson"],
    )
    found = stac.search(
        aoi, geojson_col="geojson", collections=["sentinel-2-l2a"], datetime=date_window
    )
    cloud = F.col("item_properties")["eo:cloud_cover"].cast("double")
    best = (found.withColumn("cloud", cloud)
            .filter(F.col("cloud") <= cloud_max)
            .orderBy("cloud").first())
    if best is None:
        print(f"... s2: no sentinel-2 item <= {cloud_max}% cloud in {date_window}; "
              f"leaving s2 empty (data condition, not an error)")
        return 0
    best_item = best["item_id"]
    print(f"... s2 least-cloudy item {best_item} (cloud={best['cloud']}%)")
    bands = found.filter(
        (F.col("item_id") == best_item) & F.col("asset_name").isin("B11", "B12")
    ).select("item_id", "asset_name", "href")
    s2_dl = stac.download(bands, out_dir, bbox=list(s2_bbox), bbox_crs="EPSG:4326")
    rows = s2_dl.select("asset_name", "out_file_path", "is_out_file_valid").collect()
    for r in rows:
        print(f"... s2 band: {r['asset_name']} valid={r['is_out_file_valid']} "
              f"path={r['out_file_path']}")
    return len(rows)


def main():
    from pyspark.sql import SparkSession
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="s5p")
    ap.add_argument("--window")
    ap.add_argument("--asof")
    ap.add_argument("--catalog", default="geospatial_docs")
    ap.add_argument("--schema", default="vapor_eyes_lf")
    ap.add_argument("--volume", default="data")
    ap.add_argument("--s5p-temporal", default="2024-08-23/2024-08-24")
    # Semicolon-separated lists of `YYYY-MM-DD/YYYY-MM-DD` windows. When present
    # they override the single-window params for their source, letting one run
    # accumulate a multi-year record (downloaders + Auto Loader are idempotent).
    ap.add_argument("--s5p-windows")
    ap.add_argument("--emit-windows")
    ap.add_argument("--cloud-max", type=int, default=20)
    ap.add_argument("--earthdata-secret",
                    default="geospatial_docs.vapor_eyes.earthdata_token")
    ap.add_argument("--cm-secret",
                    default="geospatial_docs.vapor_eyes.carbon_mapper_token")
    # Carbon Mapper fetch window (RFC3339 start/end). Default = full history.
    ap.add_argument("--cm-window", default="2024-01-01/2026-07-14")
    a = ap.parse_args()
    window = a.window or (asof_window(a.asof) if a.asof else "2023-07-15/2023-08-20")
    s5p_windows = ([w for w in a.s5p_windows.split(";") if w.strip()]
                   if a.s5p_windows else None)
    emit_windows = ([w for w in a.emit_windows.split(";") if w.strip()]
                    if a.emit_windows else None)
    spark = SparkSession.builder.getOrCreate()
    run_land(spark, a.sources.split(","), catalog=a.catalog, schema=a.schema,
             volume=a.volume, date_window=window, s5p_temporal=a.s5p_temporal,
             cloud_max=a.cloud_max, earthdata_secret=a.earthdata_secret,
             cm_secret=a.cm_secret, cm_window=a.cm_window,
             s5p_windows=s5p_windows, emit_windows=emit_windows)


if __name__ == "__main__":
    main()
