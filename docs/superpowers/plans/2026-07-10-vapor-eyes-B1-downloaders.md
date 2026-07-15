# vapor-eyes Plan B1 — sample-data downloaders (EmitDownloader + WellsDownloader)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add two `databricks.labs.gbx.sample` downloaders that stage vapor-eyes data to a Unity Catalog Volume with read-validation + idempotent missing-asset recovery: `EmitDownloader` (NASA EMIT CH4 via `earthaccess`) and `WellsDownloader` (TX RRC well pads via ArcGIS REST).

**Architecture:** Both mirror the existing `NaipDownloader`/`DemDownloader`/`TropomiDownloader` shape (`discover`/`download`/`read`/`repair`, Serverless-safe, injectable client for offline tests) and reproduce `StacClient.download`'s 6-column result contract. They differ only in the fetch engine: EMIT uses `earthaccess` (Earthdata-Login-authenticated, **driver-side** download — fine at demo scale), wells use a paged ArcGIS-REST GeoJSON query. COG validation reuses `stac._download.read_validate`; GeoJSON gets a small JSON validator. This is Plan B1 of Spec B; Plan B2 (config_nb + NB01–05 + README/diagrams) consumes these.

**Tech Stack:** Python 3.12, PySpark 4, `earthaccess`, `requests` (both new-ish for `[light]`/sample — see Task 0), `rasterio`/`shapely` (existing), the `geojson_gbx`/`raster_gbx` light readers. Design ref: `docs/superpowers/specs/2026-07-10-vapor-eyes-series-design.md` §5.

## Global Constraints

- **Serverless-safe:** no `spark.conf.set`, `_jvm`, `.rdd`, `.cache()`, `.persist()` in `sample/emit.py` or `sample/wells.py`.
- **Result contract (reproduce StacClient.download exactly):** the `download()` DataFrame has columns `item_id: string`, `asset_name: string`, `out_file_path: string`, `out_file_sz: long`, `is_out_file_valid: boolean`, `last_update: timestamp`. `is_out_file_valid` is true iff the file exists, is above a 1 KB floor, and passes a type-appropriate read (rasterio for `.tif`, JSON parse + non-empty `features` for `.json`).
- **Idempotent + repair:** skip assets already present and valid; `repair(target, where="is_out_file_valid = false")` re-fetches only invalid/missing rows and MERGEs into a Delta table by `(item_id, asset_name)` (mirrors `StacClient.repair`). `FORCE_REBUILD`-style full refetch via a `force` flag.
- **Injection seam:** `_earthaccess=` (EmitDownloader) / `_get=` (WellsDownloader) constructor params let offline tests bypass network — mirror `_MockStacClient` record-and-return.
- **Credential:** EMIT auth via `earthaccess.login(strategy="environment")` reading `EARTHDATA_TOKEN` (set from a Databricks secret). Never require username/password or `.netrc` on workers.
- **Tests live in** `python/geobrix/test/sample/` (already in `_LIGHT_TEST_DIRS`); no conftest/marker change. Each test module defines its own `spark` fixture (mirror `test_naip.py`) and uses `pytest.importorskip("pyspark")`.
- **Commit hygiene:** subject ≤72 chars + WHY body; end with `Co-authored-by: Isaac`. Branch: `examples/vapor-eyes`.

---

### Task 0: Add `earthaccess` to the light dependency set (three envs)

**Files:** `python/geobrix/pyproject.toml` (`[light]`... or a new note), `requirements-pyrx-ci.in`, `requirements-dev-container.in` (+ regenerate both hashed `.txt`).

**Interfaces:** Produces `import earthaccess` in all three light envs (CI, Docker, `.venv-pyrx`). `requests` is already pinned (2.32.3). Follows [[new-feature-dep-and-tier-checklist]] / [[light-ci-lock-completeness]].

- [ ] **Step 1:** Add to `pyproject.toml` `[light]` (after the netcdf4 line): `"earthaccess>=0.11,<1",` with a comment (NASA EMIT auth+download for the sample EmitDownloader).
- [ ] **Step 2:** Add `earthaccess==<pin>` to both `requirements-pyrx-ci.in` and `requirements-dev-container.in` (exact pin; check cp312 wheels + that it doesn't float `requests`/`fsspec` off the DBR base — cap if needed).
- [ ] **Step 3:** Regenerate both hashed locks (container, Linux):
  ```bash
  docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && for f in requirements-pyrx-ci requirements-dev-container; do uv pip compile --generate-hashes --python-version 3.12 --index-url https://pypi-proxy.dev.databricks.com/simple --output-file $f.txt $f.in; done'
  ```
  Inspect `git diff` for version-line changes — only `earthaccess` + its new transitives should appear; investigate any bump to an existing pin.
- [ ] **Step 4:** Install into the container test env + verify: `docker exec geobrix-dev bash -lc 'python3 -m pip install --break-system-packages --quiet earthaccess==<pin> && python3 -c "import earthaccess; print(earthaccess.__version__)"'`
- [ ] **Step 5:** Commit (`build(light): add earthaccess for EmitDownloader`).

---

### Task 1: `EmitDownloader.discover` — earthaccess search → asset rows

**Files:** Create `python/geobrix/src/databricks/labs/gbx/sample/emit.py`; Test `python/geobrix/test/sample/test_emit.py`.

**Interfaces:**
- Consumes: injected `_earthaccess` (a module-like object with `login`, `search_data`, `download`); PySpark.
- Produces: `EmitDownloader(enh_short="EMITL2BCH4ENH", plm_short="EMITL2BCH4PLM", version="002", _earthaccess=None)`; `discover(bbox, temporal=None, spark=None) -> DataFrame[item_id, asset_name, href]`. `asset_name` ∈ {`"ch4enh"`, `"plm_cog"`, `"plm_geojson"`}. Used by Tasks 2–3.

- [ ] **Step 1: Write the failing test.** Create `test/sample/test_emit.py`:
```python
"""Offline unit tests for EmitDownloader (injected earthaccess; no network)."""
from __future__ import annotations
import pytest
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from databricks.labs.gbx.sample.emit import EmitDownloader  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("emit-test")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    yield s
    s.stop()


class _FakeGranule:
    def __init__(self, name, links):
        self._name = name
        self._links = links
    def __getitem__(self, k):  # earthaccess granules are dict-like for umm
        raise KeyError(k)
    def data_links(self, access=None, in_region=False):
        return self._links


class _FakeEarthaccess:
    """Records login/search/download; returns controlled granules."""
    def __init__(self, granules_by_short):
        self._by_short = granules_by_short
        self.login_calls = 0
        self.download_calls = []
    def login(self, strategy="all", persist=False):
        self.login_calls += 1
        return object()
    def search_data(self, short_name=None, version=None, bounding_box=None,
                    temporal=None, count=-1, **kw):
        return list(self._by_short.get(short_name, []))
    def download(self, granules, local_path=None, threads=8, **kw):
        self.download_calls.append({"n": len(granules), "local_path": local_path})
        return [f"{local_path}/f{i}" for i in range(len(granules))]


def _fake_ea():
    enh = _FakeGranule("EMIT_ENH_1", [
        "https://x/EMIT_L2B_CH4ENH_002_20240823T1_o_s.tif",
        "https://x/EMIT_L2B_CH4UNCERT_002_20240823T1_o_s.tif"])
    plm = _FakeGranule("EMIT_PLM_1", [
        "https://x/EMIT_L2B_CH4PLM_002_20240823T1_p.tif",
        "https://x/EMIT_L2B_CH4PLM_002_20240823T1_p.json"])
    return _FakeEarthaccess({"EMITL2BCH4ENH": [enh], "EMITL2BCH4PLM": [plm]})


def test_discover_extracts_enh_and_plm_assets(spark):
    dl = EmitDownloader(_earthaccess=_fake_ea())
    df = dl.discover([-103.9, 31.65, -103.4, 32.15], spark=spark)
    rows = {(r["asset_name"], r["href"].split("/")[-1]) for r in df.collect()}
    assert ("ch4enh", "EMIT_L2B_CH4ENH_002_20240823T1_o_s.tif") in rows
    assert ("plm_cog", "EMIT_L2B_CH4PLM_002_20240823T1_p.tif") in rows
    assert ("plm_geojson", "EMIT_L2B_CH4PLM_002_20240823T1_p.json") in rows
    assert [f.name for f in df.schema.fields] == ["item_id", "asset_name", "href"]
```

- [ ] **Step 2: Run → fail** (`ModuleNotFoundError: ...sample.emit`). `pytest test/sample/test_emit.py -k discover -v`.

- [ ] **Step 3: Implement `discover` in `sample/emit.py`:**
```python
"""EmitDownloader — AOI-driven NASA EMIT CH4 staging via earthaccess.

Mirrors DemDownloader/NaipDownloader (discover/download/read/repair,
Serverless-safe). EMIT is on NASA LP DAAC (not Planetary Computer), so the search
step uses `earthaccess` (Earthdata Login via EARTHDATA_TOKEN, a Databricks secret)
and downloads are DRIVER-SIDE (fine at demo AOI scale; EMIT auth mints short-lived
per-request credentials that don't fan out cleanly). The result frame reproduces
StacClient.download's 6-column contract so repair()/read() behave identically.
"""
from __future__ import annotations
from typing import List, Optional, Sequence

ENH_SHORT = "EMITL2BCH4ENH"
PLM_SHORT = "EMITL2BCH4PLM"
VERSION = "002"


def _asset_of(url: str) -> Optional[str]:
    u = url.lower()
    if "ch4enh" in u and u.endswith(".tif"):
        return "ch4enh"
    if "ch4plm" in u and u.endswith(".tif"):
        return "plm_cog"
    if "ch4plm" in u and u.endswith(".json"):
        return "plm_geojson"
    return None


class EmitDownloader:
    def __init__(self, enh_short=ENH_SHORT, plm_short=PLM_SHORT, version=VERSION,
                 _earthaccess=None):
        self.enh_short = enh_short
        self.plm_short = plm_short
        self.version = version
        self._earthaccess = _earthaccess

    def _ea(self):
        if self._earthaccess is not None:
            return self._earthaccess
        import earthaccess
        return earthaccess

    def _login(self):
        # strategy="environment" reads EARTHDATA_TOKEN (Databricks secret -> env).
        self._ea().login(strategy="environment")

    def _rows(self, bbox, temporal):
        ea = self._ea()
        w, s, e, n = bbox
        out = []
        for short in (self.enh_short, self.plm_short):
            for g in ea.search_data(short_name=short, version=self.version,
                                    bounding_box=(w, s, e, n), temporal=temporal):
                for url in g.data_links(access="external"):
                    a = _asset_of(url)
                    if a is None:
                        continue
                    # item_id = the granule file stem (unique per scene/plume).
                    item_id = url.split("/")[-1].rsplit(".", 1)[0]
                    out.append((item_id, a, url))
        return out

    def discover(self, bbox: Sequence[float], temporal=None, spark=None):
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StringType, StructField, StructType
        spark = spark or SparkSession.getActiveSession()
        schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("asset_name", StringType(), False),
            StructField("href", StringType(), False)])
        return spark.createDataFrame(self._rows(bbox, temporal), schema)
```

- [ ] **Step 4: Run → pass.** `pytest test/sample/test_emit.py -k discover -v` (in the container).
- [ ] **Step 5: Commit** (`feat(sample): EmitDownloader.discover via earthaccess`).

---

### Task 2: `EmitDownloader.download` — driver-side fetch + validate + result frame

**Files:** Modify `sample/emit.py`; Modify `test/sample/test_emit.py`.

**Interfaces:**
- Consumes: `discover`; `stac._download.read_validate` (COG) + a new `_valid_geojson`.
- Produces: `download(bbox, out_dir, temporal=None, force=False, spark=None) -> DataFrame` with the 6-column result contract. Used by Task 3 (`read`/`repair`).

- [ ] **Step 1: Write the failing test.** Append to `test/sample/test_emit.py`:
```python
def test_download_validates_and_builds_result_frame(spark, tmp_path, monkeypatch):
    # Fake earthaccess.download writes a real tiny GeoTIFF + a real GeoJSON so
    # validation passes; assert the 6-col contract + is_out_file_valid True.
    import json, numpy as np, rasterio
    from rasterio.transform import from_origin
    fake = _fake_ea()
    outdir = str(tmp_path / "emit")

    def _dl(granules, local_path=None, threads=8, **kw):
        import os
        os.makedirs(local_path, exist_ok=True)
        paths = []
        for i, _ in enumerate(granules):
            p = os.path.join(local_path, f"g{i}.tif")
            with rasterio.open(p, "w", driver="GTiff", width=4, height=3, count=1,
                               dtype="float32", crs="EPSG:4326",
                               transform=from_origin(-103.9, 32.15, 0.01, 0.01)) as ds:
                ds.write(np.ones((3, 4), "float32"), 1)
            paths.append(p)
        return paths
    fake.download = _dl
    dl = EmitDownloader(_earthaccess=fake)
    res = dl.download([-103.9, 31.65, -103.4, 32.15], outdir, spark=spark)
    assert [f.name for f in res.schema.fields] == [
        "item_id", "asset_name", "out_file_path", "out_file_sz",
        "is_out_file_valid", "last_update"]
    rows = res.collect()
    assert len(rows) >= 1
    assert all(r["is_out_file_valid"] for r in rows)
    assert all(r["out_file_sz"] > 1024 for r in rows)
```
(Also add `test_download_marks_missing_invalid`: a `_dl` that writes nothing → `is_out_file_valid == False`, `out_file_path` null.)

- [ ] **Step 2: Run → fail** (`AttributeError: ... has no attribute 'download'` on EmitDownloader). 
- [ ] **Step 3: Implement `download` + validators in `sample/emit.py`:**
```python
    def download(self, bbox, out_dir, temporal=None, force=False, spark=None):
        import os
        from datetime import datetime
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (BooleanType, LongType, StringType,
                                        StructField, StructType, TimestampType)
        spark = spark or SparkSession.getActiveSession()
        self._login()
        ea = self._ea()
        rows = self._rows(bbox, temporal)
        os.makedirs(out_dir, exist_ok=True)
        results = []
        for item_id, asset, href in rows:
            dest = os.path.join(out_dir, os.path.basename(href))
            if not force and _existing_valid(dest, asset):
                sz = os.path.getsize(dest)
                results.append((item_id, asset, dest, sz, True, datetime.now()))
                continue
            # earthaccess.download works on granule objects; re-fetch the granule
            # whose data_links contains this href, download to out_dir, then map.
            try:
                ea.download([_HrefGranule(href)], local_path=out_dir)
            except Exception:
                pass
            if _existing_valid(dest, asset):
                results.append((item_id, asset, dest, os.path.getsize(dest), True,
                                datetime.now()))
            else:
                results.append((item_id, asset, None, 0, False, datetime.now()))
        schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("asset_name", StringType(), False),
            StructField("out_file_path", StringType(), True),
            StructField("out_file_sz", LongType(), True),
            StructField("is_out_file_valid", BooleanType(), True),
            StructField("last_update", TimestampType(), True)])
        return spark.createDataFrame(results, schema)
```
Add module helpers:
```python
_FLOOR = 1024

def _valid_geojson(path: str) -> bool:
    import json
    try:
        with open(path) as fh:
            gj = json.load(fh)
        return bool(gj.get("features"))
    except Exception:
        return False

def _existing_valid(path: str, asset: str) -> bool:
    import os
    if not (os.path.exists(path) and os.path.getsize(path) > _FLOOR):
        return False
    if asset == "plm_geojson":
        return _valid_geojson(path)
    from databricks.labs.gbx.stac._download import read_validate
    return read_validate(path)  # rasterio window read for the COGs

class _HrefGranule:
    """Minimal granule adapter so earthaccess.download can fetch a single href."""
    def __init__(self, href):
        self._href = href
    def data_links(self, access=None, in_region=False):
        return [self._href]
```
(Implementer note: confirm the installed `earthaccess.download` accepts objects exposing `data_links`; if it strictly requires real `DataGranule`s, switch `download` to fetch the granule list from `search_data` once and pass matching granules through — the injected fake already models `download(granules, local_path=...)`.)

- [ ] **Step 4: Run → pass.** `pytest test/sample/test_emit.py -v`.
- [ ] **Step 5: Commit** (`feat(sample): EmitDownloader.download with validate + result frame`).

---

### Task 3: `EmitDownloader` `read_enh` / `read_plumes` / `repair` + export

**Files:** Modify `sample/emit.py`, `sample/__init__.py`; Modify `test/sample/test_emit.py`, `test/sample/test_sample_bundle.py`.

**Interfaces:** Produces `read_enh(out_dir, spark=None)` (ENH COGs → `raster_gbx` tiles), `read_plumes(out_dir, spark=None)` (PLM GeoJSON → `geojson_gbx`), `repair(target, where=..., spark=None, out_dir=None)`, and `download_emit_aoi(spark, bbox, out_dir, **kw)`.

- [ ] **Step 1: Write failing tests** — `test_read_enh_uses_raster_gbx` (write two `.tif` via rasterio into a dir, assert `read_enh` returns a `tile` column, count 2 after filtering to `CH4ENH` files), `test_read_plumes_uses_geojson_gbx` (write a `.json` FeatureCollection, assert a geometry column), and `test_exports_from_sample` (import `EmitDownloader`, `download_emit_aoi` from `databricks.labs.gbx.sample`).
- [ ] **Step 2: Run → fail.**
- [ ] **Step 3: Implement:**
```python
    def read_enh(self, out_dir, spark=None):
        from pyspark.sql import SparkSession, functions as F
        spark = spark or SparkSession.getActiveSession()
        return (spark.read.format("raster_gbx")
                .option("filterRegex", r".*CH4ENH.*\.tif$")
                .load(out_dir).repartition(64, F.col("source")).select("source", "tile"))

    def read_plumes(self, out_dir, spark=None):
        from pyspark.sql import SparkSession
        spark = spark or SparkSession.getActiveSession()
        return (spark.read.format("geojson_gbx")
                .option("filterRegex", r".*CH4PLM.*\.json$").load(out_dir))

    def repair(self, target, where="is_out_file_valid = false", spark=None, out_dir=None):
        from pyspark.sql import SparkSession
        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        if invalid.count() == 0:
            return invalid
        # Re-derive bbox/out_dir from context; re-download the invalid hrefs.
        # (Store href in the table if you intend to repair — see note.)
        ...  # implement MERGE mirroring StacClient.repair (by item_id, asset_name)
```
**Design note for `repair`:** `StacClient.repair` needs `href` present on the table. So the vapor-eyes `emit_scenes` table must carry `href` (add it to the download result or a joined column) for repair to re-fetch. Simplest: `download()` also returns `href`, and NB03 persists it; `repair` re-runs `download` on the invalid subset and MERGEs on `(item_id, asset_name)`. Implement `repair` to accept a DataFrame/table that includes `item_id, asset_name, href` and MERGE the 4 mutable columns, exactly like `StacClient.repair` (reuse its MERGE shape).
```python
def download_emit_aoi(spark, bbox, out_dir, **kw):
    return EmitDownloader().download(bbox, out_dir, spark=spark, **kw)
```
Add both to `sample/__init__.py` imports + `__all__`; add `"EmitDownloader"` + `"download_emit_aoi"` to the `test_sample_package_all` assertion set.
- [ ] **Step 4: Run → pass** (`pytest test/sample/test_emit.py test/sample/test_sample_bundle.py::test_sample_package_all -v`).
- [ ] **Step 5: Commit** (`feat(sample): EmitDownloader read_enh/read_plumes/repair + export`).

---

### Task 4: `WellsDownloader` — paged TX RRC ArcGIS GeoJSON + validate + read

**Files:** Create `sample/wells.py`; Test `test/sample/test_wells.py`; Modify `sample/__init__.py`, `test/sample/test_sample_bundle.py`.

**Interfaces:** `WellsDownloader(service_url=WELLSHL_URL, _get=None)`; `download(bbox, out_dir, page_size=1000, spark=None) -> DataFrame[out_file_path, feature_count, is_out_file_valid, last_update]`; `read(out_dir, spark=None)` (GeoJSON → `geojson_gbx`); `discover(bbox, spark=None)` (count only). `_get` injects a fake HTTP getter for offline tests.

- [ ] **Step 1: Write the failing test.** `test/sample/test_wells.py`: a fake `_get(url, params)` returns a two-page GeoJSON (page 1 `exceededTransferLimit: true` with N features, page 2 fewer, no flag). Assert `download` writes one merged `wells.geojson`, `feature_count` == total, `is_out_file_valid` True; and `_get` was called twice (paging). Plus `test_exports_from_sample`.
- [ ] **Step 2: Run → fail.**
- [ ] **Step 3: Implement `sample/wells.py`:**
```python
"""WellsDownloader — TX RRC well surface-hole locations via ArcGIS REST (GeoJSON).

Open, no auth. Pages the FeatureServer (resultOffset/resultRecordCount, honoring
exceededTransferLimit), requests f=geojson (ArcGIS reprojects native EPSG:2277 ->
WGS84 lon/lat automatically), merges pages into one GeoJSON on the Volume, and
validates it. read() loads via geojson_gbx. Serverless-safe (driver-side fetch;
one merged file).
"""
from __future__ import annotations
import json
from typing import Optional, Sequence

WELLSHL_URL = ("https://services3.arcgis.com/8jYUORGmDUL39WkJ/arcgis/rest/"
               "services/WellSHL/FeatureServer/0/query")


def _default_get(url, params):
    import requests
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


class WellsDownloader:
    def __init__(self, service_url=WELLSHL_URL, _get=None):
        self.service_url = service_url
        self._get = _get or _default_get

    def _fetch_all(self, bbox, page_size):
        w, s, e, n = bbox
        feats, offset = [], 0
        while True:
            params = {
                "where": "1=1", "geometry": f"{w},{s},{e},{n}",
                "geometryType": "esriGeometryEnvelope", "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects", "outFields": "*",
                "f": "geojson", "resultOffset": offset,
                "resultRecordCount": page_size}
            gj = self._get(self.service_url, params)
            page = gj.get("features", [])
            feats.extend(page)
            if not gj.get("exceededTransferLimit") or not page:
                break
            offset += len(page)
        return feats

    def download(self, bbox: Sequence[float], out_dir: str, page_size=1000, spark=None):
        import os
        from datetime import datetime
        from pyspark.sql import SparkSession
        spark = spark or SparkSession.getActiveSession()
        os.makedirs(out_dir, exist_ok=True)
        feats = self._fetch_all(bbox, page_size)
        dest = os.path.join(out_dir, "wells.geojson")
        with open(dest, "w") as fh:
            json.dump({"type": "FeatureCollection", "features": feats}, fh)
        valid = len(feats) > 0 and os.path.getsize(dest) > 1024
        return spark.createDataFrame(
            [(dest, len(feats), valid, datetime.now())],
            "out_file_path string, feature_count long, "
            "is_out_file_valid boolean, last_update timestamp")

    def read(self, out_dir, spark=None):
        from pyspark.sql import SparkSession
        spark = spark or SparkSession.getActiveSession()
        return (spark.read.format("geojson_gbx")
                .option("filterRegex", r".*wells\.geojson$").load(out_dir))


def download_wells_aoi(spark, bbox, out_dir, **kw):
    return WellsDownloader().download(bbox, out_dir, spark=spark, **kw)
```
(`repair` for wells = re-run `download` when `is_out_file_valid = false`; add a thin `repair(out_dir, bbox, spark=None)` that re-fetches if the file is missing/invalid.)
- [ ] **Step 4:** Export from `sample/__init__.py` + add to `test_sample_package_all`. Run → pass (`pytest test/sample/test_wells.py test/sample/test_sample_bundle.py -v`).
- [ ] **Step 5: Commit** (`feat(sample): WellsDownloader (TX RRC ArcGIS GeoJSON, paged)`).

---

### Task 5: Lint + regression + integration smoke

**Files:** none new (verification task).

- [ ] **Step 1: Format** — container black/isort on the new files (from `python/geobrix`): `python3 -m isort ... && python3 -m black ...` then `--check` + `flake8` (repo config, E501 ignored). Fix any drift.
- [ ] **Step 2: Full regression** — `docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && python3 -m pytest test/sample -q -m "not integration"'`. Expected: all green (new emit/wells tests + the updated `test_sample_package_all`).
- [ ] **Step 3 (optional, gated on creds/network): integration smoke** — a `@pytest.mark.integration` test that, given `EARTHDATA_TOKEN`, pulls one EMIT ENH COG + one PLM GeoJSON for the SMALL bbox and validates; and a wells fetch over the SMALL bbox asserting ≥100 features. Skips when the token/network is absent.
- [ ] **Step 4: Commit** any format fixes (`style: format emit/wells downloaders`).

---

## Notes for the implementer

- **`repair` needs `href`.** To keep `EmitDownloader.repair` real, `download()` should also surface `href` (add a 7th column, or re-`discover` the invalid subset by `item_id`). Persist `href` in the `emit_scenes` table (Plan B2) so repair can re-fetch. Mirror `StacClient.repair`'s MERGE on `(item_id, asset_name)`.
- **Driver-side EMIT download** is a deliberate simplification (EMIT auth mints short-lived per-request creds that don't fan out cleanly like anonymous PC hrefs). Acceptable at demo AOI scale (1 scene / ~15 plume granules SMALL). Note it in the class docstring; a distributed variant is a future enhancement.
- **`earthaccess.download` granule contract** — verify the installed version accepts the `_HrefGranule` adapter, or fetch real `DataGranule`s once via `search_data` and pass the matching subset. The injected `_earthaccess` fake models `download(granules, local_path=...)`.
- **Wells geometry** arrives WGS84 lon/lat via `f=geojson` (ArcGIS reprojects the native EPSG:2277); no `outSR` needed. `geojson_gbx` yields the light vector schema (`geom_0` WKB + `geom_0_srid`/`geom_0_srid_proj`).
- **Tests** stay under `test/sample/` (already tier-gated); each module defines its own `spark` fixture + `pytest.importorskip("pyspark")`; do not add markers.
- After B1 lands and a real SMALL-AOI pull is eyeballed, **Plan B2** (config_nb + NB01–05 + README + 5 diagrams) is written against the actual staged data.
