# netcdf_gbx Lightweight Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an initial NetCDF reader to the GeoBrix lightweight (`pyrx`) API as a Spark Python DataSource named `netcdf_gbx`, with a raster mode (CF grids → GeoTIFF tile) and a vector mode (point/swath → per-cell points), plus a `TropomiDownloader` that stages real Sentinel-5P granules to validate vector mode.

**Architecture:** One `NetcdfGbxDataSource(DataSource)` branches on a `mode` option (default `raster`) in both `schema()` and `reader()` — the exact pattern `VectorGbxDataSource.schema()` uses to read `self.options`. Raster mode reuses the existing `_encode.encode_tile` path (NetCDF variable → in-memory rasterio dataset → GeoTIFF bytes → the shared `(source, tile)` struct). Vector mode builds shapely points → plain WKB rows matching the light vector reader's schema convention (attribute columns + `geom_0` WKB + `geom_0_srid` + `geom_0_srid_proj`). Pure NetCDF/CF logic lives in a helper module `ds/_netcdf.py`; the DataSource + two readers live in `ds/netcdf.py`.

**Tech Stack:** Python 3.12, PySpark 4 Python DataSource API, `xarray` (already in `[light]` via `xarray-spatial`), `netcdf4` (one new `[light]` dep), `rasterio` (existing), `shapely` (existing), `pyproj`/`rasterio.crs` (existing). Design reference: `docs/superpowers/specs/2026-07-10-netcdf-gbx-reader-design.md`.

## Global Constraints

- **Serverless-safe product code:** no `spark.conf.set`, `_jvm`, `.rdd`, `.cache()`, or `.persist()` anywhere in `ds/netcdf.py`, `ds/_netcdf.py`, or `sample/tropomi.py`.
- **GDAL-free light tier:** decode NetCDF only via `xarray` + `netcdf4`; encode rasters only via `rasterio` (its bundled GDAL). Never import `osgeo`.
- **Tile contract:** raster mode emits `struct<source: string, tile: struct<cellid: bigint, raster: binary, metadata: map<string,string>>>` — identical to `raster_gbx`/`gtiff_gbx`. `cellid` = `-1` (`_encode.CELLID_FRESH`); metadata is the exact 11-key set.
- **Vector schema convention:** attribute columns (one per requested variable, typed) in requested order, then geometry column `geom_0` as **plain WKB** (`BinaryType`), then `geom_0_srid` (`StringType`) and `geom_0_srid_proj` (`StringType`). SRID is carried in the string columns, **not** embedded as EWKB.
- **Honesty:** never resample and never apply quality thresholds — pass every requested variable through (e.g. S5P `qa_value` travels as its own column). Class 4 (raw sensor geometry + GLT) is **rejected in both modes**.
- **Not a SQL function:** no `registered_functions.txt` / `function-info.json` / binding-parity changes.
- **Naming:** DataSource format name is exactly `netcdf_gbx`. New reader option names: `mode`, `variable`/`variables`, `group`.
- **Dependency pinning — three environments, kept in lockstep (Task 1):** a new light dep must be pinned in all three places or one environment breaks:
  1. **CI light tier** — `requirements-pyrx-ci.in` → regenerate hashed `requirements-pyrx-ci.txt` (`uv pip compile --generate-hashes --python-version 3.12`). CI installs it with `--require-hashes` (`.github/actions/pyrx_build/action.yml`); a stale lock fails closed.
  2. **Docker dev/test** (`geobrix-dev`, used by `gbx:test:python`) — `requirements-dev-container.in` → regenerate hashed `requirements-dev-container.txt`.
  3. **Local `.venv-pyrx`** (`gbx:venv:sync`, floor-only resolve) — the floor pin in the pyproject extra that `gbx:venv:sync` installs.
- **Tier test gating:** heavy-tier CI skips light-only tests by **directory**, not marker — `test/conftest.py::_LIGHT_TEST_DIRS` + `collect_ignore` drops those dirs when `rasterio` is absent (the heavy env). `ds` and `sample` are **already** in `_LIGHT_TEST_DIRS` and in the explicit pytest dir list in `.github/actions/pyrx_build/action.yml`, so netcdf tests placed under `test/ds/` and `test/sample/` need **no** conftest or action change. Do not place them anywhere else.
- **Commit hygiene:** subject ≤72 chars + a WHY body; end commit messages with `Co-authored-by: Isaac`. Branch: `examples/vapor-eyes` (already checked out).

---

### Task 1: Pin `netcdf4` across all three light environments

**Files:**
- Modify: `python/geobrix/pyproject.toml` (the `light = [...]` array, ends at line 116) — floor pin for the local `.venv-pyrx`
- Modify: `python/geobrix/requirements-pyrx-ci.in` (+ regenerate hashed `requirements-pyrx-ci.txt`) — CI light tier
- Modify: `python/geobrix/requirements-dev-container.in` (+ regenerate hashed `requirements-dev-container.txt`) — `geobrix-dev` Docker
- Modify: `scripts/commands/gbx-venv-sync.sh:51` — reconcile the `[pyrx,test]` extra (see Step 4)

**Interfaces:**
- Consumes: nothing.
- Produces: `import netCDF4` and `import xarray` importable in **all three** light environments (CI pyrx job, `geobrix-dev` Docker, local `.venv-pyrx`). Every later task's tests rely on this.

- [ ] **Step 1: Add `netcdf4` to the `[light]` extra**

In `python/geobrix/pyproject.toml`, inside the `light = [` array, after the `"xarray-spatial>=0.4,<1",` line (116), add:

```toml
    # NetCDF decode for the netcdf_gbx reader. xarray is already pulled by
    # xarray-spatial; netcdf4 is the one new engine — a single wheel bundling
    # netcdf-c + HDF5, reading both NetCDF-3 and NetCDF-4/HDF5 (S5P, ERA5).
    # Folded into [light] (not a separate [netcdf] extra) — one wheel, so
    # geobrix[light] reads NetCDF out of the box. Floor pinned; add a ceiling
    # here if a future release floats/breaks on Serverless env v5 (Py 3.12),
    # same discipline as the rio-tiler / mapbox-vector-tile pins above.
    "netcdf4>=1.6,<2",
```

- [ ] **Step 2: Add `netcdf4` to both hash-pinned requirement inputs**

These `.in` files use exact `==` pins (e.g. `scipy==1.15.1`, `xarray-spatial==0.9.9`). Add the same exact pin to both. `xarray` is already locked transitively via `xarray-spatial`; `netcdf4` pulls `cftime` (also locked on regenerate).

In `python/geobrix/requirements-pyrx-ci.in` (CI light tier), add near the other decode deps:

```
# NetCDF decode for the netcdf_gbx reader (NetCDF-3 + NetCDF-4/HDF5: S5P, ERA5).
# xarray comes transitively via xarray-spatial. Pin exact; bump alongside the
# pyproject [light] floor. Verify cp312 manylinux wheels exist for the pin.
netcdf4==1.7.2
```

In `python/geobrix/requirements-dev-container.in` (the `geobrix-dev` Docker used by `gbx:test:python`), add the identical line so the Docker test env can run the netcdf tests:

```
netcdf4==1.7.2
```

- [ ] **Step 3: Regenerate BOTH hashed locks**

Each `.in` header documents its exact regenerate command (uv, Python 3.12, `--generate-hashes`). Run both:

```bash
cd python/geobrix
uv pip compile --generate-hashes --python-version 3.12 \
    --output-file requirements-pyrx-ci.txt requirements-pyrx-ci.in
uv pip compile --generate-hashes --python-version 3.12 \
    --output-file requirements-dev-container.txt requirements-dev-container.in
```
Expected: each `.txt` gains `netcdf4==1.7.2` and transitive `cftime==...`, each with `--hash=sha256:...` lines. (There is no CI recompile-diff check; `--require-hashes` fails closed at install if a `.txt` is stale, so both must be committed in sync with their `.in`.)

- [ ] **Step 4: Reconcile the local `.venv-pyrx` extra so its floor pin installs**

The pyproject floor pin (Step 1) only reaches `.venv-pyrx` if `gbx:venv:sync` installs an extra that includes `netcdf4`. `scripts/commands/gbx-venv-sync.sh:51` currently installs `-e "./python/geobrix[pyrx,test]"`, but **no `pyrx` extra exists** in `pyproject.toml` (only `light`, `test`, `vizx`, `stac`, `overture`, `databricks`) — so that extra is a silent no-op and `.venv-pyrx` gets no light-tier floor deps. Fix the command to install the real umbrella extra:

```bash
# scripts/commands/gbx-venv-sync.sh line 51
uv pip install --python "$VENV_DIR/bin/python" -e "./python/geobrix[light,test]" \
```
(If a `pyrx` alias extra is intended instead, add `pyrx = [...]` mirroring `light` in `pyproject.toml`. Either way, the extra `gbx:venv:sync` installs MUST contain `netcdf4`.)

- [ ] **Step 5: Verify imports resolve under the hash-pinned install**

Run (host with uv, or inside the dev container):

```bash
cd python/geobrix
uv pip install --require-hashes -r requirements-pyrx-ci.txt
python -c "import xarray, netCDF4; print(xarray.__version__, netCDF4.__version__)"
```
Expected: `--require-hashes` install succeeds (lock is valid), then prints two version strings, no ImportError.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/pyproject.toml \
        python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-pyrx-ci.txt \
        python/geobrix/requirements-dev-container.in python/geobrix/requirements-dev-container.txt \
        scripts/commands/gbx-venv-sync.sh
git commit -m "build(light): pin netcdf4 across CI, Docker, and venv

xarray is already present via xarray-spatial; netcdf4 is the single new
engine (NetCDF-3 + NetCDF-4/HDF5). Pinned in all three light envs that
resolve independently: requirements-pyrx-ci (CI, hashed),
requirements-dev-container (Docker, hashed), and the [light] floor for
.venv-pyrx. Also fixes gbx-venv-sync to install a real extra ([light,
test]); [pyrx] was a silent no-op leaving .venv-pyrx without light deps.

Co-authored-by: Isaac"
```

---

### Task 2: CF helper module `ds/_netcdf.py` (pure functions, no Spark)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_netcdf.py`
- Test: `python/geobrix/test/ds/test_netcdf_helpers.py`

**Interfaces:**
- Consumes: `netCDF4`, `xarray`, `numpy`, `affine`, `shapely` (all available after Task 1 / existing).
- Produces (used by Tasks 3–4):
  - Constants `GRID = "grid"`, `POINTS = "points"`, `CURVILINEAR = "curvilinear"`, `UNSUPPORTED = "unsupported"`.
  - `open_dataset(path: str, group: Optional[str]) -> "xarray.Dataset"`
  - `classify(ds: "xarray.Dataset", variable: str) -> str` (one of the four constants)
  - `grid_transform_crs(ds, variable: str) -> Tuple["affine.Affine", str]` (CRS as `"EPSG:4326"` etc.)
  - `array_2d(ds, variable: str) -> "numpy.ndarray"` (the 2-D slice, north-up)
  - `nodata_of(ds, variable: str) -> Optional[float]`
  - `point_arrays(ds, variables: List[str]) -> Tuple["np.ndarray", "np.ndarray", Dict[str, "np.ndarray"], str]` → `(lon_1d, lat_1d, {var: values_1d}, srid)`
  - `np_to_spark(dtype: "numpy.dtype") -> "pyspark.sql.types.DataType"`

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/ds/test_netcdf_helpers.py`:

```python
"""Unit tests for CF NetCDF helpers (no Spark)."""

import numpy as np
import pytest
from netCDF4 import Dataset

from databricks.labs.gbx.ds import _netcdf


def _write_regular_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]   # descending (north-up)
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("ch4", "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 5)
        lat = ds.createVariable("latitude", "f8", ("obs",))
        lon = ds.createVariable("longitude", "f8", ("obs",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
        lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
        v = ds.createVariable("value", "f4", ("obs",))
        v[:] = np.arange(5, dtype="float32")


def _write_curvilinear(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 2)
        ds.createDimension("x", 3)
        lat = ds.createVariable("latitude", "f8", ("y", "x"))
        lon = ds.createVariable("longitude", "f8", ("y", "x"))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.array([[50.0, 50.0, 50.0], [49.0, 49.0, 49.0]])
        lon[:] = np.array([[10.0, 11.0, 12.0], [10.0, 11.0, 12.0]])
        v = ds.createVariable("ch4", "f4", ("y", "x"))
        v[:] = np.arange(6, dtype="float32").reshape(2, 3)


def test_classify_grid(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "ch4") == _netcdf.GRID


def test_classify_points(tmp_path):
    p = str(tmp_path / "pts.nc")
    _write_points(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "value") == _netcdf.POINTS


def test_classify_curvilinear(tmp_path):
    p = str(tmp_path / "curv.nc")
    _write_curvilinear(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "ch4") == _netcdf.CURVILINEAR


def test_grid_transform_crs_north_up(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        transform, crs = _netcdf.grid_transform_crs(ds, "ch4")
    assert crs == "EPSG:4326"
    # origin at (lon min - half px, lat max + half px); px = 0.5
    assert transform.a == pytest.approx(0.5)   # x pixel size
    assert transform.e == pytest.approx(-0.5)  # y pixel size (north-up => negative)
    assert transform.c == pytest.approx(9.75)  # ulx = 10.0 - 0.25
    assert transform.f == pytest.approx(50.25) # uly = 50.0 + 0.25


def test_array_2d_is_north_up(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        arr = _netcdf.array_2d(ds, "ch4")
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4))


def test_point_arrays_flatten(tmp_path):
    p = str(tmp_path / "pts.nc")
    _write_points(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["value"])
    assert srid == "4326"
    assert lon.shape == (5,) and lat.shape == (5,)
    np.testing.assert_allclose(attrs["value"], np.arange(5, dtype="float32"))


def test_point_arrays_curvilinear_ravel(tmp_path):
    p = str(tmp_path / "curv.nc")
    _write_curvilinear(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["ch4"])
    assert lon.shape == (6,) and lat.shape == (6,)
    np.testing.assert_allclose(attrs["ch4"], np.arange(6, dtype="float32"))


def test_point_arrays_grid_meshgrid(tmp_path):
    # A regular grid coerced to points: lon(4) x lat(3) -> 12 aligned points.
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["ch4"])
    assert lon.shape == (12,) and lat.shape == (12,) and attrs["ch4"].shape == (12,)
    # first cell is (lon=10.0, lat=50.0)
    assert lon[0] == pytest.approx(10.0) and lat[0] == pytest.approx(50.0)


def test_np_to_spark_types():
    from pyspark.sql.types import DoubleType, FloatType, IntegerType
    assert isinstance(_netcdf.np_to_spark(np.dtype("float32")), FloatType)
    assert isinstance(_netcdf.np_to_spark(np.dtype("float64")), DoubleType)
    assert isinstance(_netcdf.np_to_spark(np.dtype("int32")), IntegerType)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python/geobrix` → `pytest test/ds/test_netcdf_helpers.py -v`
Expected: FAIL — `ModuleNotFoundError: databricks.labs.gbx.ds._netcdf` (module not created yet).

- [ ] **Step 3: Implement `ds/_netcdf.py`**

Create `python/geobrix/src/databricks/labs/gbx/ds/_netcdf.py`:

```python
"""CF-convention NetCDF helpers for the netcdf_gbx reader (pure, no Spark).

Classifies a variable's geometry (regular grid / DSG points / curvilinear swath /
unsupported), derives an affine+CRS for grids, and flattens point/swath data to
1-D lon/lat/value arrays. No resampling, no quality filtering — the caller decides.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Tuple

GRID = "grid"                # class 1/2 -> raster
POINTS = "points"            # CF discrete sampling geometries -> vector
CURVILINEAR = "curvilinear"  # class 3 (2-D lat/lon) -> vector (per-cell points)
UNSUPPORTED = "unsupported"  # class 4 (sensor geometry + GLT) / unknown

_LAT_NAMES = {"lat", "latitude", "y"}
_LON_NAMES = {"lon", "longitude", "x"}


@contextmanager
def open_dataset(path: str, group: Optional[str]) -> Iterator["object"]:
    """Open a NetCDF file as an xarray.Dataset (netcdf4 engine), optional HDF5 group."""
    import xarray as xr

    kw = {"engine": "netcdf4", "decode_coords": "all", "mask_and_scale": True}
    if group:
        kw["group"] = group
    ds = xr.open_dataset(path, **kw)
    try:
        yield ds
    finally:
        ds.close()


def _is_lat(var) -> bool:
    sn = str(getattr(var, "standard_name", "")).lower()
    un = str(getattr(var, "units", "")).lower()
    return sn == "latitude" or un in ("degrees_north", "degree_north") or var.name.lower() in _LAT_NAMES


def _is_lon(var) -> bool:
    sn = str(getattr(var, "standard_name", "")).lower()
    un = str(getattr(var, "units", "")).lower()
    return sn == "longitude" or un in ("degrees_east", "degree_east") or var.name.lower() in _LON_NAMES


def _find_lat_lon(ds):
    """Return (lat_var, lon_var) among ds coords/vars, or (None, None)."""
    lat = lon = None
    for name in list(ds.variables):
        v = ds[name]
        if lat is None and _is_lat(v):
            lat = v
        elif lon is None and _is_lon(v):
            lon = v
    return lat, lon


def classify(ds, variable: str) -> str:
    lat, lon = _find_lat_lon(ds)
    if lat is None or lon is None:
        return UNSUPPORTED
    if lat.ndim == 2 and lon.ndim == 2:
        return CURVILINEAR
    if lat.ndim == 1 and lon.ndim == 1:
        var = ds[variable]
        # DSG points: the value var shares the single obs dimension with lat/lon.
        if var.ndim == 1 and lat.dims == lon.dims == var.dims:
            return POINTS
        # Regular grid: the value var's last two dims are the lat and lon dims.
        if lat.dims[0] in var.dims and lon.dims[0] in var.dims:
            return GRID
    return UNSUPPORTED


def grid_transform_crs(ds, variable: str) -> Tuple["object", str]:
    """Affine transform (north-up) + CRS string for a regular grid variable."""
    from affine import Affine

    lat, lon = _find_lat_lon(ds)
    lats = lat.values
    lons = lon.values
    px = float(abs(lons[1] - lons[0]))
    py = float(abs(lats[1] - lats[0]))
    ulx = float(min(lons)) - px / 2.0
    uly = float(max(lats)) + py / 2.0
    transform = Affine.translation(ulx, uly) * Affine.scale(px, -py)
    crs = _crs_string(ds)
    return transform, crs


def _crs_string(ds) -> str:
    # Look for a CF grid_mapping variable carrying an EPSG/authority code.
    for name in list(ds.variables):
        v = ds[name]
        epsg = getattr(v, "epsg_code", None) or getattr(v, "spatial_epsg", None)
        if epsg is not None:
            return f"EPSG:{int(epsg)}"
    # Default: geographic lon/lat.
    return "EPSG:4326"


def array_2d(ds, variable: str) -> "object":
    """The variable's 2-D slice as a north-up numpy array (lat descending)."""
    import numpy as np

    da = ds[variable]
    # Squeeze any length-1 leading dims (e.g. time); take the first index otherwise.
    while da.ndim > 2:
        da = da.isel({da.dims[0]: 0})
    lat, _ = _find_lat_lon(ds)
    latdim = lat.dims[0]
    # Ensure north-up: descending latitude along the lat dimension.
    if latdim in da.dims and float(ds[lat.name].values[0]) < float(ds[lat.name].values[-1]):
        da = da.isel({latdim: slice(None, None, -1)})
    return np.asarray(da.values)


def nodata_of(ds, variable: str) -> Optional[float]:
    v = ds[variable]
    for attr in ("_FillValue", "missing_value"):
        val = v.attrs.get(attr)
        if val is not None:
            return float(val)
    enc = v.encoding.get("_FillValue")
    return float(enc) if enc is not None else None


def point_arrays(ds, variables: List[str]) -> Tuple["object", "object", Dict[str, "object"], str]:
    """Flatten point (DSG), grid, or curvilinear data to aligned 1-D arrays.

    - POINTS/CURVILINEAR: lat/lon already align with the value array, so ravel each.
    - GRID: 1-D lat (H) and 1-D lon (W) must be meshgridded to H*W before ravel so
      they align with the 2-D value array's ravel.
    """
    import numpy as np

    lat, lon = _find_lat_lon(ds)
    kind = classify(ds, variables[0])
    if kind == GRID:
        lon2d, lat2d = np.meshgrid(
            np.asarray(ds[lon.name].values), np.asarray(ds[lat.name].values)
        )  # indexing="xy" -> shape (H, W), matching the value array
        lon_flat = lon2d.ravel()
        lat_flat = lat2d.ravel()
    else:  # POINTS or CURVILINEAR
        lon_flat = np.asarray(ds[lon.name].values).ravel()
        lat_flat = np.asarray(ds[lat.name].values).ravel()
    attrs: Dict[str, object] = {}
    for name in variables:
        attrs[name] = np.asarray(ds[name].values).ravel()
    return lon_flat, lat_flat, attrs, "4326"


def np_to_spark(dtype) -> "object":
    import numpy as np
    from pyspark.sql import types as T

    kind = np.dtype(dtype).kind
    itemsize = np.dtype(dtype).itemsize
    if kind == "f":
        return T.FloatType() if itemsize <= 4 else T.DoubleType()
    if kind in ("i", "u"):
        return T.IntegerType() if itemsize <= 4 else T.LongType()
    if kind == "b":
        return T.BooleanType()
    return T.StringType()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest test/ds/test_netcdf_helpers.py -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_netcdf.py python/geobrix/test/ds/test_netcdf_helpers.py
git commit -m "feat(ds): CF NetCDF helpers for netcdf_gbx (classify/transform/points)

Pure, Spark-free geometry classification (grid/points/curvilinear/
unsupported), north-up affine+CRS derivation for regular grids, and
1-D flattening for point + swath data. Foundation for the raster and
vector reader modes.

Co-authored-by: Isaac"
```

---

### Task 3: Raster mode — `NetcdfGbxDataSource` + `NetcdfRasterReader`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py`
- Test: `python/geobrix/test/ds/test_netcdf_datasource.py`

**Interfaces:**
- Consumes: `_netcdf.*` (Task 2); `raster.RasterGbxReader`, `raster.reader_schema`, `raster._FilePartition`; `_encode.encode_tile`; `_serde.TILE_SCHEMA`.
- Produces (used by Tasks 4–5): `NetcdfGbxDataSource` (`name()=="netcdf_gbx"`, `schema()`/`reader()` branch on `mode`), `NetcdfRasterReader`.

- [ ] **Step 1: Write the failing tests (raster mode)**

Create `python/geobrix/test/ds/test_netcdf_datasource.py`:

```python
"""Integration tests for the netcdf_gbx DataSource (uses local Spark)."""

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
from databricks.labs.gbx.pyrx import _serde

EXPECTED_METADATA_KEYS = {
    "path", "sourcePath", "driver", "format", "last_command", "last_error",
    "all_parents", "size", "compression", "isZipped", "isSubset",
}


def _write_regular_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("ch4", "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _write_curvilinear(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 2)
        ds.createDimension("x", 3)
        lat = ds.createVariable("latitude", "f8", ("y", "x"))
        lon = ds.createVariable("longitude", "f8", ("y", "x"))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.array([[50.0, 50.0, 50.0], [49.0, 49.0, 49.0]])
        lon[:] = np.array([[10.0, 11.0, 12.0], [10.0, 11.0, 12.0]])
        v = ds.createVariable("ch4", "f4", ("y", "x"))
        v[:] = np.arange(6, dtype="float32").reshape(2, 3)


def test_raster_schema_matches_tile_schema():
    ds = NetcdfGbxDataSource(options={"path": "/tmp/none", "variable": "ch4"})
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == _serde.TILE_SCHEMA


def test_raster_read_round_trip(spark, tmp_path):
    f = tmp_path / "grid.nc"
    _write_regular_grid(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx")
        .option("variable", "ch4")
        .load(str(f))
    )
    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["tile"]["cellid"] == -1
    assert set(row["tile"]["metadata"].keys()) == EXPECTED_METADATA_KEYS
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
        assert out.crs.to_epsg() == 4326
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_raster_mode_rejects_curvilinear(spark, tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
    from databricks.labs.gbx.ds.raster import _FilePartition

    reader = NetcdfRasterReader({"path": str(f), "variable": "ch4"})
    with pytest.raises(ValueError, match="vector"):
        list(reader.read(_FilePartition(str(f), reader.size_mib)))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest test/ds/test_netcdf_datasource.py -v`
Expected: FAIL — `ModuleNotFoundError: databricks.labs.gbx.ds.netcdf`.

- [ ] **Step 3: Implement `ds/netcdf.py` (raster mode + DataSource skeleton)**

Create `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py`:

```python
"""netcdf_gbx — lightweight NetCDF reader.

One DataSource, two modes (the `mode` option, default "raster"):
  * raster — CF regular/projected grids -> the shared (source, tile) GeoTIFF struct.
  * vector — DSG points, or any 2-D field (incl. curvilinear swath) coerced to
    per-cell points -> the light vector schema (attrs + geom_0 WKB + srid cols).

Class 4 (raw sensor geometry + GLT) is rejected in both modes.
Serverless-safe: no spark.conf/_jvm/.rdd/cache/persist.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _encode, _netcdf
from databricks.labs.gbx.ds.raster import (
    RasterGbxReader,
    _FilePartition,
    reader_schema,
)


def _requested_variables(options: Dict[str, str]) -> List[str]:
    raw = options.get("variables") or options.get("variable")
    if not raw:
        raise ValueError(
            "netcdf_gbx requires a 'variable' (or 'variables') option naming the "
            "NetCDF variable(s) to read."
        )
    return [v.strip() for v in str(raw).split(",") if v.strip()]


class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode a CF grid variable to a GeoTIFF tile."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex/bbox/bboxCrs
        self.variables = _requested_variables(options)
        self.group = options.get("group")

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        import numpy as np
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.ds import _listing

        source = _listing.to_spark_uri(partition.file_path)
        var = self.variables[0]  # raster mode reads a single variable per tile
        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            kind = _netcdf.classify(ds, var)
            if kind == _netcdf.CURVILINEAR:
                raise ValueError(
                    f"netcdf_gbx: variable '{var}' in {partition.file_path} is "
                    f"curvilinear/swath (2-D lat/lon); read it with "
                    f"option('mode','vector') to get per-cell points."
                )
            if kind != _netcdf.GRID:
                raise ValueError(
                    f"netcdf_gbx: variable '{var}' is not a regular grid "
                    f"({kind}); raster mode supports CF regular/projected grids only."
                )
            transform, crs = _netcdf.grid_transform_crs(ds, var)
            arr = _netcdf.array_2d(ds, var)
            nodata = _netcdf.nodata_of(ds, var)

        h, w = arr.shape[-2], arr.shape[-1]
        profile = dict(
            driver="GTiff", width=w, height=h, count=1, dtype=str(arr.dtype),
            crs=crs, transform=transform,
        )
        if nodata is not None:
            profile["nodata"] = nodata
        # Build an in-memory rasterio dataset, then reuse the shared encode_tile so
        # the 11-key metadata + GTiff re-encode stay DRY with the other readers.
        with MemoryFile() as mf:
            with mf.open(**profile) as out:
                out.write(arr.astype(profile["dtype"]), 1)
            with mf.open() as rds:
                cellid, raster_bytes, meta = _encode.encode_tile(
                    rds, window=(0, 0, w, h),
                    source_path=partition.file_path, all_parents="",
                )
        yield (source, (cellid, raster_bytes, meta))


class NetcdfGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "netcdf_gbx"

    def _mode(self) -> str:
        return self.options.get("mode", "raster").lower()

    def schema(self) -> StructType:
        mode = self._mode()
        if mode == "raster":
            return reader_schema()
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader
            return NetcdfVectorReader(self.options).schema()
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        mode = self._mode()
        if mode == "raster":
            return NetcdfRasterReader(self.options)
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader
            return NetcdfVectorReader(self.options)
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest test/ds/test_netcdf_datasource.py -v`
Expected: PASS for `test_raster_schema_matches_tile_schema`, `test_raster_read_round_trip`, `test_raster_mode_rejects_curvilinear`. (Vector tests do not exist yet.)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/netcdf.py python/geobrix/test/ds/test_netcdf_datasource.py
git commit -m "feat(ds): netcdf_gbx raster mode (CF grid -> GeoTIFF tile)

NetcdfGbxDataSource branches on the mode option (schema + reader), like
VectorGbxDataSource. Raster mode transcodes a regular-grid variable to
the shared (source, tile) struct via the existing encode_tile path;
curvilinear/unsupported geometries raise an actionable error.

Co-authored-by: Isaac"
```

---

### Task 4: Vector mode — `NetcdfVectorReader`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_netcdf_vector.py`
- Modify: `python/geobrix/test/ds/test_netcdf_datasource.py` (add vector tests)

**Interfaces:**
- Consumes: `_netcdf.point_arrays`, `_netcdf.np_to_spark`, `_netcdf.classify`, `_listing`; `shapely`.
- Produces: `NetcdfVectorReader` (`schema()` returns attrs + `geom_0` WKB + `geom_0_srid` + `geom_0_srid_proj`; `read()` yields row tuples). Consumed by `NetcdfGbxDataSource` (already wired in Task 3).

- [ ] **Step 1: Write the failing tests (vector mode)**

Append to `python/geobrix/test/ds/test_netcdf_datasource.py`:

```python
def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 5)
        lat = ds.createVariable("latitude", "f8", ("obs",))
        lon = ds.createVariable("longitude", "f8", ("obs",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
        lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
        v = ds.createVariable("ch4", "f4", ("obs",))
        v[:] = np.arange(5, dtype="float32")
        qa = ds.createVariable("qa_value", "i4", ("obs",))
        qa[:] = np.array([0, 1, 0, 1, 1], dtype="int32")


def test_vector_schema_columns(tmp_path):
    from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    reader = NetcdfVectorReader(
        {"path": str(f), "variables": "ch4,qa_value"}
    )
    schema = reader.schema()
    assert [f.name for f in schema.fields] == [
        "ch4", "qa_value", "geom_0", "geom_0_srid", "geom_0_srid_proj",
    ]
    from pyspark.sql.types import BinaryType, FloatType, IntegerType, StringType
    assert isinstance(schema["ch4"].dataType, FloatType)
    assert isinstance(schema["qa_value"].dataType, IntegerType)
    assert isinstance(schema["geom_0"].dataType, BinaryType)
    assert isinstance(schema["geom_0_srid"].dataType, StringType)


def test_vector_read_dsg_points(spark, tmp_path):
    import shapely
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4,qa_value")
        .load(str(f))
    )
    rows = df.orderBy("ch4").collect()
    assert len(rows) == 5
    assert rows[0]["geom_0_srid"] == "4326"
    pt = shapely.from_wkb(bytes(rows[0]["geom_0"]))
    assert pt.x == pytest.approx(10.0) and pt.y == pytest.approx(50.0)
    assert rows[1]["qa_value"] == 1  # ch4==1 -> qa 1


def test_vector_read_curvilinear_to_points(spark, tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4")
        .load(str(f))
    )
    assert df.count() == 6  # one point per cell (2x3)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest test/ds/test_netcdf_datasource.py -k vector -v`
Expected: FAIL — `ModuleNotFoundError: databricks.labs.gbx.ds._netcdf_vector`.

- [ ] **Step 3: Implement `ds/_netcdf_vector.py`**

Create `python/geobrix/src/databricks/labs/gbx/ds/_netcdf_vector.py`:

```python
"""netcdf_gbx vector mode: DSG points, or any 2-D field coerced to per-cell points.

Output schema mirrors the light vector reader convention: attribute columns (one
per requested variable, typed) then geom_0 (plain WKB) + geom_0_srid +
geom_0_srid_proj string columns. SRID travels in the string column, not as EWKB.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from databricks.labs.gbx.ds import _listing, _netcdf


class _NcFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class NetcdfVectorReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("netcdf_gbx requires a 'path' (e.g. .load(path)).")
        raw = options.get("variables") or options.get("variable")
        if not raw:
            raise ValueError(
                "netcdf_gbx vector mode requires a 'variables' option naming the "
                "NetCDF variable(s) to emit as point attributes."
            )
        self.variables: List[str] = [v.strip() for v in str(raw).split(",") if v.strip()]
        self.group: Optional[str] = options.get("group")
        self.filter_regex = options.get("filterRegex", ".*")

    def _members(self) -> List[str]:
        return _listing.list_files(self.path, self.filter_regex)

    def schema(self) -> StructType:
        member = self._members()[0]
        fields: List[StructField] = []
        with _netcdf.open_dataset(member, self.group) as ds:
            for name in self.variables:
                fields.append(
                    StructField(name, _netcdf.np_to_spark(ds[name].values.dtype), True)
                )
        fields.append(StructField("geom_0", BinaryType(), True))
        fields.append(StructField("geom_0_srid", StringType(), True))
        fields.append(StructField("geom_0_srid_proj", StringType(), True))
        return StructType(fields)

    def partitions(self) -> Sequence[InputPartition]:
        return [_NcFilePartition(f) for f in self._members()]

    def read(self, partition: "_NcFilePartition") -> Iterator[Tuple]:
        import shapely

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            kind = _netcdf.classify(ds, self.variables[0])
            if kind == _netcdf.UNSUPPORTED:
                raise ValueError(
                    f"netcdf_gbx: variable '{self.variables[0]}' in "
                    f"{partition.file_path} has no per-pixel lon/lat (sensor "
                    f"geometry / unsupported); orthorectify it first."
                )
            lon, lat, attrs, srid = _netcdf.point_arrays(ds, self.variables)

        wkb = shapely.to_wkb(shapely.points(lon, lat))
        proj = f"EPSG:{srid}"
        n = len(lon)
        for i in range(n):
            row = tuple(attrs[name][i].item() for name in self.variables)
            yield row + (bytes(wkb[i]), srid, proj)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest test/ds/test_netcdf_datasource.py -v`
Expected: PASS (all raster + vector tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_netcdf_vector.py python/geobrix/test/ds/test_netcdf_datasource.py
git commit -m "feat(ds): netcdf_gbx vector mode (points + swath->per-cell points)

DSG point data reads natively; any 2-D field (incl. curvilinear swath)
is coerced to one point per cell. Output matches the light vector schema
(attrs + geom_0 WKB + srid columns), so gbx_st_*/H3 compose directly.
Requested variables pass through untouched (e.g. S5P qa_value).

Co-authored-by: Isaac"
```

---

### Task 5: Register `netcdf_gbx`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/register.py` (imports 8-25; `_SOURCES` 27-37)
- Test: `python/geobrix/test/ds/test_netcdf_datasource.py` (add a register test)

**Interfaces:**
- Consumes: `NetcdfGbxDataSource` (Task 3).
- Produces: `netcdf_gbx` resolvable via `spark.read.format("netcdf_gbx")` after `register(spark)` and via `register(spark, only=["netcdf"])`.

- [ ] **Step 1: Write the failing test**

Append to `python/geobrix/test/ds/test_netcdf_datasource.py`:

```python
def test_register_exposes_netcdf_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register
    f = tmp_path / "grid.nc"
    _write_regular_grid(str(f))
    register(spark, only=["netcdf"])
    df = spark.read.format("netcdf_gbx").option("variable", "ch4").load(str(f))
    assert df.count() == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest test/ds/test_netcdf_datasource.py::test_register_exposes_netcdf_gbx -v`
Expected: FAIL — `register(... only=["netcdf"])` raises `ValueError` (unknown format) because `netcdf_gbx` is not in `_SOURCES`.

- [ ] **Step 3: Wire into `register.py`**

Add the import alongside the others (after the `gtiff` import, line 15):

```python
from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
```

Add it to `_SOURCES` (after `GTiffGbxDataSource,`):

```python
    NetcdfGbxDataSource,
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest test/ds/test_netcdf_datasource.py -v`
Expected: PASS (all tests, including the register test).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/register.py python/geobrix/test/ds/test_netcdf_datasource.py
git commit -m "feat(ds): register netcdf_gbx DataSource

Adds NetcdfGbxDataSource to the light _SOURCES tuple so
spark.read.format('netcdf_gbx') resolves after register(spark).

Co-authored-by: Isaac"
```

---

### Task 6: `TropomiDownloader` (`gbx.sample`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/sample/tropomi.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/__init__.py`
- Test: `python/geobrix/test/sample/test_tropomi.py`

**Interfaces:**
- Consumes: `StacClient` (injectable via `_stac_client`); `netcdf_gbx` reader (Task 5) in `read()`.
- Produces: `TropomiDownloader` (`discover`/`download`/`read`) + `download_tropomi_aoi`, re-exported from `sample/__init__.py`.

**Verify before finalizing (spec risk R4):** confirm the Planetary Computer collection id (`sentinel-5p-l2-netcdf`), the CH4 asset name, and the `/PRODUCT/` group + variable names (`methane_mixing_ratio_bias_corrected`, `qa_value`) against a real granule. Adjust the constants below if they differ.

- [ ] **Step 1: Write the failing test (offline, injected StacClient)**

Create `python/geobrix/test/sample/test_tropomi.py`:

```python
"""Offline unit tests for TropomiDownloader (injected StacClient, no network)."""

from unittest.mock import MagicMock

from databricks.labs.gbx.sample.tropomi import TropomiDownloader


def test_discover_filters_to_ch4_asset(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [("S5P_1", "ch4", [10.0, 49.0, 12.0, 51.0], "https://x/ch4.nc"),
         ("S5P_1", "other", [10.0, 49.0, 12.0, 51.0], "https://x/other.nc")],
        ["item_id", "asset_name", "item_bbox", "href"],
    )
    fake.search.return_value = search_df
    dl = TropomiDownloader(_stac_client=fake)
    out = dl.discover([10.0, 49.0, 12.0, 51.0], spark=spark)
    assert out.count() == 1
    assert out.first()["asset_name"] == "ch4"


def test_download_delegates_to_stacclient(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [("S5P_1", "ch4", "https://x/ch4.nc")],
        ["item_id", "asset_name", "href"],
    )
    fake.search.return_value = search_df
    fake.download.return_value = spark.createDataFrame(
        [("S5P_1", "ch4", "/vol/ch4.nc", 123, True)],
        ["item_id", "asset_name", "out_file_path", "out_file_sz", "is_out_file_valid"],
    )
    dl = TropomiDownloader(_stac_client=fake)
    res = dl.download([10.0, 49.0, 12.0, 51.0], "/vol/out", spark=spark)
    assert res.count() == 1
    fake.download.assert_called_once()
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest test/sample/test_tropomi.py -v`
Expected: FAIL — `ModuleNotFoundError: databricks.labs.gbx.sample.tropomi`.

- [ ] **Step 3: Implement `sample/tropomi.py`**

Create `python/geobrix/src/databricks/labs/gbx/sample/tropomi.py` (structure cloned from `sample/dem.py`):

```python
"""TropomiDownloader — AOI-driven Sentinel-5P L2 CH4 staging via Planetary Computer.

Mirrors DemDownloader: driver-side discovery (metadata-only), then DISTRIBUTED
asset download via StacClient.download(). S5P L2 CH4 is netCDF-4 swath — read()
loads it through the netcdf_gbx reader in VECTOR mode (per-pixel points).

ONLINE-ONLY (pystac-client + planetary-computer). Injection seam: _stac_client.
Serverless-safe: no spark.conf.set/_jvm/.rdd/cache/persist.
"""

from __future__ import annotations

from typing import Optional, Sequence

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"
# VERIFY (R4): collection id, CH4 asset name, group, and variable names.
S5P_COLLECTION = "sentinel-5p-l2-netcdf"
_CH4_ASSET = "ch4"
_S5P_GROUP = "/PRODUCT"
_S5P_VARIABLES = "methane_mixing_ratio_bias_corrected,qa_value"
_S5P_DATETIME = "2018-01-01/2030-01-01"


def _bbox_to_geojson_polygon(bbox: Sequence[float]) -> str:
    import json

    minx, miny, maxx, maxy = bbox
    coords = [[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]
    return json.dumps({"type": "Polygon", "coordinates": [coords]})


class TropomiDownloader:
    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = S5P_COLLECTION,
        asset: str = _CH4_ASSET,
        _stac_client=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self.collection = collection
        self.asset = asset
        self._stac_client = _stac_client

    def _get_stac_client(self):
        if self._stac_client is not None:
            return self._stac_client
        from databricks.labs.gbx.stac import StacClient

        return StacClient(catalog=self.catalog, sign=self.sign)

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return spark.createDataFrame([(_bbox_to_geojson_polygon(bbox),)], ["geojson"])

    def discover(self, bbox: Sequence[float], spark=None):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        raw = client.search(
            self._aoi_dataframe(bbox, spark),
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_S5P_DATETIME,
        )
        return (
            raw.filter(F.col("asset_name") == self.asset)
            .select("item_id", "asset_name", "item_bbox", "href")
            .distinct()
        )

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        bbox_crs: str = "EPSG:4326",
        partitions: Optional[int] = None,
        spark=None,
    ):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        raw = client.search(
            self._aoi_dataframe(bbox, spark),
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_S5P_DATETIME,
        )
        granules = raw.filter(F.col("asset_name") == self.asset).select(
            "item_id", "asset_name", "href"
        )
        return client.download(
            granules, out_dir, bbox=list(bbox), bbox_crs=bbox_crs, partitions=partitions
        )

    def read(
        self,
        out_dir: str,
        variables: str = _S5P_VARIABLES,
        group: str = _S5P_GROUP,
        spark=None,
    ):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("netcdf_gbx")
            .option("mode", "vector")
            .option("group", group)
            .option("variables", variables)
            .option("filterRegex", r".*\.nc$")
            .load(out_dir)
            .repartition(64, F.col("geom_0_srid"))
        )


def download_tropomi_aoi(spark, bbox: Sequence[float], out_dir: str, **kw):
    """One-shot: default TropomiDownloader + download S5P CH4 for an AOI."""
    return TropomiDownloader().download(bbox, out_dir, spark=spark, **kw)
```

- [ ] **Step 4: Export from `sample/__init__.py`**

In `python/geobrix/src/databricks/labs/gbx/sample/__init__.py`, add the import (after the `dem` import) and the `__all__` entries:

```python
from databricks.labs.gbx.sample.tropomi import TropomiDownloader, download_tropomi_aoi
```
and add `"TropomiDownloader",` and `"download_tropomi_aoi",` to the `__all__` list (keep it sorted with the existing entries).

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest test/sample/test_tropomi.py -v`
Expected: PASS (2 tests). Also `python -c "from databricks.labs.gbx.sample import TropomiDownloader, download_tropomi_aoi"` succeeds.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/sample/tropomi.py python/geobrix/src/databricks/labs/gbx/sample/__init__.py python/geobrix/test/sample/test_tropomi.py
git commit -m "feat(sample): TropomiDownloader for S5P L2 CH4 (vector-mode proof)

Clones the DemDownloader shape (discover/download/read, Serverless-safe,
injectable StacClient). read() loads granules via netcdf_gbx vector mode
(S5P is netCDF-4 swath -> per-pixel points). Constants flagged for
against-granule verification (R4).

Co-authored-by: Isaac"
```

---

### Task 7: Docs — `readers/netcdf.mdx` + doc-test + sidebar

**Files:**
- Create: `docs/docs/readers/netcdf.mdx`
- Create: `docs/tests/python/readers/netcdf_gbx_read_examples.py`
- Create: `docs/tests/python/readers/test_netcdf_gbx_read_examples.py`
- Modify: `docs/sidebars.js` (the "Named" readers array)

**Interfaces:**
- Consumes: the registered `netcdf_gbx` reader (Task 5).
- Produces: a docs page rendering raster + vector examples, and a doc-test asserting they run.

- [ ] **Step 1: Write the doc-example source (named constants + verifiers)**

Create `docs/tests/python/readers/netcdf_gbx_read_examples.py` (mirror `raster_gbx_read_examples.py`):

```python
"""Executable examples for the netcdf_gbx reader docs (imported via raw-loader)."""

# --8<-- [start:register]
from databricks.labs.gbx.ds.register import register

register(spark)  # noqa: F821  (spark provided by the notebook/test)
# --8<-- [end:register]


# --8<-- [start:read_raster]
grid = (
    spark.read.format("netcdf_gbx")  # noqa: F821
    .option("variable", "t2m")       # a regular lat/lon grid variable (e.g. ERA5)
    .load("/Volumes/main/geobrix_samples/netcdf/era5_sample.nc")
)
# grid has the standard (source, tile) raster schema
# --8<-- [end:read_raster]


# --8<-- [start:read_vector]
points = (
    spark.read.format("netcdf_gbx")  # noqa: F821
    .option("mode", "vector")
    .option("group", "/PRODUCT")
    .option("variables", "methane_mixing_ratio_bias_corrected,qa_value")
    .load("/Volumes/main/geobrix_samples/netcdf/s5p_ch4_sample.nc")
)
# points has: <vars...>, geom_0 (WKB), geom_0_srid, geom_0_srid_proj
# --8<-- [end:read_vector]


def netcdf_gbx_raster_example(spark, path):
    register(spark)
    return spark.read.format("netcdf_gbx").option("variable", "t2m").load(path)


def netcdf_gbx_vector_example(spark, path, variables, group):
    register(spark)
    return (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector").option("group", group)
        .option("variables", variables).load(path)
    )
```

- [ ] **Step 2: Write the doc-test wrapper (fails first — no fixtures yet)**

Create `docs/tests/python/readers/test_netcdf_gbx_read_examples.py`:

```python
"""Doc-test: netcdf_gbx examples run against synthesized NetCDF fixtures."""

import numpy as np
from netCDF4 import Dataset

from netcdf_gbx_read_examples import (
    netcdf_gbx_raster_example,
    netcdf_gbx_vector_example,
)


def _write_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",)); lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",)); lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]; lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("t2m", "f4", ("lat", "lon")); v[:] = np.arange(12).reshape(3, 4)


def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 4)
        lat = ds.createVariable("latitude", "f8", ("obs",)); lat.standard_name = "latitude"
        lon = ds.createVariable("longitude", "f8", ("obs",)); lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3]; lon[:] = [10.0, 10.1, 10.2, 10.3]
        ds.createVariable("methane_mixing_ratio_bias_corrected", "f4", ("obs",))[:] = np.arange(4)
        ds.createVariable("qa_value", "i4", ("obs",))[:] = np.array([1, 1, 0, 1])


def test_raster_example_runs(spark, tmp_path):
    p = str(tmp_path / "grid.nc"); _write_grid(p)
    assert netcdf_gbx_raster_example(spark, p).count() == 1


def test_vector_example_runs(spark, tmp_path):
    p = str(tmp_path / "pts.nc"); _write_points(p)
    df = netcdf_gbx_vector_example(
        spark, p, "methane_mixing_ratio_bias_corrected,qa_value", None
    )
    assert df.count() == 4
    assert "geom_0" in df.columns
```

- [ ] **Step 3: Run the doc-test to verify it passes**

Run (in Docker per the doc-test convention): `gbx:test:python-docs --path docs/tests/python/readers/test_netcdf_gbx_read_examples.py`
Expected: PASS (2 tests). (If run outside Docker, use `pytest docs/tests/python/readers/test_netcdf_gbx_read_examples.py`.)

- [ ] **Step 4: Write the MDX page**

Create `docs/docs/readers/netcdf.mdx` (mirror `docs/docs/readers/geotiff.mdx` front-matter + `!!raw-loader!` import + Options table). Include:
- front-matter `sidebar_label: netcdf_gbx`, an appropriate `sidebar_position`.
- Intro: lightweight NetCDF reader, two modes, class-1/2 grids in raster mode, points/swath in vector mode, class 4 rejected.
- Options table: `mode` (raster|vector), `variable`/`variables`, `group`, and the inherited `bbox`/`bboxCrs`/`filterRegex`/`sizeInMB`.
- Two `<CodeFromTest>` snippets pulling the `read_raster` and `read_vector` regions from `netcdf_gbx_read_examples.py`.
- A short "GeoBrix voice" note (no internal vocabulary): S5P swath is read as points; ERA5 grids as tiles.

- [ ] **Step 5: Add the sidebar entry**

In `docs/sidebars.js`, add `"readers/netcdf"` to the "Named" readers array (next to `"readers/geotiff"`).

- [ ] **Step 6: Commit**

```bash
git add docs/docs/readers/netcdf.mdx docs/tests/python/readers/netcdf_gbx_read_examples.py docs/tests/python/readers/test_netcdf_gbx_read_examples.py docs/sidebars.js
git commit -m "docs(readers): document netcdf_gbx (raster + vector modes)

Single-source doc-test examples (raster grid + vector points), MDX page
with an options table, and a sidebar entry under Named readers.

Co-authored-by: Isaac"
```

---

## Notes for the implementer

- **R1 (mode-dependent schema)** is handled: `NetcdfGbxDataSource.schema()` reads `self.options["mode"]` and returns either the tile schema or the vector schema — the same mechanism `VectorGbxDataSource.schema()` uses (`ds/vector.py:779`). PySpark passes options to `schema()` via `self.options`.
- **R2 (vector schema)** matches the light vector reader exactly: attribute columns in requested order, then `geom_0` (plain WKB `BinaryType`), `geom_0_srid`, `geom_0_srid_proj` (both `StringType`). Not EWKB.
- **R3 (Serverless `netcdf4` pin)** is a deploy-time concern; Task 1 pins a floor and documents adding a ceiling if env-v5 floats it.
- **R4 (S5P names)** — verify collection/asset/group/variable names against a real granule in Task 6 before finalizing the constants.
- **Local test env:** the `ds/conftest.py` fixture sets `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON = sys.executable`; a `test/sample/conftest.py` may need the same `spark` fixture — reuse or import the `ds` one. If `test/sample/` lacks a Spark fixture, add a minimal `conftest.py` mirroring `test/ds/conftest.py`'s `spark` fixture.
- **Raster encode reuse:** `NetcdfRasterReader.read` builds an in-memory rasterio dataset then calls `_encode.encode_tile(...)` so the 11-key metadata and GTiff re-encode stay DRY. The extra in-memory encode is negligible for the initial reader.
- **`sizeInMB` in raster mode:** the option is accepted (inherited via `RasterGbxReader.__init__`) but the initial raster read emits a **single whole tile per variable/file** — it does not sub-tile. NetCDF grids in scope (ERA5-class) are modest, so this is acceptable for the initial cut; sub-tiling large grids is a follow-up (pairs naturally with the multi-time-step fan-out increment).
- **`array_2d` north-up vs vector order:** raster mode reorders to north-up (descending latitude) to match the GeoTIFF convention; vector mode leaves points in stored order (each point carries its own coordinate, so order is irrelevant). Both are correct.
- **Tier gating — do NOT add markers or import-skips.** The repo separates tiers by directory: `test/conftest.py::_LIGHT_TEST_DIRS` + `collect_ignore` drop `ds/`, `sample/`, etc. when `rasterio` is absent (heavy env). `test_gtiff_datasource.py` does an unguarded `import rasterio`; match that convention — a plain `from netCDF4 import Dataset` at module top is fine because `ds/` and `sample/` are already gated. Since netcdf4 is now pinned in all three light envs (Task 1), it is always present wherever `ds/`/`sample/` are collected. Keep the new test files under `test/ds/` and `test/sample/` — placing them elsewhere would run them in the heavy tier and fail on the module-level netCDF4 import.
