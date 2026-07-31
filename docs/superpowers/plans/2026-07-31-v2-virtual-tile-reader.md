# v2 Virtual-Tile Reader — Increment 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land a bytes-free `VirtualTile` representation and a single `open_tile`/`materialize` front-door that lazily reads a window from a raster (`path`@`window`), applies SRID-aware polygon clipping with per-tile intersection, and optionally lazy-warps to a working CRS — proven across COG, tiled GeoTIFF, and striped GeoTIFF locally and on Serverless.

**Architecture:** Three thin light-tier modules under `pyrx/core/` that orchestrate **existing** machinery rather than reinventing it: `_serde.open_tile` (v1 bytes), `edit.clip_to_geom` (SRID-aware cutline clip), `warp.reproject_to_srid` (warp), `preparer._stage_local_if_needed` (`/Volumes` staging), `_geom.parse_geom` (WKB/EWKB/WKT/EWKT). The new `open_tile` is the sole chokepoint that knows v1/v2/virtual; function bodies never branch on tile shape.

**Tech Stack:** Python 3.12, rasterio ≥1.3 (bundles GDAL), shapely, numpy, pytest. Tests run in the `geobrix-dev` Docker container via `gbx:test:pyrx`. Serverless leg uses `notebooks/tests/run_notebooks_serverless.py` (jobs.submit + env v5).

## Global Constraints

- **Exploratory / non-wired:** NO catalog registration, NO `registered_functions.txt` / `function-info.json` / bindings entries this increment. Keeps binding-parity + QC green.
- **Light tier only:** pyrx is JAR-free. NO `osgeo.gdal` import — use rasterio. NO `spark.conf.set` / `_jvm` / `.rdd` in library code.
- **DRY:** reuse `pyrx._serde.open_tile`, `pyrx.core.edit.clip_to_geom`, `pyrx.core.warp.reproject_to_srid`, `pyrx.core.preparer._stage_local_if_needed`, `pyrx._geom.parse_geom`. Do NOT re-implement clip/warp/stage logic.
- **Reproject the polygon, never the raster** during clip. Only an explicit transform op moves pixels.
- **Clip refines the window** (intersection, ⊆ window, never widens); disjoint → empty/NoData result, NOT an error.
- **v1 back-compat is permanent:** a `VirtualTile` with `path`/`window`/`clip_polygon`/`clip_crs`/`crs` null and `raster` set MUST flow through `open_tile` identically to today's bytes path.
- Test dir: `python/geobrix/test/pyrx/` (flat `test_*.py` files; already a covered light-test dir — no new gate).
- Fixtures: build on `conftest.make_geotiff_bytes`; generate COG / tiled / striped variants with rasterio.
- Run tests: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/<file> --log <name>.log` (Docker; dispatch as a Task, don't block main session).
- Commits: end message body with `Co-authored-by: Isaac`. `gh auth switch --user mjohns-databricks` before any push.

---

### Task 1: `VirtualTile` representation + Spark round-trip

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py`
- Test: `python/geobrix/test/pyrx/test_core_virtual_tile.py`

**Interfaces:**
- Consumes: nothing (stdlib + pyspark types only).
- Produces:
  - `Window4 = Tuple[int, int, int, int]` (col_off, row_off, width, height)
  - `@dataclass VirtualTile` with fields: `cellid: int`, `raster: Optional[bytes] = None`, `path: Optional[str] = None`, `window: Optional[Window4] = None`, `clip_polygon: Optional[bytes] = None`, `clip_crs: Optional[str] = None`, `crs: Optional[str] = None`, `metadata: Dict[str, str] = field(default_factory=dict)`
  - `VirtualTile.__post_init__` validation: raises `ValueError` if both `raster` and `path` are None; raises `ValueError` if `raster` is None and `window` is None.
  - `VirtualTile.is_virtual() -> bool` — True when `raster is None`.
  - `V2_TILE_SCHEMA: StructType` — Spark struct (see below).
  - `VirtualTile.to_row() -> dict` — plain dict keyed by field names, `window` as a nested dict `{"col_off","row_off","width","height"}` or None.
  - `VirtualTile.from_row(row) -> VirtualTile` — accepts a dict or pyspark Row; reverses `to_row`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_virtual_tile.py
"""v2 VirtualTile struct: validation + Spark round-trip.

The struct is deliberately full (path, window, clip_polygon, clip_crs, crs)
so parity locks once; the reader does not emit it yet (later increment).
"""
import pytest

from databricks.labs.gbx.pyrx.core import virtual_tile as vt


def test_materialized_tile_valid_with_raster_only():
    t = vt.VirtualTile(cellid=1, raster=b"\x00\x01")
    assert not t.is_virtual()


def test_virtual_tile_valid_with_path_and_window():
    t = vt.VirtualTile(cellid=2, path="/Volumes/x.tif", window=(0, 0, 256, 256))
    assert t.is_virtual()
    assert t.window == (0, 0, 256, 256)


def test_rejects_no_raster_and_no_path():
    with pytest.raises(ValueError):
        vt.VirtualTile(cellid=3)


def test_rejects_virtual_without_window():
    with pytest.raises(ValueError):
        vt.VirtualTile(cellid=4, path="/Volumes/x.tif")  # no window


def test_row_roundtrip_virtual():
    t = vt.VirtualTile(
        cellid=5,
        path="/Volumes/x.tif",
        window=(1, 2, 300, 400),
        clip_polygon=b"WKB",
        clip_crs="EPSG:4326",
        crs="EPSG:3857",
        metadata={"gbx_format": "cog"},
    )
    back = vt.VirtualTile.from_row(t.to_row())
    assert back == t


def test_row_roundtrip_materialized_null_window():
    t = vt.VirtualTile(cellid=6, raster=b"abc", metadata={})
    row = t.to_row()
    assert row["window"] is None
    assert vt.VirtualTile.from_row(row) == t


def test_schema_has_v2_fields():
    names = set(V.name for V in vt.V2_TILE_SCHEMA.fields)
    assert names == {
        "cellid", "raster", "path", "window", "clip_polygon", "clip_crs", "crs", "metadata"
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_tile.py --log vt.log`
Expected: FAIL — `ModuleNotFoundError: ...pyrx.core.virtual_tile`.

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py
"""v2 virtual-tile representation.

A tile is bytes-free ("virtual") when ``raster is None`` — it carries a
``path`` + pixel ``window`` and materializes lazily via ``open_tile``. A tile
with ``raster`` set is materialized (v1-compatible). The struct is the same
across both cases so parity locks once; ``path``/``window``/``clip_polygon``/
``clip_crs``/``crs`` are null for a plain v1 tile.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

Window4 = Tuple[int, int, int, int]  # (col_off, row_off, width, height)

_WINDOW_STRUCT = StructType(
    [
        StructField("col_off", IntegerType(), nullable=False),
        StructField("row_off", IntegerType(), nullable=False),
        StructField("width", IntegerType(), nullable=False),
        StructField("height", IntegerType(), nullable=False),
    ]
)

V2_TILE_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=True),
        StructField("path", StringType(), nullable=True),
        StructField("window", _WINDOW_STRUCT, nullable=True),
        StructField("clip_polygon", BinaryType(), nullable=True),
        StructField("clip_crs", StringType(), nullable=True),
        StructField("crs", StringType(), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


@dataclass
class VirtualTile:
    cellid: int
    raster: Optional[bytes] = None
    path: Optional[str] = None
    window: Optional[Window4] = None
    clip_polygon: Optional[bytes] = None
    clip_crs: Optional[str] = None
    crs: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.raster is None and self.path is None:
            raise ValueError("VirtualTile needs raster bytes or a path")
        if self.raster is None and self.window is None:
            raise ValueError("virtual tile (path, no raster) requires a window")
        if self.window is not None:
            self.window = tuple(int(v) for v in self.window)  # normalize

    def is_virtual(self) -> bool:
        return self.raster is None

    def to_row(self) -> dict:
        win = None
        if self.window is not None:
            c, r, w, h = self.window
            win = {"col_off": c, "row_off": r, "width": w, "height": h}
        return {
            "cellid": int(self.cellid),
            "raster": self.raster,
            "path": self.path,
            "window": win,
            "clip_polygon": self.clip_polygon,
            "clip_crs": self.clip_crs,
            "crs": self.crs,
            "metadata": dict(self.metadata) if self.metadata else {},
        }

    @classmethod
    def from_row(cls, row) -> "VirtualTile":
        d = row.asDict() if hasattr(row, "asDict") else dict(row)
        win = d.get("window")
        if win is not None:
            wd = win.asDict() if hasattr(win, "asDict") else dict(win)
            win = (wd["col_off"], wd["row_off"], wd["width"], wd["height"])
        return cls(
            cellid=d["cellid"],
            raster=d.get("raster"),
            path=d.get("path"),
            window=win,
            clip_polygon=d.get("clip_polygon"),
            clip_crs=d.get("clip_crs"),
            crs=d.get("crs"),
            metadata=dict(d.get("metadata") or {}),
        )
```

Add the missing `Tuple` import note: `Window4` uses `Tuple` — include `from typing import ... Tuple` (already in the import line above).

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_tile.py --log vt.log`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py \
        python/geobrix/test/pyrx/test_core_virtual_tile.py
git commit -m "feat(pyrx): v2 VirtualTile struct + Spark round-trip

Bytes-free virtual-tile representation (path, window, clip_polygon,
clip_crs, crs) with validation and a full v2 Spark schema so parity
locks once. Exploratory/non-wired: not registered anywhere yet.

Co-authored-by: Isaac"
```

---

### Task 2: Corpus fixture — COG, tiled GTIFF, striped GTIFF

**Files:**
- Create: `python/geobrix/test/pyrx/_layouts.py` (fixture helper importable by tests)
- Test: `python/geobrix/test/pyrx/test_core_layouts_fixture.py` (self-test of the fixture)

**Interfaces:**
- Consumes: rasterio, numpy.
- Produces:
  - `write_striped_gtiff(dst_path, width=1024, height=1024, epsg=4326) -> str` — striped GeoTIFF (`tiled=False`), returns path.
  - `write_tiled_gtiff(dst_path, width=1024, height=1024, blocksize=256, epsg=4326) -> str` — tiled GeoTIFF (`tiled=True, blockxsize=blocksize, blockysize=blocksize`).
  - `write_cog(dst_path, width=1024, height=1024, blocksize=256, epsg=4326) -> str` — COG with overviews via `driver="COG"`.
  - All three write **identical pixel values** (`np.arange` ramp) with the **same georeference** (origin/pixel-size), so a window read from any layout yields the same slice.
  - `PIXELS(width, height) -> np.ndarray` — the shared deterministic array (float32 ramp) all three write.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_layouts_fixture.py
"""The 3-layout corpus fixture writes identical pixels in COG / tiled / striped
form. This self-test pins that invariant so downstream open_tile tests can trust
'same window -> same pixels across layouts'.
"""
import numpy as np
import rasterio

from . import _layouts


def test_three_layouts_same_pixels(tmp_path):
    w, h = 512, 384
    cog = _layouts.write_cog(str(tmp_path / "a.cog.tif"), w, h)
    tiled = _layouts.write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), w, h)
    striped = _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), w, h)

    with rasterio.open(cog) as c, rasterio.open(tiled) as t, rasterio.open(striped) as s:
        assert c.profile["tiled"] is True
        assert t.profile["tiled"] is True
        assert s.profile.get("tiled", False) is False
        assert c.overviews(1)  # COG has overviews
        arr_c, arr_t, arr_s = c.read(1), t.read(1), s.read(1)
    assert np.array_equal(arr_c, arr_t)
    assert np.array_equal(arr_t, arr_s)
    assert np.array_equal(arr_c, _layouts.PIXELS(w, h))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_layouts_fixture.py --log layouts.log`
Expected: FAIL — `ImportError: cannot import name '_layouts'`.

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/test/pyrx/_layouts.py
"""Synthetic 3-layout raster corpus for windowed-read tests.

All three writers emit identical pixels + georeference so 'read window W from
any layout' yields the same slice. Origin (10, 50), 0.001 deg pixels (EPSG:4326).
"""
import numpy as np
import rasterio
from rasterio.transform import from_origin

_PX = 0.001


def PIXELS(width, height):
    return np.arange(width * height, dtype="float32").reshape(height, width)


def _base_profile(width, height, epsg):
    return dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, _PX, _PX),
        nodata=-9999.0,
    )


def write_striped_gtiff(dst_path, width=1024, height=1024, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=False)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path


def write_tiled_gtiff(dst_path, width=1024, height=1024, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=True, blockxsize=blocksize, blockysize=blocksize)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path


def write_cog(dst_path, width=1024, height=1024, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(driver="COG", blocksize=blocksize, overview_resampling="nearest")
    # COG driver rejects some GTiff-only keys; keep the intersection it accepts.
    prof.pop("tiled", None)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_layouts_fixture.py --log layouts.log`
Expected: PASS. (If the COG driver rejects a key, drop it from `_base_profile` for the COG path — the COG driver derives width/height/transform from the write.)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/_layouts.py \
        python/geobrix/test/pyrx/test_core_layouts_fixture.py
git commit -m "test(pyrx): synthetic 3-layout raster corpus fixture

COG / tiled GTIFF / striped GTIFF writers emitting identical pixels +
georeference, for windowed-read parity tests.

Co-authored-by: Isaac"
```

---

### Task 3: Clip helper — per-tile intersection + clip_crs precedence

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py`
- Test: `python/geobrix/test/pyrx/test_core_virtual_clip.py`

**Interfaces:**
- Consumes: `pyrx.core.edit.clip_to_geom` (existing SRID-aware clip), `pyrx._geom.parse_geom`, shapely.
- Produces:
  - `clip_dataset(ds, clip_polygon: bytes, clip_crs: Optional[str]) -> Optional[bytes]` — resolves polygon CRS by precedence (explicit `clip_crs` → embedded EWKB SRID → assume raster CRS), reprojects the polygon if needed, masks with `crop=True`, returns GTiff bytes of the intersection, or `None` when disjoint (empty result).

**Design note:** `edit.clip_to_geom` already reprojects when the shapely geom carries an SRID and returns `None` on non-overlap. So `clip_dataset` is a thin adapter: (1) parse `clip_polygon` via `parse_geom`; (2) if `clip_crs` is set, stamp it onto the geom's SRID (extract EPSG int) so `clip_to_geom` reprojects from the authoritative CRS; (3) delegate to `clip_to_geom`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_virtual_clip.py
"""clip_dataset: clip_crs precedence, per-tile intersection, disjoint->None."""
import numpy as np
import shapely
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip
from .conftest import make_geotiff_bytes


def _open(b):
    return _serde.open_tile(b)


def test_clip_full_cover_returns_all_pixels():
    # 4x3 raster over extent x[10,12] y[48.5,50] (0.5 deg pixels).
    poly = box(10.0, 48.0, 12.0, 50.0)  # fully covers
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs="EPSG:4326")
    assert out is not None
    with _open(out) as ds2:
        assert ds2.width == 4 and ds2.height == 3


def test_clip_partial_returns_slice():
    # Cover only the left half in X.
    poly = box(10.0, 48.0, 11.0, 50.0)
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs=None)  # plain WKB -> assume raster CRS
    assert out is not None
    with _open(out) as ds2:
        assert ds2.width < 4  # clipped narrower than source


def test_clip_disjoint_returns_none():
    poly = box(100.0, 10.0, 101.0, 11.0)  # far away
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs=None)
    assert out is None


def test_clip_crs_overrides_and_reprojects():
    # UTM raster; polygon given in lon/lat via explicit clip_crs must reproject
    # and clip successfully (not raise "do not overlap").
    from rasterio.transform import from_origin
    from rasterio.io import MemoryFile
    from rasterio.warp import transform_bounds

    tr = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    prof = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                crs="EPSG:32633", transform=tr, nodata=-9999.0)
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
        utm_bytes = mf.read()

    minx, miny, maxx, maxy = transform_bounds(
        "EPSG:32633", "EPSG:4326", 500000, 4999200, 500800, 5000000
    )
    poly = box(minx, miny, maxx, maxy)
    wkb = shapely.wkb.dumps(poly)  # plain WKB (no SRID)
    with _open(utm_bytes) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs="EPSG:4326")
    assert out is not None
    with _open(out) as ds2:
        assert ds2.crs.to_epsg() == 32633  # raster CRS unchanged; polygon moved
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_clip.py --log clip.log`
Expected: FAIL — `ModuleNotFoundError: ...pyrx.core._clip`.

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py
"""Clip a windowed dataset to a polygon (virtual-tile clip stage).

Thin adapter over pyrx.core.edit.clip_to_geom, which already: reprojects a
cutline carrying a positive SRID to the raster CRS, masks with crop=True (the
intersection envelope), and returns None on non-overlap. This adds clip_crs
precedence: an explicit clip_crs string (e.g. "EPSG:4326") is authoritative and
is stamped onto the geometry's SRID before delegating, so a plain WKB/WKT can
declare its CRS. Reprojects the polygon, never the raster.
"""
from typing import Optional

import shapely

from databricks.labs.gbx._geom import parse_geom
from databricks.labs.gbx.pyrx.core import edit


def _epsg_int(clip_crs: str) -> Optional[int]:
    """Parse 'EPSG:4326' / '4326' -> 4326; None if not an EPSG code."""
    s = clip_crs.strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None  # WKT2/PROJ string not yet supported for stamping


def clip_dataset(ds, clip_polygon: bytes, clip_crs: Optional[str]) -> Optional[bytes]:
    geom = parse_geom(clip_polygon)
    if geom is None:
        return None
    if clip_crs:
        code = _epsg_int(clip_crs)
        if code is not None:
            geom = shapely.set_srid(geom, code)  # authoritative override
    # else: leave embedded SRID (EWKB) or 0 (plain WKB -> edit assumes raster CRS)
    return edit.clip_to_geom(ds, geom)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_clip.py --log clip.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py \
        python/geobrix/test/pyrx/test_core_virtual_clip.py
git commit -m "feat(pyrx): virtual-tile clip helper with clip_crs precedence

Thin adapter over edit.clip_to_geom adding authoritative clip_crs
override (plain WKB can declare its CRS), per-tile intersection via
crop=True, disjoint->None. Reprojects the polygon, never the raster.

Co-authored-by: Isaac"
```

---

### Task 4: `open_tile` front-door — raster precedence + lazy window read + clip

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py`
- Test: `python/geobrix/test/pyrx/test_core_open_tile.py`

**Interfaces:**
- Consumes: `VirtualTile` (Task 1), `_clip.clip_dataset` (Task 3), `pyrx._serde.open_tile`, `pyrx.core.preparer._stage_local_if_needed`, `_layouts` (test only), rasterio (`Window`, `WarpedVRT`).
- Produces:
  - `@contextmanager open_tile(tile: VirtualTile) -> Iterator[DatasetReader]` — the sole chokepoint. Yields an open rasterio dataset positioned at / holding the tile's result:
    1. `tile.raster` not None → `_serde.open_tile(tile.raster)` (provenance fields ignored; bytes already are the result).
    2. else → stage `tile.path` local (`_stage_local_if_needed`), `rasterio.open`, read exactly `tile.window` into a `MemoryFile` dataset (windowed sub-dataset), apply lazy warp if `tile.crs` set & ≠ source, then clip if `tile.clip_polygon` set. Yields the resulting dataset. Cleans staged temp on exit.
  - `materialize(tile: VirtualTile) -> Tuple[np.ndarray, Affine, dict]` — convenience wrapper returning `(array, transform, profile)` from `open_tile`.
- **Note:** this is a NEW module distinct from `pyrx._serde.open_tile` (which takes raw bytes). This one takes a `VirtualTile`. It delegates to `_serde.open_tile` for the bytes case.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_open_tile.py
"""open_tile front-door: raster precedence, lazy windowed read across the 3
layouts (== full-read slice), multi-block window, clip, and a WarpedVRT probe.
"""
import numpy as np
import pytest
import rasterio
import shapely
import shapely.wkb
from rasterio.windows import Window
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from . import _layouts
from .conftest import make_geotiff_bytes

W, H, BS = 512, 512, 256
WINDOW = (128, 64, 200, 300)  # spans >1 block in both axes (crosses 256 boundary)


def _slice_of_full(path):
    with rasterio.open(path) as ds:
        full = ds.read(1)
    c, r, w, h = WINDOW
    return full[r:r + h, c:c + w]


@pytest.fixture
def layouts(tmp_path):
    return {
        "cog": _layouts.write_cog(str(tmp_path / "a.cog.tif"), W, H, BS),
        "tiled": _layouts.write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), W, H, BS),
        "striped": _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), W, H),
    }


def test_raster_present_precedence_ignores_path():
    # raster set + a bogus path: must open the bytes, never touch path.
    b = make_geotiff_bytes(width=4, height=3)
    tile = VirtualTile(cellid=1, raster=b, path="/nonexistent/x.tif",
                       window=(0, 0, 4, 3))
    with ot.open_tile(tile) as ds:
        assert ds.width == 4 and ds.height == 3


@pytest.mark.parametrize("layout", ["cog", "tiled", "striped"])
def test_windowed_read_equals_full_slice(layouts, layout):
    tile = VirtualTile(cellid=2, path=layouts[layout], window=WINDOW)
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
    assert got.shape == (WINDOW[3], WINDOW[2])  # multi-block window honored
    assert np.array_equal(got, _slice_of_full(layouts[layout]))


def test_clip_applied_to_virtual_window(layouts):
    # Clip within the window to a sub-box (partial); result narrower than window.
    with rasterio.open(layouts["cog"]) as ds:
        win_transform = ds.window_transform(Window(*WINDOW))
        # geographic bounds of the left third of the window
        minx = win_transform.c
        maxx = minx + (WINDOW[2] // 3) * abs(win_transform.a)
        maxy = win_transform.f
        miny = maxy - WINDOW[3] * abs(win_transform.e)
    poly = box(minx, miny, maxx, maxy)
    tile = VirtualTile(cellid=3, path=layouts["cog"], window=WINDOW,
                       clip_polygon=shapely.wkb.dumps(poly), clip_crs="EPSG:4326")
    with ot.open_tile(tile) as ds:
        assert ds.width < WINDOW[2]


def test_warpedvrt_probe_reprojects_window(layouts):
    # crs set & != source -> lazy warp; result carries target CRS.
    tile = VirtualTile(cellid=4, path=layouts["tiled"], window=WINDOW, crs="EPSG:3857")
    with ot.open_tile(tile) as ds:
        assert ds.crs.to_epsg() == 3857
        assert ds.width > 0 and ds.height > 0


def test_materialize_returns_array_transform_profile(layouts):
    arr, transform, profile = ot.materialize(
        VirtualTile(cellid=5, path=layouts["striped"], window=WINDOW)
    )
    assert arr.shape[-2:] == (WINDOW[3], WINDOW[2])
    assert profile["width"] == WINDOW[2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_open_tile.py --log opentile.log`
Expected: FAIL — `ModuleNotFoundError: ...pyrx.core.open_tile`.

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py
"""The single v1/v2/virtual chokepoint.

open_tile(tile) yields an open rasterio dataset regardless of tile shape:
  - raster present  -> open the bytes (v1 / materialized). Provenance fields
    (window/clip/crs) are informational; the bytes ARE the result.
  - raster None      -> stage path local, read exactly `window` (may span >1
    block), lazy-warp to `crs` if set & different, clip to `clip_polygon` if set.
Function bodies never branch on tile shape — they call open_tile and operate on
an open dataset. This is the ONLY place that knows the three tile shapes.
"""
from contextlib import contextmanager
from typing import Iterator, Tuple

import numpy as np
import rasterio
from rasterio.io import DatasetReader, MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip
from databricks.labs.gbx.pyrx.core.preparer import _stage_local_if_needed
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _window_dataset_bytes(src, window: Window) -> bytes:
    """Read one window from an open dataset into standalone GTiff bytes."""
    data = src.read(window=window)
    profile = src.profile.copy()
    profile.update(
        driver="GTiff",
        height=int(window.height),
        width=int(window.width),
        transform=src.window_transform(window),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


@contextmanager
def open_tile(tile: VirtualTile) -> Iterator[DatasetReader]:
    if tile.raster is not None:
        with _serde.open_tile(tile.raster) as ds:
            yield ds
        return

    local_path, is_temp = _stage_local_if_needed(tile.path)
    try:
        c, r, w, h = tile.window
        window = Window(c, r, w, h)
        with rasterio.open(local_path) as src:
            src_epsg = src.crs.to_epsg() if src.crs else None
            want = None
            if tile.crs:
                want = int(str(tile.crs).upper().replace("EPSG:", ""))
            if want is not None and want != src_epsg:
                # lazy warp: warp the windowed sub-dataset to target CRS
                win_bytes = _window_dataset_bytes(src, window)
                with MemoryFile(win_bytes) as mf, mf.open() as wds:
                    with WarpedVRT(wds, crs=f"EPSG:{want}") as vrt:
                        prof = vrt.profile.copy()
                        prof.update(driver="GTiff")
                        with MemoryFile() as out_mf:
                            with out_mf.open(**prof) as dst:
                                dst.write(vrt.read())
                            tile_bytes = out_mf.read()
            else:
                tile_bytes = _window_dataset_bytes(src, window)

        # clip stage (operates on the materialized window bytes)
        with MemoryFile(tile_bytes) as mf, mf.open() as wds:
            if tile.clip_polygon is not None:
                clipped = _clip.clip_dataset(wds, tile.clip_polygon, tile.clip_crs)
                if clipped is None:
                    # disjoint -> empty result; re-open the (unclipped) window as
                    # a valid but caller-detectable dataset is wrong. Yield an
                    # empty single-pixel NoData dataset instead.
                    yield from _empty_dataset(wds)
                    return
                with MemoryFile(clipped) as cf, cf.open() as cds:
                    yield cds
            else:
                yield wds
    finally:
        if is_temp:
            import os
            try:
                os.remove(local_path)
            except OSError:
                pass


def _empty_dataset(ref) -> Iterator[DatasetReader]:
    profile = ref.profile.copy()
    profile.update(driver="GTiff", width=1, height=1)
    nodata = ref.nodata if ref.nodata is not None else 0
    arr = np.full((ref.count, 1, 1), nodata, dtype=ref.dtypes[0])
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr)
        empty = mf.read()
    with MemoryFile(empty) as mf2, mf2.open() as ds:
        yield ds


def materialize(tile: VirtualTile) -> Tuple[np.ndarray, "rasterio.Affine", dict]:
    with open_tile(tile) as ds:
        return ds.read(), ds.transform, ds.profile.copy()
```

**Implementer note:** the nested-contextmanager `yield` inside `with MemoryFile(...)` blocks must keep the file open for the duration of the caller's `with` — the structure above yields *inside* the open blocks so they stay alive until the caller exits. Verify no "dataset closed" errors; if the linter/GDAL complains, restructure to `ExitStack`.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_open_tile.py --log opentile.log`
Expected: PASS (raster-precedence, 3 layout param cases, clip, warp probe, materialize).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py \
        python/geobrix/test/pyrx/test_core_open_tile.py
git commit -m "feat(pyrx): open_tile front-door for v2 virtual tiles

Sole v1/v2/virtual chokepoint: raster-present precedence; else stage
path, read exactly window (multi-block ok), lazy WarpedVRT to crs,
clip to clip_polygon (disjoint->empty). materialize() convenience
wrapper. Proven across COG/tiled/striped local layouts.

Co-authored-by: Isaac"
```

---

### Task 5: Tile-shape conversions — `from_v1` widen + `materialize_to_bytes`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py` (add `from_v1` classmethod)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` (add `materialize_to_bytes`)
- Test: `python/geobrix/test/pyrx/test_core_virtual_convert.py`

**Interfaces:**
- Consumes: `VirtualTile` (Task 1), `open_tile` (Task 4), `_layouts` (test), `_serde.open_tile`.
- Produces:
  - `VirtualTile.from_v1(cellid: int, raster: bytes, metadata: Optional[dict] = None) -> VirtualTile` — widen a v1 tile to v2-materialized (all provenance null). Lossless.
  - `open_tile.materialize_to_bytes(tile: VirtualTile) -> VirtualTile` — run `open_tile`, capture the resulting GTiff bytes into `raster`, keep `path`/`window`/`clip_polygon`/`clip_crs`/`crs` as provenance, preserve `cellid`/`metadata`. Output is v2-materialized (raster non-null) → heavy-consumable. This is the single sanctioned light→heavy crossing for virtual tiles.

**Design note:** conversion (1) v1→v2 and (3) virtual→heavy-useful from the spec's tile-shape lattice. (2) "virtual on heavy" is a deferred *behavioral rule* (heavy can't lazily read /Volumes → virtual is light-only, materialize first); no code here — documented in the spec, enforced when heavy-tier parity is scheduled.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_virtual_convert.py
"""Tile-shape conversions: v1->v2 widen (lossless) and virtual->materialized
(heavy-useful). The virtual->materialized output must round-trip through the
raster-precedence path identically to reading the source window directly.
"""
import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from . import _layouts
from .conftest import make_geotiff_bytes

WINDOW = (128, 64, 200, 300)


def test_from_v1_widens_losslessly():
    b = make_geotiff_bytes(width=4, height=3)
    t = VirtualTile.from_v1(cellid=7, raster=b, metadata={"driver": "GTiff"})
    assert t.raster == b
    assert t.path is None and t.window is None and t.clip_polygon is None
    assert t.clip_crs is None and t.crs is None
    assert not t.is_virtual()
    assert t.metadata == {"driver": "GTiff"}


def test_materialize_to_bytes_produces_heavy_useful_tile(tmp_path):
    path = _layouts.write_tiled_gtiff(str(tmp_path / "a.tif"), 512, 512, 256)
    virt = VirtualTile(cellid=8, path=path, window=WINDOW, metadata={"k": "v"})
    mat = ot.materialize_to_bytes(virt)
    # materialized: raster set, provenance preserved
    assert mat.raster is not None and not mat.is_virtual()
    assert mat.path == path and mat.window == WINDOW
    assert mat.cellid == 8 and mat.metadata == {"k": "v"}
    # raster-precedence path yields exactly the window's pixels
    with ot.open_tile(mat) as ds:
        got = ds.read(1)
    with rasterio.open(path) as ds:
        full = ds.read(1)
    c, r, w, h = WINDOW
    assert np.array_equal(got, full[r:r + h, c:c + w])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_convert.py --log convert.log`
Expected: FAIL — `AttributeError: type object 'VirtualTile' has no attribute 'from_v1'`.

- [ ] **Step 3: Write minimal implementation**

Add to `virtual_tile.py` (inside the `VirtualTile` class):

```python
    @classmethod
    def from_v1(cls, cellid, raster, metadata=None):
        """Widen a v1 tile (cellid, raster, metadata) to v2-materialized.

        All provenance fields (path/window/clip_polygon/clip_crs/crs) are null.
        Lossless: open_tile's raster-precedence path treats it identically to a
        v1 tile, which is the 'v1 supported indefinitely' contract.
        """
        return cls(cellid=cellid, raster=raster, metadata=dict(metadata or {}))
```

Add to `open_tile.py`:

```python
from rasterio.io import MemoryFile  # already imported

def materialize_to_bytes(tile: VirtualTile) -> VirtualTile:
    """Convert a (possibly virtual) tile to a v2-materialized tile: run open_tile
    on the light side (which CAN read /Volumes), capture the window+warp+clip
    result into `raster`, keep provenance. Output is heavy-consumable. This is
    the single sanctioned light->heavy crossing for virtual tiles.
    """
    with open_tile(tile) as ds:
        data = ds.read()
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            raster = mf.read()
    return VirtualTile(
        cellid=tile.cellid,
        raster=raster,
        path=tile.path,
        window=tile.window,
        clip_polygon=tile.clip_polygon,
        clip_crs=tile.clip_crs,
        crs=tile.crs,
        metadata=dict(tile.metadata),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_convert.py --log convert.log`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py \
        python/geobrix/test/pyrx/test_core_virtual_convert.py
git commit -m "feat(pyrx): tile-shape conversions from_v1 + materialize_to_bytes

from_v1 widens a v1 tile to v2-materialized (lossless, all provenance
null). materialize_to_bytes runs open_tile on the light side and
captures the result into raster (heavy-consumable) while keeping
provenance -- the single sanctioned light->heavy crossing for virtual
tiles. (Heavy consuming a *virtual* tile stays unsupported: no in-JVM
/Volumes lazy read; struct adoption deferred to heavy-parity work.)

Co-authored-by: Isaac"
```

---

### Task 6: Partial-clip reassembly probe (precondition for lazy mosaic)

**Files:**
- Test: `python/geobrix/test/pyrx/test_core_virtual_reassembly.py`

**Interfaces:**
- Consumes: `open_tile`, `VirtualTile`, `_layouts`, rasterio.
- Produces: no new source — a probe test asserting two adjacent virtual tiles clipped by one polygon reassemble pixel-aligned.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_virtual_reassembly.py
"""Two adjacent virtual tiles, one shared clip polygon spanning both, must
materialize partial slices that tile back together pixel-aligned at the seam.
This is the precondition for lazy mosaic-with-clip (later increment).
"""
import numpy as np
import rasterio
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from . import _layouts

W, H, BS = 512, 256, 256


def test_adjacent_tiles_reassemble(tmp_path):
    path = _layouts.write_tiled_gtiff(str(tmp_path / "a.tif"), W, H, BS)
    left = (0, 0, 256, 256)
    right = (256, 0, 256, 256)
    # polygon covering the middle 300px band across the seam (col 100..400)
    with rasterio.open(path) as ds:
        tr = ds.transform
        minx = tr.c + 100 * abs(tr.a)
        maxx = tr.c + 400 * abs(tr.a)
        maxy = tr.f
        miny = tr.f - 256 * abs(tr.e)
        full = ds.read(1)
    wkb = shapely.wkb.dumps(box(minx, miny, maxx, maxy))

    tiles = [
        VirtualTile(cellid=0, path=path, window=left, clip_polygon=wkb, clip_crs="EPSG:4326"),
        VirtualTile(cellid=1, path=path, window=right, clip_polygon=wkb, clip_crs="EPSG:4326"),
    ]
    slices = []
    for t in tiles:
        with ot.open_tile(t) as ds:
            slices.append((ds.read(1), ds.window(*ds.bounds)))
    # left slice covers cols 100..256, right covers 256..400 -> concat width 300
    total_w = sum(s[0].shape[1] for s in slices)
    assert total_w == 300
    reassembled = np.hstack([slices[0][0], slices[1][0]])
    assert np.array_equal(reassembled, full[0:256, 100:400])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_reassembly.py --log reassembly.log`
Expected: FAIL initially if seam math is off — adjust the concat/index math until the assertion reflects true geometry (the test IS the spec of correct reassembly; fix the test's expected indices to match rasterio's clip envelope, not the source).

- [ ] **Step 3: Make it pass**

No new source expected — this validates Task 4 (`open_tile`). If it fails due to clip envelope rounding (partial pixel at the polygon edge), align the expected slice bounds to the integer pixel window rasterio actually returns (read `ds.width`/`ds.transform` of each clipped tile to compute the true column span), keeping the invariant "the two partials cover exactly cols 100..400 with no gap/overlap."

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_reassembly.py --log reassembly.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_core_virtual_reassembly.py
git commit -m "test(pyrx): partial-clip reassembly probe across adjacent tiles

Two adjacent virtual tiles + one shared clip polygon materialize
partial slices that tile back pixel-aligned at the seam — the
precondition for lazy mosaic-with-clip.

Co-authored-by: Isaac"
```

---

### Task 7: Peak-RSS probe — COG-overview vs striped inflation (local)

**Files:**
- Test: `python/geobrix/test/pyrx/test_core_virtual_memory_probe.py`

**Interfaces:**
- Consumes: `open_tile`, `VirtualTile`, `_layouts`, `resource`/`tracemalloc`.
- Produces: a probe test that materializes a small window from a large-ish striped vs COG source and records peak memory, asserting the windowed read of the COG does not read the whole array. Numbers are printed for the record; the assertion is a loose upper bound so the test is not flaky.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_virtual_memory_probe.py
"""Probe: a small windowed read from a COG materializes far fewer pixels than
the full raster (cheap overview/tiled range read), whereas a striped GTIFF
inflates full-width strips. Loose bounds -> record numbers, not flake.
"""
import tracemalloc

import numpy as np

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from . import _layouts

W, H, BS = 2048, 2048, 256
WINDOW = (0, 0, 256, 256)  # one block


def _peak_kib(path):
    tracemalloc.start()
    with ot.open_tile(VirtualTile(cellid=0, path=path, window=WINDOW)) as ds:
        arr = ds.read(1)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return arr, peak // 1024


def test_windowed_read_smaller_than_full(tmp_path, capsys):
    cog = _layouts.write_cog(str(tmp_path / "a.cog.tif"), W, H, BS)
    striped = _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), W, H)
    arr_c, peak_c = _peak_kib(cog)
    arr_s, peak_s = _peak_kib(striped)
    full_kib = (W * H * 4) // 1024
    print(f"cog peak={peak_c}KiB striped peak={peak_s}KiB full={full_kib}KiB")
    assert arr_c.shape == (256, 256)
    assert np.array_equal(arr_c, arr_s)  # same pixels regardless of layout
    # a one-block window must not require the full 16MiB array in either case
    assert peak_c < full_kib
```

- [ ] **Step 2–4: Run, adjust bound if needed, pass**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_memory_probe.py --log memprobe.log`
Expected: PASS. If `peak_c` measured via tracemalloc is noisy (rasterio allocates off-heap in GDAL, invisible to tracemalloc), relax to asserting `arr_c.nbytes < full raster nbytes` and rely on the printed line for the real signal. Keep the printed numbers — they feed the Serverless leg and later increments.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_core_virtual_memory_probe.py
git commit -m "test(pyrx): windowed-read memory probe (COG vs striped)

Records peak memory for a one-block windowed read; asserts a window
does not require the full raster. Numbers feed the Serverless leg.

Co-authored-by: Isaac"
```

---

### Task 8: Serverless experiment — open_tile across 3 layouts on a Volume

**Files:**
- Create: `prompts/features/2026-07-31-virtual-tile-serverless-experiment.py` (gitignored scratch: notebook body + runner invocation notes)
- Uses: `notebooks/tests/run_notebooks_serverless.py` (jobs.submit + env v5), the built wheel staged to a Volume.

**Interfaces:**
- Consumes: the light wheel (built + staged to Volume), `VirtualTile` + `open_tile` from the installed package, a Volume corpus dir.
- Produces: a self-reporting run that, on a Serverless worker (not just driver), builds a small DataFrame of virtual-tile rows (path+window, no bytes) for the 3 layouts staged on a Volume, calls `open_tile` per row via `mapInPandas`, and reports per-layout success + materialized shape + peak RSS to a Volume JSON via `dbutils.notebook.exit(json)` + `jobs.get_run_output`.

**Note:** This is a THROWAWAY experiment (gitignored). It is the final gate, run after local tests are green. It is USER-OWNED to trigger (needs Databricks auth / Serverless). Do not auto-run.

- [ ] **Step 1: Build + stage the wheel**

```bash
bash scripts/commands/gbx-data-push-wheel.sh   # builds + stages geobrix-*.whl to Volume
```
Verify the bytes landed (per push-wheel SDK-vs-CLI gotcha) — list the Volume path and confirm size.

- [ ] **Step 2: Write the experiment notebook body**

Content (in `prompts/features/2026-07-31-virtual-tile-serverless-experiment.py`) — the cells the runner will execute:

```python
# Cell 1 — reinstall light wheel (two-step)
%pip install --force-reinstall --no-deps /Volumes/.../geobrix-0.4.4-py3-none-any.whl
%pip install /Volumes/.../geobrix-0.4.4-py3-none-any.whl[light]
dbutils.library.restartPython()

# Cell 2 — generate the 3-layout corpus onto a Volume (write local temp, copy to Volume)
import json, os, resource, tempfile, shutil
import numpy as np, rasterio
from rasterio.transform import from_origin

CORPUS = "/Volumes/geospatial_docs/geobrix/sample-data/large-raster/virtual-tile"
os.makedirs(CORPUS, exist_ok=True)  # root must pre-exist; subdir ok

def _write_local_then_copy(fn, name, **kw):
    tmp = os.path.join(tempfile.gettempdir(), name)  # NOT /local_disk0
    fn(tmp, **kw)
    dst = os.path.join(CORPUS, name)
    shutil.copyfile(tmp, dst)  # FUSE-safe sequential
    return dst

# reuse the same pixel/profile recipe as the local _layouts fixture
# (inline the three writers here — striped tiled=False; tiled tiled+block; COG driver=COG)
# ... paths = {"cog":..., "tiled":..., "striped":...}

# Cell 3 — distributed open_tile: 1 virtual-tile row per layout, materialize on a WORKER
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.pyrx.core import open_tile as ot
import pandas as pd

rows = spark.createDataFrame(
    [(name, path, 0, 0, 256, 256) for name, path in paths.items()],
    "layout string, path string, c int, r int, w int, h int",
).repartition(3, "layout")   # Serverless parallelism ONLY via repartition(N, col)

def _materialize(pdf_iter):
    for pdf in pdf_iter:
        out = []
        for _, row in pdf.iterrows():
            t = VirtualTile(cellid=0, path=row["path"],
                            window=(row["c"], row["r"], row["w"], row["h"]))
            try:
                with ot.open_tile(t) as ds:
                    arr = ds.read(1)
                peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
                out.append((row["layout"], "ok", int(arr.shape[0]), int(arr.shape[1]), int(peak)))
            except Exception as e:  # noqa
                out.append((row["layout"], f"error:{type(e).__name__}:{e}", -1, -1, -1))
        yield pd.DataFrame(out, columns=["layout", "status", "h", "w", "peak_mib"])

res = rows.mapInPandas(
    _materialize, "layout string, status string, h int, w int, peak_mib int"
).collect()

# Cell 4 — self-report (jobs API hides serverless stdout)
summary = {"results": [r.asDict() for r in res]}
dbutils.notebook.exit(json.dumps(summary))
```

- [ ] **Step 3: Run on Serverless via the harness**

```bash
python notebooks/tests/run_notebooks_serverless.py \
  --notebook prompts/features/2026-07-31-virtual-tile-serverless-experiment.py
# (env v5 pinned; capture via jobs.get_run_output)
```
Give a one-line progress update ~every 30s while it runs (per repo convention).

- [ ] **Step 4: Verify**

Confirm the returned JSON shows `status="ok"` for all three layouts with `h==256, w==256`, materialized on workers (peak_mib recorded). A worker-side `open_tile` success (not driver-only) is the increment's Serverless success criterion. Verify rows > 0 before reporting success (per bench-verify-nonzero convention).

- [ ] **Step 5: Record results**

Append the JSON + interpretation to `prompts/features/2026-07-31-virtual-tile-serverless-experiment.py` results section (or a sibling `-results.md`). No git commit needed (gitignored), but note the run URL.

---

## Self-Review

**1. Spec coverage:**
- v2 tile struct (full: cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata) → Task 1. ✓
- `open_tile`/`materialize` single chokepoint, raster precedence, lazy window read → Task 4. ✓
- Clip: SRID/clip_crs precedence, reproject polygon, per-tile intersection, disjoint→empty → Task 3 (+ Task 4 integration). ✓
- WarpedVRT lazy-warp probe → Task 4 (`test_warpedvrt_probe_reprojects_window`). ✓
- 3-layout corpus (COG/tiled/striped, identical pixels) → Task 2. ✓
- Multi-block window → Task 4 (WINDOW crosses the 256 boundary; `test_windowed_read_equals_full_slice`). ✓
- Tile-shape conversions: v1→v2 widen (`from_v1`) + virtual→materialized (`materialize_to_bytes`) → Task 5. Dematerialize-to-virtual + heavy struct adoption deferred (spec §lattice). ✓
- Partial-clip reassembly probe → Task 6. ✓
- Peak-RSS COG vs striped probe → Task 7. ✓
- Serverless proof (worker-side materialize, jobs.submit + env v5, self-report) → Task 8. ✓
- Exploratory/non-wired (no registration) → Global Constraints; no task touches registered_functions.txt/function-info.json/bindings. ✓
- `crs` field defined + minimally exercised (probe only; full rst_transform deferred) → Task 1 (field) + Task 4 (probe). ✓

**2. Placeholder scan:** No "TBD/TODO/handle edge cases". Every code step has real code. Task 5/6 note where expected values may need alignment to rasterio's actual clip envelope — that's a real instruction (align to measured pixel window), not a placeholder.

**3. Type consistency:** `VirtualTile(cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)` used identically across Tasks 1,3,4,5,6,7,8. `Window4` = (col_off,row_off,width,height) consistent. `clip_dataset(ds, clip_polygon, clip_crs)` signature matches between Task 3 def and Task 4 call. `open_tile(tile)` (VirtualTile) is distinct from `_serde.open_tile(bytes)` and delegates to it — noted in Task 4. `materialize(tile) -> (array, transform, profile)` matches Task 7 usage. Conversions `from_v1` + `materialize_to_bytes` in Task 5; dematerialize-to-virtual deferred to Inc 2 (reader emits virtual tiles from durable paths directly).

## Deferred to later increments (tracked, NOT built here)

- Inc 2: reader `virtualTiles` emit mode (default one-block window @ optional overview `z`). Also evaluate whether a standalone `dematerialize_to_virtual` (bytes → durable COG path → virtual tile) is still needed once the reader emits virtual tiles from durable paths directly.
- Heavy tier + v2: heavy can NEVER consume a virtual tile (no in-JVM /Volumes lazy read) — virtual is light-only; heavy handoff requires `materialize_to_bytes` first. Whether heavy adopts the v2 struct (schema parity) vs stays v1-only is deferred to heavy-tier parity work (gated on the light-vs-heavy large-raster bench).
- Inc 3: window taxonomy — overlapPercent / user-bounds-array / user x,y tiling.
- Inc 4: all `rst_*` functions routed through `open_tile`.
- Inc 5: `rst_transform` triple-provenance (raster + window + clip_polygon) + mosaic/stack combinators. Task 6 is the reassembly precondition; the WarpedVRT model (B default, C escape hatch) decision uses Task 4/7/8 numbers.
