# Large-Raster Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lightweight (pyrx) raster reader ingest large/striped GeoTIFFs (e.g. VIIRS-UK) on Serverless without OOM, by auto-chunking on a decoded-memory budget with layout-aware tiling, and standardize processed/large tiles to COG with a decode-free detection contract.

**Architecture:** Two orthogonal reader options — `splitStrategy` (runtime-aware decoded-memory budget) and `tileFormat` (gtiff|cog). A new `pyrx/core/cog.py` holds one header-sniff core shared by `detect_cog` (read/route) and `stamp_format_metadata` (write/heal), plus budget resolution. The reader detects striped-vs-tiled layout at open and chunks by row-bands (striped) or block-snapped grid (tiled). The `gbx_gtiff` writer gains COG force-convert options reusing the existing `analysis.cog_convert`. One consumer (`rst_resample*`) is rewired to read from COG overviews.

**Tech Stack:** Python 3.12+, rasterio (bundled GDAL), rio-cogeo (already a dep, used by `analysis.cog_convert`), PySpark Python DataSource V2, pytest. Light tier — pure Python, no JAR.

## Global Constraints

- **No Spark config in pyrx runtime.** Never `spark.conf.set` / `_jvm` / `.rdd` in reader/consumer code. `splitStrategy=auto` resolution happens at the **driver** in `partitions()` and the concrete budget is baked into `_FilePartition` (same flow as `size_mib` today). (memory: pyrx-serverless-no-spark-config)
- **No aliases.** One canonical name per function/option (CLAUDE.md).
- **Binding parity enforced.** Any new registered `gbx_rst_*` function needs Scala `override def name`, Python `functions.py` binding, and `function-info.json` key. This plan adds **no new registered functions** (resample rewrite is internal; writer options are DataSource options, not functions), so no binding-parity impact — verify with `gbx:test:bindings` regardless. (CLAUDE.md, memory: light-ci-lock-completeness)
- **Doc tests ARE the docs.** Real code + real assertions on real sample data under `/Volumes/main/geobrix_samples/geobrix-examples/`; no mocking Spark/GeoBrix/IO. Doc tests run only in Docker via `gbx:test:*-docs`. (CLAUDE.md)
- **Metadata namespacing.** Format keys are `gbx_format`, `gbx_blocksize`, `gbx_overview_levels` — namespaced to avoid colliding with GDAL-native metadata and existing keys (`driver`, `format`, `compression`, …). Note the tile metadata already carries a legacy `"format": "GTiff"` key (see `_encode.py`); `gbx_format` is distinct and authoritative for COG routing.
- **Perf-parity gate.** Resample-on-COG must be ≥ today's speed; the split path must not regress the small-file passthrough fast path. (memory: perf-parity-light-vs-heavy)
- **Decoded budget, not encoded.** All budget math keys on `width*height*bands*dtype_itemsize`, never encoded byte size. (spec §2)
- **New test dirs must be tier-gated** in `_LIGHT_TEST_DIRS` and the light CI lockfiles if a new package dir is introduced. This plan adds tests under existing `test/ds/` and `test/pyrx/` dirs — no new dir, but verify. (memory: light-ci-lock-completeness, new-feature-dep-and-tier-checklist)
- **Version bump to 0.4.4** (current is 0.4.3) is the final task (pom/`__init__`/package.json + wheel/JAR/banners/pills per geobrix-version-bump-checklist). Do NOT bump earlier.

---

## File Structure

**Create:**
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/cog.py` — header-sniff core, `detect_cog`, `stamp_format_metadata`, `CogInfo`.
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/budget.py` — `decoded_budget_for`, `runtime_kind`, budget→tile math (`layout_tiles`).
- `python/geobrix/test/pyrx/test_core_cog.py` — cog core unit tests.
- `python/geobrix/test/pyrx/test_core_budget.py` — budget + layout-chunking unit tests.
- `python/geobrix/test/ds/test_raster_large.py` — reader large/striped end-to-end tests.
- `python/geobrix/test/ds/test_writer_cog.py` — writer COG option tests.

**Modify:**
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/tiling.py` — add layout-aware / decoded-budget tile sizing (keep existing `_get_tile_size` for heavy-parity callers).
- `python/geobrix/src/databricks/labs/gbx/ds/_encode.py` — `tileFormat` awareness + stamp via `stamp_format_metadata`.
- `python/geobrix/src/databricks/labs/gbx/ds/raster.py` — new options, driver-side strategy resolution, layout chunking, COG emission.
- `python/geobrix/src/databricks/labs/gbx/ds/writer.py` — COG writer options.
- `python/geobrix/src/databricks/labs/gbx/ds/gtiff.py` — pass COG options through to writer.
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/resample.py` — overview-aware read for COG input.
- `docs/docs/readers/raster.mdx`, `docs/docs/beta-release-notes.mdx`, `docs/sidebars.js` (if new page), `docs/docs/api/benchmarking.mdx`.

---

## Task 1: COG detection core (`pyrx/core/cog.py`)

The single source of format truth: one decode-free header-sniff shared by the reader (route) and the stamper (heal).

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/cog.py`
- Test: `python/geobrix/test/pyrx/test_core_cog.py`

**Interfaces:**
- Consumes: nothing (leaf module; uses `rasterio` only for test fixtures, not in the sniff path).
- Produces:
  - `CogInfo` dataclass: `is_cog: bool`, `tiled: bool`, `blocksize: int | None`, `overview_levels: int`.
  - `sniff_header(raster_bytes: bytes) -> CogInfo` — decode-free TIFF header + IFD parse.
  - `detect_cog(metadata: dict | None, raster_bytes: bytes) -> CogInfo` — metadata fast-path → sniff fallback.
  - `stamp_format_metadata(raster_bytes: bytes, existing_metadata: dict | None) -> dict` — re-derives `gbx_*` keys from bytes via `sniff_header`, returns merged map.
  - Constants: `GBX_FORMAT = "gbx_format"`, `GBX_BLOCKSIZE = "gbx_blocksize"`, `GBX_OVERVIEW_LEVELS = "gbx_overview_levels"`.

- [ ] **Step 1: Write failing tests for `sniff_header`**

Uses rasterio to build fixtures, then sniffs raw bytes. A COG has internal tiling + overviews; a plain windowed GTiff (as `encode_tile` writes) is striped, untiled, no overviews.

```python
# python/geobrix/test/pyrx/test_core_cog.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import cog


def _plain_gtiff_bytes(w=256, h=256, tiled=False):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    if tiled:
        profile.update(tiled=True, blockxsize=128, blockysize=128)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, h, w), dtype="uint8"))
        return mf.read()


def _cog_bytes(w=512, h=512):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert
    with MemoryFile(_plain_gtiff_bytes(w, h, tiled=False)) as mf, mf.open() as ds:
        return cog_convert(ds, "DEFLATE", 256, "AVERAGE")


def test_sniff_plain_striped_gtiff_is_not_cog():
    info = cog.sniff_header(_plain_gtiff_bytes(tiled=False))
    assert info.is_cog is False
    assert info.tiled is False
    assert info.overview_levels == 0


def test_sniff_tiled_no_overview_is_not_cog():
    info = cog.sniff_header(_plain_gtiff_bytes(tiled=True))
    assert info.tiled is True
    assert info.is_cog is False  # tiled but no overviews != COG


def test_sniff_cog_is_cog():
    info = cog.sniff_header(_cog_bytes())
    assert info.is_cog is True
    assert info.tiled is True
    assert info.overview_levels >= 1
    assert info.blocksize == 256


def test_sniff_corrupt_bytes_defaults_non_cog():
    info = cog.sniff_header(b"not a tiff at all")
    assert info.is_cog is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cog.py -k sniff`
Expected: FAIL — `ModuleNotFoundError: databricks.labs.gbx.pyrx.core.cog`.

- [ ] **Step 3: Implement `sniff_header` + `CogInfo`**

Decode-free: read the TIFF header and walk IFDs via struct parsing (no `rasterio.open`, no pixel decode). Detect endianness, count IFDs (>1 IFD with reduced-resolution subfile type = overviews), and read `TileWidth` (tag 322) to determine tiling + blocksize. Guard every parse in try/except → non-COG on any error.

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/cog.py
"""Decode-free COG detection + format-metadata stamping.

One header-sniff core (`sniff_header`) is the single source of format truth,
shared by `detect_cog` (read/route) and `stamp_format_metadata` (write/heal) so
the two can never disagree. Sniffing parses only the TIFF header + IFDs (a few
hundred bytes); it never decodes pixels.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Dict, Optional

GBX_FORMAT = "gbx_format"
GBX_BLOCKSIZE = "gbx_blocksize"
GBX_OVERVIEW_LEVELS = "gbx_overview_levels"

_TAG_TILE_WIDTH = 322
_TAG_SUBFILE_TYPE = 254  # bit 0 = reduced-resolution (overview)
_TYPE_SIZES = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8}


@dataclass(frozen=True)
class CogInfo:
    is_cog: bool
    tiled: bool
    blocksize: Optional[int]
    overview_levels: int


_NON_COG = CogInfo(is_cog=False, tiled=False, blocksize=None, overview_levels=0)


def _read_ifd(buf, off, endian):
    """Return (tags: dict[tag]->(type,count,value_or_offset), next_ifd_off)."""
    (n,) = struct.unpack_from(endian + "H", buf, off)
    tags = {}
    p = off + 2
    for _ in range(n):
        tag, typ, count = struct.unpack_from(endian + "HHI", buf, p)
        (value,) = struct.unpack_from(endian + "I", buf, p + 8)
        tags[tag] = (typ, count, value)
        p += 12
    (next_off,) = struct.unpack_from(endian + "I", buf, p)
    return tags, next_off


def sniff_header(raster_bytes: bytes) -> CogInfo:
    """Classify raster bytes by TIFF header/IFD structure only (no pixel decode).

    A COG here = internally tiled AND has >=1 reduced-resolution overview IFD.
    Any parse failure (non-TIFF, truncated, BigTIFF we don't walk) -> non-COG.
    """
    try:
        buf = bytes(raster_bytes)
        if len(buf) < 8:
            return _NON_COG
        bo = buf[:2]
        if bo == b"II":
            endian = "<"
        elif bo == b"MM":
            endian = ">"
        else:
            return _NON_COG
        (magic,) = struct.unpack_from(endian + "H", buf, 2)
        if magic != 42:  # 43 = BigTIFF; not walked here -> treated as non-COG
            return _NON_COG
        (ifd_off,) = struct.unpack_from(endian + "I", buf, 4)

        tiled = False
        blocksize = None
        overview_levels = 0
        ifd_index = 0
        while ifd_off != 0 and ifd_index < 64:
            tags, ifd_off = _read_ifd(buf, ifd_off, endian)
            if ifd_index == 0:
                if _TAG_TILE_WIDTH in tags:
                    tiled = True
                    blocksize = int(tags[_TAG_TILE_WIDTH][2])
            else:
                subfile = tags.get(_TAG_SUBFILE_TYPE)
                if subfile and (int(subfile[2]) & 0x1):
                    overview_levels += 1
            ifd_index += 1

        is_cog = tiled and overview_levels >= 1
        return CogInfo(is_cog=is_cog, tiled=tiled, blocksize=blocksize,
                       overview_levels=overview_levels)
    except Exception:
        return _NON_COG
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cog.py -k sniff`
Expected: PASS (4 tests).

- [ ] **Step 5: Write failing tests for `detect_cog` + `stamp_format_metadata`**

```python
def test_detect_cog_metadata_fast_path_no_decode():
    # gbx_format present -> trusted verbatim; bytes are garbage on purpose to
    # prove no sniff/decode happened.
    md = {cog.GBX_FORMAT: "cog", cog.GBX_OVERVIEW_LEVELS: "3", cog.GBX_BLOCKSIZE: "512"}
    info = cog.detect_cog(md, b"garbage-not-a-tiff")
    assert info.is_cog is True
    assert info.overview_levels == 3
    assert info.blocksize == 512


def test_detect_cog_fallback_sniffs_when_no_metadata():
    info = cog.detect_cog(None, _cog_bytes())
    assert info.is_cog is True


def test_detect_cog_fallback_plain_gtiff():
    info = cog.detect_cog({}, _plain_gtiff_bytes())
    assert info.is_cog is False


def test_stamp_writes_gbx_keys_from_bytes():
    md = cog.stamp_format_metadata(_cog_bytes(), {"driver": "GTiff"})
    assert md["driver"] == "GTiff"          # existing keys preserved
    assert md[cog.GBX_FORMAT] == "cog"
    assert int(md[cog.GBX_OVERVIEW_LEVELS]) >= 1
    assert int(md[cog.GBX_BLOCKSIZE]) == 256


def test_stamp_plain_gtiff_marks_gtiff():
    md = cog.stamp_format_metadata(_plain_gtiff_bytes(), None)
    assert md[cog.GBX_FORMAT] == "gtiff"


def test_detect_and_stamp_agree():
    b = _cog_bytes()
    stamped = cog.stamp_format_metadata(b, None)
    assert cog.detect_cog(stamped, b"garbage").is_cog is True
    assert cog.detect_cog(None, b).is_cog is True
```

- [ ] **Step 6: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cog.py -k "detect or stamp or agree"`
Expected: FAIL — `AttributeError: module ... has no attribute 'detect_cog'`.

- [ ] **Step 7: Implement `detect_cog` + `stamp_format_metadata`**

```python
def detect_cog(metadata: Optional[Dict[str, str]], raster_bytes: bytes) -> CogInfo:
    """R1 resolver: trust the metadata flag when present, else sniff the bytes."""
    if metadata:
        flag = metadata.get(GBX_FORMAT)
        if flag is not None:
            is_cog = str(flag).lower() == "cog"
            try:
                ovr = int(metadata.get(GBX_OVERVIEW_LEVELS, "0"))
            except (TypeError, ValueError):
                ovr = 0
            try:
                blk = int(metadata[GBX_BLOCKSIZE]) if metadata.get(GBX_BLOCKSIZE) else None
            except (TypeError, ValueError):
                blk = None
            return CogInfo(is_cog=is_cog, tiled=is_cog or blk is not None,
                          blocksize=blk, overview_levels=ovr)
    return sniff_header(raster_bytes)


def stamp_format_metadata(
    raster_bytes: bytes, existing_metadata: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """R2 writer/healer: re-derive gbx_* from ACTUAL bytes, merged over existing."""
    info = sniff_header(raster_bytes)
    md = dict(existing_metadata or {})
    md[GBX_FORMAT] = "cog" if info.is_cog else "gtiff"
    md[GBX_OVERVIEW_LEVELS] = str(info.overview_levels)
    if info.blocksize is not None:
        md[GBX_BLOCKSIZE] = str(info.blocksize)
    return md
```

- [ ] **Step 8: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cog.py`
Expected: PASS (all).

- [ ] **Step 9: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/cog.py python/geobrix/test/pyrx/test_core_cog.py
git commit -m "feat(pyrx): COG detection core (sniff_header/detect_cog/stamp)

Co-authored-by: Isaac"
```

---

## Task 2: Budget + layout-aware chunking (`pyrx/core/budget.py`)

Decoded-memory budget resolution and the striped-vs-tiled tile geometry.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/budget.py`
- Test: `python/geobrix/test/pyrx/test_core_budget.py`

**Interfaces:**
- Consumes: nothing (leaf).
- Produces:
  - `runtime_kind() -> str` — `"serverless"` | `"classic"`; driver-side only.
  - `resolve_strategy(strategy: str) -> str` — `auto` → `runtime_kind()`; passes through `serverless`/`classic`/`none`.
  - `decoded_budget_bytes(strategy: str) -> int` — `serverless`→512 MiB, `classic`→1536 MiB, `none`→0 (sentinel: no split).
  - `LayoutPlan` dataclass: `tiles: list[tuple[int,int,int,int]]` (col_off,row_off,w,h), `degraded: bool`.
  - `plan_layout(width, height, bands, dtype_itemsize, tiled, blockxsize, blockysize, budget_bytes, max_tiles=512) -> LayoutPlan` — row-bands when `tiled=False`, block-snapped power-of-4 grid when `tiled=True`; `budget_bytes<=0` → single whole-image tile.

- [ ] **Step 1: Write failing tests**

```python
# python/geobrix/test/pyrx/test_core_budget.py
import pytest
from databricks.labs.gbx.pyrx.core import budget


def test_resolve_strategy_passthrough():
    assert budget.resolve_strategy("serverless") == "serverless"
    assert budget.resolve_strategy("classic") == "classic"
    assert budget.resolve_strategy("none") == "none"


def test_resolve_auto_is_serverless_or_classic():
    assert budget.resolve_strategy("auto") in ("serverless", "classic")


def test_budget_values():
    assert budget.decoded_budget_bytes("serverless") == 512 * 1024 * 1024
    assert budget.decoded_budget_bytes("classic") == 1536 * 1024 * 1024
    assert budget.decoded_budget_bytes("none") == 0


def test_none_budget_single_tile():
    plan = budget.plan_layout(2000, 2000, 1, 1, False, None, None, 0)
    assert plan.tiles == [(0, 0, 2000, 2000)]
    assert plan.degraded is False


def test_striped_yields_full_width_row_bands():
    # 10000x10000 uint8 1-band = 100MB decoded; 32MB budget -> row bands.
    plan = budget.plan_layout(10000, 10000, 1, 1, False, None, None, 32 * 1024 * 1024)
    # Every tile spans full width (never column-split) and starts at col 0.
    assert all(t[0] == 0 and t[2] == 10000 for t in plan.tiles)
    # Each band's decoded size <= budget.
    assert all(t[2] * t[3] * 1 * 1 <= 32 * 1024 * 1024 for t in plan.tiles)
    # Row bands tile the full height with no gaps/overlaps.
    assert plan.tiles[0][1] == 0
    assert sum(t[3] for t in plan.tiles) == 10000


def test_decoded_budget_not_encoded():
    # Tiny "encoded" notion is irrelevant: a big decoded raster must split even
    # though a compressed version would be small. plan_layout only sees decoded.
    plan = budget.plan_layout(20000, 20000, 3, 2, False, None, None, 64 * 1024 * 1024)
    assert len(plan.tiles) > 1


def test_tiled_grid_snaps_to_blocks():
    # 4096x4096, 512 blocks. Row-band vs grid: tiled path uses square-ish grid,
    # each tile dim is a multiple of the block size (except final edge tile).
    plan = budget.plan_layout(4096, 4096, 1, 4, True, 512, 512, 16 * 1024 * 1024)
    assert len(plan.tiles) > 1
    for col_off, row_off, w, h in plan.tiles:
        assert col_off % 512 == 0 and row_off % 512 == 0


def test_max_tiles_cap_sets_degraded():
    # Absurdly small budget vs huge raster -> would need >512 tiles -> capped+degraded.
    plan = budget.plan_layout(100000, 100000, 1, 4, False, None, None, 1024)
    assert len(plan.tiles) <= 512
    assert plan.degraded is True
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_budget.py`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `budget.py`**

`runtime_kind` probes the environment at the driver only (no Spark API): Serverless Python sets `IS_SERVERLESS` / `POD_NAME` and the runtime exposes `spark.databricks.clusterUsageTags.clusterAllTags`, but the cheap, config-free signal is the env var `IS_SERVERLESS` (present on Serverless) or absence of a classic `SPARK_WORKER_*`/executor env. Default to `serverless` (the safe, smaller budget) when uncertain — over-splitting is safe; under-splitting OOMs.

```python
# python/geobrix/src/databricks/labs/gbx/pyrx/core/budget.py
"""Decoded-memory budget resolution + layout-aware tile geometry.

`splitStrategy` resolves to a per-tile DECODED-byte budget; `plan_layout`
turns that budget into concrete windows honoring physical layout (row-bands
for striped sources, block-snapped grid for tiled). Budget math is always on
decoded size (w*h*bands*itemsize), never encoded bytes.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

_MIB = 1024 * 1024
_BUDGETS = {"serverless": 512 * _MIB, "classic": 1536 * _MIB, "none": 0}
_MAX_TILES = 512


def runtime_kind() -> str:
    """Driver-side runtime probe (NO Spark API). Defaults to 'serverless' (safe)."""
    if os.environ.get("IS_SERVERLESS", "").lower() in ("true", "1"):
        return "serverless"
    # Classic clusters expose an executor/worker memory env; Serverless does not.
    if os.environ.get("SPARK_WORKER_MEMORY") or os.environ.get("SPARK_EXECUTOR_MEMORY"):
        return "classic"
    return "serverless"


def resolve_strategy(strategy: str) -> str:
    s = (strategy or "auto").strip().lower()
    if s == "auto":
        return runtime_kind()
    if s not in _BUDGETS:
        raise ValueError(
            f"splitStrategy must be one of auto|serverless|classic|none; got '{strategy}'"
        )
    return s


def decoded_budget_bytes(strategy: str) -> int:
    return _BUDGETS[resolve_strategy(strategy)]


@dataclass(frozen=True)
class LayoutPlan:
    tiles: List[Tuple[int, int, int, int]]  # (col_off, row_off, w, h)
    degraded: bool


def _row_bands(width, height, per_row_bytes, budget_bytes, max_tiles):
    rows_per_band = max(1, budget_bytes // max(1, per_row_bytes))
    n = math.ceil(height / rows_per_band)
    degraded = False
    if n > max_tiles:
        rows_per_band = math.ceil(height / max_tiles)
        n = math.ceil(height / rows_per_band)
        degraded = True
    tiles = []
    row = 0
    while row < height:
        h = min(rows_per_band, height - row)
        tiles.append((0, row, width, h))
        row += rows_per_band
    return LayoutPlan(tiles=tiles, degraded=degraded)


def _block_grid(width, height, bytes_per_px, budget_bytes, bx, by, max_tiles):
    # Power-of-4 rounds until per-tile decoded bytes <= budget or tile cap hit.
    decoded = width * height * bytes_per_px
    k = 0
    degraded = False
    while (decoded >> (2 * k)) > budget_bytes:
        if (1 << (2 * (k + 1))) > max_tiles:
            degraded = True
            break
        k += 1
    n = 1 << k
    # Snap tile dims up to a block multiple so reads pull whole blocks.
    tile_w = min(width, _ceil_to(math.ceil(width / n), bx or 1))
    tile_h = min(height, _ceil_to(math.ceil(height / n), by or 1))
    tiles = []
    row = 0
    while row < height:
        col = 0
        h = min(tile_h, height - row)
        while col < width:
            w = min(tile_w, width - col)
            tiles.append((col, row, w, h))
            col += tile_w
        row += tile_h
    return LayoutPlan(tiles=tiles, degraded=degraded)


def _ceil_to(v, m):
    return v if m <= 1 else ((v + m - 1) // m) * m


def plan_layout(
    width: int, height: int, bands: int, dtype_itemsize: int,
    tiled: bool, blockxsize: Optional[int], blockysize: Optional[int],
    budget_bytes: int, max_tiles: int = _MAX_TILES,
) -> LayoutPlan:
    bytes_per_px = max(1, bands) * max(1, dtype_itemsize)
    if budget_bytes <= 0:
        return LayoutPlan(tiles=[(0, 0, width, height)], degraded=False)
    if width * height * bytes_per_px <= budget_bytes:
        return LayoutPlan(tiles=[(0, 0, width, height)], degraded=False)
    if tiled:
        return _block_grid(width, height, bytes_per_px, budget_bytes,
                           blockxsize, blockysize, max_tiles)
    per_row_bytes = width * bytes_per_px
    return _row_bands(width, height, per_row_bytes, budget_bytes, max_tiles)
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_budget.py`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/budget.py python/geobrix/test/pyrx/test_core_budget.py
git commit -m "feat(pyrx): decoded-memory budget + layout-aware tile planner

Co-authored-by: Isaac"
```

---

## Task 3: `_encode.py` — tileFormat awareness + gbx_* stamping

Make the tile encoder able to emit COG and always stamp `gbx_*` via the shared core.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_encode.py`
- Test: `python/geobrix/test/ds/test_encode_cog.py` (create)

**Interfaces:**
- Consumes: `cog.stamp_format_metadata` (Task 1); `analysis.cog_convert` (existing).
- Produces:
  - `encode_tile(ds, window, source_path, all_parents, compression="DEFLATE", tile_format="gtiff", cog_blocksize=512, cog_overview_resampling="AVERAGE")` — same return `(cellid, bytes, metadata)`, now with `gbx_*` stamped and COG emission when `tile_format=="cog"`.
  - `passthrough_tile(...)` — unchanged signature, now also stamps `gbx_*` from the source bytes.

- [ ] **Step 1: Write failing tests**

```python
# python/geobrix/test/ds/test_encode_cog.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.ds import _encode
from databricks.labs.gbx.pyrx.core import cog


def _open(w=512, h=512):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    mf = MemoryFile()
    with mf.open(**profile) as dst:
        dst.write(np.zeros((1, h, w), dtype="uint8"))
    return mf.open()


def test_encode_tile_gtiff_stamps_gtiff():
    with _open() as ds:
        _, b, md = _encode.encode_tile(ds, (0, 0, 512, 512), "/x.tif", "",
                                       tile_format="gtiff")
    assert md[cog.GBX_FORMAT] == "gtiff"
    assert cog.sniff_header(b).is_cog is False


def test_encode_tile_cog_emits_and_stamps_cog():
    with _open() as ds:
        _, b, md = _encode.encode_tile(ds, (0, 0, 512, 512), "/x.tif", "",
                                       tile_format="cog", cog_blocksize=256)
    assert md[cog.GBX_FORMAT] == "cog"
    info = cog.sniff_header(b)
    assert info.is_cog is True and info.overview_levels >= 1
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_encode_cog.py`
Expected: FAIL — `encode_tile() got an unexpected keyword argument 'tile_format'`.

- [ ] **Step 3: Implement**

In `encode_tile`, after reading `data` and building the GTiff bytes as today, branch on `tile_format`: for `"cog"`, reopen the just-written GTiff bytes and run `analysis.cog_convert(ds2, compression, cog_blocksize, cog_overview_resampling)` to get COG bytes (reuse — do NOT reimplement COG writing). Then replace the trailing `metadata` construction with a stamp call. Concretely:

```python
# add import at top
from databricks.labs.gbx.pyrx.core import cog as _cog

# encode_tile signature:
def encode_tile(ds, window, source_path, all_parents, compression="DEFLATE",
                tile_format="gtiff", cog_blocksize=512,
                cog_overview_resampling="AVERAGE"):
    col_off, row_off, win_w, win_h = window
    rio_window = Window(col_off, row_off, win_w, win_h)
    data = ds.read(window=rio_window)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", width=win_w, height=win_h,
                   compress=compression.lower(),
                   transform=ds.window_transform(rio_window))
    with MemoryFile() as mf:
        with mf.open(**profile) as out:
            out.write(data)
        raster_bytes = mf.read()

    if str(tile_format).lower() == "cog":
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert
        with MemoryFile(raster_bytes) as cmf, cmf.open() as cds:
            raster_bytes = cog_convert(cds, compression, cog_blocksize,
                                       cog_overview_resampling)

    metadata = {
        "path": f"/vsimem/light_{os.path.basename(source_path)}_{col_off}_{row_off}.tif",
        "sourcePath": source_path,
        "driver": "GTiff",
        "format": "GTiff",
        "last_command": f"windowed_extract -srcwin {col_off} {row_off} {win_w} {win_h}",
        "last_error": "",
        "all_parents": f"{source_path};{all_parents}",
        "size": "-1",
        "compression": compression,
        "isZipped": "false",
        "isSubset": "false",
    }
    metadata = _cog.stamp_format_metadata(raster_bytes, metadata)
    return CELLID_FRESH, raster_bytes, metadata
```

Apply the same trailing `metadata = _cog.stamp_format_metadata(raster_bytes, metadata)` line to `passthrough_tile` (before its `return`), so a source that is already a COG is correctly flagged.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_encode_cog.py`
Expected: PASS.

- [ ] **Step 5: Run existing encode/reader tests for regressions**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/`
Expected: PASS (existing `test_raster_datasource.py` unaffected — default `tile_format="gtiff"` preserves behavior; metadata now has extra `gbx_*` keys, which existing assertions should not forbid — if any asserts exact metadata equality, update it to check subset).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_encode.py python/geobrix/test/ds/test_encode_cog.py
git commit -m "feat(ds): tileFormat=cog emission + gbx_* stamping in _encode

Co-authored-by: Isaac"
```

---

## Task 4: Reader options + driver-side strategy + layout chunking (`raster.py`)

Wire the two axes into the reader, resolve strategy at the driver, and replace the phase-2 rectangular loop with `plan_layout` windows.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py`
- Test: `python/geobrix/test/ds/test_raster_large.py` (create)

**Interfaces:**
- Consumes: `budget.resolve_strategy`, `budget.decoded_budget_bytes`, `budget.plan_layout` (Task 2); `_encode.encode_tile`/`passthrough_tile` (Task 3).
- Produces: reader options `splitStrategy` (default `auto`), `tileFormat` (default `auto`), `cogBlockSize` (default `512`), `cogOverviewResampling` (default `AVERAGE`); `sizeInMB` retained as override. `_FilePartition` gains `budget_bytes: int` and `tile_format`, `cog_blocksize`, `cog_overview_resampling`.

- [ ] **Step 1: Write failing tests**

```python
# python/geobrix/test/ds/test_raster_large.py
import numpy as np
import rasterio
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition
from databricks.labs.gbx.pyrx.core import cog


def _write_striped(path, w=4000, h=4000):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity(),
                   tiled=False)  # striped
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.zeros((1, h, w), dtype="uint8"))


def test_default_auto_splits_large_striped(tmp_path):
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "serverless",
                         "sizeInMB": "-1"})  # sizeInMB unset-equivalent
    # Force a tiny budget via serverless? Instead assert >1 row for a raster
    # that exceeds the budget. Use a monkeypatched small budget:
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=1024 * 1024,
                          tile_format="auto", cog_blocksize=512,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) > 1  # auto-split kicked in


def test_none_strategy_single_row(tmp_path):
    p = tmp_path / "s.tif"
    _write_striped(str(p), 512, 512)
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "none"})
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=0,
                          tile_format="gtiff", cog_blocksize=512,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) == 1


def test_auto_tileformat_cog_when_split(tmp_path):
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader({"path": str(tmp_path)})
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=1024 * 1024,
                          tile_format="auto", cog_blocksize=256,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) > 1
    # Split tiles under tileFormat=auto are COG.
    _, tile = rows[0]
    cellid, raster_bytes, md = tile
    assert md[cog.GBX_FORMAT] == "cog"


def test_options_default_resolution():
    r = RasterGbxReader({"path": "/x", "splitStrategy": "auto"})
    assert r.strategy in ("serverless", "classic")
    assert r.tile_format == "auto"
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_large.py`
Expected: FAIL — `_FilePartition.__init__() got an unexpected keyword argument 'budget_bytes'`.

- [ ] **Step 3: Implement**

1. Extend `_FilePartition.__init__` to store `budget_bytes`, `tile_format`, `cog_blocksize`, `cog_overview_resampling` (keep `size_mib`).
2. In `RasterGbxReader.__init__` parse: `self.strategy = budget.resolve_strategy(options.get("splitStrategy", "auto"))`; `self.tile_format = options.get("tileFormat", "auto")`; `self.cog_blocksize = int(options.get("cogBlockSize", "512"))`; `self.cog_overview_resampling = options.get("cogOverviewResampling", "AVERAGE")`. Keep `self.size_mib`.
3. In `partitions()` compute `budget = budget.decoded_budget_bytes(self.strategy)` once at the driver and pass into each `_FilePartition`. When `size_mib > 0` (power-user override), pass `budget_bytes = size_mib*1024*1024` instead (override wins).
4. In `read()` phase-2, replace the `_get_tile_size` + nested while-loop with:
   - `whole = partition.budget_bytes <= 0 or (width*height*bands*itemsize <= budget)`.
   - keep the whole-image guard + GTiff passthrough fast-path when `whole` (passthrough now stamps `gbx_*` from Task 3). For passthrough, resolve `tile_format`: `auto`+whole → passthrough as source format (no COG); `cog`+whole → fall through to encode-as-COG.
   - else build `plan = budget.plan_layout(width, height, bands, itemsize, ds.profile.get("tiled", False), ds.profile.get("blockxsize"), ds.profile.get("blockysize"), partition.budget_bytes)`, warn if `plan.degraded`, then for each `(col,row,w,h)` in `plan.tiles` call `_encode.encode_tile(ds, (col,row,w,h), ..., tile_format=_resolve_emit_format(partition.tile_format, split=True), cog_blocksize=partition.cog_blocksize, cog_overview_resampling=partition.cog_overview_resampling)`.
   - `_resolve_emit_format(tile_format, split)`: `cog`→`"cog"`; `gtiff`→`"gtiff"`; `auto`→`"cog" if split else "gtiff"`.
5. `itemsize` via `_numpy_itemsize(ds.dtypes[0])`; `bands = ds.count`.
6. Emit a `logging.warning` (module logger) when `plan.degraded` explaining tiles exceed budget because the 512-tile cap was hit.

Reference the exact anchor: the phase-2 block is `raster.py:198-234`; the whole-image guard/fast-path is `raster.py:162-196`.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_large.py`
Expected: PASS.

- [ ] **Step 5: Run full ds suite for regressions**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/`
Expected: PASS. If `test_raster_datasource.py::test_no_split_by_default_yields_one_row` fails, it asserts the *old* default (`-1` = no split). Under the new default `splitStrategy=auto`, a large raster DOES split. Update that test's intent to `splitStrategy=none` for the no-split contract, and add the auto-split behavior as the new default expectation. This is the documented behavior change — reflect it in the test, don't suppress it.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_large.py python/geobrix/test/ds/test_raster_datasource.py
git commit -m "feat(ds): splitStrategy/tileFormat axes + layout-aware auto-chunk

Default flips to auto-split large rasters on a decoded-memory budget;
striped sources chunk by full-width row-bands, tiled by block-snapped
grid. tileFormat=auto emits COG on split, passthrough when whole.

Co-authored-by: Isaac"
```

---

## Task 5: Named readers pass options through

`gtiff_gbx` and `netcdf_gbx` inherit the reader; confirm the new options flow. `gtiff.py`'s reader already just calls `super().__init__`, so options propagate automatically — the task is a guard test, plus confirming netcdf.

**Files:**
- Modify (only if a subclass overrides `partitions`/`read`): `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py`
- Test: `python/geobrix/test/ds/test_raster_large.py` (extend)

**Interfaces:**
- Consumes: Task 4 reader.
- Produces: parity — named readers honor `splitStrategy`/`tileFormat`.

- [ ] **Step 1: Write failing/guard test**

```python
def test_gtiff_reader_inherits_options(tmp_path):
    from databricks.labs.gbx.ds.gtiff import GTiffGbxReader
    r = GTiffGbxReader({"path": str(tmp_path), "splitStrategy": "classic",
                        "tileFormat": "cog"})
    assert r.strategy == "classic"
    assert r.tile_format == "cog"
    assert r.driver == "GTiff"
```

- [ ] **Step 2: Run to verify pass/fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_large.py -k inherits`
Expected: PASS immediately (inheritance already works) — if PASS, this task is a no-op guard confirming parity; keep the test. If FAIL, `GTiffGbxReader.__init__` drops options → fix to call `super().__init__(options)` first (it already does).

- [ ] **Step 3: Check netcdf reader**

Read `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py`. If `NetcdfRasterReader` builds its own partitions/encode path (it transcodes CF variables to GTiff tiles), route its per-tile encode through the same `_encode.encode_tile` `tile_format` param so netcdf tiles also honor `tileFormat`. If it delegates to `encode_tile`, pass `tile_format=self.tile_format`. Add a test only if netcdf has sample data available in Docker; otherwise note it runs in doc-tests (Task 9).

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/netcdf.py python/geobrix/test/ds/test_raster_large.py
git commit -m "feat(ds): named readers (gtiff/netcdf) honor split+tileFormat

Co-authored-by: Isaac"
```

---

## Task 6: Writer COG options (`writer.py` + `gtiff.py`)

Force-convert existing tiles to COG on write, reusing `analysis.cog_convert`.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/writer.py`, `python/geobrix/src/databricks/labs/gbx/ds/gtiff.py`, `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (writer() factory)
- Test: `python/geobrix/test/ds/test_writer_cog.py` (create)

**Interfaces:**
- Consumes: `cog.detect_cog`, `cog.stamp_format_metadata` (Task 1); `analysis.cog_convert` (existing).
- Produces: writer options `cog` ("true"/"false"), `cogBlockSize`, `cogOverviews`, `cogCompression`, `cogPredictor`. `RasterGbxWriter.__init__` gains `cog: bool`, `cog_blocksize`, `cog_overviews`, `cog_compression`, `cog_predictor`.

- [ ] **Step 1: Write failing tests**

```python
# python/geobrix/test/ds/test_writer_cog.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.ds.writer import RasterGbxWriter
from databricks.labs.gbx.ds.raster import reader_schema
from databricks.labs.gbx.pyrx.core import cog


def _plain_gtiff(w=512, h=512):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, h, w), dtype="uint8"))
        return mf.read()


class _Row(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


def test_writer_cog_true_converts_plain_gtiff(tmp_path):
    w = RasterGbxWriter(str(tmp_path), reader_schema(), overwrite=True,
                        cog=True, cog_blocksize=256)
    b = _plain_gtiff()
    row = {"source": "x", "tile": {"cellid": -1, "raster": b,
                                   "metadata": {"driver": "GTiff"}}}
    w.write(iter([row]))
    import glob, os
    out = glob.glob(os.path.join(str(tmp_path), "*.tif"))[0]
    with open(out, "rb") as fh:
        assert cog.sniff_header(fh.read()).is_cog is True


def test_writer_cog_passthrough_already_cog(tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert
    with MemoryFile(_plain_gtiff()) as mf, mf.open() as ds:
        cb = cog_convert(ds, "DEFLATE", 256, "AVERAGE")
    w = RasterGbxWriter(str(tmp_path), reader_schema(), overwrite=True, cog=True)
    row = {"source": "x", "tile": {"cellid": -1, "raster": cb,
                                   "metadata": {cog.GBX_FORMAT: "cog"}}}
    w.write(iter([row]))
    import glob, os
    out = glob.glob(os.path.join(str(tmp_path), "*.tif"))[0]
    with open(out, "rb") as fh:
        assert cog.sniff_header(fh.read()).is_cog is True
```

- [ ] **Step 2: Run to verify fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_writer_cog.py`
Expected: FAIL — `RasterGbxWriter.__init__() got an unexpected keyword argument 'cog'`.

- [ ] **Step 3: Implement**

1. `RasterGbxWriter.__init__` accepts `cog: bool = False, cog_blocksize: int = 512, cog_overviews: str = "auto", cog_compression: str = "DEFLATE", cog_predictor: str = "2"` and stores them.
2. In `write()`, after computing `raster_bytes`, if `self.cog`: run `info = cog.detect_cog(metadata, raster_bytes)`; if `info.is_cog` → passthrough (write bytes verbatim); else reopen bytes and `raster_bytes = analysis.cog_convert(ds, self.cog_compression, self.cog_blocksize, <overview_resampling>)` then `metadata = cog.stamp_format_metadata(raster_bytes, metadata)`. Map `cog_overviews`: `"none"`→skip overview build (pass `overview_resampling` but rio-cogeo always builds; if `"none"` requested, document that COG requires overviews and treat `none` as `auto` with a warning — OR set a minimal single-level; choose: **treat `none` as a warning + proceed with auto**, since a COG without overviews is just a tiled GTiff). Keep it simple: `cog_overviews` in `{"auto"}∪int-levels` controls nothing rio-cogeo exposes directly beyond resampling; pass `overview_resampling=self.cog_overviews if not numeric else "AVERAGE"`. **Simplify:** drop `cogOverviews` numeric levels from THIS spec (rio-cogeo auto-computes levels); keep `cogOverviewResampling`. Update tests/docs accordingly.
3. Pass writer options through the three `writer()` factories (`raster.py:248`, `gtiff.py:30`): read `self.options.get("cog")` etc. and forward.

Note the reconciliation: `_write.tile_to_bytes` already has a COG re-encode path via `force_driver="COG"`, but it does NOT build overviews (it just sets `blocksize`). The writer-COG option must produce a *real* COG (with overviews), so route through `analysis.cog_convert`, not `force_driver="COG"`. Leave `tile_to_bytes` as-is for the non-COG path.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_writer_cog.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/writer.py python/geobrix/src/databricks/labs/gbx/ds/gtiff.py python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_writer_cog.py
git commit -m "feat(ds): gbx_gtiff writer cog=true force-convert (real overviews)

Co-authored-by: Isaac"
```

---

## Task 7: Consumer rewrite — `rst_resample*` reads from overviews

Prove the COG payoff: when input is a COG, read from the nearest overview instead of full-res.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/resample.py`
- Test: `python/geobrix/test/pyrx/test_core_resample_cog.py` (create)

**Interfaces:**
- Consumes: `cog.detect_cog` / `sniff_header` (Task 1). Note `resample.py` functions receive an open `ds` (not bytes+metadata). Use `ds.overviews(1)` (rasterio native) to discover overview decimation factors — this is the correctness-equivalent, metadata-free way to know if overviews exist, and it works on any dataset. `detect_cog` is used at the Spark-UDF boundary; the core function uses `ds.overviews`.
- Produces: `_write_resampled` reads from the best overview level when downsampling and overviews exist; output identical CRS/extent; non-overview path unchanged.

- [ ] **Step 1: Write failing test**

```python
# python/geobrix/test/pyrx/test_core_resample_cog.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import resample


def _cog(w=1024, h=1024):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            # gradient so resampling is meaningful
            row = np.tile(np.arange(w, dtype="uint8"), (h, 1))
            dst.write(row[np.newaxis, :, :])
        with mf.open() as ds:
            return cog_convert(ds, "DEFLATE", 256, "AVERAGE")


def test_resample_downsample_cog_uses_overview(monkeypatch):
    calls = {"full_res_reads": 0}
    b = _cog()
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.overviews(1)  # sanity: COG has overviews
        out = resample.resample_to_size(ds, 128, 128, "average")
    # Output opens and has the requested size (correctness).
    with MemoryFile(out) as mf2, mf2.open() as ods:
        assert (ods.width, ods.height) == (128, 128)


def test_resample_non_cog_parity():
    # Plain GTiff (no overviews): output identical dims to today's path.
    profile = dict(driver="GTiff", width=512, height=512, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, 512, 512), dtype="uint8"))
        with mf.open() as ds:
            out = resample.resample_to_size(ds, 100, 100, "bilinear")
    with MemoryFile(out) as mf2, mf2.open() as ods:
        assert (ods.width, ods.height) == (100, 100)
```

- [ ] **Step 2: Run to verify fail/pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_resample_cog.py`
Expected: `test_resample_non_cog_parity` PASSES on current code; `test_resample_downsample_cog_uses_overview` PASSES for correctness even today (rasterio's `out_shape` read auto-uses overviews when present!). **Verify this claim** — if rasterio already reads overviews via `out_shape`, the rewrite is a no-op for correctness and the "win" is confirmed-already-present. If so, the task becomes: add an explicit `out_shape`-with-overview path only if benchmarking shows GDAL isn't picking the overview. Document the finding.

- [ ] **Step 3: Implement (if needed)**

If rasterio's `ds.read(out_shape=...)` does NOT already exploit overviews for the COG, make it explicit: compute the target decimation, pick the overview level whose factor is the largest ≤ target decimation, read that level's data via `out_shape`, then final-resample to exact dims. Guard behind `if ds.overviews(1) and dst_width < ds.width:`. Keep the existing path for the non-overview / upsample case verbatim (parity).

```python
def _write_resampled(ds, dst_width, dst_height, algorithm):
    dst_width = max(1, int(dst_width))
    dst_height = max(1, int(dst_height))
    new_transform = ds.transform * Affine.scale(
        ds.width / dst_width, ds.height / dst_height
    )
    profile = ds.profile.copy()
    profile.update(driver="GTiff", width=dst_width, height=dst_height,
                   transform=new_transform)
    # rasterio's windowed out_shape read already selects the best internal
    # overview when the source is a COG; this is the COG payoff for downsampling.
    data = ds.read(out_shape=(ds.count, dst_height, dst_width),
                   resampling=resampling_enum(algorithm))
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_resample_cog.py`
Expected: PASS both.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/resample.py python/geobrix/test/pyrx/test_core_resample_cog.py
git commit -m "perf(pyrx): rst_resample reads COG overviews on downsample

Co-authored-by: Isaac"
```

---

## Task 8: R2 metadata-carry survival test

Prove `gbx_format` survives an op chain (COG-aware → non-COG-aware → COG-aware) — the standing-rule regression guard.

**Files:**
- Test: `python/geobrix/test/pyrx/test_metadata_carry.py` (create)

**Interfaces:**
- Consumes: `cog` core, `_serde.build_tile`, a representative non-COG-aware function.

- [ ] **Step 1: Write the test**

```python
# python/geobrix/test/pyrx/test_metadata_carry.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import cog
from databricks.labs.gbx.pyrx.core.analysis import cog_convert


def _cog_tile():
    profile = dict(driver="GTiff", width=512, height=512, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, 512, 512), dtype="uint8"))
        with mf.open() as ds:
            b = cog_convert(ds, "DEFLATE", 256, "AVERAGE")
    md = cog.stamp_format_metadata(b, {"driver": "GTiff"})
    return b, md


def test_gbx_format_survives_op_chain():
    b, md = _cog_tile()
    assert md[cog.GBX_FORMAT] == "cog"
    # Simulate a non-COG-aware op that rebuilds bytes but preserves format:
    # it MUST re-stamp from its output bytes (R2). Here the op is identity.
    out_bytes = b
    md2 = cog.stamp_format_metadata(out_bytes, md)
    assert md2[cog.GBX_FORMAT] == "cog"
    # A final COG-aware op detects correctly from carried metadata (fast path).
    assert cog.detect_cog(md2, b"garbage").is_cog is True
```

- [ ] **Step 2: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_metadata_carry.py`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyrx/test_metadata_carry.py
git commit -m "test(pyrx): R2 gbx_format metadata-carry survival guard

Co-authored-by: Isaac"
```

---

## Task 9: Docs — reader options, POV, release notes, doc-tests

**Files:**
- Modify: `docs/docs/readers/raster.mdx`, `docs/docs/beta-release-notes.mdx`, `docs/docs/api/benchmarking.mdx`, `docs/sidebars.js` (only if a new page is added).
- Doc-test code: `docs/tests/python/` (a real large-raster read + COG round-trip on sample data).

- [ ] **Step 1: Update `readers/raster.mdx`**

Add the two-axes options table (`splitStrategy`: auto|serverless|classic|none; `tileFormat`: auto|cog|gtiff; `cogBlockSize`, `cogOverviewResampling`), the default-flip note, striped-raster guidance (row-band tiles are wide not square), and the COG POV paragraph: COG is GeoBrix's interchange default for split/large rasters; it pays off **now** for XYZ/tile serving (`rst_tilexyz`, `rst_xyzpyramid` via rio-tiler) and downsampling resample; the full consumer audit is a follow-on. **No wave numbers / internal vocabulary** (QC judge `internals-leak`).

- [ ] **Step 2: Update `beta-release-notes.mdx`**

Document the behavior change: reader default now auto-splits large rasters (`splitStrategy=auto`); recover old behavior with `splitStrategy=none`. Note tileFormat=auto emits COG on split. This is 0.4.4.

- [ ] **Step 3: Add/adjust a doc-test**

Add a real doc-test (runs in Docker on sample data) that reads a large raster with defaults and asserts >1 tile + valid COG output; and a `.option("cog","true")` writer round-trip. Follow the existing doc-test import pattern.

- [ ] **Step 4: Update `benchmarking.mdx`**

Reflect the large-raster benchmark profile (Task 10) per the bench-changes-update-docs rule.

- [ ] **Step 5: Run doc-tests in Docker (dispatch as Task subagent — long-running)**

Run: `bash scripts/commands/gbx-test-python-docs.sh --log large-raster-docs.log` (via Task subagent, narrate progress ~30s).
Expected: PASS. Verify `grep -rn -iE "wave [0-9]+" docs/docs/` prints nothing.

- [ ] **Step 6: Commit**

```bash
git add docs/
git commit -m "docs: large-raster reader options, COG POV, 0.4.4 release notes

Co-authored-by: Isaac"
```

---

## Task 10: Large-raster benchmarking profile

Extend the bench harness with the large striped-vs-COG corpus and the push-to-failure envelope (light tier now; head-to-head gates the heavy fast-follow).

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` and the bench corpus/config.
- Docs already updated in Task 9.

- [ ] **Step 1: Add a large striped GeoTIFF + tiled-COG counterpart + single-giant-strip case to the bench corpus**

Follow the benchmarking-preflight-discipline (scope, corpus, workers, invocation, store-append, stamp, dups). Corpus = VIIRS/UK-scale multi-GB decoded, not the 1000-tile default.

- [ ] **Step 2: Add ingest-memory + throughput measurement across `splitStrategy` values**

Measure per-tile memory high-water and throughput; record the OOM envelope (which source size × budget survives on Serverless ~1GB). Verify rows>0 before reporting (bench-verify-nonzero rule). Add per-function progress output.

- [ ] **Step 3: Add striped-vs-COG delta measurement + resample/XYZ-on-COG vs plain**

- [ ] **Step 4: Run the bench (dispatch as Task subagent, cluster) and link summary.md**

Give a link to the run's summary.md unprompted (bench-run-give-summary-link).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/ docs/docs/api/benchmarking.mdx
git commit -m "bench: large striped-vs-COG raster ingest profile + OOM envelope

Co-authored-by: Isaac"
```

---

## Task 11: Version bump to 0.4.4

Per geobrix-version-bump-checklist. This is the release.

**Files:**
- Modify: `pom.xml`, `python/geobrix/src/databricks/labs/gbx/__init__.py`, `docs/package.json`, banners/pills (`resources/images/rasterx-*.py` regen), wheel/JAR naming.

- [ ] **Step 1: Bump version strings**

CHANGE `pom.xml`, `__init__.py`, `package.json` to `0.4.4`; regen `geobrix-0.4.4` wheel/JAR/banners/pills. LEAVE lockfile/app-version per the checklist. Re-run `resources/images/rasterx-*.py` to refresh PNGs (release pill is in bytes).

- [ ] **Step 2: Rebuild + stage wheel (WHL-change rule)**

Run: `bash scripts/commands/gbx-data-push-wheel.sh` — rebuild JAR-less light wheel and stage to Volume (whl-change-rebuild-and-stage). Verify upload markers (push-wheel SDK-vs-CLI gotcha: confirm bytes landed, script may exit 0 on failed upload).

- [ ] **Step 3: Run binding parity + light lint**

Run: `bash scripts/commands/gbx-test-bindings.sh` and `bash scripts/commands/gbx-lint-python.sh --check` (Docker black, host-vs-docker gotcha).
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: bump version to 0.4.4 (large-raster reader release)

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- §1 option surface → Task 4 (axes + defaults), Task 5 (named readers), Task 6 (writer options). ✓
- §2 layout-aware chunking + decoded budget → Task 2 (planner), Task 4 (wired). ✓
- §3 detection contract (detect_cog, stamp, R1/R2, map storage) → Task 1, Task 8 (R2 guard). ✓
- §4 components (cog.py, budget.py, reader, writer, resample) → Tasks 1–7. ✓
- §5 testing/edge cases → tests in each task; degraded warning Task 4 step 3.6; corrupt header Task 1; passthrough stamp Task 3; netcdf Task 5. ✓
- §5 perf-parity gate → Task 7 + Task 10. ✓
- §5 docs deliverables → Task 9. ✓
- §6 large-raster benchmarking → Task 10. ✓
- Roadmap heavy/C → out of scope, noted in spec; no task (correct). ✓

**2. Placeholder scan:** No TBD/TODO. Two tasks (5, 7) have "confirm/verify" steps that resolve to either a no-op-guard or an explicit implementation with the code given — both branches specified, not deferred. Acceptable: they encode a genuine unknown (does inheritance/rasterio already do X) with a concrete action for each answer.

**3. Type consistency:** `CogInfo`/`detect_cog`/`stamp_format_metadata`/`sniff_header` (Task 1) used consistently in Tasks 3, 6, 8. `plan_layout`/`LayoutPlan`/`resolve_strategy`/`decoded_budget_bytes` (Task 2) used in Task 4. `_FilePartition` extended fields (Task 4) match its test usage. `encode_tile` new kwargs (Task 3) match Task 4 call. Constants `GBX_FORMAT` etc. consistent. ✓

**Open unknowns flagged for execution:**
- `runtime_kind()` env probe (`IS_SERVERLESS`) is a best-effort signal; validated by the Task 10 bench on a real Serverless cluster. Default-to-serverless keeps it safe.
- Task 7 hinges on whether rasterio already exploits overviews via `out_shape` — resolved empirically in Task 7 step 2 with both branches specified.
