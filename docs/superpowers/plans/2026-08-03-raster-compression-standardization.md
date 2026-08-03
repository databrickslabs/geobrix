# Raster Compression Standardization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One consistent, efficient materialize story — ZSTD + dtype-predictor baseline at every raster-bytes site (both tiers), a size-adaptive `auto` default level grounded by a real benchmark, a `compress` writer-option surface, and a user-facing Materialized Compression doc page.

**Architecture:** A single light-tier authority `pyrx.core.compression.creation_opts(dtype, decoded_bytes=None, compress="auto", level=None, predictor=None) -> dict` returns rasterio creation-options; every light write site routes profile-building through it. The heavy `OperatorOptions.appendOptions` is moved to the same ZSTD+predictor/auto standard and the two bypass sites routed through it. `auto` picks the ZSTD level from a decoded-size estimate using a table grounded by a powers-of-2-to-1GB benchmark (also published on the doc page). Both tiers on ZSTD so the bit-parity gate holds.

**Tech Stack:** Python 3.12 / rasterio 1.5 / GDAL 3.12 (light), Scala/GDAL-JNI (heavy), PySpark. Tests: pytest (`.venv-pyrx`, `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` set); heavy Scala tests in Docker; benchmark via the bench harness.

## Global Constraints

- **Baseline codec = ZSTD + dtype-predictor** (predictor: 3 float32/64, 2 int16/uint16/int32/uint32, 1 uint8/int8). Everywhere raster bytes are produced, both tiers.
- **`auto` is the default** `compress` value for row-data writes; level from decoded-size estimate (`count*width*height*itemsize`), no trial encode. Thresholds come from Task 1's benchmark — NOT the provisional guesses.
- **Size-adaptive is a CORRECTNESS requirement:** high level on a large payload risks the Serverless worker ~1 GB ceiling (OOM) and `driverMode` ~1 GB/min write cancellation. `auto` MUST drop the level as decoded size grows.
- **Central authority, no drift:** all light write sites route through `pyrx.core.compression.creation_opts`; heavy through `OperatorOptions.appendOptions`. No site hardcodes compression independently (except documented measurement-only exemptions).
- **Measurement-only encodes are exempt** (`tiling._encoded_size_bytes` and any throwaway encode) — stated reason, no compression policy.
- **Writer surface:** `compress="auto"|"zstd"|"deflate"|"lzw"|"none"`; when not `auto`, optional `compressLevel`+`predictor`; `auto`+explicit level/predictor → warn. `cogCompression` kept as documented alias→`compress`.
- **Parity:** both tiers ZSTD so the bit-parity gate on the gridded fixture holds; verify heavy JNI ZSTD early; re-baseline fixture; fall back to decoded-pixel parity only if byte-equality across the two GDAL builds proves brittle.
- **pyrx never uses** `spark.conf.set`/`_jvm`/`.rdd`. No new SQL-registered function names. No v2 tile-struct schema change.
- **Docs voice:** no wave/inc N; present-tense; new page wired into `sidebars.js`.
- **Tests execute real code with real assertions** on real rasters; assert actual `Compression.zstd` + predictor tag on output bytes. No mocking rasterio/GDAL.

---

## Task 1: Grounding benchmark (powers-of-2 to 1 GB) — sets the `auto` table AND the doc evidence

**Files:**
- Create/extend: `python/geobrix/src/databricks/labs/gbx/bench/compression_sweep.py` (or extend `bench/synth.py`/`readers.py`)
- Output: `.superpowers/sdd/compression-benchmark.md` (results table + chosen thresholds)

**Interfaces:**
- Produces: the decoded-size→ZSTD-level breakpoints consumed by Task 2's `creation_opts`, and the codec×size×dtype results table rendered on the doc page (Task 8).

- [ ] **Step 1: Write the sweep harness**

A script that, for each dtype in {float32, uint16, int16, uint8} and each decoded size in powers of two {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} MiB:
- generates a synthetic tile of that decoded size (realistic, not all-constant — use a gradient + noise so compression is non-trivial; reuse `bench/synth.py` patterns),
- encodes it with each config: `none`; `lzw`+pred; `deflate`(zlevel=6)+pred; `zstd` at levels {3,6,9,12,16,19,22}+pred; and `zstd` L9 **without** predictor (one per dtype, to show predictor effect),
- measures: encoded bytes (→ ratio), write wall-time, **peak RSS during write** (`tracemalloc` or `resource.getrusage`/`psutil` — RSS is the OOM signal), read wall-time (full decode).

Guard memory: skip the biggest sizes locally if they'd exceed the dev box; mark which sizes need the Serverless confirm.

- [ ] **Step 2: Run the sweep**

Run locally for sizes up to where the dev box is safe (≥256 MiB may need care). For the large sizes that drive the OOM thresholds, run on Serverless via the notebook runner if local RSS is unrepresentative (controller-paced). Capture the full table.

- [ ] **Step 3: Derive the `auto` ladder**

From the table, choose the decoded-size breakpoints + ZSTD level per band, where the rule is: pick the highest level whose write-time and peak-RSS stay in the "flat/linear" regime for that size (before the exponential climb). Write the chosen ladder (e.g. `[(64MiB, 19), (256MiB, 12), (1GiB, 6), (inf, 3)]` — ACTUAL values from data) into `.superpowers/sdd/compression-benchmark.md` with the table and a one-paragraph rationale.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/compression_sweep.py
git commit -m "bench: powers-of-2 compression sweep (codecs + zstd levels) to ground auto

Co-authored-by: Isaac"
```
(The `.superpowers/` results doc is gitignored scratch; it feeds Task 2 + Task 8.)

---

## Task 2: `pyrx.core.compression` authority

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/compression.py`
- Test: `python/geobrix/test/pyrx/test_compression.py`

**Interfaces:**
- Consumes: the `auto` ladder from Task 1.
- Produces: `creation_opts(dtype, decoded_bytes=None, compress="auto", level=None, predictor=None) -> dict[str,str]`; `predictor_for(dtype) -> int`; `auto_level(decoded_bytes) -> int`; module constants `DEFAULT_COMPRESS="auto"`, `_AUTO_LADDER`. Tasks 3-7 consume these.

- [ ] **Step 1: Write failing tests**

```python
import pytest
from databricks.labs.gbx.pyrx.core import compression as C


def test_predictor_for_dtype():
    assert C.predictor_for("float32") == 3
    assert C.predictor_for("float64") == 3
    assert C.predictor_for("int16") == 2
    assert C.predictor_for("uint16") == 2
    assert C.predictor_for("uint8") == 1
    assert C.predictor_for("int8") == 1


def test_auto_level_scales_down_with_size():
    small = C.auto_level(1 * 1024**2)     # 1 MiB
    mid = C.auto_level(200 * 1024**2)     # 200 MiB
    big = C.auto_level(2 * 1024**3)       # 2 GiB
    assert small > mid > big              # monotonic non-increasing (strict across bands)
    assert 1 <= big <= 6                  # large payloads stay low (OOM guard)


def test_creation_opts_auto_zstd_with_predictor():
    o = C.creation_opts("float32", decoded_bytes=1 * 1024**2, compress="auto")
    assert o["compress"] == "zstd"
    assert o["predictor"] == "3"
    assert int(o["zstd_level"]) == C.auto_level(1 * 1024**2)


def test_creation_opts_explicit_codec_and_level():
    o = C.creation_opts("int16", compress="deflate", level=9)
    assert o["compress"] == "deflate"
    assert o["zlevel"] == "9"
    assert o["predictor"] == "2"


def test_creation_opts_explicit_predictor_override():
    o = C.creation_opts("float32", compress="zstd", level=9, predictor=1)
    assert o["predictor"] == "1"


def test_creation_opts_none():
    o = C.creation_opts("uint8", compress="none")
    assert o.get("compress") in (None, "none")  # no compression
    # a 'none' profile must not carry zstd_level/zlevel/predictor
    assert "zstd_level" not in o and "zlevel" not in o


def test_auto_plus_explicit_level_warns():
    with pytest.warns(UserWarning, match="auto"):
        C.creation_opts("int16", decoded_bytes=1024, compress="auto", level=22)


def test_auto_without_decoded_bytes_uses_balanced_default():
    o = C.creation_opts("int16", decoded_bytes=None, compress="auto")
    assert o["compress"] == "zstd"
    assert int(o["zstd_level"]) == C._AUTO_DEFAULT_LEVEL
```

- [ ] **Step 2: Run — verify RED**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_compression.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement `compression.py`**

Generalize the logic that already exists in `ds/_write.py:_creation_opts`. Skeleton (fill `_AUTO_LADDER` from Task 1):

```python
"""Single source of truth for raster creation-options (compression + predictor).

Every light-tier write site routes profile-building through creation_opts so the
materialize story is consistent. Mirrors heavy OperatorOptions.appendOptions.
"""
import warnings

_FLOAT = {"float32", "float64"}
_SMALL_INT = {"uint8", "int8"}

# Grounded by bench/compression_sweep.py (see .superpowers/sdd/compression-benchmark.md).
# (decoded_bytes_ceiling, zstd_level) ascending; last entry ceiling = inf.
_AUTO_LADDER = [
    (64 * 1024**2, 19),
    (256 * 1024**2, 12),
    (1024**3, 6),
    (float("inf"), 3),
]
_AUTO_DEFAULT_LEVEL = 9  # used when decoded size is unknown (balanced)
DEFAULT_COMPRESS = "auto"


def predictor_for(dtype: str) -> int:
    if dtype in _FLOAT:
        return 3
    if dtype in _SMALL_INT:
        return 1
    return 2  # int16/uint16/int32/uint32


def auto_level(decoded_bytes) -> int:
    if decoded_bytes is None:
        return _AUTO_DEFAULT_LEVEL
    for ceiling, level in _AUTO_LADDER:
        if decoded_bytes <= ceiling:
            return level
    return _AUTO_LADDER[-1][1]


def creation_opts(dtype, decoded_bytes=None, compress="auto", level=None, predictor=None):
    """Return rasterio creation-options for a raster write.

    compress: 'auto' (size-adaptive ZSTD+predictor) | 'zstd' | 'deflate' | 'lzw' | 'none'.
    When compress != 'auto', level/predictor refine it. When 'auto', explicit
    level/predictor are ignored with a warning (auto owns them).
    """
    dtype = str(dtype)
    pred = predictor if predictor is not None else predictor_for(dtype)

    if compress == "auto":
        if level is not None or predictor is not None:
            warnings.warn(
                "creation_opts: compress='auto' ignores explicit level/predictor "
                "(auto derives them from tile size + dtype).",
                UserWarning, stacklevel=2,
            )
        return {"compress": "zstd", "zstd_level": str(auto_level(decoded_bytes)),
                "predictor": str(predictor_for(dtype))}

    c = compress.lower()
    if c in ("none", "raw", None):
        return {}  # no compression keys
    if c == "zstd":
        opts = {"compress": "zstd", "zstd_level": str(level if level is not None else _AUTO_DEFAULT_LEVEL),
                "predictor": str(pred)}
    elif c == "deflate":
        opts = {"compress": "deflate", "zlevel": str(level if level is not None else 6),
                "predictor": str(pred)}
    elif c == "lzw":
        opts = {"compress": "lzw", "predictor": str(pred)}
    else:
        opts = {"compress": c}  # GDAL-supported name, no predictor assumption
    return opts
```

- [ ] **Step 4: Run — verify GREEN**

Run the test file. Expected: PASS. Adjust `_AUTO_LADDER`/`_AUTO_DEFAULT_LEVEL` to Task 1's actual values; update the `test_auto_level_scales_down_with_size` bounds if the ladder differs.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/compression.py python/geobrix/test/pyrx/test_compression.py
git commit -m "feat(pyrx): compression authority (creation_opts, auto size-adaptive ZSTD+predictor)

Co-authored-by: Isaac"
```

---

## Task 3: Route the light HOT PATH through the authority

**Files:**
- Modify: `ds/_encode.py` (`encode_tile` GTiff + COG paths), `pyrx/core/open_tile.py` (`_window_dataset_bytes`, `_warp_window_bytes`, `_empty_dataset_bytes`, `materialize_to_bytes`)
- Test: `python/geobrix/test/pyrx/test_compression_sites.py` (create)

**Interfaces:** consumes `creation_opts` (Task 2).

- [ ] **Step 1: Failing tests — hot-path output is ZSTD+predictor**

```python
import glob, numpy as np, rasterio
from rasterio.enums import Compression
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _tif(tmp, dt="int16", sz=64):
    import rasterio
    from rasterio.transform import from_bounds
    p = str(tmp / f"{dt}.tif")
    a = (np.random.rand(sz, sz) * 1000).astype(dt)
    with rasterio.open(p, "w", driver="GTiff", height=sz, width=sz, count=1,
                       dtype=dt, crs="EPSG:4326",
                       transform=from_bounds(-1, -1, 1, 1, sz, sz)) as d:
        d.write(a, 1)
    return p, sz


def test_materialize_to_bytes_is_zstd_predictor(tmp_path):
    p, sz = _tif(tmp_path, "int16")
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    mat = ot.materialize_to_bytes(vt)
    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.compression == Compression.zstd
        assert ds.tags(ns="IMAGE_STRUCTURE").get("PREDICTOR") in ("2", None) or \
               ds.profile.get("predictor") in (2, "2")  # predictor tag surfaced by GDAL
```

(Note: rasterio surfaces predictor inconsistently; assert `ds.compression == Compression.zstd` as the hard check and predictor via `ds.profile` where available. The reviewer can tighten.)

- [ ] **Step 2: Run — RED** (currently DEFLATE/inherited).

- [ ] **Step 3: Route each hot site**

For each site, compute `decoded_bytes = count * win_w * win_h * np.dtype(dtype).itemsize` and merge `creation_opts(dtype, decoded_bytes, compress=<threaded or "auto">)` into the profile. E.g. `_window_dataset_bytes` and `materialize_to_bytes` gain the creation-opts merge; `encode_tile` GTiff path replaces its `compress=compression.lower()` with the authority (threading the reader's `compress` option, default `auto`). `_empty_dataset_bytes` (1x1) → `auto` (tiny → high level, negligible).

- [ ] **Step 4: Run — GREEN** + confirm virtual-tile behavior unbroken: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_pending_instructions.py python/geobrix/test/ds/test_raster_virtual.py -q` (ignore netCDF4-collection modules).

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(pyrx): route reader/materialize hot path through compression authority

Co-authored-by: Isaac"
```

---

## Task 4: Route remaining light ops through the authority

**Files:**
- Modify: `pyrx/core/edit.py` (`_write` + pass dtype/size), `agg.py`, `cellraster.py`, `tiling.py` (EXEMPT `_encoded_size_bytes` with a comment), `resample.py`, `warp.py`, `analysis.py` (proximity/viewshed/cog_convert/cog_convert_file), `ops.py`; ancillary `stac/_download.py`, `ds/_xyz_mosaic.py`, `vizx/_simplify.py` (route or exempt-with-reason).
- Test: extend `test_compression_sites.py`

**Interfaces:** consumes `creation_opts`.

- [ ] **Step 1: Failing tests** — a representative assertion per site-family that output is ZSTD+predictor (edit result, agg merge, tiling split tile, warp, resample, analysis proximity). Use real rasters.

- [ ] **Step 2: Run — RED.**

- [ ] **Step 3: Route each site.** `_write(profile, data)` gains dtype+decoded_bytes (or a `compress` arg) and merges `creation_opts`. COG paths (`cog_convert`, `cog_convert_file`) pass their `compress` through the authority for the codec/level/predictor while keeping COG `blocksize`/`overview_resampling`. `_encoded_size_bytes` gets a `# EXEMPT: measurement-only encode, no compression policy` comment (or switch it to compute decoded size directly). vizx `_simplify` LZW-for-overviews: route through `creation_opts(compress="lzw")` or exempt-with-reason.

- [ ] **Step 4: Run — GREEN** + affected-suite regression (pyrx + ds, ignore netCDF4/JAR-gated).

- [ ] **Step 5: Commit.**

---

## Task 5: Writer option surface (`compress` / `compressLevel` / `predictor` + `cogCompression` alias)

**Files:**
- Modify: `ds/raster.py` (writer option parse), `ds/writer.py`, `ds/cog_writer.py`, `pyrx/core/preparer.py`
- Test: `python/geobrix/test/ds/test_compress_option.py` (create)

**Interfaces:** the writer reads `compress`/`compressLevel`/`predictor` and threads them into the authority; `cogCompression` maps to `compress`.

- [ ] **Step 1: Failing tests** — write a tile DF with `.option("compress","auto")` → output ZSTD; `.option("compress","deflate").option("compressLevel","9")` → DEFLATE zlevel 9; `.option("compress","none")` → uncompressed; `.option("cogCompression","deflate")` (alias) → DEFLATE; `.option("compress","auto").option("compressLevel","22")` → warns. Assert on the written file's `rasterio.open(...).compression`.

- [ ] **Step 2: Run — RED.**

- [ ] **Step 3: Implement** the option parse (default `compress="auto"`), alias `cogCompression`→`compress` (with a one-time deprecation note in the docstring), thread through to the authority + the COG conversion paths. `prepare_cogs`/`prepare_cog` gain `compress`/`compress_level`/`predictor` kwargs (keep `compression=` as alias).

- [ ] **Step 4: Run — GREEN.**

- [ ] **Step 5: Commit.**

---

## Task 6: Heavy tier — ZSTD+auto standard + route the two bypass sites

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operator/OperatorOptions.scala`, `.../expressions/analysis/RST_CogConvert.scala`, `.../operations/GDALRasterize.scala`
- Test: heavy Scala test(s) for the two bypass sites; run in Docker.

**Interfaces:** heavy write standard matches light (ZSTD+predictor, size-adaptive where a size is knowable).

- [ ] **Step 1: Verify heavy JNI ZSTD support (BLOCKING GATE)**

In the geobrix-dev container, confirm the GDAL JNI build encodes ZSTD GeoTIFF (a tiny Scala/GDAL snippet or existing test writing `COMPRESS=ZSTD` and reading it back). If ZSTD is ABSENT, STOP and report — the shared-ZSTD-baseline design is blocked; fall back per the spec (DEFLATE+predictor shared baseline). Record the result in the ledger.

- [ ] **Step 2: Move `OperatorOptions` default to ZSTD**

Change `writeOptions.getOrElse("compression", "DEFLATE")` → `"ZSTD"` as the default, keeping predictor derivation. Size-adaptive level: if a decoded-size hint is available in `writeOptions` (thread one from the tile dims `ds.GetRasterXSize*YSize*bands*itemsize`), map it to a level with the SAME ladder as light (mirror `_AUTO_LADDER` — keep the two ladders documented as intentionally identical); else default 9. Keep DEFLATE/LZW branches for explicit `compression=`.

- [ ] **Step 3: Route `RST_CogConvert` + `GDALRasterize` through `OperatorOptions`**

`RST_CogConvert` builds `-of COG -co COMPRESS=... -co BLOCKSIZE=...` directly — route it through `appendOptions` so it gains the predictor + standard. `GDALRasterize` hardcodes `COMPRESS=DEFLATE TILED=YES` at `Create` — add the predictor (and align to the standard). Preserve each op's required flags (COG blocksize, rasterize output type).

- [ ] **Step 4: Bump the Scala count-assert if any** (per the BenchDispatch memory) — grep for hardcoded counts touching these files; adjust if needed.

- [ ] **Step 5: Run heavy tests in Docker** (dispatch, don't inline). Confirm the two sites + OperatorOptions ZSTD path green.

- [ ] **Step 6: Commit.**

---

## Task 7: Cross-tier parity re-baseline

**Files:**
- Modify: the parity fixture / bench parity gate (`bench/cluster.py` raster parity ~line 704) if the expected bytes are stored; `docs/docs/api/benchmarking.mdx` if the gate description changes.

- [ ] **Step 1: Determine the gate's real assertion** — read the raster parity path in `bench/cluster.py`; confirm whether it stores expected bytes or compares light-vs-heavy at runtime.

- [ ] **Step 2: Re-baseline** — with both tiers on ZSTD+predictor/auto, regenerate the fixture's expected bytes (or, if runtime light-vs-heavy compare, confirm they now match). Run the parity check.

- [ ] **Step 3: If byte-equality across the two GDAL builds is brittle** (different ZSTD lib versions can differ byte-wise even at the same level), switch the raster gate to **decoded-pixel parity** (open both, `np.array_equal` on pixels + assert matching georeference/nodata/dtype) — the spec-sanctioned fallback. Update `benchmarking.mdx` to describe the gate honestly.

- [ ] **Step 4: Commit.**

---

## Task 8: Docs — Materialized Compression page + option tables + evidence

**Files:**
- Create: `docs/docs/api/materialized-compression.mdx`; modify `docs/sidebars.js`
- Modify: `docs/docs/writers/cog.mdx`, `docs/docs/writers/geotiff*.mdx`, `docs/docs/api/large-rasters.mdx` (option tables + cross-link)

- [ ] **Step 1: Write the page** — approach (ZSTD+predictor baseline; `auto` size-adaptive, small-squeeze/large-light with the memory-limit link); the `compress`/`compressLevel`/`predictor` control surface + when to use each (`deflate` for off-cluster hand-off; fixed high level for write-once catalogs); the portability note (ZSTD needs a ZSTD-enabled GDAL off-cluster); and the **benchmark evidence table** from Task 1 (popular codecs vs ZSTD levels × sizes × dtypes → ratio/write/read/RSS). End-user voice, no internal vocab. Keep it small.

- [ ] **Step 2: Wire into `sidebars.js`** (near tile-structure / large-rasters).

- [ ] **Step 3: Option tables** — add `compress`/`compressLevel`/`predictor` rows to the writer pages; note `cogCompression` is a deprecated alias; cross-link the new page from large-rasters "Writing COGs".

- [ ] **Step 4: Voice grep + docs build** — `grep -rn -iE "wave [0-9]+|inc [0-9]+" docs/docs/` empty; Docker `gbx:docs:static-build` compiled + no broken links.

- [ ] **Step 5: Commit.**

---

## Task 9: End-to-end validation

- [ ] **Step 1: Affected-suite regression** (local): pyrx + ds compression/site tests + pending-instruction + reader-virtual green.
- [ ] **Step 2: Real reader → materialize check:** read a real raster with `gtiff_gbx` (virtual default), materialize, assert the bytes are ZSTD+predictor and `auto` picked a size-appropriate level; a large synthetic tile picks a lower level than a small one.
- [ ] **Step 3: (Controller-paced) Serverless smoke** — a forced-compute example run (or the compression sweep's large sizes) confirming no OOM/cancellation regression at the large-payload end. Controller decides if a cluster run is warranted.
- [ ] **Step 4: Ledger + done** — feature ready for final whole-branch review.

---

## Self-Review

**Spec coverage:** T1 benchmark (spec §3 + §5b evidence). T2 authority (§4). T3-4 light site routing (§4 + exemptions). T5 writer surface (§5). T6 heavy + bypass sites + parity codec (§4, §6). T7 parity re-baseline (§6). T8 docs incl. the dedicated page (§5b). T9 validation (§Testing). CRS not in any task (Non-Goal). ✓

**Placeholder scan:** `_AUTO_LADDER` values are explicitly "from Task 1" (benchmark output), not shipped guesses — the dependency is sequenced (T1 before T2). The predictor-tag test assertion is flagged as rasterio-version-dependent for the reviewer to tighten. No TBD. ✓

**Type consistency:** `creation_opts(dtype, decoded_bytes, compress, level, predictor)` signature consistent across T2-T5; `predictor_for`/`auto_level`/`_AUTO_LADDER` defined T2, reused T3-4 and mirrored in heavy T6 (documented as intentionally-identical ladders). Verified against current `ds/_write.py:_creation_opts` and `OperatorOptions.scala` (both already derive predictor from float-ness — the authority generalizes them). ✓

**Open risks for the review loop:** (1) predictor surfacing in rasterio metadata is inconsistent — tests assert `Compression.zstd` hard + predictor best-effort. (2) heavy JNI ZSTD support is a T6 gate that can block the shared-baseline design — fallback stated. (3) byte-parity across two GDAL/ZSTD builds may force the decoded-pixel gate (T7). (4) T1's large sizes may need Serverless for representative RSS.
