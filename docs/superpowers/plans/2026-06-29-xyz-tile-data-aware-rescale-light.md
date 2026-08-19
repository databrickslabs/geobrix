# XYZ Tile Data-Aware Rescale — Light Tier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the light-tier XYZ tilers (`rst_tilexyz`, `rst_xyzpyramid`) recover contrast for non-8-bit imagery by default, via a new `rescale` parameter (default `"auto"`), so the washed-out Helios NB02 raster basemap renders correctly with zero customer config.

**Architecture:** Add `rescale` resolution to `pyrx/core/xyz.py`: a helper computes the effective per-band `(min,max)` once per source (uint8 → pass-through; non-8-bit → whole-dataset min/max; `"none"` → today's behavior; explicit `(min,max)` → use as-is) and threads it into rio-tiler's `img.render(in_range=...)`. The scalar UDF and pyramid UDTF gain a trailing `rescale` arg with the same default. No Spark needed to test the core logic.

**Tech Stack:** Python 3.12, rasterio, rio-tiler 9.x (`ImageData.render(in_range=...)`), morecantile, PySpark UDF/UDTF, pytest in `.venv-pyrx`.

## Global Constraints

- Cross-language naming: the new parameter is `rescale` in Python (Scala/SQL parity comes in the Phase 2 heavy plan). Keep `_geom` / canonical-name rules — N/A here (no new function).
- No aliases; one canonical parameter name `rescale`.
- This is the **light tier only**. Both tiers currently MATCH (full-dtype-range). Landing light first KNOWINGLY diverges from heavy until the Phase 2 heavy plan reconciles parity. This is an accepted, temporary divergence per the approved sequencing — do NOT attempt heavy/Scala changes in this plan.
- Tiling parity is pixel/value-level, not byte-level (documented convention).
- Run all tests in the project venv: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest ...` (NOT Docker — these are pure-Python core/UDF tests).
- rio-tiler is imported LAZILY inside functions in `core/xyz.py` (Serverless import constraint) — keep new rio-tiler usage lazy too.
- `rescale` accepted values: the string `"auto"` (default), the string `"none"`, or a 2-element `(min, max)` numeric tuple/list. Anything else raises `ValueError` from `_validate`.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- Test: `python/geobrix/test/pyrx/test_core_xyz.py` (extend)

---

### Task 1: `rescale` validation + range-resolution helper in `core/xyz.py`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py`
- Test: `python/geobrix/test/pyrx/test_core_xyz.py`

**Interfaces:**
- Produces:
  - `_validate_rescale(rescale) -> "auto" | "none" | (float, float)` — normalizes/validates the rescale arg. `None` is treated as `"auto"`.
  - `_resolve_in_range(ds, rescale) -> list[tuple[float,float]] | None` — returns the per-band `in_range` list to pass to rio-tiler, or `None` when no rescale should be applied (uint8 pass-through, or `"none"`). For `"auto"` on a non-uint8 dataset: per-band whole-dataset `(min,max)` via `ds.statistics(b, approx=False)` (rasterio) → `[(min_b, max_b), ...]` for each band. For an explicit `(min,max)` tuple: the SAME pair repeated for every band. Constant band (min==max) → widen to `(min, min+1)` to avoid a zero-width range.

- [ ] **Step 1: Write the failing tests**

Add to `python/geobrix/test/pyrx/test_core_xyz.py` (it already imports `xyz`, `np`, `MemoryFile`, `from_origin`, and defines `_make_rgb`, `_open`). Add a uint16 narrow-range fixture and the helper tests:

```python
def _make_uint16_narrow(width=64, height=64, epsg=4326, lo=8000, hi=12000):
    """Single-band uint16 raster with values spread across [lo, hi] (narrow band)."""
    transform = from_origin(10.0, 50.0, 0.03125, 0.03125)
    profile = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="uint16",
        crs=f"EPSG:{epsg}", transform=transform,
    )
    ramp = np.linspace(lo, hi, width * height).astype("uint16").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(ramp, 1)
        return mf.read()


def test_validate_rescale_normalizes():
    assert xyz._validate_rescale(None) == "auto"
    assert xyz._validate_rescale("auto") == "auto"
    assert xyz._validate_rescale("AUTO") == "auto"
    assert xyz._validate_rescale("none") == "none"
    assert xyz._validate_rescale((10, 200)) == (10.0, 200.0)
    assert xyz._validate_rescale([10, 200]) == (10.0, 200.0)


def test_validate_rescale_rejects_bad():
    with pytest.raises(ValueError):
        xyz._validate_rescale("stretch")
    with pytest.raises(ValueError):
        xyz._validate_rescale((1, 2, 3))
    with pytest.raises(ValueError):
        xyz._validate_rescale((200, 10))  # min must be < max


def test_resolve_in_range_uint8_passthrough_is_none():
    mf, ds = _open(_make_rgb())  # uint8
    try:
        assert xyz._resolve_in_range(ds, "auto") is None
    finally:
        ds.close(); mf.close()


def test_resolve_in_range_none_is_none():
    mf, ds = _open(_make_uint16_narrow())
    try:
        assert xyz._resolve_in_range(ds, "none") is None
    finally:
        ds.close(); mf.close()


def test_resolve_in_range_auto_uint16_uses_data_minmax():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        rng = xyz._resolve_in_range(ds, "auto")
        assert rng is not None and len(rng) == 1
        lo, hi = rng[0]
        # Whole-dataset min/max ~ [8000, 12000], NOT the dtype range [0, 65535].
        assert 7900 <= lo <= 8100
        assert 11900 <= hi <= 12100
    finally:
        ds.close(); mf.close()


def test_resolve_in_range_explicit_pair_repeats_per_band():
    mf, ds = _open(_make_rgb())  # 3-band uint8
    try:
        rng = xyz._resolve_in_range(ds, (10, 200))
        assert rng == [(10.0, 200.0), (10.0, 200.0), (10.0, 200.0)]
    finally:
        ds.close(); mf.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -k "rescale or resolve_in_range" -v`
Expected: FAIL with `AttributeError: module ... has no attribute '_validate_rescale'` / `_resolve_in_range`.

- [ ] **Step 3: Implement the helpers in `core/xyz.py`**

Add after `_validate` (around line 79), before `render_tile`:

```python
def _validate_rescale(rescale):
    """Normalize/validate the rescale arg.

    Returns the string ``"auto"`` / ``"none"``, or a normalized ``(min, max)``
    float tuple. ``None`` -> ``"auto"``. Raises ValueError on anything else.
    """
    if rescale is None:
        return "auto"
    if isinstance(rescale, str):
        r = rescale.lower()
        if r in ("auto", "none"):
            return r
        raise ValueError(
            f"rst_tilexyz: rescale must be 'auto', 'none', or a (min, max) pair; "
            f"got string '{rescale}'"
        )
    # Sequence -> (min, max)
    try:
        lo, hi = rescale  # unpacks exactly two; else ValueError
    except (TypeError, ValueError):
        raise ValueError(
            f"rst_tilexyz: rescale tuple must have exactly two numbers (min, max); "
            f"got {rescale!r}"
        )
    lo, hi = float(lo), float(hi)
    if not (lo < hi):
        raise ValueError(
            f"rst_tilexyz: rescale (min, max) must have min < max; got ({lo}, {hi})"
        )
    return (lo, hi)


def _resolve_in_range(ds, rescale):
    """Resolve the per-band ``in_range`` for rio-tiler render, or None for no rescale.

    - ``"none"`` -> None (today's full-dtype-range behavior).
    - explicit ``(min, max)`` -> that pair repeated for every band.
    - ``"auto"``:
        * uint8 source -> None (already display-ready; pass through unchanged).
        * non-uint8 -> per-band whole-dataset (min, max) via rasterio statistics.
          A constant band (min == max) is widened to (min, min + 1).
    """
    mode = _validate_rescale(rescale)
    if mode == "none":
        return None
    nbands = ds.count
    if isinstance(mode, tuple):
        return [mode] * nbands
    # mode == "auto"
    if np.dtype(ds.dtypes[0]) == np.uint8:
        return None
    out = []
    for b in range(1, nbands + 1):
        stats = ds.statistics(b, approx=False)
        lo, hi = float(stats.min), float(stats.max)
        if not (lo < hi):
            hi = lo + 1.0
        out.append((lo, hi))
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -k "rescale or resolve_in_range" -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py python/geobrix/test/pyrx/test_core_xyz.py
git commit -m "feat(pyrx): add rescale resolution helpers for XYZ tiling

_validate_rescale + _resolve_in_range resolve the new rescale arg to a
per-band rio-tiler in_range: uint8 pass-through, non-8-bit auto whole-
dataset min/max, none = today, explicit (min,max). Core logic only.

Co-authored-by: Isaac"
```

---

### Task 2: Thread `rescale` through `render_tile`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py:82-106` (`render_tile`)
- Test: `python/geobrix/test/pyrx/test_core_xyz.py`

**Interfaces:**
- Consumes: `_resolve_in_range` (Task 1).
- Produces: `render_tile(ds, z, x, y, fmt="PNG", size=256, resampling="bilinear", rescale="auto")` — same return (image bytes / transparent PNG), now applying `in_range` to `img.render` when resolved. Accepts an optional precomputed `in_range=` keyword (used by Task 3's pyramid to avoid recomputing stats per tile); when `in_range` is passed it OVERRIDES `rescale` resolution.

- [ ] **Step 1: Write the failing tests**

Add to `test_core_xyz.py`. These decode the rendered PNG and assert the auto path spans the 8-bit range while `none` stays crushed:

```python
def _decode_png_rgb(png_bytes):
    import io
    from PIL import Image
    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    a = np.asarray(img)
    # data pixels = alpha > 0
    mask = a[..., 3] > 0
    rgb = a[..., :3][mask]
    return rgb  # (N, 3) uint8 of covered pixels


def _center_tile_zxy(ds):
    # Pick a zoom/tile that intersects the fixture extent (lon~10-12, lat~48-50).
    import morecantile
    tms = morecantile.tms.get("WebMercatorQuad")
    west, south, east, north = xyz._wgs84_bounds(ds)
    t = next(iter(tms.tiles(west, south, east, north, [8])))
    return t.z, t.x, t.y


def test_render_tile_auto_uint16_spans_full_range():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        png = xyz.render_tile(ds, z, x, y, rescale="auto")
        rgb = _decode_png_rgb(png)
        assert rgb.size > 0
        # Auto rescale maps [8000,12000] -> ~full 8-bit; expect a wide spread,
        # NOT crushed into the ~[31,46] full-dtype-range band.
        assert int(rgb.max()) - int(rgb.min()) > 100
    finally:
        ds.close(); mf.close()


def test_render_tile_none_uint16_stays_crushed():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        png = xyz.render_tile(ds, z, x, y, rescale="none")
        rgb = _decode_png_rgb(png)
        assert rgb.size > 0
        # Full-dtype-range: 8000..12000 / 65535 * 255 -> ~[31, 46]; crushed.
        assert int(rgb.max()) < 80
    finally:
        ds.close(); mf.close()


def test_render_tile_uint8_auto_matches_none():
    mf, ds = _open(_make_rgb())
    try:
        z, x, y = _center_tile_zxy(ds)
        auto = xyz.render_tile(ds, z, x, y, rescale="auto")
        none = xyz.render_tile(ds, z, x, y, rescale="none")
        assert auto == none  # uint8 pass-through: byte-identical
    finally:
        ds.close(); mf.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -k "render_tile_auto or render_tile_none or uint8_auto" -v`
Expected: FAIL — `render_tile()` got an unexpected keyword argument `rescale`.

- [ ] **Step 3: Implement — add `rescale`/`in_range` to `render_tile`**

Replace the current `render_tile` body (lines 82-106) with:

```python
def render_tile(
    ds, z, x, y, fmt="PNG", size=256, resampling="bilinear",
    rescale="auto", in_range=None,
) -> bytes:
    """Render a single web-mercator (z, x, y) tile from open dataset ``ds``.

    Validates inputs (raises ValueError on bad format/size/resampling/rescale).
    Out-of-extent / empty tiles, or any hard render failure, return a transparent
    PNG of ``size`` x ``size`` (mirrors heavyweight: PNG regardless of ``fmt``).

    ``rescale`` controls 8-bit encoding contrast (see _resolve_in_range): "auto"
    (default) rescales non-8-bit rasters by whole-dataset min/max and passes uint8
    through unchanged; "none" keeps the raw full-dtype-range mapping; a (min, max)
    pair sets explicit bounds. ``in_range`` (internal) lets the pyramid path pass a
    precomputed per-band range so stats are read once, not per tile; when given it
    overrides ``rescale``.
    """
    from rio_tiler.errors import TileOutsideBounds  # lazy: see module-top note
    from rio_tiler.io import Reader

    fmt_u, s, resamp_name = _validate(fmt, size, resampling)
    if in_range is None:
        in_range = _resolve_in_range(ds, rescale)  # may raise ValueError on bad rescale
    try:
        with Reader(None, dataset=ds) as cog:
            img = cog.tile(
                int(x), int(y), int(z), tilesize=s, resampling_method=resamp_name
            )
            if in_range is not None:
                out = img.render(img_format=fmt_u, in_range=in_range)
            else:
                out = img.render(img_format=fmt_u)
        if not out:
            return transparent_png(s)
        return out
    except TileOutsideBounds:
        return transparent_png(s)
    except Exception:
        # Slippy-map servers need a non-null 200 body even on failure.
        return transparent_png(s)
```

Note: `_resolve_in_range` is called OUTSIDE the try/except so a bad `rescale` value raises ValueError (fail fast) rather than being swallowed into a transparent PNG.

- [ ] **Step 4: Run tests to verify they pass**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -v`
Expected: PASS (all existing xyz tests + the new render_tile tests). If an existing test calls `render_tile` positionally it still works (new args are trailing with defaults).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py python/geobrix/test/pyrx/test_core_xyz.py
git commit -m "feat(pyrx): apply rescale in_range in render_tile

render_tile resolves rescale to a per-band in_range and passes it to
rio-tiler's render; auto recovers contrast for non-8-bit rasters, uint8
passes through byte-identical, none keeps today's behavior. Accepts a
precomputed in_range for the pyramid path (stats read once).

Co-authored-by: Isaac"
```

---

### Task 3: Thread `rescale` through `iter_pyramid` / `pyramid` (stats read once)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py:141-175` (`iter_pyramid`, `pyramid`)
- Test: `python/geobrix/test/pyrx/test_core_xyz.py`

**Interfaces:**
- Consumes: `_resolve_in_range` (Task 1), `render_tile(..., in_range=)` (Task 2).
- Produces: `iter_pyramid(ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear", rescale="auto")` and `pyramid(...)` with the same trailing `rescale`. The pyramid resolves `in_range` ONCE before the tile loop and passes it to every `render_tile`, so all tiles share one mapping (no seams) and stats are read once.

- [ ] **Step 1: Write the failing test**

```python
def test_pyramid_shares_one_mapping_no_per_tile_stats(monkeypatch):
    """All pyramid tiles use ONE resolved in_range; stats resolved once, not per tile."""
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        calls = {"n": 0}
        real = xyz._resolve_in_range

        def _spy(dataset, rescale):
            calls["n"] += 1
            return real(dataset, rescale)

        monkeypatch.setattr(xyz, "_resolve_in_range", _spy)
        tiles = xyz.pyramid(ds, 6, 8, rescale="auto")
        assert len(tiles) >= 2  # multiple tiles across the range
        # Resolved exactly once for the whole pyramid (not once per tile).
        assert calls["n"] == 1
        # And the tiles are contrast-recovered (spot check one non-empty tile).
        nonempty = [t for t in tiles if t["bytes"]]
        assert nonempty
    finally:
        ds.close(); mf.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py::test_pyramid_shares_one_mapping_no_per_tile_stats -v`
Expected: FAIL — `pyramid()` got an unexpected keyword argument `rescale` (and/or `_resolve_in_range` called per tile).

- [ ] **Step 3: Implement — resolve once, pass `in_range` per tile**

Replace `iter_pyramid` (lines 141-163) and `pyramid` (lines 166-175):

```python
def iter_pyramid(
    ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear", rescale="auto"
):
    """Render every intersecting (z, x, y) tile across [min_z, max_z], streaming.

    Yields ``(z, x, y, bytes)`` tuples one tile at a time — never buffers the full
    pyramid (large-fan-out OOM guard). Validates zoom guards, the render args, the
    rescale arg, and the tile-count guard BEFORE rendering any tile. The rescale
    ``in_range`` is resolved ONCE so every tile shares one 8-bit mapping (no seams)
    and source statistics are read a single time.
    """
    lo, hi = _validate_zoom_range(min_z, max_z)
    # Validate render args up front (so bad format/size fails fast, not per-tile).
    _validate(fmt, size, resampling)
    in_range = _resolve_in_range(ds, rescale)  # once; also validates rescale
    west, south, east, north = _wgs84_bounds(ds)

    # Count guard first — never materialize a giant list to count.
    total = 0
    for z in range(lo, hi + 1):
        total += _zoom_tile_count(west, south, east, north, z)
        if total > MAX_TILE_COUNT:
            _raise_count(lo, hi)

    for z in range(lo, hi + 1):
        for t in _TMS.tiles(west, south, east, north, [z]):
            b = render_tile(
                ds, t.z, t.x, t.y, fmt, size, resampling, in_range=in_range
            )
            yield (t.z, t.x, t.y, b)


def pyramid(
    ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear", rescale="auto"
) -> list:
    """Render every intersecting (z, x, y) tile across [min_z, max_z].

    Returns a list of ``{"z","x","y","bytes"}`` dicts. List-materializing wrapper
    around :func:`iter_pyramid`.
    """
    return [
        {"z": z, "x": x, "y": y, "bytes": b}
        for z, x, y, b in iter_pyramid(
            ds, min_z, max_z, fmt, size, resampling, rescale
        )
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -v`
Expected: PASS (all xyz core tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/xyz.py python/geobrix/test/pyrx/test_core_xyz.py
git commit -m "feat(pyrx): thread rescale through pyramid, resolve in_range once

iter_pyramid/pyramid take rescale and resolve the per-band in_range a
single time, passing it to every render_tile so all tiles share one 8-bit
mapping (no tile-to-tile seams) and stats are read once.

Co-authored-by: Isaac"
```

---

### Task 4: Expose `rescale` on the public UDF/UDTF surface (`functions.py`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
  - `_tilexyz_udf` (lines 2487-2499)
  - `_RstXyzPyramidUDTF.eval` (lines 2521-2538)
  - `rst_tilexyz` public binding (lines 2541-2571)
  - `rst_xyzpyramid` public docstring (lines 2574-2609) — add `rescale` to Args
- Test: `python/geobrix/test/pyrx/test_core_xyz.py` (UDF-level via the registered function is in test_functions_spark.py; here we add a direct `_tilexyz_udf` call test that needs no SparkSession by passing a plain tile dict)

**Interfaces:**
- Consumes: `xyz.render_tile(..., rescale=)` (Task 2), `xyz.iter_pyramid(..., rescale=)` (Task 3).
- Produces:
  - `_tilexyz_udf(tile, z, x, y, format, size, resampling, rescale=None)` — trailing optional arg; `None` → `"auto"`.
  - `_RstXyzPyramidUDTF.eval(self, tile, min_z, max_z, format=None, size=None, resampling=None, rescale=None)`.
  - `rst_tilexyz(tile, z, x, y, format="PNG", size=256, resampling="bilinear", rescale="auto")`.
  - The SQL registrations pick up the new arity automatically (scalar UDF registered from the bare function; UDTF from the class).

- [ ] **Step 1: Write the failing test**

Add to `test_core_xyz.py` (it can build a tile dict from raster bytes the same way `_serde.open_tile` expects — but to avoid Spark/serde coupling, test the `_tilexyz_udf` path through a tiny tile dict). Place this test in `test_core_xyz.py`:

```python
def test_tilexyz_udf_accepts_rescale_and_recovers_contrast():
    import io
    from PIL import Image
    from databricks.labs.gbx.pyrx import functions as fns

    raster = _make_uint16_narrow(lo=8000, hi=12000)
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
    finally:
        ds.close(); mf.close()

    tile = {"raster": raster}
    auto = fns._tilexyz_udf(tile, z, x, y, "PNG", 256, "bilinear", "auto")
    none = fns._tilexyz_udf(tile, z, x, y, "PNG", 256, "bilinear", "none")

    def _spread(png):
        a = np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))
        rgb = a[..., :3][a[..., 3] > 0]
        return 0 if rgb.size == 0 else int(rgb.max()) - int(rgb.min())

    assert _spread(auto) > 100   # contrast recovered
    assert _spread(none) < 80    # today's crushed behavior preserved


def test_tilexyz_udf_rescale_defaults_to_auto():
    from databricks.labs.gbx.pyrx import functions as fns
    raster = _make_uint16_narrow(lo=8000, hi=12000)
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
    finally:
        ds.close(); mf.close()
    tile = {"raster": raster}
    # rescale omitted -> defaults to auto (contrast recovered)
    default = fns._tilexyz_udf(tile, z, x, y, "PNG", 256, "bilinear")
    explicit_auto = fns._tilexyz_udf(tile, z, x, y, "PNG", 256, "bilinear", "auto")
    assert default == explicit_auto
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -k "tilexyz_udf" -v`
Expected: FAIL — `_tilexyz_udf()` takes 7 positional arguments but 8 were given.

- [ ] **Step 3: Implement — add `rescale` to the UDF, UDTF, and public binding**

In `functions.py`, replace `_tilexyz_udf` (lines 2487-2499):

```python
def _tilexyz_udf(tile, z, x, y, format, size, resampling, rescale=None):
    # Mirror heavyweight: rst_tilexyz NEVER returns null — a null/empty tile or
    # any hard failure yields a transparent PNG (slippy-map servers need a 200).
    sz = int(size) if size is not None else 256
    if tile is None or tile["raster"] is None:
        return xyz.transparent_png(sz)
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    fmt = str(format) if format is not None else "PNG"
    resamp = str(resampling) if resampling is not None else "bilinear"
    rsc = rescale if rescale is not None else "auto"
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return xyz.render_tile(ds, int(z), int(x), int(y), fmt, sz, resamp, rescale=rsc)
```

Replace `_RstXyzPyramidUDTF.eval` (lines 2521-2538):

```python
    def eval(self, tile, min_z, max_z, format=None, size=None, resampling=None, rescale=None):
        # Defaults make format/size/resampling/rescale optional in the SQL UDTF call
        # (gbx_rst_xyzpyramid(tile, min_z, max_z)). None maps to PNG/256/bilinear/auto.
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        fmt = str(format) if format is not None else "PNG"
        sz = int(size) if size is not None else 256
        resamp = str(resampling) if resampling is not None else "bilinear"
        rsc = rescale if rescale is not None else "auto"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for z, x, y, b in xyz.iter_pyramid(
                ds, int(min_z), int(max_z), fmt, sz, resamp, rsc
            ):
                yield (z, x, y, b)
```

Replace the `rst_tilexyz` signature (lines 2541-2549) and its tail (lines 2568-2571). New signature:

```python
def rst_tilexyz(
    tile: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
    rescale: ColLike = "auto",
) -> Column:
```

And add to its Args docstring (after the `resampling:` line, before `Returns:`):

```python
        rescale:    8-bit encoding contrast. "auto" (default) rescales non-8-bit
                    rasters by whole-dataset per-band min/max and passes uint8
                    through unchanged; "none" keeps the raw full-dtype-range
                    mapping; a (min, max) pair sets explicit bounds.
```

New tail (replace lines 2568-2571):

```python
    fmt = f.lit(format) if isinstance(format, str) else _col(format)
    sz = f.lit(size) if isinstance(size, int) else _col(size)
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    rsc = f.lit(rescale) if isinstance(rescale, str) else _col(rescale)
    return _tilexyz_udf(_col(tile), _col(z), _col(x), _col(y), fmt, sz, resamp, rsc)
```

Add `rescale` to the `rst_xyzpyramid` docstring Args (after `resampling:` near line 2603):

```python
        rescale:    8-bit encoding contrast: "auto" (default), "none", or a
                    (min, max) pair. See rst_tilexyz.
```

Note on the explicit `(min, max)` pair via SQL/Column: the `rescale` Column path passes through `_col(rescale)` when not a string. A tuple literal is not a plain `str`, so `f.lit("auto")`/`f.lit("none")` cover the string cases and a Column expression covers dynamic values; an explicit numeric pair through the Python API is supported by passing it as `rescale=(min,max)` only to the core/UDF path (not the Column wrapper) — document that the Column wrapper supports the string modes and a Column, while the (min,max) tuple is for the direct/core API. (Heavy-tier SQL pair support is a Phase 2 concern.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_xyz.py -v`
Expected: PASS (all, including the two new `_tilexyz_udf` tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_core_xyz.py
git commit -m "feat(pyrx): expose rescale on rst_tilexyz/rst_xyzpyramid (light)

Adds trailing rescale arg (default auto) to the scalar UDF, the pyramid
UDTF eval, and the public rst_tilexyz binding + docstrings. SQL picks up
the new arity automatically.

Co-authored-by: Isaac"
```

---

### Task 5: Regression-guard the SQL surface + run the full pyrx light suite

**Files:**
- Test: `python/geobrix/test/pyrx/test_functions_spark.py` (extend, if a local SparkSession is already used there) OR `test_sql_registration.py`
- No source changes expected (this task is a verification gate; only add a test if a gap exists).

**Interfaces:**
- Consumes: the registered `gbx_rst_tilexyz` / `gbx_rst_xyzpyramid` (Task 4).

- [ ] **Step 1: Inspect how the existing Spark tests invoke the tilers**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_functions_spark.py -k "tilexyz or xyzpyramid" -v`
Expected: existing tests PASS (the new trailing arg is optional, so existing calls are unaffected). If there are NO such tests, note it and skip adding Spark-level ones here (covered by the core + UDF tests); the SQL arity is exercised by registration.

- [ ] **Step 2: If a Spark tiler test exists, add a `rescale` regression**

Only if `test_functions_spark.py` already builds a local SparkSession + a tile column, add (mirroring its existing fixture style — adapt names to that file's helpers):

```python
def test_sql_rst_tilexyz_accepts_rescale(spark, sample_uint16_tile_df):
    # gbx_rst_tilexyz(tile, z, x, y, format, size, resampling, rescale)
    from databricks.labs.gbx.pyrx import functions as fns
    fns.register(spark)
    row = (
        sample_uint16_tile_df
        .selectExpr("gbx_rst_tilexyz(tile, 8, 41, 90, 'PNG', 256, 'bilinear', 'auto') AS png")
        .collect()[0]
    )
    assert row["png"] is not None and len(row["png"]) > 0
```

(If the file has no such fixture, DO NOT invent one — the core/UDF tests already prove the behavior. Skip to Step 3.)

- [ ] **Step 3: Run the full pyrx light suite**

Run: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/ -v --tb=short`
Expected: PASS (no regressions across the pyrx light tier).

- [ ] **Step 4: Lint check (CI gate, host)**

Run: `bash scripts/commands/gbx-lint:python.sh --check 2>/dev/null || bash scripts/commands/gbx-lint-python.sh --check`
Expected: clean. If it reports formatting, run the `--fix` variant ONLY for these files, re-check, and confirm. (Note: per project memory, the host black may differ from CI; the Docker `--check` is authoritative, but for a pure-Python diff the host check is a reasonable pre-push gate.)

- [ ] **Step 5: Commit (only if Step 2/4 changed files)**

```bash
git add -A
git commit -m "test(pyrx): regression-guard rescale on the SQL tiler surface

Co-authored-by: Isaac"
```

---

### Task 6: Rebuild + restage the light wheel; re-run Helios NB02 to visually confirm

**Files:** none (operational verification).

**Interfaces:** Consumes the registered light tier with `rescale="auto"` default.

- [ ] **Step 1: Build the wheel (pure-Python; no JAR rebuild)**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
rm -rf python/geobrix/dist
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m build --no-isolation /Users/mjohns/IdeaProjects/geobrix/python/geobrix
ls -la python/geobrix/dist/geobrix-*.whl
```
Expected: `geobrix-0.4.0-py3-none-any.whl` produced.

- [ ] **Step 2: Upload to the canonical sample-data Volume wheel path**

```bash
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python - <<'PY'
import glob
from databricks.sdk import WorkspaceClient
whl = sorted(glob.glob("/Users/mjohns/IdeaProjects/geobrix/python/geobrix/dist/geobrix-*.whl"))[-1]
dest = "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
WorkspaceClient(profile="oauth-fe").files.upload_from(
    file_path=dest, source_path=whl, overwrite=True, use_parallel=False
)
print("uploaded", whl, "->", dest)
PY
```
Expected: `uploaded ... -> /Volumes/.../geobrix-0.4.0-py3-none-any.whl`.

- [ ] **Step 3: Re-run Helios NB02 on Serverless**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook "notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb" \
  --ws-dir "/Users/<your-workspace-user>/GeoBrix/helios" \
  --extra-deps rich \
  --log notebooks-serverless-rescale.log
```
Expected: `RunResultState.SUCCESS`; capture the `run_page_url`.

- [ ] **Step 4: Report the run_page_url for visual confirmation**

Surface the `run_page_url` so the user opens section 7 and confirms the basemap now has full contrast (no longer washed-out). This is the customer-facing definition of done for Phase 1.

- [ ] **Step 5: No commit (operational task).** Hand off to the user for live notebook testing.

---

## Phase 2 (separate plan — heavy/classic tier)

NOT in this plan. After the light tier is confirmed, a follow-up plan will:
- Mirror `rescale` in `RST_TileXYZ.scala` / `RST_XYZPyramid.scala` (compute band stats once, inject `gdal_translate -scale min max 0 255`).
- Add the `rescale` arg to `OperatorOptions` PNG/JPEG/WEBP branches.
- Update bindings/parity: `registered_functions.txt`, `function-info.json` (via `gbx:docs:function-info`), the `*_sql_example()` in `docs/tests/python/api/rasterx_functions_sql.py`, and run `gbx:test:bindings`.
- Add the Docker cross-tier pixel-parity test (same uint16 narrow-range fixture; assert equivalent value distribution for "auto", byte-identical uint8 pass-through within each tier).
- Reconcile the temporary light-vs-heavy divergence introduced by this plan.

## Self-Review

- **Spec coverage:** `rescale` param + default auto (Tasks 1-4); uint8 pass-through (Task 1/2); non-8-bit whole-dataset min/max (Task 1); "none" escape hatch (Task 1/2); explicit (min,max) (Task 1/4); no seams via resolve-once (Task 3); light-tier TDD locally (Tasks 1-5); wheel restage + NB02 visual confirm (Task 6). Heavy tier + Docker parity test + bindings explicitly deferred to Phase 2 (matches approved light-first sequencing). Covered.
- **Placeholder scan:** none — all steps have concrete code/commands. Task 5 Step 2 is conditional by design (don't invent a fixture) and is explicitly gated, not a placeholder.
- **Type consistency:** `_validate_rescale` / `_resolve_in_range` / `render_tile(..., rescale, in_range)` / `iter_pyramid(..., rescale)` / `pyramid(..., rescale)` / `_tilexyz_udf(..., rescale=None)` / `_RstXyzPyramidUDTF.eval(..., rescale=None)` / `rst_tilexyz(..., rescale="auto")` are consistent across tasks. `in_range` is the precomputed per-band list `[(min,max),...]`; `rescale` is the user-facing knob. Consistent.
