# Heavy-tier RGBA convergence for `rst_tilexyz` / `rst_xyzpyramid` — Design

**Date:** 2026-07-27
**Status:** Design (approved direction); pending plan.
**Branch:** `beta/0.4.0` (targets a future beta release)
**Relates:** `render-divergent-generators-rgba-parity`, `pyrx-xyz-tile-contrast-uint16` (the `rescale`
contrast fix, already shipped both tiers), `pyrx-nodata-edge-divergences`, `perf-parity-light-vs-heavy`,
`justify-by-utility-not-mosaic`

## 1. Problem & goal

The heavyweight (Scala / GDAL Java bindings) XYZ tile renderers `rst_tilexyz` and `rst_xyzpyramid` emit
PNG tiles whose **band count equals the source band count** (post-warp `-ot Byte`, no alpha added except
the out-of-extent fallback). The lightweight tier (`pyrx/core/xyz.py`, rio-tiler) always renders a
display **RGBA** web-map tile with a binary alpha mask.

**The bug this fixes:** an in-extent tile with **internal NoData** (a hole inside the raster footprint,
not just the tile edge) renders **opaque / black on heavy** but **transparent on light**. On a slippy /
web map, heavy tiles show black gaps where light tiles correctly show the basemap through. This is a real
display-fidelity defect, not cosmetic byte drift.

**Goal:** converge heavy XYZ tile output **up to the light tier's shape and transparency** — heavy emits
display RGB(A) tiles matching rio-tiler's band mapping and binary alpha — so the two tiers are visually
equivalent (within encoder tolerance) for the same input.

**Non-goal — byte parity.** Heavy encodes via GDAL's libpng/libwebp; light via rio-tiler's. Identical
pixels still yield different compressed bytes, so byte-identical output and a shared bench fingerprint are
impossible AND not the target. The bench stays `timing-only`; cross-tier confidence comes from a decoded,
tolerance-based parity test (§6).

## 2. Scope

**In scope:** `RST_TileXYZ` and `RST_XYZPyramid` (the latter delegates per-tile to the former via
`executeWithScale`, so it inherits the change for free). All three output formats:

- **PNG → RGBA** (alpha supported)
- **WEBP → RGBA** where the GDAL build's WEBP driver supports alpha, else RGB fallback (§4)
- **JPEG → RGB** (JPEG has no alpha channel)

**Out of scope:**
- `rst_color_relief` — its cross-tier divergence is the interpolation ENGINE (heavy `gdal.DEMProcessing`
  C `color-relief` vs light `np.interp`); converging would require GDAL in the pure-Python light tier
  (rejected). Left divergent, documented separately. Not touched here.
- The lightweight tier — already correct (rio-tiler RGBA); unchanged.
- The bench — stays `timing-only`; unchanged.
- The `rescale` 8-bit contrast behavior — already shipped on both tiers (`rescale="auto"` default); reused
  here unchanged, not redesigned.

## 3. Grounding facts (verified 2026-07-27, recon)

- Heavy `RST_TileXYZ` (`src/main/scala/.../rasterx/expressions/web/RST_TileXYZ.scala`): warps source to the
  3857 tile bbox via `gdal.Warp` → `/vsimem`, then encodes via `gdal.Translate` (**JNI, not a CLI
  shell-out**). PNG branch emits `-ot Byte` with optional `-scale` from `resolveScale`. Output band count =
  warped-intermediate band count = source band count. The ONLY existing 4-band RGBA path is `transparentPng`
  (out-of-extent fallback: all-zero-alpha RGBA MEM ds → encode).
- `resolveScale(ds, rescale)` is `private[web]`, resolves `-scale lo hi 0 255` flags once from the
  pre-warp source (so pyramids share one mapping, no seams). uint8 source / `"none"` → `""` passthrough.
- `RST_XYZPyramid` resolves `rescale` once before its tile loop and calls
  `RST_TileXYZ.executeWithScale` per tile.
- GDAL Java `Translate` accepts arbitrary option strings (`-b`, `-scale`, `-ot Byte`, `-colorinterp_*`,
  mask/alpha selection) — **no JNI gap** for building RGBA. `BandAccessors.getMinMax` already exists.
- Light `xyz.py::render_tile` uses rio-tiler `cog.tile(...)` then `img.render(img_format=..., add_mask=True)`
  — binary (0/255) alpha from the tile mask; RGB mapping per rio-tiler's rules (below).

## 4. Design

### 4.1 Band mapping — match rio-tiler exactly

Between the warp and the encode, map the warped intermediate to a display RGB(A) `GDT_Byte` MEM dataset.
The mapping reproduces rio-tiler's band-selection rules so the two tiers agree:

| Source bands | RGB mapping | Alpha (PNG / WEBP) |
|---|---|---|
| 1 (greyscale) | band 1 replicated → R = G = B | derived binary mask (§4.2) |
| 2 | band 1 → grey (R = G = B); **band 2 = alpha** (rio-tiler rule) | band 2 |
| 3 | bands 1, 2, 3 → R, G, B | derived binary mask |
| 4 | bands 1, 2, 3 → R, G, B; band 4 = alpha | band 4 |
| ≥5 | first 3 bands → R, G, B | derived binary mask |

- The existing `resolveScale` 8-bit rescale runs on the mapped **RGB** bands, unchanged (`"auto"` default:
  uint8 passthrough, else whole-dataset per-band min/max → 0..255). Alpha bands are never rescaled (they are
  already 0/255 or a source mask).
- **JPEG:** same RGB mapping, alpha dropped entirely (3-band output).
- **2-band decision (pinned):** we adopt rio-tiler's "band 2 = alpha" rather than "grey + derived-NoData
  alpha," to keep exact cross-tier parity. Documented as the chosen rule.

### 4.2 Alpha derivation — binary from the warp mask

For source band counts that do NOT already carry an alpha band (1, 3, ≥5), alpha is **binary 0/255 from the
warped tile's mask**: 255 where the 3857-warped tile has valid data, 0 where NoData OR outside the source
footprint. One mask covers both the tile-edge and internal-NoData cases — matching rio-tiler's default
binary `add_mask`. For 2- and 4-band sources the source's own last band IS the alpha (per §4.1), carried
through, not re-derived. The out-of-extent `transparentPng` fallback (all-zero alpha RGBA) is unchanged.

### 4.3 Implementation shape

- Add a private helper in `RST_TileXYZ`, e.g.
  `toDisplayRGBA(warpedDs, format, scaleFlags): /vsimem path or MEM ds`, that performs §4.1 mapping + §4.2
  alpha and returns the dataset/bytes ready to encode. Keeps the mapping isolated and unit-testable.
- Build the RGB(A) MEM dataset from the warped intermediate: create a `GDT_Byte` MEM ds with the target
  band count (4 for PNG/WEBP-with-alpha, 3 for JPEG), copy/rescale the mapped RGB bands, set the alpha band,
  and set color interpretation (`GCI_RedBand`/`GreenBand`/`BlueBand`/`AlphaBand`) so the encoder writes a
  proper RGBA image. Then `gdal.Translate` to the target format in `/vsimem` and read bytes back — the
  existing encode/read path.
- `RST_XYZPyramid` needs **no change** — it calls `RST_TileXYZ.executeWithScale` per tile, which now returns
  RGBA bytes.
- Reuse `resolveScale` (RGB rescale) and the `transparentPng` construction idiom (already builds RGBA).

### 4.4 WEBP-alpha risk & fallback

WEBP alpha depends on the deployed GDAL build's WEBP driver. **Mitigation:** detect at encode time whether
the WEBP driver advertises alpha support; if not, encode WEBP as 3-band RGB and log a one-line note (and
document it). WEBP-with-alpha is thus best-effort, never a hard failure. PNG (always alpha-capable) is the
guaranteed path and where the bug lives.

## 5. Behavior change & docs

Heavy PNG/WEBP output changes from **N source bands → 4 (RGBA)**; JPEG → **3 (RGB)**. This is a **beta
breaking change** to heavy XYZ output shape: a consumer reading heavy `rst_tilexyz` bytes as a raw N-band
raster (rather than as a display tile) is affected. Unusual for slippy-map tiles, but real.

- **beta-release-notes.mdx:** a behavior-change entry under the in-flight list — heavy XYZ tiles now emit
  RGBA (PNG/WEBP) / RGB (JPEG) to match the lightweight tier and render internal-NoData transparently.
- **raster-functions.mdx / `rst_tilexyz` docs:** state the output is a display RGB(A) web-map tile, the
  band-mapping table (§4.1), binary alpha from NoData, and the JPEG-no-alpha / WEBP-best-effort notes.

## 6. Testing

- **Cross-tier parity test (decode + tolerance)** — the primary gate. For a shared fixture (incl. a raster
  with **internal NoData** so the transparency case is actually exercised — not an all-valid tile where
  alpha is trivially uniform), render the same (z,x,y) on both tiers, decode both to arrays, and assert:
  - identical tile dimensions;
  - identical band shape per format (RGBA for PNG/WEBP-with-alpha, RGB for JPEG);
  - **exact alpha-mask position match** (the set of 0-alpha pixels is identical across tiers) — this is the
    bug being fixed;
  - RGB channels agree **within a tolerance** (encoder + rescale-rounding noise; a generous per-channel
    tolerance, the light-readers parity pattern — NOT byte or exact equality).
  Run in Docker (needs full GDAL + rio-tiler + sample data).
- **Heavy unit tests** (Scala): one per source band count (1 / 2 / 3 / 4 / ≥5) asserting correct output band
  count and color interpretation; one per format (PNG RGBA, WEBP RGBA-or-RGB-fallback, JPEG RGB); the
  out-of-extent fallback still all-transparent RGBA; a source with internal NoData yields 0-alpha at the
  hole.
- **Bench:** unchanged — `rst_tilexyz` / `rst_xyzpyramid` stay `pure-core`, `fingerprint=False`,
  `timing-only`.

## 7. Risks

- **WEBP alpha unsupported in the build** — mitigated by the RGB fallback (§4.4); PNG unaffected.
- **rio-tiler band-mapping rules drift** across rio-tiler versions — the parity test pins the current
  behavior; if rio-tiler changes, the test flags it and the §4.1 table is updated in lockstep.
- **Rescale interaction** — alpha bands must never be fed to `resolveScale`; the helper applies scale to RGB
  bands only. Unit-tested via the internal-NoData fixture (a bug here would rescale the mask).
- **Behavior-change blast radius** — mitigated by release-note + doc callout (§5); genuinely raw-band
  consumers of a display-tile function are expected to be rare.

## 8. Surfaces to update

- Heavy: `src/main/scala/.../rasterx/expressions/web/RST_TileXYZ.scala` (add `toDisplayRGBA` helper; wire
  into the PNG/WEBP/JPEG encode paths). `RST_XYZPyramid.scala` unchanged (inherits).
- Tests: a new cross-tier parity test (Python, decodes both tiers) + heavy Scala unit tests under
  `src/test/scala/.../web/`.
- Docs: `docs/docs/beta-release-notes.mdx`, `docs/docs/api/raster-functions.mdx` (`rst_tilexyz` entry).
- No new dependencies. No bench change. No light-tier change.
