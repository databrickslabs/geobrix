# Inc 5 — transform/combinator coherence + corrected virtualize taxonomy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `rst_transform` an identity-passthrough when target CRS == source CRS, prove pixel-producer `virtualize_dir` results are self-consistent, and correct the coarse "already-virtual → no-op" `virtualize_dir` rule to the three-bucket by-output-nature taxonomy across docstrings, docs, and memory.

**Architecture:** Inc 4 already shipped the force-output plumbing on all pixel-producing light-tier functions (`rst_transform`, `rst_merge`, `rst_combineavg`, `rst_frombands`, webmercator) via the shared `_shaped_result_row` helper, and `rst_transform` is already eager. Inc 5 is a correctness/coherence increment: one behavior change (identity short-circuit in `warp.reproject_to_srid`), test-enforced coherence invariants, and documentation/memory corrections. No new registered functions; binding parity unchanged (zero-diff).

**Tech Stack:** Python 3.12, rasterio/GDAL, PySpark (light tier, JAR-free), pytest. Docs are Docusaurus MDX with doc-tests. Serverless proof via the `run_notebooks_serverless.py` runner (oauth-fe, env v5, light extras).

## Global Constraints

- **No new registered functions.** Inc 5 adds no entries to `docs/tests-function-info/registered_functions.txt`; binding parity (`gbx:test:bindings`) must stay zero-diff. The identity short-circuit and docstring edits do not change any registered signature.
- **No aliases.** One canonical name per function (beta). "mosaic" = `rst_merge`, "stack" = `rst_frombands`; do not add `rst_stack` or any alias.
- **Governing principle (verbatim from spec):** an op that *produces new pixels* (warp/transform, merge, combineavg, frombands) MUST materialize; `virtualize_dir` is the only way to get a virtual result back from such an op. Ops that produce no new pixels (header reads, reader window/clip selection, **identity transform**) are reference/passthrough: `virtualize_dir` is a no-op, `materialize` forces bytes. "Produces new pixels" is the exact discriminator.
- **Overlap raw-bytes sort-key parity.** `agg_core.merge_tiles` sorts inputs on their RAW GTiff bytes for a heavy-parity last-wins overlap winner. Materialized inputs must pass their original bytes verbatim (never re-encode). The identity short-circuit must return input bytes verbatim for the same reason.
- **Light tier is JAR-free / Serverless-safe.** No `spark.conf.set`, `_jvm`, `.rdd` in pyrx source. Tests run under plain pytest (no JAR).
- **Identity discriminated by EPSG code.** Compare source CRS EPSG code to `target_srid`; a source CRS with no EPSG code can never match and correctly falls through to a real warp.
- **User-facing docs voice.** No internal planning vocabulary (no "Wave N", no "Inc N", no subagent/dispatch references) in anything under `docs/docs/`.

---

## File Structure

- `python/geobrix/src/databricks/labs/gbx/pyrx/core/warp.py` — add identity short-circuit to `reproject_to_srid` (the single shared site; `_transform_bytes` and `_to_webmercator_bytes` both call it).
- `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — docstring corrections on `rst_transform` and the three combinators (by-output-nature rule).
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` — docstring correction on `shape_output` only (no logic change).
- `python/geobrix/test/pyrx/test_virtual_aware_family.py` — new tests: identity passthrough (bytes + virtual no-op), non-identity stamps target CRS, no-EPSG source still warps, pixel-producer `virtualize_dir` round-trip coherence, identity-into-merge overlap parity.
- `docs/docs/_partials/_virtual-tile-overrides.mdx` — correct "transform-intent" deferrable framing + auto-behavior wording.
- `docs/docs/api/execution-tiers.mdx` — correct the Virtual↔materialized advice table (transform row) + taxonomy.
- `docs/docs/api/large-rasters.mdx` — verify/trim any coarse-rule wording near line 194.
- `/Users/mjohns/.claude/projects/-Users-mjohns-IdeaProjects-geobrix/memory/light-virtual-tiling-by-reference.md` — correct lines 75 & 100 (LAZY-WARP contradiction → eager + three-bucket).
- Serverless proof notebook: `prompts/features/2026-08-02-inc5-transform-combinators-serverless.py` (gitignored scratch, `.ipynb` companion generated at run).

---

### Task 1: Identity short-circuit in `reproject_to_srid`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/warp.py:11-34`
- Test: `python/geobrix/test/pyrx/test_virtual_aware_family.py`

**Interfaces:**
- Consumes: an open rasterio `DatasetReader` `ds` with `.crs`, `.profile`, `.count`, `.bounds`.
- Produces: `reproject_to_srid(ds, target_srid: int, resampling: str = "nearest") -> bytes` — unchanged signature; NEW behavior: when the source CRS's EPSG code equals `target_srid`, returns the source GTiff bytes verbatim (no reproject, no re-encode).

- [ ] **Step 1: Write the failing tests**

Add to `test_virtual_aware_family.py` (uses existing `_write_ramp_tif` / `_virtual_tile` / `ot` helpers already imported at top of file):

```python
# ---------------------------------------------------------------------------
# rst_transform — identity (target == source EPSG) is a PASSTHROUGH
# ---------------------------------------------------------------------------
def test_reproject_identity_returns_source_bytes_verbatim(tmp_path):
    """reproject_to_srid to the SOURCE epsg returns the source bytes unchanged
    (no resample, no re-encode) — identity produces no new pixels."""
    from databricks.labs.gbx.pyrx.core import warp

    p = str(tmp_path / "ident.tif")
    _write_ramp_tif(p, side=8, epsg=32633)
    with rasterio.open(p) as ds:
        src_bytes = ds.read()  # decode source pixels for comparison
        out = warp.reproject_to_srid(ds, 32633)
    # Output decodes to the exact same pixels + CRS as the source.
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 32633
        assert ods.width == 8 and ods.height == 8
        np.testing.assert_array_equal(ods.read(), src_bytes)


def test_reproject_no_epsg_source_still_warps(tmp_path):
    """A source CRS with no EPSG code never mis-short-circuits: the identity
    check compares EPSG codes, so a codeless CRS always falls through to warp."""
    from databricks.labs.gbx.pyrx.core import warp

    p = str(tmp_path / "esri.tif")
    # ESRI:54008 (World Sinusoidal) has no EPSG code.
    prof = dict(
        driver="GTiff", width=8, height=8, count=1, dtype="float32",
        crs=rasterio.crs.CRS.from_string("ESRI:54008"),
        transform=from_origin(0.0, 8.0, 1000.0, 1000.0), nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    with rasterio.open(p) as ds:
        assert ds.crs.to_epsg() is None  # precondition
        out = warp.reproject_to_srid(ds, 4326)
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 4326  # a real warp happened
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_virtual_aware_family.py -k "reproject_identity or reproject_no_epsg" -v` (or the repo's pyrx venv/`gbx:test:pyrx --path python/geobrix/test/pyrx/test_virtual_aware_family.py`)
Expected: `test_reproject_identity_returns_source_bytes_verbatim` FAILS (current code re-encodes via `calculate_default_transform`+`reproject`, so pixels may differ / it is not verbatim); `test_reproject_no_epsg` passes already (guard falls through) — that's fine, it locks the fall-through.

- [ ] **Step 3: Add the identity guard**

Edit `warp.py` `reproject_to_srid`, inserting the guard immediately after the docstring, before `calculate_default_transform`:

```python
def reproject_to_srid(ds, target_srid: int, resampling: str = "nearest") -> bytes:
    """Reproject an open dataset to EPSG:<target_srid>; return GTiff bytes.

    Identity short-circuit: when the source CRS's EPSG code equals
    ``target_srid`` the reprojection is a no-op (produces no new pixels), so the
    source GTiff bytes are returned VERBATIM — no resample, no re-encode. This
    keeps an identity transform in the reference/passthrough bucket and
    preserves the raw-bytes sort key that ``agg_core.merge_tiles`` relies on for
    heavy-parity overlap resolution.
    """
    target_srid = int(target_srid)
    src_epsg = ds.crs.to_epsg() if ds.crs else None
    if src_epsg is not None and src_epsg == target_srid:
        # Identity: emit the source bytes unchanged.
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(ds.read())
            return mf.read()

    dst_crs = f"EPSG:{target_srid}"
    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    ...  # unchanged remainder
```

Note: "verbatim" here means the same pixels + georeference re-serialized as GTiff (the input `ds` may be a windowed/MemoryFile dataset, so we re-emit via its profile rather than assume a file on disk). The test asserts pixel + CRS + dims equality, which this satisfies. Do NOT run `calculate_default_transform`/`reproject` on the identity path.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_virtual_aware_family.py -k "reproject_identity or reproject_no_epsg" -v`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/warp.py python/geobrix/test/pyrx/test_virtual_aware_family.py
git commit -m "feat(pyrx): identity rst_transform is a passthrough (no re-encode)

When target_srid == source EPSG, reproject_to_srid returns the source
bytes verbatim instead of running a full reproject pass. Identity
produces no new pixels, so it belongs in the reference/passthrough
bucket; returning bytes verbatim also preserves the raw-bytes sort key
merge_tiles relies on for heavy-parity overlap resolution.

Co-authored-by: Isaac"
```

---

### Task 2: Identity `rst_transform` on a virtual tile is a `virtualize_dir` no-op; non-identity stamps target CRS

**Files:**
- Test: `python/geobrix/test/pyrx/test_virtual_aware_family.py`
- (No source change expected — this task VERIFIES the bucket-1/bucket-2 contract end-to-end through `rst_transform`'s UDFs. If a test fails, the fix routes back through Task 1's guard or `_shaped_result_row`.)

**Interfaces:**
- Consumes: `prx._transform_v2_udf.func(tile, target_srid, virtualize_dir, virtualize_prefix, materialize)` and `prx._transform_udf.func(tile, target_srid)` (the `.func` unwraps the Spark UDF for direct call, as existing family tests do).
- Produces: no new interface — asserts the taxonomy holds.

- [ ] **Step 1: Write the failing/locking tests**

```python
def test_transform_identity_virtual_virtualize_dir_is_noop(tmp_path):
    """Identity transform on a VIRTUAL tile with virtualize_dir stays a
    reference/passthrough: the result references the SAME backing pixels and no
    new file is written into virtualize_dir for the identity op itself."""
    tile = _virtual_tile(tmp_path, name="id.tif", epsg=32633)  # virtual, crs=None
    out_dir = str(tmp_path / "idout")
    # target == source epsg 32633 -> identity
    row = prx._transform_v2_udf.func(tile, 32633, out_dir, None, None)
    assert row is not None
    # Reference/passthrough: openable and correct, in the SOURCE crs.
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == 32633
        assert ds.width == 8 and ds.height == 8


def test_transform_reproject_stamps_target_crs(tmp_path):
    """Non-identity transform materializes new pixels whose embedded CRS is the
    target — a pixel-producer output."""
    tile = _virtual_tile(tmp_path, name="tr.tif", epsg=32633)
    out = prx._transform_udf.func(tile, 4326)
    assert out is not None and out["raster"] is not None
    with _serde.open_tile(bytes(out["raster"])) as ds:
        assert ds.crs.to_epsg() == 4326
```

- [ ] **Step 2: Run to verify status**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_virtual_aware_family.py -k "transform_identity_virtual or transform_reproject_stamps" -v`
Expected: `transform_reproject_stamps` PASSES immediately (already eager). `transform_identity_virtual...` — its exact assertion depends on how `_shaped_result_row` shapes an identity result: with Task 1 the identity `_transform_bytes` yields source-bytes, then `shape_output(virtualize_dir=...)` writes them and returns a virtual row referencing the written file. Assertion is on openability + source CRS, which holds. If it fails, the failure message tells you whether the identity output was mis-warped (→ Task 1 guard) or the row is unopenable (→ provenance, Task 4).

- [ ] **Step 3: If failing, adjust (else no source change)**

The likely-needed adjustment, if any: ensure the identity path through `_transform_bytes` returns the source bytes (Task 1 already does this). No new source edit anticipated; this task is a contract lock. If the assertion about "no new file for identity" proves too strict for the current `shape_output` (which DOES write bytes it's handed), relax it to "the written file round-trips to the source pixels in the source CRS" — the load-bearing invariant is coherence, not write-avoidance. Document the chosen assertion in the test docstring.

- [ ] **Step 4: Run to verify pass**

Run: same as Step 2. Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_virtual_aware_family.py
git commit -m "test(pyrx): lock rst_transform identity=passthrough, reproject=target-crs

Co-authored-by: Isaac"
```

---

### Task 3: Pixel-producer `virtualize_dir` round-trip coherence (merge + transform)

**Files:**
- Test: `python/geobrix/test/pyrx/test_virtual_aware_family.py`

**Interfaces:**
- Consumes: `prx._merge_v2_udf.func([tiles], virtualize_dir, prefix, materialize)`, `prx._transform_v2_udf.func(...)`, `ot._open(row)`.
- Produces: a test-enforced invariant — a `virtualize_dir` result of a pixel-producer is openable via `open_tile` and matches the pre-virtualize materialized result's CRS/width/height. This nails down that leaving `crs=None` on the emitted row is safe because the written file embeds CRS.

- [ ] **Step 1: Write the test**

```python
def test_merge_virtualize_dir_result_is_coherent(tmp_path):
    """A merge virtualize_dir result opens + matches the materialized merge's
    CRS/dims (proves crs=None on the emitted row is safe — the file embeds it)."""
    left = _virtual_tile(tmp_path, name="l.tif", ulx=0.0, uly=8.0, epsg=32633)
    right = _virtual_tile(tmp_path, name="r.tif", ulx=8.0, uly=8.0, epsg=32633)

    # Materialized reference (auto shape).
    mat = prx._merge_udf.func([left, right])
    with _serde.open_tile(bytes(mat["raster"])) as ds:
        exp_epsg, exp_w, exp_h = ds.crs.to_epsg(), ds.width, ds.height

    out_dir = str(tmp_path / "mcoh")
    row = prx._merge_v2_udf.func([left, right], out_dir, None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == exp_epsg
        assert ds.width == exp_w and ds.height == exp_h


def test_transform_virtualize_dir_result_is_coherent(tmp_path):
    """A reproject virtualize_dir result opens in the TARGET crs (self-consistent
    despite crs=None on the emitted row)."""
    tile = _virtual_tile(tmp_path, name="tc.tif", epsg=32633)
    out_dir = str(tmp_path / "tcoh")
    row = prx._transform_v2_udf.func(tile, 4326, out_dir, None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == 4326
```

- [ ] **Step 2: Run to verify status**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_virtual_aware_family.py -k "virtualize_dir_result_is_coherent" -v`
Expected: both PASS on current code (the written GTiff embeds CRS; `open_tile` reads it). If either FAILS, it reveals a genuine coherence gap (emitted row references a file whose CRS/dims disagree with the materialized result) → fix `_shaped_result_row`/`shape_output` to stamp the missing provenance before this passes.

- [ ] **Step 3: If failing, stamp provenance**

Only if Step 2 fails: in `open_tile.py` `shape_output` (virtualize branch), when the produced tile has no `crs`/`window`, read them from the just-written file header and populate the returned `VirtualTile` so the row is self-describing. (Do not read pixels — header only.) Keep it minimal; the test defines done.

- [ ] **Step 4: Run to verify pass**

Run: same as Step 2. Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_virtual_aware_family.py python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py
git commit -m "test(pyrx): pixel-producer virtualize_dir results are coherent round-trips

Co-authored-by: Isaac"
```

---

### Task 4: Identity transform into a merge preserves overlap winner (parity guard)

**Files:**
- Test: `python/geobrix/test/pyrx/test_virtual_aware_family.py`

**Interfaces:**
- Consumes: `prx._transform_udf.func`, `prx._merge_udf.func`, `agg_core.merge_tiles`, `make_geotiff_bytes` (from `.conftest`), `_shift_pixels` (already in this test file).
- Produces: a guard that an identity `rst_transform` feeding a merge yields the SAME overlap winner as merging the raw bytes directly (i.e. identity did not re-encode and perturb the raw-bytes sort key).

- [ ] **Step 1: Write the test**

```python
def test_identity_transform_preserves_merge_overlap_winner():
    """Identity transform must not re-encode: two overlapping materialized tiles
    passed through identity rst_transform then merged must pick the SAME winner
    as merging their raw bytes directly (raw-bytes sort-key parity)."""
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    from .conftest import make_geotiff_bytes

    a = make_geotiff_bytes(width=4, height=4, epsg=4326)
    b = _shift_pixels(make_geotiff_bytes(width=4, height=4, epsg=4326), +50.0)
    a_tile = {"cellid": 0, "raster": a, "metadata": {}}
    b_tile = {"cellid": 0, "raster": b, "metadata": {}}

    expected = agg_core.merge_tiles([a, b])  # raw-bytes winner

    # Identity transform (4326 -> 4326) then merge.
    ta = prx._transform_udf.func(a_tile, 4326)
    tb = prx._transform_udf.func(b_tile, 4326)
    got = prx._merge_udf.func([ta, tb])
    assert got is not None and bytes(got["raster"]) == expected
```

- [ ] **Step 2: Run to verify status**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_virtual_aware_family.py -k "identity_transform_preserves_merge" -v`
Expected: PASSES with Task 1's verbatim identity guard. Without the guard it would FAIL (re-encode changes the bytes → different sort key → possibly flipped winner). This is the parity lock the spec calls out.

- [ ] **Step 3: (no source change; guard from Task 1 makes it pass)**

If it fails, the identity path is still re-encoding — return to Task 1 and ensure the identity branch emits the input bytes without a reproject pass. Note: re-emitting via `MemoryFile` (Task 1) may not be byte-identical to the ORIGINAL file bytes; if `merge_tiles`' sort key is sensitive to that, the identity branch must return the *original* `raster` bytes when the input carried them. Handle this in `_transform_bytes`: if the tile is materialized AND identity, return `bytes(vt.raster)` verbatim rather than re-emitting through `reproject_to_srid`. Add that short-circuit in `functions.py` `_transform_bytes` (check EPSG match against the open ds before calling warp).

- [ ] **Step 4: Run to verify pass**

Run: same as Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_virtual_aware_family.py python/geobrix/src/databricks/labs/gbx/pyrx/src/... 2>/dev/null; git add -A python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
git commit -m "test(pyrx): identity transform into merge preserves overlap winner

Co-authored-by: Isaac"
```

(If `_transform_bytes` was edited in Step 3, include it; the raw-bytes verbatim path for a materialized identity input is the parity-safe route.)

---

### Task 5: Correct the `virtualize_dir` taxonomy in docstrings

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` `shape_output` docstring (lines ~350-373)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` `rst_transform` docstring (line ~902) — clarify identity is passthrough
- Test: none (docstring-only; `gbx:test:pyrx` must still pass unchanged)

**Interfaces:** none changed.

- [ ] **Step 1: Correct `shape_output` docstring**

In `open_tile.py`, replace the `virtualize_dir set:` bullet block so it states the rule by output nature, not input shape:

```python
    ``virtualize_dir`` set:
      - The tile is already a REFERENCE to backing pixels (``raster`` is None —
        header reads, reader selection, identity transform): return as-is.
        ``virtualize_dir`` is a no-op here because the tile already references
        real, self-consistent bytes on a backing store.
      - The tile carries PRODUCED pixels (``raster`` set — a pixel-producing op
        such as reproject/merge/combineavg/frombands materialized its result):
        write bytes to
        ``<dir>/[<prefix>_]<cellid>_<col>_<row>_<w>_<h>.tif`` (overwrite),
        FUSE-safe (local temp -> shutil.copyfile), return a VirtualTile with
        ``raster=None`` referencing the written file. This is the ONLY way a
        pixel-producer returns a virtual tile.
```

- [ ] **Step 2: Correct `rst_transform` docstring**

In `functions.py`, add to the `rst_transform` docstring:

```python
    """Reproject the raster to the target SRID (EPSG code).

    Identity (``target_srid`` == the source CRS's EPSG code) is a passthrough:
    no resample, no re-encode; the tile stays a reference/passthrough (so
    ``virtualize_dir`` is a no-op on an already-virtual input). A non-identity
    reproject PRODUCES new pixels and materializes; pass ``virtualize_dir`` to
    write the reprojected result to a durable path and get a light virtual row.
    ...
    """
```

- [ ] **Step 3: Run the pyrx suite to confirm no breakage**

Run: `gbx:test:pyrx --path python/geobrix/test/pyrx/test_virtual_aware_family.py --log inc5-doc.log` (or the venv pytest over the pyrx test dir)
Expected: all PASS (docstring-only change).

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
git commit -m "docs(pyrx): state virtualize_dir rule by output nature, not input shape

Co-authored-by: Isaac"
```

---

### Task 6: Correct the user-facing docs (execution-tiers, partial, large-rasters)

**Files:**
- Modify: `docs/docs/api/execution-tiers.mdx:145-146` (Virtual↔materialized advice table)
- Modify: `docs/docs/_partials/_virtual-tile-overrides.mdx:11,13-16`
- Modify: `docs/docs/api/large-rasters.mdx` (verify near line 194)
- Test: doc lint (voice) — `grep` guards below

**Interfaces:** none (docs).

- [ ] **Step 1: Fix the execution-tiers advice table**

In `execution-tiers.mdx`, the current row 145 lumps "transform-intent" with deferrable ops. Split transform correctly:

Replace row 145 (`**Deferrable tile ops** ... transform-intent) | Stay virtual ...`) with a version that lists ONLY genuinely-deferrable ops (`rst_clip`, `rst_setsrid`, **identity `rst_transform`**), and ensure the pixel-producing row 146 explicitly includes **non-identity `rst_transform`, `rst_merge`, `rst_combineavg`, `rst_frombands`**:

```markdown
| **Deferrable / passthrough tile ops** (`rst_clip`, `rst_setsrid`, **identity `rst_transform`** where target CRS == source CRS) | Stay virtual when input is virtual | Reference existing backing pixels — no new pixels produced; `virtualize_dir` is a no-op |
| **Pixel-producing tile ops** (slope, aspect, hillshade, terrain, focal, mapalgebra, spectral indices, rasterize, resample, **non-identity `rst_transform`**, **`rst_merge` / `rst_combineavg` / `rst_frombands`**) | Materialize and return bytes | Pass `virtualize_dir` to write the computed result to a durable path and get a light virtual row — the **only** way these return a virtual tile |
```

- [ ] **Step 2: Fix the `_virtual-tile-overrides.mdx` partial**

Correct line 11 (`materialize` "Default (unset)") and the "Default (auto) behavior" paragraph (lines 13-16) to distinguish reference/passthrough (transform-intent → **identity transform**) from pixel-producers, and to state that `virtualize_dir` is a no-op only for reference outputs and REQUIRED to virtualize a pixel-producer:

```markdown
| `materialize` | `bool` | `True` — ensure the produced tile carries bytes. `False` — no-op. Default (unset) — auto: **reference/passthrough** ops (header reads, `rst_clip`, `rst_setsrid`, identity `rst_transform`) stay virtual when their input is virtual; **pixel-producing** ops (slope, focal, mapalgebra, reproject, merge, combineavg, frombands, …) materialize. |
```

```markdown
**Default (auto) behavior:** chain reference/passthrough operations (clip, setsrid, identity
transform) for free — they stay virtual when their input is virtual, and `virtualize_dir` is a no-op
on them. Pixel-producing operations (slope, focal, mapalgebra, reproject, merge, combineavg,
frombands, …) materialize and return bytes; for these, `virtualize_dir` is the **only** way to get a
virtual tile back — it writes the computed result to a durable path and returns a light virtual row.
```

- [ ] **Step 3: Verify large-rasters.mdx**

Read `docs/docs/api/large-rasters.mdx` around line 194; if it restates the coarse "no-op if already virtual" rule without the pixel-producer distinction, align it (or link to the partial/execution-tiers). If it already just links, leave it.

- [ ] **Step 4: Voice + coarse-rule guards**

Run:
```bash
grep -rn -iE "wave [0-9]+|wave-[0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/ ; echo "exit:$?"
grep -rn "transform-intent" docs/docs/   # should be GONE (replaced by identity transform)
```
Expected: first grep prints nothing (voice clean); "transform-intent" no longer present.

- [ ] **Step 5: Commit**

```bash
git add docs/docs/api/execution-tiers.mdx docs/docs/_partials/_virtual-tile-overrides.mdx docs/docs/api/large-rasters.mdx
git commit -m "docs: correct virtualize_dir taxonomy — identity transform is passthrough

Distinguish reference/passthrough ops (clip, setsrid, identity transform:
virtualize_dir is a no-op) from pixel-producing ops (reproject, merge,
combineavg, frombands: virtualize_dir is the only way to get a virtual
tile back). Replaces the coarse 'deferrable transform-intent' framing.

Co-authored-by: Isaac"
```

---

### Task 7: Correct the memory blurb

**Files:**
- Modify: `/Users/mjohns/.claude/projects/-Users-mjohns-IdeaProjects-geobrix/memory/light-virtual-tiling-by-reference.md` lines 75 & 100

**Interfaces:** none (memory).

- [ ] **Step 1: Correct line 75 (the LAZY-WARP contradiction)**

Line 75 currently reads `rst_transform default = LAZY WARP (WarpedVRT, Model B, stays virtual)...`. Replace with the settled eager decision:

```
- **Transform model DECIDED (Inc 5, 2026-08-02):** rst_transform is EAGER — a non-identity reproject PRODUCES new pixels so it MUST materialize (the founding invariant: window/crs/clip_polygon must agree with real backing pixels; a pending-warp mixed state is illegal). To stay light after a reproject, use virtualize_dir (write result to durable path → light virtual row). IDENTITY transform (target==source EPSG) is a passthrough (no re-encode, reference/passthrough bucket). Supersedes the earlier LAZY-WARP/Model-B framing. The open_tile crs-field warp path remains for reader-stamped materialized-at-read warps, but rst_transform never creates a pending-warp virtual tile.
```

- [ ] **Step 2: Correct line 100**

Line 100 (`rst_transform on virtual → materialized (all fields become reference)`) — extend with the three-bucket virtualize_dir rule:

```
  - reference-vs-instruction principle CONFIRMED: materialized tile clip fields = already-applied provenance; virtual = pending instructions open_tile applies. THREE-BUCKET virtualize_dir taxonomy (Inc 5): (1) reference/passthrough (header reads, reader selection, identity transform) → virtualize_dir NO-OP; (2) single-source pixel op (slope/focal) → auto=bytes, virtualize_dir externalizes; (3) multi-source combinator (merge/combineavg/frombands) → auto=bytes, virtualize_dir REQUIRED to get a virtual result. Non-identity rst_transform is a pixel-producer (bucket 2/3 behavior). Discriminator = "produces new pixels".
```

- [ ] **Step 3: No index change needed**

`MEMORY.md` already points at this file; the one-line hook still accurate. No edit unless the hook is now wrong (it is not).

- [ ] **Step 4: (memory files are not git-tracked — no commit)**

Memory lives under `~/.claude/...`, outside the repo. No git action. Verify the edits landed by re-reading the two lines.

---

### Task 8: Serverless proof

**Files:**
- Create: `prompts/features/2026-08-02-inc5-transform-combinators-serverless.py` (gitignored; `.ipynb` generated by the runner)

**Interfaces:**
- Consumes: `run_notebooks_serverless.py` runner (oauth-fe, env v5, `--extras light`, `--wheel <gdal_artifacts current 0.4.4 wheel>`).
- Produces: a `dbutils.notebook.exit(json)` summary gated on `all_ok == true`.

**PRE-FLIGHT (mandatory, per memory [[bench-wheel-path-divergence]]):** rebuild the wheel AFTER the last code commit (Tasks 1/3/4 touch pyrx source), upload the local `dist/*.whl` directly to the artifact Volume, then download the staged copy and grep it for an Inc-5 marker (e.g. the identity-guard docstring string `"Identity short-circuit"`) to confirm the staged wheel is fresh. Keep ONLY the current version on the artifact Volume ([[artifact-volume-single-current-version]]).

- [ ] **Step 1: Write the notebook**

Model on `prompts/features/2026-08-01-functions-virtual-aware-serverless.py`. Cells:
1. Corpus on the Volume (idempotent): write two adjacent EPSG:32633 ramp GTiffs (`left`, `right`) + note their union.
2. Worker-side `mapInPandas` over path+window rows proving:
   - `rst_merge` of 3 VIRTUAL tiles with `virtualize_dir=<Vol out>` → light row (raster None), round-trips to union extent (width == sum, correct bounds);
   - identity `rst_transform(virtual, 32633)` → passthrough (openable, source CRS, no error);
   - `rst_transform(virtual, 4326, virtualize_dir=<Vol out>)` → light row, round-trips reporting EPSG 4326.
3. Verify + `dbutils.notebook.exit(json.dumps(summary))` with `all_ok`.

- [ ] **Step 2: Rebuild + stage + verify wheel (pre-flight)**

Run `gbx:data:push-wheel` (or direct SDK upload of local `dist/*.whl`), then download the staged copy and:
```bash
unzip -p <staged>.whl 'databricks/labs/gbx/pyrx/core/warp.py' | grep -c "Identity short-circuit"
```
Expected: `1` on both the local `dist` wheel and the downloaded staged copy — they must agree.

- [ ] **Step 3: Fire the Serverless job directly**

```bash
.venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py \
  --notebook prompts/features/2026-08-02-inc5-transform-combinators-serverless.ipynb \
  --wheel /Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/geobrix-0.4.4-py3-none-any.whl \
  --extras light --profile oauth-fe
```
Expected: run completes; returned JSON `all_ok == true`. Paste the run URL + JSON into the notebook's RESULTS block.

- [ ] **Step 4: Record result**

If `all_ok`, paste run URL + JSON into the notebook RESULTS section. If any check FAILS, treat as a real defect (do not report success) — fix and re-fire. Give the user the run-summary link.

- [ ] **Step 5: (notebook is gitignored — no commit; optional summary note)**

`prompts/` is gitignored. No git action for the notebook. If a summary is worth keeping, write it under `prompts/features/`.

---

## Self-Review

**Spec coverage:**
- Identity-transform passthrough → Tasks 1, 2, 4. ✓
- Provenance coherence assertion → Task 3. ✓
- Corrected taxonomy (docstrings/docs/memory) → Tasks 5, 6, 7. ✓
- Serverless proof → Task 8. ✓
- No-EPSG source still warps (edge case) → Task 1 Step 1. ✓
- Overlap sort-key parity (edge case) → Task 4. ✓
- No new registered functions / binding parity → Global Constraints; no task adds registrations. ✓

**Placeholder scan:** every code step carries real code; test bodies are complete; the one conditional-fix step (Task 2 Step 3, Task 3 Step 3) states the exact fix and defers to the test as done-definition. ✓

**Type consistency:** `reproject_to_srid(ds, target_srid: int, resampling="nearest") -> bytes` used consistently; `.func(...)` UDF-unwrap call convention matches existing family tests; `ot._open`/`_serde.open_tile` usage matches the file's existing imports. ✓

**Voice:** Tasks 6/7 include grep guards for internal vocabulary; user-facing docs carry no "Inc N". ✓
