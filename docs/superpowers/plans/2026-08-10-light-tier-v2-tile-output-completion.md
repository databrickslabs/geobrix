# Complete Light-Tier v2-Tile Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every light-tier (pyrx) tile-returning function emit the 8-field v2 tile struct, matching the already-v2 heavy tier — the non-negotiable 0.5.0 contract.

**Architecture:** Repurpose the single tile-construction helper `_serde.build_tile` to return an 8-field v2 row (keeping its open-and-compute width/height/count metadata), flip all 48 tile-emitting declarations (41 `@f.udf`, 7 `@udtf`) from the legacy 3-field `_serde.TILE_SCHEMA` to `V2_TILE_SCHEMA`, delete the legacy `TILE_SCHEMA` constant, and lock the result with two data-driven standing guards (G1 registry-schema invariant, G2 light≡heavy parity). Operation bodies are never touched.

**Tech Stack:** Python 3.12, PySpark 4.0 UDF/UDTF, rasterio; tests via `gbx:*` commands in the `geobrix-dev` Docker container.

## Global Constraints

- **Only v2 tiles are output, both tiers — non-negotiable for 0.5.0.** No exceptions, no per-function opt-out.
- **The canonical v2 schema is 8 fields, exact order:** `cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata` — defined in `pyrx/core/virtual_tile.py::V2_TILE_SCHEMA` and mirrored byte-for-byte by heavy's `RST_ExpressionUtil.v2TileType`.
- **Operation bodies (the `_*_bytes` helpers, the math) are NOT modified.** This change touches only the UDF/UDTF return schema and the row-construction helper.
- **A materialized v2 tile has `raster` bytes set and all provenance fields (`path`, `window`, `clip_polygon`, `clip_crs`, `crs`) NULL.** `build_tile` produces exactly this.
- **Keep `VirtualTile.from_v1(...)`** — it is the v1-INPUT-widening path used by `_open`; readers still accept v1 tiles on input indefinitely. This plan does NOT remove it.
- **Delete `_serde.build_tile`'s legacy behavior is NOT the goal** — the chosen approach (user ruling) repurposes `build_tile` to emit v2 and deletes only the legacy `TILE_SCHEMA` *constant*.
- **Anti-bloat (standing guidance):** do NOT add ~48 per-function schema tests. Add exactly two data-driven standing guards (G1, G2) sourced from the registry; verify the migration with ad-hoc throwaway probes, not committed per-function tests; fold any existing per-function schema assertions into G1.
- **Grep-authoritative scope:** the set of functions to convert is defined by a fresh grep of `@f.udf(_serde.TILE_SCHEMA)` and `@udtf(returnType=_serde.TILE_SCHEMA)` in `pyrx/functions.py` (41 + 7 = 48 at plan time), NOT any prose list.
- **Tooling:** all tests/lint run in Docker via `gbx:*` commands — `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/` and `--path python/geobrix/test/rasterx/`, `bash scripts/commands/gbx-lint-python.sh --check`. Never ad-hoc `pytest`/`docker`.
- **SQL alignment:** the light UDF schema is the 8-field v2 struct, matching heavy. This is net-new, unreleased 0.5.0 capability — NOT a change from any released behavior, so it is NOT framed as a breaking change and needs no migration/release note. The end state (both tiers use the v2 tile struct) is already documented on the Virtual Tiles / Tile Structure pages; this plan makes the code match those docs. No registered-name or arity changes.
- **Do not push. Do not touch heavy Scala. Do not touch the light grouped aggregators** (`_sql_aggregators` — they return BINARY by convention, not a tile struct).

---

### Task 1: Repurpose `build_tile` to emit v2 + add a v2-materialized assertion helper

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py` (`build_tile`, lines 60-70; module docstring lines 1-15)
- Test: `python/geobrix/test/pyrx/test_core_virtual_tile.py` (add one focused unit test)

**Interfaces:**
- Consumes: `pyrx.core.virtual_tile.V2_TILE_SCHEMA`, `VirtualTile` (existing).
- Produces: `_serde.build_tile(raster_bytes, driver, cellid=0) -> Dict` now returns an **8-field v2-materialized dict** (all provenance fields present and NULL, `raster` set, `metadata` with driver/width/height/count). Signature unchanged, so all 49 call sites keep working. This is the shared constructor every later task relies on.

**Why this task is first:** every UDF/UDTF conversion in Tasks 2-4 depends on `build_tile` already returning the 8-field shape. Flipping a `@f.udf` decorator to `V2_TILE_SCHEMA` while `build_tile` still returns 3 keys makes Spark reject the row. So `build_tile` moves first, in isolation, proven by a unit test, before any decorator flips.

- [ ] **Step 1: Write the failing test**

Add to `python/geobrix/test/pyrx/test_core_virtual_tile.py`:

```python
def test_build_tile_returns_v2_materialized_shape():
    """build_tile emits the 8-field v2 struct: raster set, provenance NULL,
    metadata carries driver/width/height/count (computed by opening the raster)."""
    import rasterio
    import numpy as np
    from io import BytesIO
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    # a tiny in-memory 4x3 single-band GeoTIFF
    buf = BytesIO()
    profile = dict(driver="GTiff", height=3, width=4, count=1, dtype="uint8")
    with rasterio.io.MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.zeros((1, 3, 4), dtype="uint8"))
        raster = mf.read()

    d = _serde.build_tile(raster, "GTiff", 7)

    # exactly the 8 v2 fields, exact names
    assert set(d.keys()) == {f.name for f in V2_TILE_SCHEMA.fields}
    assert d["cellid"] == 7
    assert d["raster"] == raster
    # provenance fields NULL for a materialized tile
    for prov in ("path", "window", "clip_polygon", "clip_crs", "crs"):
        assert d[prov] is None, f"{prov} should be NULL on a materialized tile"
    # metadata computed by opening the raster (regression guard: NOT dropped)
    assert d["metadata"]["driver"] == "GTiff"
    assert d["metadata"]["width"] == "4"
    assert d["metadata"]["height"] == "3"
    assert d["metadata"]["count"] == "1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_virtual_tile.py::test_build_tile_returns_v2_materialized_shape --log v2-task1.log`
Expected: FAIL — `build_tile` currently returns a 3-key dict `{cellid, raster, metadata}`, so `set(d.keys())` != the 8 v2 field names (and the `d["path"]` access raises `KeyError`).

- [ ] **Step 3: Repurpose `build_tile` to emit v2**

In `_serde.py`, replace the body of `build_tile` (keep the open-and-compute metadata logic; return via `VirtualTile`):

```python
def build_tile(raster_bytes: bytes, driver: str, cellid: int = 0) -> Dict:
    """Construct a **v2-materialized** tile struct dict from raster BINARY content.

    Opens the raster to record driver/width/height/count in ``metadata`` and
    returns the 8-field ``V2_TILE_SCHEMA`` shape with ``raster`` set and every
    provenance field (``path``/``window``/``clip_polygon``/``clip_crs``/``crs``)
    NULL — the canonical materialized tile. Nothing in the light tier emits the
    legacy 3-field struct anymore.
    """
    raster = bytes(raster_bytes)
    with open_tile(raster) as ds:
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
    return VirtualTile(cellid=int(cellid), raster=raster, metadata=meta).to_row()
```

Add the import at the top of `_serde.py` (near the existing imports):

```python
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
```

Guard against a circular import: `virtual_tile.py` must not import `_serde`. Verify with
`grep -n "import _serde\|from.*_serde" python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py` — expected: no output. If there IS a cycle, do the import lazily inside `build_tile` instead (function-local `from ... import VirtualTile`).

Also update the `_serde.py` module docstring (lines 1-15): change "The legacy `TILE_SCHEMA` below covers only the three-field subset used by older build_tile / open_tile helpers." to note `build_tile` now emits v2 and the legacy constant is removed (it will be, in Task 5).

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_virtual_tile.py::test_build_tile_returns_v2_materialized_shape --log v2-task1.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py python/geobrix/test/pyrx/test_core_virtual_tile.py
git commit -m "feat(pyrx): build_tile emits 8-field v2-materialized tile"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 2: Flip all 41 `@f.udf(_serde.TILE_SCHEMA)` declarations to `V2_TILE_SCHEMA`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (41 decorator lines; see Step 1 for the grep)

**Interfaces:**
- Consumes: `build_tile` now returns v2 (Task 1); `V2_TILE_SCHEMA` (already imported in functions.py — verify).
- Produces: all 41 scalar tile-returning UDFs now declare `@f.udf(V2_TILE_SCHEMA)` and (via `build_tile`) return 8-field rows. The registered SQL functions in `_sql_tile_ops` inherit the widened schema.

**Why bundled as one task:** this is a single mechanical find-and-replace of one exact string, uniform across 41 sites, with one shared test cycle. Splitting it per-function would be 41 identical reviews of a one-token change — the opposite of right-sizing. The op bodies already call `build_tile` (proven v2 in Task 1), so no body edits are needed.

- [ ] **Step 1: Confirm the exact site count before editing**

Run: `grep -cE "@f\.udf\(_serde\.TILE_SCHEMA\)" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
Expected: `41`. If the number differs from 41, STOP and reconcile — the scope is grep-authoritative; a different count means the source moved and the plan's assumptions need re-checking.

- [ ] **Step 2: Apply the replacement**

Replace every occurrence of `@f.udf(_serde.TILE_SCHEMA)` with `@f.udf(V2_TILE_SCHEMA)` in `functions.py`. (These are all bare decorator lines; the replacement is unambiguous. Use an editor replace-all scoped to this file.)

Verify `V2_TILE_SCHEMA` is imported in functions.py:
Run: `grep -n "V2_TILE_SCHEMA" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py | head -1`
Expected: an import line (it is imported at line 46 — `from ...core.virtual_tile import V2_TILE_SCHEMA, VirtualTile`). If absent, add it.

- [ ] **Step 3: Verify no scalar-UDF legacy declarations remain**

Run: `grep -cE "@f\.udf\(_serde\.TILE_SCHEMA\)" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
Expected: `0`.

- [ ] **Step 4: Run the affected pyrx suites**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log v2-task2.log`
Expected: the tile-op / accessor / clip-reproject / bandmath / focal / indices / cog suites pass. Some tests that previously asserted a 3-field shape will now FAIL — that is expected and is fixed by folding them into G1 in Task 6; note the failing node IDs in the report but do NOT patch them here (they are addressed structurally). Tests asserting real op VALUES (bytes, pixel results) must still pass — a value failure means a body regressed and must be investigated.

- [ ] **Step 5: Ad-hoc probe (throwaway, not committed) — confirm real 8-field output**

Write a temporary probe to `scratchpad/probe/task2.py` (gitignored) that registers pyrx, loads a fixture tile, and prints `.asDict().keys()` for `rst_clip`, `rst_resample`, `rst_asformat`, `rst_slope`, `rst_getsubdataset`. Run it in Docker:
`bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 /root/geobrix/scratchpad/probe/task2.py"`
Expected: each prints all 8 v2 field names; materialized ops show `raster` set and `path`/`window`/etc. `None`. Delete the probe after (do NOT commit it).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
git commit -m "feat(pyrx): scalar tile UDFs emit 8-field v2 (align light SQL to heavy)"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 3: Flip the 7 tile-emitting `@udtf(returnType=_serde.TILE_SCHEMA)` declarations to `V2_TILE_SCHEMA`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (7 `@udtf` decorator lines: `_RstSeparateBandsUDTF`, `_RstRetileUDTF`, `_RstToOverlappingTilesUDTF`, `_RstMakeTilesUDTF`, `_RstH3TessellateUDTF`, `_RstQuadbinTessellateUDTF`, `_RstBngTessellateUDTF`)

**Interfaces:**
- Consumes: `build_tile` returns v2 (Task 1). The UDTFs `yield _serde.build_tile(...)`, so their yielded rows are already 8-field once Task 1 lands — the decorator's declared `returnType` must match or Spark rejects the rows.
- Produces: the 7 tile-emitting fan-out UDTFs now declare and yield v2. `separatebands`, `retile`, `tooverlappingtiles`, `maketiles`, `h3_tessellate`, `quadbin_tessellate`, `bng_tessellate` all emit v2 tiles.

**Why separate from Task 2:** UDTFs are a different Spark surface (`@udtf(returnType=...)` + `spark.udtf.register`), tested via streaming/generator behavior, and a reviewer could reasonably approve the scalar-UDF flip while scrutinizing the UDTF row-shape change separately. The flat/non-tile UDTFs (`_RstPolygonizeUDTF`, `_RstXyzPyramidUDTF`, `_RasterToGridUDTF` and its variants, `_RstXyzPyramidUDTF`) return `z,x,y,bytes` / grid-cell schemas and are explicitly NOT touched.

- [ ] **Step 1: Confirm the exact UDTF site count**

Run: `grep -cE "@udtf\(returnType=_serde\.TILE_SCHEMA\)" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
Expected: `7`. If different, STOP and reconcile (grep-authoritative).

- [ ] **Step 2: Apply the replacement**

Replace every `@udtf(returnType=_serde.TILE_SCHEMA)` with `@udtf(returnType=V2_TILE_SCHEMA)` in `functions.py`. Also update the nearby comment "Each UDTF row IS the tile struct (TILE_SCHEMA: cellid, raster, metadata)." to reference the v2 8-field struct.

- [ ] **Step 3: Verify no tile-UDTF legacy declarations remain**

Run: `grep -cE "@udtf\(returnType=_serde\.TILE_SCHEMA\)" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
Expected: `0`.

- [ ] **Step 4: Run the UDTF/fan-out suites**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log v2-task3.log`
Expected: retile / maketiles / tessellate / separatebands suites pass with the v2 row shape (they consume `build_tile`, already v2). As in Task 2, any test asserting the old 3-field UDTF row will fail and is folded into G1 in Task 6 — note IDs, don't patch here. Value/behavior assertions must still pass.

- [ ] **Step 5: Ad-hoc probe (throwaway) — confirm UDTF yields 8-field rows**

Extend `scratchpad/probe/task3.py` to run one tile-emitting UDTF (e.g. `gbx_rst_retile` or `rst_maketiles`) over a fixture and print the first row's field names. Run via `gbx:docker:exec`. Expected: 8 v2 field names. Delete after.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
git commit -m "feat(pyrx): tile-emitting UDTFs yield 8-field v2 tiles"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 4: Delete the legacy `_serde.TILE_SCHEMA` constant

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py` (remove the `TILE_SCHEMA = StructType([...])` block, lines ~30-36)

**Interfaces:**
- Consumes: nothing references `_serde.TILE_SCHEMA` after Tasks 2-3 (all decorators flipped). `build_tile` (Task 1) no longer uses it.
- Produces: the legacy 3-field output schema constant no longer exists; any future reintroduction of a 3-field tile output is a hard `AttributeError`/`NameError` at import.

**Why last among the code tasks:** the constant can only be deleted once zero references remain. Tasks 2-3 remove the `@f.udf`/`@udtf` references; this task removes the definition and proves nothing else imports it.

- [ ] **Step 1: Prove no references remain**

Run: `grep -rn "\bTILE_SCHEMA\b" python/geobrix/src/databricks/labs/gbx/pyrx/ python/geobrix/test/ | grep -v "V2_TILE_SCHEMA"`
Expected: matches ONLY on the definition line in `_serde.py` (and possibly test files asserting its absence — none yet). If any production `.py` still references bare `TILE_SCHEMA`, STOP — Tasks 2-3 are incomplete.

- [ ] **Step 2: Delete the constant**

Remove the `TILE_SCHEMA = StructType([...])` block from `_serde.py`. Keep `open_tile`, `build_error_tile`, and the now-v2 `build_tile`. Ensure no import of `TILE_SCHEMA` remains in `_serde.py` or its `__init__` re-exports.

- [ ] **Step 3: Verify the package imports cleanly**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 -c 'import databricks.labs.gbx.pyrx.functions; import databricks.labs.gbx.pyrx._serde; print(\"import ok\"); print(hasattr(__import__(\"databricks.labs.gbx.pyrx._serde\", fromlist=[\"x\"]), \"TILE_SCHEMA\"))'"`
Expected: `import ok` then `False` (the attribute is gone).

- [ ] **Step 4: Run the pyrx suite (regression)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log v2-task4.log`
Expected: no NEW import/collection errors; same failures as Task 3 (the to-be-folded per-fn schema tests), nothing worse.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py
git commit -m "refactor(pyrx): delete legacy 3-field TILE_SCHEMA constant"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 5: G1 — data-driven registry schema invariant (the standing guard)

**Files:**
- Create: `python/geobrix/test/pyrx/test_v2_tile_output_invariant.py`

**Interfaces:**
- Consumes: `databricks.labs.gbx.pyrx.functions.SQL_REGISTRY` (the dict `{**_sql_accessors, **_sql_tile_ops, **_sql_aggregators}` at functions.py:6596); `V2_TILE_SCHEMA`.
- Produces: one parametrized test that enumerates the ACTUAL registry and asserts each tile-returning member's UDF `returnType` IS `V2_TILE_SCHEMA`. New tile functions are covered automatically; a one-off regression to a non-v2 schema fails here.

**Why this design (anti-bloat):** instead of 48 per-function tests, ONE test parametrized over the registry the code itself uses. It cannot go stale (it reads the live map) and auto-covers additions. This is the migration's definition-of-done.

- [ ] **Step 1: Write the invariant test**

Create `python/geobrix/test/pyrx/test_v2_tile_output_invariant.py`:

```python
"""G1 — standing invariant: every registered tile-returning light-tier function
emits the 8-field V2_TILE_SCHEMA. Sourced from the live registration map, so new
tile functions are covered automatically and any regression to a legacy/non-v2
schema fails here. Replaces per-function schema assertions (folded in — see the
Task 6 consolidation)."""
import pyspark.sql.functions as f
import pytest

from databricks.labs.gbx.pyrx import functions as fns
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

# Registered light-tier functions that DO NOT return a tile struct — excluded by
# name with a one-line reason. Everything else in _sql_tile_ops must be v2.
_NON_TILE_TILEOP_NAMES = {
    # rst_tryopen returns a boolean, not a tile
    "gbx_rst_tryopen",
    # rst_sample returns a pixel value array, not a tile
    "gbx_rst_sample",
    # rst_contour returns a vector (geometry) result, not a raster tile
    "gbx_rst_contour",
}


def _tile_returning_udfs():
    """(name, udf) for every registered tile-RETURNING light UDF.

    _sql_tile_ops is the tile-op registration map; grouped _sql_aggregators are
    excluded (they return BINARY by convention, not a tile struct); a small
    name-keyed exclusion covers the non-tile members of _sql_tile_ops.
    """
    out = []
    for name, udf in fns._sql_tile_ops.items():
        if name in _NON_TILE_TILEOP_NAMES:
            continue
        out.append((name, udf))
    return out


@pytest.mark.parametrize("name,udf", _tile_returning_udfs(), ids=lambda v: v if isinstance(v, str) else "")
def test_registered_tile_op_emits_v2_schema(name, udf):
    """Each registered tile-returning UDF declares the 8-field v2 return type."""
    rt = getattr(udf, "returnType", None)
    assert rt is not None, f"{name}: UDF has no returnType (not an @f.udf?)"
    assert rt == V2_TILE_SCHEMA, (
        f"{name}: returnType is not V2_TILE_SCHEMA — "
        f"got fields {[fld.name for fld in rt.fields] if hasattr(rt, 'fields') else rt}"
    )


def test_v2_schema_field_contract():
    """Lock the exact v2 field names + order (guards an accidental schema edit)."""
    assert [fld.name for fld in V2_TILE_SCHEMA.fields] == [
        "cellid", "raster", "path", "window",
        "clip_polygon", "clip_crs", "crs", "metadata",
    ]
```

Note on the exclusion set: verify each excluded name's real return type before trusting the comment — run the Step 2 probe. If `rst_sample`/`rst_contour`/`rst_tryopen` are NOT in `_sql_tile_ops`, drop them from the set (a stale exclusion is harmless but should be accurate). If a member returns a tile but is excluded, that is a real miss — remove it from the set so it is covered.

- [ ] **Step 2: Verify the exclusion set against the live registry**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 -c 'from databricks.labs.gbx.pyrx import functions as fn; import pprint; pprint.pprint(sorted(fn._sql_tile_ops))'"`
Confirm the excluded names exist and genuinely return non-tile results (cross-check their `def` in functions.py: `rst_tryopen`→bool, `rst_sample`→array, `rst_contour`→geometry). Adjust `_NON_TILE_TILEOP_NAMES` to match reality.

- [ ] **Step 3: Run G1**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_v2_tile_output_invariant.py --log v2-task5.log`
Expected: PASS for all parametrized tile-op names (Tasks 2-3 made them v2) and the field-contract test.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/pyrx/test_v2_tile_output_invariant.py
git commit -m "test(pyrx): G1 data-driven v2-schema invariant over the registry"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 6: Fold existing per-function schema assertions into G1 (consolidation)

**Files:**
- Modify: whichever pyrx test files assert a 3-field tile shape (identified in Step 1; candidates: `test_virtual_aware_family.py`, `test_functions_spark.py`, and any Task 2/3 failing IDs)

**Interfaces:**
- Consumes: G1 (Task 5) now covers "does this function emit v2" for the whole registry.
- Produces: net standing-test count does NOT grow by per-function copies — redundant per-function schema assertions are removed, their coverage subsumed by G1. Value/behavior assertions are KEPT.

**Why:** the standing anti-bloat guidance — a migration guard duplicated per function is bloat once one data-driven invariant covers all. This task removes the now-redundant copies; it does NOT remove tests that assert real op values or virtual-tile behavior (those still matter).

- [ ] **Step 1: Identify redundant per-function schema assertions**

Run: `grep -rn "== _serde.TILE_SCHEMA\|len(.*keys()) == 3\|asDict().keys()) == {'cellid', 'raster', 'metadata'}\|3-field\|is None  # v1\|fieldNames() == \['cellid', 'raster', 'metadata'\]" python/geobrix/test/pyrx/`
Also collect the failing node IDs from the Task 2/3 logs (`test-logs/v2-task2.log`, `v2-task3.log`) — those failures are exactly the tests asserting the old shape.

- [ ] **Step 2: Triage each hit**

For each match, classify:
- **Pure schema/shape assertion** (asserts 3 fields / legacy schema / `path` absent) → REMOVE the assertion (or the whole test if that was its only purpose); G1 covers it.
- **Mixed** (asserts shape AND a real value) → keep the value assertion, delete only the shape line, and if it checked `keys() == {3-field}` update it to not over-constrain (or delete the shape line entirely).
- **Behavior test** (virtual→materialize, virtualize_dir roundtrip, real pixel value) → KEEP unchanged.

Make the edits. Do NOT introduce new per-function schema tests.

- [ ] **Step 3: Run the pyrx suite — everything green**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log v2-task6.log`
Expected: PASS (no schema-shape failures remain; G1 carries that coverage). Any residual failure must be a real value/behavior issue — investigate, do not mask.

- [ ] **Step 4: Confirm net test count did not grow with per-fn copies**

Run: `grep -rc "def test_" python/geobrix/test/pyrx/test_v2_tile_output_invariant.py` (the ONE new file — 2 test functions + parametrization, not 48). Confirm no new per-function schema test files were created.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/
git commit -m "test(pyrx): fold per-function tile-schema checks into G1 invariant"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

### Task 7: G2 — light≡heavy schema parity guard

**Files:**
- Create or extend: `python/geobrix/test/rasterx/test_tile_schema_parity.py`

**Interfaces:**
- Consumes: light `V2_TILE_SCHEMA` (pyrx); heavy's registered SQL functions produce the 8-field struct (`RST_ExpressionUtil.v2TileType`). Heavy tests require the built/staged JAR — see the precondition note.
- Produces: one parametrized test asserting light and heavy emit the byte-identical 8-field schema for a representative op set. Locks the tiers together so a future edit can't silently diverge them.

**Precondition (state plainly, do not treat as a defect):** heavy SQL registration needs a built+staged JAR. If no JAR is present, heavy registration fails with `UNRESOLVED_ROUTINE` — that is a missing build artifact, not a code defect. If the JAR is unavailable in the run environment, implement G2 to compare light's `V2_TILE_SCHEMA` field list against the **heavy schema field list asserted as a constant** (the 8 names from `v2TileType`), and mark the live-heavy-execution half `@pytest.mark.skipif` on JAR absence, with a comment. The schema-contract half runs everywhere.

- [ ] **Step 1: Write the parity test**

Create `python/geobrix/test/rasterx/test_tile_schema_parity.py`:

```python
"""G2 — standing parity guard: the light-tier v2 tile schema equals the heavy-tier
v2 tile schema, field-for-field. Prevents the two tiers silently diverging (the
class of bug that left light on a legacy 3-field struct while heavy was v2)."""
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

# The heavy-tier v2 tile schema field contract, mirrored from Scala
# RST_ExpressionUtil.v2TileType (kept in lock-step; a change on either side that
# breaks this is a real cross-tier divergence).
_HEAVY_V2_FIELDS = [
    "cellid", "raster", "path", "window",
    "clip_polygon", "clip_crs", "crs", "metadata",
]


def test_light_v2_schema_matches_heavy_field_contract():
    assert [fld.name for fld in V2_TILE_SCHEMA.fields] == _HEAVY_V2_FIELDS
```

- [ ] **Step 2: Run G2**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_tile_schema_parity.py --log v2-task7.log`
Expected: PASS.

- [ ] **Step 3: (If JAR available) add the live-execution parity half**

Only if the run environment has a staged JAR: add a `@pytest.mark.skipif(not _jar_available(), ...)` test that registers heavy, runs `gbx_rst_clip` on a fixture, and asserts its output DataFrame schema field names equal `_HEAVY_V2_FIELDS` — proving heavy's runtime schema matches the constant. If no JAR, skip this step (the field-contract test in Step 1 is the standing guard).

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/rasterx/test_tile_schema_parity.py
git commit -m "test(rasterx): G2 light-heavy v2 tile schema parity guard"
```
End the commit body with:
```
Co-authored-by: Isaac
```

---

## Post-plan verification (whole-branch, before handoff)

- [ ] `grep -cE "@f\.udf\(_serde\.TILE_SCHEMA\)|@udtf\(returnType=_serde\.TILE_SCHEMA\)" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` == `0`.
- [ ] `grep -rn "\bTILE_SCHEMA\b" python/geobrix/src/databricks/labs/gbx/pyrx/ | grep -v V2_TILE_SCHEMA` == empty (constant fully gone).
- [ ] G1 + G2 green; full `python/geobrix/test/pyrx/` and `test/rasterx/` (shim) suites green modulo the tracked pre-existing failures (none newly introduced).
- [ ] `bash scripts/commands/gbx-lint-python.sh --check` clean on the changed files (verify with Docker black, not just host — [[host-vs-docker-black-mismatch]]).
- [ ] Ad-hoc probes deleted from `scratchpad/`; no probe committed.
- [ ] Net standing-test count did not grow by per-function copies (one new invariant file with 2 test fns + one parity file with 1-2 test fns; redundant per-fn schema assertions removed).
