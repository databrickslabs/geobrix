# Design: Light-Tier pip Extras — DBR-Runtime-Matched Variants

**Date:** 2026-08-14
**Status:** Spec (architectural) — no pyproject or code changes in this document
**Scope:** `python/geobrix/pyproject.toml` — `[project.optional-dependencies]` restructuring
**Author:** mjohns-databricks

---

## 1. Problem and Motivation

> **Positioning — the principle this design encodes.** GeoBrix Light targets, and **defaults
> to, Databricks Serverless environments (currently environment v5)**: the default
> `geobrix[light]` extra is pinned to match the Serverless base and installs cleanly there
> with no user thought. **When running on a *classic* DBR cluster instead, additional
> dependencies must be accounted for** — each classic DBR generation (17 / 18 / 19 …) ships a
> different immutable base (different `grpcio-status`/`protobuf`, `idna`, `typing_extensions`,
> …), so a classic user pins the opt-in variant matching their runtime (`geobrix[light_dbr19]`).
> Serverless remains the zero-config default; classic is an explicit, per-DBR opt-in.

The single `[light]` extra today must satisfy every DBR runtime simultaneously. That forces
all pins to the **lowest-common-denominator**: whichever runtime imposes the tightest
constraint wins, and every other runtime inherits it. Currently, Serverless (environment v5)
sets this floor; classic DBR 17.3 shares the same constraint set.

There are two classes of harm:

### 1a. Lowest-common-denominator correctness cost

Pins like `idna<3.8`, `rio-tiler<9.3`, and `typing_extensions>=4.13` exist solely to
match the base-package state common to Serverless (environment v5) and classic DBR 17.3. A classic DBR 19 cluster doesn't need these
constraints — they just keep the user on older packages unnecessarily.

### 1b. Disjoint-constraint unsatisfiability (the crash case)

The `mapbox-vector-tile` constraint is in direct conflict across runtime generations:

- **Serverless (environment v5):** `grpcio-status` pins `protobuf<6`. Installing
  `mapbox-vector-tile>=2.2` drags in `protobuf>=6.31.1`, which violates this constraint.
  The Serverless `%pip` magic treats the resulting `ERROR:`-prefixed conflict report as a
  hard install failure. The current `mapbox-vector-tile>=2.1,<2.2` pin keeps protobuf at
  the base 5.29.4 and installs cleanly.

- **Classic DBR ≤ 18:** `grpcio-status` also pins `protobuf<6`. The same
  `mapbox-vector-tile>=2.2` conflict applies; the `>=2.1,<2.2` cap is required here too.

- **DBR 19:** `grpcio-status` now *requires* `protobuf>=6.31.1`. The `<2.2` cap forces
  protobuf to stay at 5.x, which conflicts with what DBR 19's grpcio-status demands. The
  result is a dependency conflict that pip silently or noisily "resolves" by downgrading or
  leaving an inconsistent state — **observed in the field as hangs and kernel crashes** on
  DBR 19 clusters when `geobrix[light]` is installed.

No single `mapbox-vector-tile` pin can satisfy both `protobuf<6` (required by Serverless (environment v5) and by classic DBR ≤ 18) and
`protobuf>=6.31.1` (DBR 19). These constraints are provably disjoint.

### 1c. The `test` extra mirrors the same root problem

The `[test]` extra pins `mapbox-vector-tile>=2.1,<2.2` for the same reason (it uses
`mapbox-vector-tile` as the MVT decode oracle). Any DBR-variant scheme will need
corresponding `test_dbr*` mirrors or a separate oracle strategy for CI; this is noted but
out of scope for this spec.

---

## 2. Goals and Non-goals

### Goals

- Allow `geobrix[light_dbr19]` to install on DBR 19 without triggering protobuf conflicts,
  hangs, or kernel crashes.
- Keep `geobrix[light]` (Serverless env v5 default) **entirely unchanged** — zero change to
  its resolved set.
- Establish the pattern and naming convention for future DBR-variant extras.
- Produce an architecture that is maintainable: each variant's divergence from the base is
  small and explicit.

### Non-goals

- **Not** a change to `geobrix[light]`'s resolved dependency set (Serverless behavior is
  unchanged).
- **Not** the actual pyproject.toml edits (that is the implementation plan).
- **Not** a change to any Python or Scala source code.
- **Not** automatic runtime detection — the user selects their variant explicitly.
- **Not** a solution for the `[test]` extra's protobuf pin (separate workstream).

---

## 3. Pin Taxonomy: Every `[light]` Dependency

All 21 entries in the current `[light]` extra, classified by whether the pin exists to
match a runtime's immutable base state or to express a GeoBrix functional requirement.

| Dep | Pin in pyproject | Class | Rationale (from inline comments + analysis) |
|---|---|---|---|
| `rasterio` | `>=1.3.0` | **Functional** | Core pyrx raster I/O; 1.3 API floor. |
| `shapely` | `>=2.0.0` | **Functional** | 2.x Shapely API used throughout (e.g., `get_coordinates`). |
| `numpy` | `>=1.24.0` | **Functional** | Array dtype handling; 1.24 floor for `numpy.dtypes`. |
| `pandas` | `>=2.0.0` | **Functional** | `pandas_udf` Arrow batch serialization; 2.x DataFrame API. |
| `pyarrow` | `>=11.0.0` | **Functional** | Arrow IPC/batch serialization for PySpark pandas UDFs. |
| `scipy` | `>=1.11.0` | **Functional** | Spatial interpolation, Delaunay/TIN; 1.11 API floor. |
| `numexpr` | `>=2.8.0` | **Functional** | Vectorised expression evaluation in raster math ops. |
| `typing_extensions` | `>=4.13` | **Runtime-base-compat** | *DBR 17.3 floor.* The base ships an older version; `rio_tiler/types.py` TypedDict raises `TypeError` without 4.13. Harmless on newer runtimes. |
| `rio-tiler` | `>=9.0,<9.3` | **Runtime-base-compat** | *Serverless env v5 / Python 3.12 cap.* 9.3.0 regressed PEP 728 `TypedDict(extra_items=...)` import on Python 3.12 even with `typing_extensions>=4.13`. 9.2.1 imports cleanly. Cap should be revisited when a fixed 9.3.x ships. |
| `httpcore` | `>=1.0.9` | **Runtime-base-compat** | *DBR base conflict floor.* DBR ships `httpcore 1.0.2` which pins `h11<0.15`; the `rio-tiler→httpx→httpcore` chain needs `h11>=0.16`. Forcing `>=1.0.9` resolves the on-cluster pip conflict. |
| `idna` | `<3.8` | **Runtime-base-compat** | *Serverless v5 base cap.* The Serverless base pins `idna 3.7`; without the cap, pip upgrades to a newer version and the Serverless notebook fires a "core Python package changed: idna" user warning. Nothing in the stack needs `>3.7`. |
| `rio-cogeo` | `>=7.0,<8` | **Functional** | COG encoder/decoder; 7.x API. `<8` is a precautionary forward cap (no 8.x exists yet). Cap is defensive/functional, not runtime-specific. |
| `h3` | `>=4.0,<5` | **Functional** | H3 grid ops; 4.x Python API (cell types differ from 3.x). |
| `quadbin` | `>=0.2,<0.3` | **Functional** | QuadBin grid ops; 0.2 API floor. |
| `pmtiles` | `>=3.4,<4` | **Functional** | PMTiles archive writer/reader; 3.x API. |
| `pyogrio` | `>=0.12,<1` | **Functional** | OGR-free vector reading for `*_gbx` vector DataSources; bundles its own libgdal. |
| `pyproj` | `>=3.6,<4` | **Mixed** | *Floor:* functional (CRS→srid/proj4, needed by pyogrio). *Cap:* runtime-base-compat — "same float-and-break risk as rio-tiler 9.3.0" (comment); no 4.x exists today so `<4` is a no-op but future-proofs against Serverless float-and-break. |
| `mapbox-vector-tile` | `>=2.1,<2.2` | **Runtime-base-compat** | *Serverless (environment v5) and classic DBR ≤ 18: protobuf<6 constraint.* 2.2.0 hard-requires `protobuf>=6.31.1`, conflicting with Serverless grpcio-status's `protobuf<6` pin. 2.1.x keeps protobuf at base 5.29.4. Functionally, the 2.x encode API (native-typed attributes) is present in both 2.1 and 2.2; the 2.1 floor is the real functional requirement, not the `<2.2` cap. |
| `scikit-image` | `>=0.22,<1` | **Functional** | Image processing ops in pyrx; 0.22 API floor. `<1` is precautionary. |
| `xarray-spatial` | `>=0.4,<1` | **Functional** | Spatial raster interpolation (slope, hillshade); 0.4 API. |
| `netcdf4` | `>=1.6,<2` | **Functional** | NetCDF-3/4/HDF5 reading (S5P, ERA5). `<2` is precautionary defensive cap. Comment flags it should be tightened if a future 1.x breaks on Serverless/Py 3.12. |

**Summary:** 5 definitive runtime-base-compat pins (`typing_extensions`, `rio-tiler` cap,
`httpcore` floor, `idna` cap, `mapbox-vector-tile` cap) + `pyproj`'s `<4` cap (partial).
The remaining 15 deps (plus `pyproj`'s floor and `mapbox-vector-tile`'s `>=2.1` floor) are
functional and stay invariant across all variants.

---

## 4. Extras Layout

### 4a. The pip additive-extras mechanic (why `light + delta` is not viable)

pip extras are **additive**: installing `geobrix[light,light_dbr19]` unions the requirement
sets of both extras. A union of `mapbox-vector-tile<2.2` (from `[light]`) and
`mapbox-vector-tile>=2.2` (from a hypothetical delta `[light_dbr19]`) is an empty
intersection — **pip will fail with an unsatisfiable constraint**. There is no way to
"override" a constraint in a dependent extra; extras only add requirements, never
override or relax them.

This rules out a design like `light_dbr19 = ["geobrix[light]", "mapbox-vector-tile>=2.2"]`.

### 4b. Option A — Standalone variants (user installs exactly one)

Each variant re-declares the full dependency set with its own runtime-matched pins. `[light]`
is untouched; `[light_dbr19]` is a completely independent list.

```toml
[project.optional-dependencies]
light = [
    # ... full list with Serverless pins ...
    "mapbox-vector-tile>=2.1,<2.2",
    "idna<3.8",
    ...
]
light_dbr19 = [
    # ... same functional deps, relaxed runtime pins ...
    "mapbox-vector-tile>=2.1",  # no <2.2 cap; protobuf 6 is expected on DBR 19
    # idna cap dropped (DBR 19 base manages its own idna)
    ...
]
```

**Pros:** Trivially simple; `[light]` is literally untouched (no structural sharing).
**Cons:** The shared functional list (~15 deps) is duplicated in N variants. Adding a new
functional dependency means touching every variant. Easy to drift.

### 4c. Option B — Shared base + per-variant delta (RECOMMENDED)

A private `[_light_base]` extra holds the ~runtime-agnostic deps. Each named variant
depends on `_light_base` plus its own small runtime-sensitive delta.

```toml
[project.optional-dependencies]
# Private base — not for direct user installation; named with _ prefix to signal that.
_light_base = [
    "rasterio>=1.3.0",
    "shapely>=2.0.0",
    "numpy>=1.24.0",
    "pandas>=2.0.0",
    "pyarrow>=11.0.0",
    "scipy>=1.11.0",
    "numexpr>=2.8.0",
    "mapbox-vector-tile>=2.1",  # functional floor only; cap is per-variant
    "rio-cogeo>=7.0,<8",
    "h3>=4.0,<5",
    "quadbin>=0.2,<0.3",
    "pmtiles>=3.4,<4",
    "pyogrio>=0.12,<1",
    "pyproj>=3.6,<4",
    "scikit-image>=0.22,<1",
    "xarray-spatial>=0.4,<1",
    "netcdf4>=1.6,<2",
]

# Serverless (env v5) — primary target, resolved set unchanged; classic DBR 17–18 also use [light]
light = [
    "geobrix[_light_base]",
    "typing_extensions>=4.13",
    "rio-tiler>=9.0,<9.3",
    "httpcore>=1.0.9",
    "idna<3.8",
    "mapbox-vector-tile>=2.1,<2.2",  # protobuf<6 constraint
]

# Classic DBR 19
light_dbr19 = [
    "geobrix[_light_base]",
    "typing_extensions>=4.13",  # floor still correct for DBR 19; harmless to keep
    "rio-tiler>=9.0",           # <9.3 cap not needed on DBR 19 (Python 3.12+ typing fixed)
    "httpcore>=1.0.9",          # floor still safe; DBR 19 may ship this or newer
    # idna cap dropped: DBR 19 manages its own idna; no notebook-warning risk
    "mapbox-vector-tile>=2.2",  # protobuf>=6.31.1 now required/expected on DBR 19
]
```

**How `[light]`'s resolved set stays identical:** The `_light_base` carries only the
functional floor for `mapbox-vector-tile` (`>=2.1`). The `[light]` variant re-adds
`mapbox-vector-tile>=2.1,<2.2`, which tightens the combined constraint to `>=2.1,<2.2` —
exactly what `[light]` resolved before. No change.

**Pros:** Only the ~5 runtime-sensitive pins live in each variant delta. Adding a new
functional dep means touching one place (`_light_base`). Adding a new runtime variant means
writing ~5 lines. Drift between variants is minimal and auditable.

**Cons:** The `_light_base` naming convention ("don't install this directly") is informal
— pip has no mechanism to mark an extra as private. Documentation and the compat matrix
must be clear about this.

**Fallback:** If the shared-base approach proves confusing to contributors or tooling,
Option A (standalone variants) can be adopted with no loss of correctness, at the cost of
per-variant duplication. Option A is always available as a mechanical rewrite of Option B.

---

### 4d. Umbrella `_all` extras (feature-complete, per runtime)

Beyond the runtime-core extras, a user who wants the *full* light-tier feature set today must
know to combine several: `geobrix[light,stac,vizx,overture]`. To remove that reasoning burden
— and to stop hand-picked partial installs from silently omitting a required dep (e.g.
`pmtiles`, which lives **only** in `[light]`; a minimal hand-picked install drops it and the
light import chain fails) — add feature-complete umbrellas, one per runtime:

- **`light_all`** = `geobrix[light]` + `geobrix[stac]` + `geobrix[vizx]` + `geobrix[overture]`
  — Serverless (environment v5), everything.
- **`light_dbr19_all`** = `geobrix[light_dbr19]` + `geobrix[stac]` + `geobrix[vizx]` +
  `geobrix[overture]` — classic DBR 19, everything.

Mechanically these are extras that **reference other extras**
(`light_all = ["geobrix[light]", "geobrix[stac]", "geobrix[vizx]", "geobrix[overture]"]`), so
the umbrella inherits the runtime-correct `light` / `light_dbr19` pins automatically plus the
feature deps. Each new `light_dbrNN` gets a matching `light_dbrNN_all`.

**`[databricks]` is NOT included in `_all`** (decision, confirmable): it is the Databricks SDK
integration, not a light-tier geo feature, and Databricks compute already provides its runtime.
Users add `[databricks]` explicitly when needed.

**Runtime-safety of the feature extras:** the *direct* deps of `stac` / `vizx` / `overture`
carry **no** protobuf / grpcio / mapbox-vector-tile coupling (verified), so they compose
cleanly with either runtime's `light`. The residual risk is **transitive** (a cloud SDK —
`botocore` / `earthaccess` / `planetary-computer` / `pystac-client` — pulling a
protobuf-adjacent dep). This is caught by resolving + hashing each `_all` variant's lock (§6);
if a feature extra turns out to carry a runtime-sensitive transitive pin, it gets its own
`_dbrNN` variant on the same base + delta pattern.

**Our own bench/gen installs** should likewise use the complete `[light_dbr19]` (or
`[light_dbr19_all]`) rather than a hand-picked minimal set — the runtime-matched extra is the
"don't reason about deps" install for CI/bench too, and avoids the `pmtiles`-omission class of
failure.

---

## 5. pyspark Is Not pip-Installed

No `[light]` variant (base, Serverless, or DBR-N) includes `pyspark` in its extras. pyspark
is provided by the cluster/Serverless runtime as a preloaded package — installing it via pip
on an active cluster would either conflict with the in-process Spark session or install a
redundant incompatible copy. The extras only pin the pyspark-runtime-*compatible* non-pyspark
deps (numpy, pandas, pyarrow, etc.). This is an invariant across all variants.

Note: `[project.dependencies]` does carry `pyspark>=4.0.0` as a base dep for local
development purposes (so `pip install geobrix` works off-cluster). This is separate from the
`[light]` extras and does not affect cluster installs where pyspark is pre-provided.

---

## 6. Per-Variant Hashed Lock Files

Each variant must have its own `requirements-<variant>-ci.txt` lock file, generated by:

```bash
uv pip compile --generate-hashes --python-version 3.12 \
    --output-file requirements-light-ci.txt requirements-light-ci.in
```

(substitute `light_dbr19` etc. for the target variant)

Per the repo's supply-chain pinning rules (`[[supply-chain-pin-and-hash]]`): all packages
must be exact-version + hash-pinned in CI. A range in `pyproject.toml` is not sufficient
for CI; the **lock file** captures the resolved, hashed set for reproducible installs.

**Maintenance cost:** Each new variant adds one `.in` source file and one hashed `.txt`
lock. On each release, all lock files must be regenerated (`uv pip compile` with the new
version). The version-bump checklist (`[[geobrix-version-bump-checklist]]`) must be updated
to include regeneration of each variant's lock.

The current lock structure (one file, `requirements-pyrx-ci.txt`) maps to the current
single `[light]` extra. Option B adds one lock file per variant:

```
requirements-light-ci.in        # Serverless (env v5) (current, renamed for clarity)
requirements-light-ci.txt       # generated, hashed
requirements-light-dbr19-ci.in  # DBR 19 variant
requirements-light-dbr19-ci.txt # generated, hashed
```

The `_light_base` extra is NOT locked separately — it is a build-time composition tool,
not an installable target.

---

## 7. First Concrete Variant: `light_dbr19`

### Why DBR 19 first

DBR 19 is the first runtime where `grpcio-status` requires `protobuf>=6.31.1`, making the
current `[light]` extra not just suboptimal but **actively harmful** (observed hangs and
kernel crashes). This is the only currently confirmed disjoint-constraint case.

### Concrete delta from `_light_base` / from Serverless `[light]`

| Dep | `[light]` pin | `[light_dbr19]` pin | Reason for change |
|---|---|---|---|
| `mapbox-vector-tile` | `>=2.1,<2.2` | `>=2.2` (no upper cap) | DBR 19 grpcio-status requires protobuf>=6.31.1; 2.2+ enables that. |
| `idna` | `<3.8` | *(omit cap)* | DBR 19 base manages idna; notebook-change warning doesn't apply. |
| `rio-tiler` | `>=9.0,<9.3` | `>=9.0` | The 9.3.x typing regression is a Serverless (environment v5) / Python 3.12 issue; verify on DBR 19's Python before removing entirely. Initially relax to `>=9.0,<9.4` to stay conservative. |
| `typing_extensions` | `>=4.13` | `>=4.13` | DBR 19 certainly ships 4.13+; floor is harmless and keeps rio_tiler import clean. Keep. |
| `httpcore` | `>=1.0.9` | `>=1.0.9` | DBR 19 ships a newer httpcore; floor is safe and harmless. Keep. |

The **only structurally necessary change** is the `mapbox-vector-tile` cap removal. The
`idna` cap removal is cleanliness. The `rio-tiler` cap relaxation is optional but
recommended. `typing_extensions` and `httpcore` floors are harmless to leave.

### Validation before declaring `light_dbr19` stable

1. On a DBR 19 classic cluster: `%pip install geobrix[light_dbr19]`
2. Verify pip outputs no `ERROR:`-prefixed conflict lines.
3. `import databricks.labs.gbx.rasterx.functions` — should succeed.
4. Run a smoke MVT encode: `gbx_st_asmvt` on a small vector DataFrame.
5. Verify `protobuf` version is ≥6.31.1 post-install (`import google.protobuf; print(google.protobuf.__version__)`).

---

## 8. Testing Approach

The primary validation mechanism is **cluster-side install testing**, not unit tests:

1. `%pip install geobrix[light]` on Serverless env v5 — must be clean (regression check).
2. `%pip install geobrix[light_dbr19]` on DBR 19 classic — must be clean (new target).
3. For each: read pip's resolver output (no `ERROR:` or `WARNING: pip's dependency resolver
   does not currently take into account all the packages ...` lines).
4. Run `gbx:test:python` against the installed wheel for the functional smoke of the affected
   subpackages (pyvx MVT path most critical for `mapbox-vector-tile`).

CI lock-based regression: `requirements-light-ci.txt` currently captures the Serverless
resolved set. After Option B restructuring, regenerate it and verify the resolved hash set
is bit-for-bit identical to the current lock (the `[light]` resolved set must not change).

---

## 9. Docs and Process Impact

### Installation documentation

The `docs/docs/installation.mdx` (or equivalent) must **lead its light-tier install section
with the Serverless-default positioning** — in user-facing voice — *before* the matrix, so a
reader immediately understands the default and when they must deviate. Required framing (copy,
adapt to page voice):

> **GeoBrix Light defaults to Databricks Serverless environments (currently environment v5).**
> On Serverless, `%pip install geobrix[light]` installs cleanly with no further thought.
> **When running on a *classic* DBR cluster, additional dependencies must be accounted for** —
> each classic DBR generation ships a different base runtime (different `protobuf`/`grpcio-status`,
> `idna`, `typing_extensions`, …), so the default `[light]` can conflict there (notably DBR 19).
> On classic, pin the extra that matches your DBR version from the table below.

Then the per-runtime compatibility matrix:

| Runtime | Install command | Notes |
|---|---|---|
| Serverless (env v5) | `%pip install geobrix[light]` | Default; protobuf 5.x base |
| Classic DBR 17.3–18.x | `%pip install geobrix[light]` | Same as Serverless |
| Classic DBR 19+ | `%pip install geobrix[light_dbr19]` | protobuf 6.x base |

The `_light_base` extra must **not** appear in user-facing docs — it is an internal
composition tool.

### Version-bump checklist

The `[[geobrix-version-bump-checklist]]` memory entry must be updated to include:
- Regenerate `requirements-light-ci.txt` (Serverless env v5 lock)
- Regenerate `requirements-light-dbr19-ci.txt` (DBR 19 lock)
- For each new DBR-N variant added in the future: add its lock to the checklist.

### CI

The `_LIGHT_TEST_DIRS` (from `[[light-ci-lock-completeness]]`) picks up tests via the
existing `requirements-pyrx-ci.txt` lock. After the restructure:
- Rename/create `requirements-light-ci.txt` (Serverless) as the primary CI lock.
- Add `requirements-light-dbr19-ci.txt` as a secondary CI job (can run on a lower
  cadence if DBR 19 clusters are expensive to provision).

---

## 10. Rollout

1. **This spec** — architectural decision, no code.
2. **Implementation plan** — pyproject edits, `.in` source files, lock regeneration,
   CI job wiring, docs update.
3. **Execution** — subagent-driven: (a) pyproject restructure + verify `[light]` lock
   unchanged, (b) `light_dbr19` delta + new lock + cluster smoke test, (c) docs update +
   checklist update.
4. **Ship** — included in the next 0.5.x release; compat matrix in release notes (user-
   facing language: "DBR 19 users should install `geobrix[light_dbr19]`").

There is no migration requirement for existing users: `geobrix[light]` behaviour is
unchanged.

---

## 11. Open Questions

| # | Question | Current stance |
|---|---|---|
| OQ1 | **Option A vs B:** Is the `_light_base` private-extra convention clear enough to contributors? If it creates confusion, Option A (standalone, duplicated) is always available with no correctness loss. | Recommend B; document the `_` convention explicitly in CLAUDE.md and the pyproject comment. Revisit if a contributor reports confusion. |
| OQ2 | **`rio-tiler<9.3` cap on DBR 19:** Is the 9.3.x typing regression fixed in DBR 19's Python 3.12 environment, or does it require `typing_extensions>=4.13` alone? | Cannot resolve without installing 9.3.x on DBR 19 and observing. Conservative initial cap `<9.4` recommended for `light_dbr19`; remove when verified. |
| OQ3 | **Future DBR 20+:** DBR 20 may introduce new base-package constraints. The variant pattern is established; defer `light_dbr20` until breakage is observed. | No action now. The architecture is the precedent. |
| OQ4 | **`[test]` extra:** The `test` extra's `mapbox-vector-tile>=2.1,<2.2` pin is the same root issue. Does the test suite need a `test_dbr19` mirror, or is an unambiguous test oracle (separate mock or version-conditional) a better answer? | Out of scope for this spec; tracked separately. |
| OQ5 | **`pyproj<4` cap:** Currently in `_light_base` (functional classification, defensive). If pyproj 4 ships and breaks, this cap must move into the Serverless delta rather than the base. Monitor. | Accept current placement; revisit if pyproj 4 announces. |

---

## Self-Review

**Placeholder scan:** No `TODO`, `TBD`, `FIXME`, or `???` markers in the spec body.
Open questions are numbered and bounded.

**Internal consistency:**
- The taxonomy table drives the Option B delta table (Section 7) — verified that only the
  5 runtime-base-compat deps appear in the `[light]` delta, and only 3 of those change in
  `light_dbr19`.
- "light's resolved set stays identical" claim in Section 4c is technically supported by
  the additive-extras mechanics explained in Section 4a.
- `pyspark` absence is stated in Section 5 and consistent with current `pyproject.toml`
  (pyspark is only in `[project.dependencies]`, not in any extra).
- Lock file naming in Section 6 is consistent with the repo's existing
  `requirements-pyrx-ci.txt` naming convention.

**Scope (single plannable spec):** The spec describes one coherent architectural decision
(extras restructuring + DBR-19 first variant). It does not prescribe implementation order
beyond the rollout section. It does not touch code. A single implementation plan can cover
all of it.

**Ambiguity fixed:**
- Clarified that `_light_base`'s `mapbox-vector-tile>=2.1` is the functional floor; the
  `<2.2` cap belongs to the `[light]` runtime delta only (section 4c example).
- Clarified that `[light]`'s pin union resolves to `>=2.1,<2.2` as before (section 4c
  explanation of why the resolved set is unchanged).
- Noted the `<9.3` question for DBR 19 as OQ2 rather than asserting a definitive answer.

**User-facing language check:** No wave numbers, subagent references, or internal planning
vocabulary in this document (it is under `docs/superpowers/`, not `docs/docs/`, so QC
internals-leak check does not apply; but kept clean anyway for good practice).
