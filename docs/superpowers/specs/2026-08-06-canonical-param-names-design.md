# Canonical Parameter Names — Design (Spec 1a: Decide & Ratify)

**Date:** 2026-08-06
**Status:** Design — ready for review
**Branch:** `beta/0.5.0`

## Context

`DESCRIBE FUNCTION EXTENDED gbx_*` renders `name(usageArgs) - description`. As of v0.5.0
(`7ce35781`) the `usageArgs` signature is **derived** from Scala case-class fields +
`builder()` arity, and written to `src/main/resources/com/databricks/labs/gbx/function-info.json`.
The derivation strips the `Expr` suffix and converts camelCase → snake_case.

This exposed a latent problem: for many functions the derived name (published to
`DESCRIBE FUNCTION`) disagrees with the Scala wrapper, the heavy-Python shim, the
light-Python binding, and the docs `**Signature:**` line — **up to five surfaces can
disagree with each other for one function**. A 2026-08-05 inventory
(`prompts/refactoring/2026-08-05-param-naming-inventory.md`, 180 functions × 5 surfaces)
ranked the divergences (R1–R6, N1–N10) and is judged a sufficient, accurate foundation.

This work is split:

- **Spec 1a (this doc):** decide and *ratify* the canonical parameter-name table, with
  the rulings and their rationales. Produces the design doc + a checked-in canonical-names
  fixture. Judgment-heavy, low-volume.
- **Spec 1b (next):** execute the renames across all name-bearing surfaces in lockstep,
  driven entirely by the fixture. Mechanical, high-volume.
- **Spec 2 (later):** description population + three-element (`signature` / `description` /
  `primary_example`) docs↔`DESCRIBE` sync + re-validation sweep — built on the now-correct names.
- **Spec 3 (later):** multi-language per-function tabs (SQL / Python-light / Python-heavy /
  Scala-heavy) + named-parameter support in the bindings.

Spec 1a does **not** rename anything. It settles *what the names should be*.

## Governing principle

**The type-accurate name wins; mismatched surfaces align to it.** Where a parameter name
encodes a real *type* or *direction* distinction, that name is canonical, and the surfaces
that flattened it are the ones that get fixed. This is the existing
`param-naming-domain-distinctions` ruling, confirmed here against source.

Per-tier casing is unchanged — camelCase in Scala, snake_case in SQL/Python — **except
`cellid`**, a deliberate single-token exception (see R4).

## The canonical name table (ratified rulings)

| # | Rule | Canonical name | Applies to | Surfaces to fix | Rationale |
|---|---|---|---|---|---|
| **R1** | tile expression | **`tile`** | ~113 raster functions | Scala case-class fields + wrappers (`tileExpr`→`tile`); Python/SQL already `tile` | Mechanical, no type ambiguity; Scala-internal — Python/SQL unaffected |
| **R2** | ✅ applied | `size_in_mb` | `rst_maketiles` | — (done in `5ddf4efb`) | Fixed the tile-dimensions-vs-MB-budget bug |
| **R3** | slope scale | **`xscale` + `yscale`** | `rst_slope` | heavy grows anisotropic to match light | Ruling: anisotropic is canonical; matches GDAL x/y scaling and light's current surface |
| **R4a** | bare cell id | **`cellid`** | single-cell-id functions | all tiers | one lowercase token, identical Scala/Python/SQL (see "cellid" below) |
| **R4b** | two bare ids | **`cellid1` / `cellid2`** | `BNG_Distance`, `BNG_EuclideanDistance`, etc. (`c1`/`c2`) | all tiers; retire `c1`/`c2`, `cell_a`/`cell_b` | same token rule |
| **R4c** | chip struct | **`chip`** — `left_chip` / `right_chip` / `input_chip` | `BNG_CellIntersection`, `BNG_CellUnion`, `_agg` forms | fix heavy-Python `cell_id1/2`→chip; light `left`/`right`→chip | Input is the chip STRUCT `{cellid, core, chip}`, not a scalar — `BNG_CellUnion.scala:20` reads `leftChip.dataType.asInstanceOf[StructType]`. Naming it `cellid` would misdescribe the type. |
| **R4d** | output struct fields | **`cellid`** | explode / output structs currently `cellId` | Scala `elementSchema`, docs signatures, result-schema break | "cellid everywhere" — one token on outputs too |
| **R5** | resolution | **`resolution`** | ~18 functions (`res`) | all tiers | Mechanical; `res` is an unhelpful abbreviation |
| **R6** | output CRS | **`out_srid` / `out_crs`** | 7 output-producing functions | heavy (`srid`→`out_srid`); light already correct | Names the OUTPUT CRS; bare `srid` is ambiguous vs the input geom's embedded SRID. Direction distinction preserved. |
| **N1** | geometry arg | **`geom`** | `gbx_st_*` geom-accepting functions (`geomWkb`/`geom_wkb`) | Scala, heavy+light Python, docs; **update CLAUDE.md canonical example** | The param accepts WKB/EWKB/WKT/EWKT (`geom-input-consistency-across-st`), so `_wkb` is a misnomer. Bare `geom` is honest. |
| **N9** | point arrays | **`points_array` / `breaklines_array`** | `st_triangulate`, `st_interpolateelevation*` | fix heavy-Python `*_geom`→`*_array`; docs | Input is genuinely `ArrayType` — `ST_Triangulate.scala:57` does `pointsArray.dataType.asInstanceOf[ArrayType].elementType`, `:90` builds a MultiPoint from the array. `*_geom` (single geom) is the misleading surface. |

### On `cellid` (the single-token exception)

`cellid` is chosen as one lowercase token, **identical in Scala and Python** (not `cellId`,
not `cell_id`). It is deliberately excepted from CLAUDE.md's snake_case param rule
(CLAUDE.md:171–172, which currently names `cellId` as the form *not* to use and `cell_id`
as canonical). Four independent reasons converge on `cellid`:

1. It is already the GeoBrix **v1/v2 tile struct** field name.
2. It is already the struct field in source: `BNG.scala:35` `StructField("cellid", …)` (chip
   struct) and `RST_ExpressionUtil.scala:103` `StructField("cellid", LongType, …)` (raster
   tile), and `docs/docs/advanced/library-integration.mdx:56` documents tiles as having
   fields `cellid`, `raster`, ….
3. It matches what the **Databricks product** calls it.
4. It matches what **Mosaic** called it.

Naming the *parameter* `cellid` makes the parameter match the struct field it carries, on
every surface, and aligns with established external vocabulary. It also happens to survive
the parser's camelCase→snake_case step unchanged. Spec 1b must update CLAUDE.md:171–172 to
record `cellid` / `cellid1` / `cellid2` as canonical so the no-regression guard and future
contributors do not "correct" it back to `cell_id`.

**R4d expands scope to result schemas.** Harmonizing output struct fields (`cellId` in
`BNG_KRingExplode.scala:42`, `BNG_TessellateExplode.scala:26`, etc.) to `cellid` changes
*result schemas*, a compatibility surface distinct from parameter names. This is a breaking
change — acceptable in beta (no aliases) — and must be recorded in
`docs/docs/beta-release-notes.mdx`.

### Items explicitly not ruled here

- **R3, R4, N1, N9 were the judgment/semantics calls; all are now resolved in the table above.**
  R4c and N9 were resolved by reading source (struct vs scalar; ArrayType vs single geom),
  which *inverted* the 2026-08-05 inventory's original recommendation — source overrules the
  inventory. N1 (`geom`) was a ruling because it collides with the current CLAUDE.md canonical
  example.
- **The table above is the complete set of ratified rulings.** No R- or N-series item outside
  it is settled by 1a. If 1b execution surfaces a parameter name not covered by this table, it
  is flagged for a ruling, not guessed.

## Guardrails & CI invariants

The ratified table is worthless without machinery to hold it. Today only `usage_args` has a
no-regression guard, and `check-binding-parity.py` compares function names only — it cannot
see parameter lists.

- **Canonical-names fixture (the artifact).** A checked-in fixture (e.g.
  `docs/tests-function-info/canonical_param_names.txt`) is the single source the guards
  compare against, and the artifact Spec 2 syncs docs/`DESCRIBE` to. The `cellid`, `chip`,
  `out_*`, and `*_array` exceptions live here, each with a one-line rationale, so the guards
  *expect* them rather than flag them.

- **Invariant A — cross-tier parameter-name equality (hard gate).** For each function,
  heavy-Python and light-Python parameter names must match the fixture (both tiers are
  snake_case, so there is no casing excuse). This is the sharpest lens: the inventory found
  39 current violations. Enforced in CI.

- **Invariant B — arity-range parity (hard gate, scoped).** Light and heavy must accept
  compatible argument counts, closing the silent-positional-drop hazard
  (`signature-change-touches-seven-surfaces`). **Scope boundary:** B enforces parity for the
  functions Spec 1b actually touches, and *inventories with an explicit waiver baseline* the
  ~32 pre-existing light/heavy arity divergences the recon found — so they are tracked and
  cannot grow, without forcing an arity-standardization workstream into this spec. If the
  reviewer prefers B to block on all 32 up front, that is a scope change to record here.

- **SQL is positional-only — confirmed.** No `SupportsNamedArguments` anywhere in
  `src/main/scala/`. SQL callers are unaffected by renames (argument *order* is load-bearing,
  not names). Named-parameter support is Spec 3.

## Handoff to Spec 1b (execution)

Spec 1a terminates by committing **two artifacts**: (i) this design doc with the ratified
table + rationales, and (ii) the canonical-names fixture.

Spec 1b is a mechanical plan driven entirely by the fixture. It renames across the
name-bearing surfaces in lockstep:

1. Scala case-class fields
2. Scala public wrappers in `<pkg>/functions.scala` (arg count must match `builder()`)
3. heavy Python shim (`python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py`)
4. light Python binding (`.../pyrx|pyvx|pygx/functions.py`)
5. docs `**Signature:**` lines in `docs/docs/api/*-functions.mdx`
6. doc-test `*_sql_example()` bodies where they hardcode names
7. `function-info.json` — **auto-regenerated** from Scala via `gbx:docs:function-info`
   (not hand-edited)

**Sequencing:** each rule (R1 / R3 / R4 / R5 / R6 / N1 / N9) is an **independent commit**,
for small reviewable diffs and easy revert (`signature-change-touches-seven-surfaces`).
R4d's result-schema change and the `cellid` CLAUDE.md exception land with R4. Every commit
runs Invariants A + B and the affected package tests before it is considered done.
`beta-release-notes.mdx` records the `cellid` result-schema break.

## Out of scope (tracked, not addressed here)

- Description population and docs↔`DESCRIBE` three-element sync (Spec 2).
- Multi-language tabs + named-parameter bindings (Spec 3).
- The ~32 light/heavy arity divergences beyond Invariant B's waiver baseline
  (arity-standardization workstream).
- `pyrx` bare-CRS-string footgun (`pyrx-crs-bare-string-footgun`) — related but separate.
- Heavy GridX `.get` crash on malformed cell input — separate error-handling work.
