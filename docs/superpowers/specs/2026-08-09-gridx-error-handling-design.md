# Consistent GridX Error Handling (both tiers) — design

**Date:** 2026-08-09
**Status:** ratified (this doc), pending plan
**Scope:** GridX only — BNG + Quadbin + Custom SQL surfaces, heavy Scala (`gridx`)
+ light `pygx`. Item 2 of the CRS thread (order: VectorX → **GridX** → PROJ
grid-shift). Sibling of the shipped RasterX and VectorX error-handling work
(`2026-08-08-rasterx-error-handling-design.md`, `2026-08-08-vectorx-error-handling-design.md`).

## Goal

Bring GridX under the ratified GeoBrix error-handling axis and reconcile the three
grid families onto one contract. Today heavy GridX has **zero** error handling:
a single malformed cell id throws and **kills the whole stage** across the BNG family
(the fatal site is `BNG.parse`, `grid/BNG.scala:427-455`: an unguarded
`letterMap.find(_.contains(prefix)).getOrElse(throw …)` at `:429-434` on a bad 100km
prefix, followed by an unguarded `binDigits.dropRight(…).toInt` / `.drop(…).toInt` at
`:447-448` that raises `NumberFormatException` on a non-digit body). The
light tier has the same shape (`pygx/_bng.py` `parse()` raises `StopIteration` on a
bad prefix). This design closes that and makes the three families agree.

## The organizing principle (already ratified)

> **A bad / non-executable PARAMETER is a usage error → raise an exception.
> Bad DATA flowing through a column → degrade (NULL), never kill the stage.**

This is the same axis RasterX (shipped) and VectorX (shipped) live by. GridX is the
third and last of the metadata-carrier-less packages, so — like VectorX — **NULL is the
only data-degrade signal** a GridX function can carry.

## Constraints & context (verified against current code)

- **GridX has NO metadata carrier.** Functions return bare scalars (`Long`/`String`
  cell id, `Double` area/distance, `Int`), geometry (`BinaryType` WKB / `StringType`
  WKT), `Array[Long]` (kring/kloop/polyfill), or a plain struct (tessellate
  `{cellid, core, chip}`). There is nowhere to attach a reason (unlike RasterX's
  `tile.metadata.last_error`). NULL is the whole degrade vocabulary.
- **Heavy GridX has ZERO `safeEval`.** `grep safeEval|RST_ErrorHandler` under
  `gridx/` returns nothing. Each expression is an independent `InvokedExpression`
  with a companion `eval(...)` invoked reflectively via `invoke(<Companion>)` —
  the same rail VectorX's `ST_TransformCrs` uses. There is no shared error base and
  no `CrsOutcome`-style sealed type in gridx.
- **Today GridX raises or returns a sentinel, never NULLs on bad data.** Internal
  fallbacks use sentinels (`Try(h3Distance).getOrElse(0)` at `H3.scala:258`,
  `resolutionMap.find(…).getOrElse("")` at `BNG.scala:553`) — a sentinel, not a
  degrade. Every degenerate-input path either raises or produces a silently wrong
  value.
- **The three families already AGREE on out-of-range resolution — all raise.** BNG
  (`BNG.scala:400`), Quadbin (`Quadbin.scala:43,67` via `require`), Custom
  (`CustomGridSystem.scala:108`), and H3 (`H3.scala:108`). This is already correct
  under the axis (resolution is a parameter); **no reconciliation is needed there.**
  This narrows the handoff-doc claim that the families "disagree on every input class."
- **Where they genuinely DISAGREE: a finite coordinate outside the grid's valid
  extent.** BNG *encodes anyway* (`BNG.scala:305-314`) producing a computable-but-invalid
  cell for a point outside Great Britain; Quadbin *silently clamps* latitude to the
  web-mercator limit (`Quadbin.scala:48-62` `lonLatToTile`); Custom *raises*
  (`CustomGridSystem.scala:257,261`). NaN coords also split: BNG + Custom `require`-raise
  (`BNG.scala:329`, `CustomGridSystem.scala:250`), Quadbin clamps.
- **H3 is not a GridX SQL surface.** `grid/H3.scala` is a pure core library (no
  `gbx_*` registration); it is called internally by RasterX tessellation, already
  wrapped by RasterX's `safeEval`. The product's `h3_*` functions are native
  Databricks. So the GridX registered surface is **BNG + Quadbin + Custom only**.
- **Custom is the generic user-defined regular grid**, not a fourth named family:
  user supplies extent + root cell size + recursion factor; cells are bit-packed
  `Long`s with no string representation.
- **`crashExpressions` is a heavy RasterX mechanism; GridX has none, and this spec
  adds none** (per `strict-mode-workstream` — SQL binds positionally, no per-call knob).
  The raise-on-bad-parameter path is the "loud" signal.
- Prior thinking: `prompts/refactoring/2026-08-05-crs-loose-ends-and-consistency-handoff.md`
  Part 1.4 + 2.1 open-question 4; memories `strict-mode-workstream`,
  `light-rasterize-null-cellid-misalign` (now FIXED — see below).

## The contract

| Failure source | Treatment | Rationale |
|---|---|---|
| Out-of-range **resolution**; bad **grid spec / config**; wrong **call arity**; wrong **argument type** | **RAISE** (`IllegalArgumentException` / `IllegalStateException` heavy, `ValueError` light) | One value for the whole query; already consistent across families — keep as-is. |
| Malformed **cell-id DATA** — bad BNG prefix, non-digit BNG body, undecodable Custom cell bits | **NULL** | Per-row data; one bad cell id must not kill the stage. Retires the `BNG.parse` fatal site (both tiers). |
| **NaN / Inf coordinates** | **NULL** | Non-finite input is unambiguously bad data. BNG + Custom stop `require`-raising. |
| Finite coordinate **outside the grid's valid geographic extent** | **BNG + Custom → NULL**; **Quadbin → clamp** (see below) | A point that cannot be honestly gridded is bad data. BNG stops "encode anyway" (a silently-wrong cell — the same finite-nonsense failure VectorX just closed). Custom stops raising. |

### Quadbin latitude clamp — documented intended behavior, NOT a degrade

Quadbin keeps its web-mercator latitude clamp to **±85.05112878°** (the standard slippy-map
/ XYZ-tiler convention; `Quadbin.scala:28` `LAT_MIN`/`LAT_MAX`, applied in `lonLatToTile`
`:50`, which also clamps longitude to ±180°). A point at latitude 89° returns a **real cell
at +85.05112878°**, not NULL — deliberately different from BNG/Custom, because clamping is
what every web-mercator tiler does and users depend on it. This clamp is a **hard documentation
requirement**: it must appear in (1) this contract table, (2) the user-facing
`error-handling.mdx` GridX section, and (3) the `pygx/_quadbin.py` module docstring, so
the relocation is never a silent surprise.

## Per-shape degrade signal

GridX has more return shapes than VectorX (which was all-geometry), so "NULL is the
only signal" lands per shape:

| Shape | Functions (examples) | Bad-data degrade |
|---|---|---|
| **Scalar** | cellid (`pointascell`), distance, area, resolution | **NULL** |
| **Geometry** | centroid, aswkb, aswkt | **NULL** |
| **Array** | kring, kloop, polyfill (`Array[Long]`) | **NULL** (whole array — an empty array is ambiguous with a legit empty result) |
| **Struct** | tessellate (`{cellid, core, chip}`) | **NULL** struct |
| **Aggregators** | `CellUnionAgg`, `CellIntersectionAgg` | **skip the bad member**, aggregate the rest (mirrors RasterX aggregators) |
| **Explode UDTFs** | `kringexplode`, `kloopexplode`, `tessellateexplode`, `geometrykring/kloopexplode` | **zero rows** for that input (no metadata carrier → a lone all-NULL row is noise; `LEFT JOIN LATERAL` still surfaces which inputs produced nothing) |

## Mechanism

### Heavy (Scala)
A small **shared guard helper**, not a sealed type (GridX's five heterogeneous return
shapes make a `CrsOutcome`-style sealed type all ceremony, no benefit):

- Add `GridErrorHandler.safeEval[T](nullValue: T)(body: => T): T` in the gridx package.
  It runs `body`; on a **`NonFatal`** exception thrown from inside the body it returns
  `nullValue` (the shape's null: `null` for `UTF8String` / `Array[Byte]` / `Array[Long]` /
  struct row, a boxed `null` for numeric scalars so Catalyst sees SQL NULL); fatal
  `Throwable` (OOM, StackOverflow) propagates.
- **Parameter errors MUST raise BEFORE the guarded body** — this is the load-bearing rule,
  and the exact trap VectorX had: a `safeEval` that wraps *everything* would swallow a
  bad-resolution or bad-grid-spec exception and turn a usage error into a silent all-NULL
  column. Mirroring how VectorX resolves the CRS parameter before the transform: resolve
  the resolution, validate the grid spec, and check arity FIRST (these already raise
  today), then wrap ONLY the data-touching work (cell-id parse, coordinate encode/decode,
  geometry build) in `safeEval`. The guard's scope is the data path, never the parameter
  path.
- The fatal `BNG.parse` path (`BNG.scala:429-434` prefix throw + `:447-448` `.toInt` on the
  digit body) is moved inside the guarded region so a malformed cell id returns the shape's
  null instead of killing the stage.
- Aggregators guard per-member `rowToTile`/accumulation so a bad member is skipped, not
  fatal (they bypass any guard today).
- Explode UDTFs guard the per-input generation so a bad input yields an empty iterator
  (zero rows).

### Light (pygx)
Mirror the heavy contract with a never-error parse boundary:

- Add per-family `_parse_*_safe` helpers (the analogue of pyvx's `_parse_geom_safe`) at
  each cell-id / coordinate boundary in `_bng.py`, `_quadbin.py`, `_custom.py`, returning
  `None` on bad data (retiring the `StopIteration` in `_bng.py` `parse()`).
- Keep the existing `ValueError` raises for parameter errors (resolution range, grid
  config, arity) — they are already correct.
- The scalar/geometry/array/struct UDFs return `None`; the aggregator UDFs skip the bad
  member; the explode `@udtf`s yield nothing for a bad input.
- Quadbin keeps its clamp (documented).

### No new knob
Consistent with `strict-mode-workstream`: no `strict=`/crash argument is added to GridX.
A loud-signal switch across all packages is that separate future workstream.

## Docs — GridX section in the existing Error Handling page

- Extend `docs/docs/api/error-handling.mdx` with a **GridX** section after RasterX and
  VectorX. Lead with the shared axis, then: bad cell-id data → NULL, bad resolution/spec
  → raise, and the **Quadbin clamp** (explicit — out-of-range latitude is relocated to
  ±85.05112878°, not NULL'd, unlike BNG/Custom).
- User-facing voice, no internal planning vocabulary (per `user-facing-docs-voice`; QC
  `internals-leak` check).

## Testing / acceptance

Cross-tier parity is the spine — same degenerate input, same signal on both tiers.

1. **Shared degenerate corpus per family:** (a) malformed cell id — bad BNG prefix
   (`"!!"`), non-digit BNG body; undecodable Custom cell bits; (b) NaN / Inf coordinate;
   (c) finite out-of-extent coordinate (BNG point outside GB; Custom point outside grid;
   Quadbin latitude > 85.0511°); (d) out-of-range resolution (must RAISE); (e) wrong
   argument type (must RAISE).
2. **Per-behavior assertions (both tiers):**
   - Bad cell-id data (a) → **NULL**, no raise. Explicit regression: `BNG.parse("!!")` and
     a non-digit body return NULL through the registered function, no longer killing the
     stage (the fatal-site fix).
   - NaN/Inf (b) → **NULL** on BNG + Custom (no longer a `require` raise).
   - Out-of-extent (c) → **NULL** on BNG + Custom; **clamped cell (not NULL)** on Quadbin.
   - Bad resolution / bad type (d, e) → **RAISE** with a clear message.
   - Aggregators: a group containing one bad member still produces the aggregate over the
     valid members.
   - Explode UDTFs: a bad input cell id yields **zero rows** (row count parity across tiers).
3. **Re-confirm the RasterX `*_rasterize_agg` null-cellid alignment tests stay green**
   (`test/pyrx/test_{h3,quadbin,bng}_rasterize_agg.py`) — see "Already fixed" below.
4. **Where run:** heavy Scala suites + `gbx:test:python` (pygx) in the `geobrix-dev`
   container via the `gbx:*` palette; cross-tier parity via the JAR-staging integration
   gate (`gbx:test:parity`). Real cell ids / coordinates, not mocks.
5. **Docs (required deliverable, not optional):** the `error-handling.mdx` GridX section is
   authored as part of THIS work — the implementation is not complete until the page is
   updated. Page builds clean (`npm run build`, no broken-link warnings); the Quadbin clamp
   is documented in all three required places (contract-table equivalent prose on the page,
   the page's GridX section, and the `_quadbin.py` docstring). This is the last surface of
   the "bad parameter → exception, bad data → NULL" story and must ship with the code.

## Already fixed — verified, not re-worked

The RasterX `rst_{h3,quadbin,bng}_rasterize_agg` **null-cellid misalignment** (folded into
scope by the user) is **already fixed** in current code (verified 2026-08-09). All three
light UDFs zip cellid+value together THEN filter (`pairs = [(c, v) for c, v in
zip(cellid, value) if pd.notna(c)]`, `pyrx/functions.py` ~5666/5758/5836), and dedicated
interleaved-null regression tests exist per family (`test/pyrx/test_{h3,quadbin,bng}_rasterize_agg.py`,
the `*_value_alignment` tests). It was resolved during the `rasterize_agg` → `*_rasterize_agg`
rename. This spec records it as **verified fixed** and re-runs those tests in the test pass;
no re-implementation.

## Out of scope

- **H3 core-lib hardening** (`grid/H3.scala`) — no SQL surface; already behind RasterX's
  `safeEval`.
- **PROJ grid-shift / custom datum grids** (item 3 of the CRS thread — its own spec).
- A `crashExpressions`-style knob or any new per-call `strict`/`check` parameter for GridX
  (deferred to `strict-mode-workstream`).
- The non-CRS bug track from the handoff (the `rst_maketiles` broken wrapper, `rst_slope`
  param divergence, SQL-arity portability, `tileExpr`→`tile` rename, signature-parity CI).
- The `geom-aware kring/kloop → h3 + quadbin` port (`geom-aware-kring-kloop-h3-quadbin`).

## Outcome

Every GridX function (BNG + Quadbin + Custom, both tiers) degrades bad geometry / cell-id
data to NULL and raises a clear error for a bad parameter — uniformly. The `BNG.parse`
stage-killer is gone (malformed cell id → NULL on both tiers), NaN/out-of-extent
coordinates degrade consistently (BNG + Custom → NULL), Quadbin's documented latitude clamp
is preserved and made explicit, and the Error Handling docs page gains a GridX section that
completes the "bad parameter → exception, bad data → NULL" story across all three GeoBrix
packages.
