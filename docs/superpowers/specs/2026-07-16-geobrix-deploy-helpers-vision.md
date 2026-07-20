# GeoBrix Deploy & Genie-Space Helpers (forward-looking vision)

- **Date:** 2026-07-16
- **Status:** Vision / backlog. NOT MVP. Extract-from-example — do not build ahead of a real consumer. Release vehicle if shipped: **v0.4.2**.
- **Related:** [[genie-map-app]], [[genie-map-layer-dsl-vision]], [[vapor-eyes-lakeflow-sdp]], [[aibi-custom-geometry-choropleth]], [[gbx-wkb-to-native-st-bridge]], [[geobrix-onramp-to-databricks-native]], [[05x-roadmap-backlog]]

## The question

Could GeoBrix add functions that help a **non-developer** stand up the surrounding
Databricks plumbing for a spatial app — DABs, Genie Spaces, and similar — more easily?
Motivated by Genie Map (`setup_genie_space`, a "DAB configurer"), but the same helpers
would serve the vapor-eyes DAB deploy and any future map example.

## Assessment — commodity vs. differentiated

Split the surface honestly:

- **Generic DAB deploy and generic Genie-space creation are commodity.** Databricks owns
  DABs; Genie provisioning is an emerging (currently preview) platform capability.
  Reimplementing either wholesale pushes GeoBrix from "spatial processing library" toward
  "deployment framework" — scope creep, dependency bloat, and a maintenance burden chasing
  a **preview Genie API that will change**. It also cuts against GeoBrix's positioning as a
  complement that drives users *into* Databricks-native capabilities
  ([[geobrix-onramp-to-databricks-native]]), not a replacement for them.

- **The geospatial-readiness layer IS GeoBrix-shaped and defensible.** The single hardest,
  most error-prone thing for a non-developer putting a *map* on Databricks is making the
  data map-ready: native `GEOMETRY` at **SRID 4326** (not WKB bytes, not raw H3 ids),
  `ST_ASGEOJSON` companion columns, the AI/BI `geo(col)` choropleth contract. These are
  exactly the hard-won rules in [[gbx-wkb-to-native-st-bridge]] and
  [[aibi-custom-geometry-choropleth]]. A **"will this render on a map?" preflight +
  auto-fixer** is unique to a spatial library — no generic tool ships it.

**Architectural principle:** GeoBrix owns **map-readiness and geospatial-aware defaults**;
it **composes over** (never reimplements) generic DAB/SDK deploy and the Genie API. Thin
wrappers, not a framework.

## Logical grouping — priority order (user-set 2026-07-16)

User's two top problems to tackle: **(P1) "help me configure a (map) app to work against
this Genie space"** and **(P2) "help me generate the DAB yaml."** Both sit on the
**map-readiness (P0)** foundation, which is the GeoBrix-only differentiated value that makes
P1/P2 actually work. So:

**P0 — Map-readiness — the differentiated foundation (build first; everything depends on it).**
   - `check_map_readiness(table) -> Report` — lints a table for map rendering:
     SRID ≠ 4326, WKB-not-native geometry, missing `ST_ASGEOJSON` companion column,
     geometry-as-GROUP-BY-key (not orderable), H3 ids without a `hex_geom`, lon/lat out of
     range. Human-readable findings + fix suggestions.
   - `make_map_ready(table_or_df, ...) -> DataFrame/DDL` — auto-adds the `geo(col)` /
     `ST_ASGEOJSON(col)` companion field(s) and re-tags SRID to 4326. Encodes the exact
     AI/BI choropleth contract from [[aibi-custom-geometry-choropleth]].
   - This is the "80% of demo failures" fixer and the docs headline.

**P1 — Wire a map app to an EXISTING Genie space (the user's #1).**
   Note this is distinct from *creating* a space — the common case is "I have a space,
   make my app render its answers." The helper owns the **contract**, not the API.
   - `configure_genie_app(space_id, warehouse, ...) -> config` — resolve space/warehouse by
     name → id; emit the app's env/resource wiring (`DATABRICKS_GENIE_SPACE_ID`,
     `sql-warehouse-id`); validate the space is reachable.
   - `check_genie_space_map_ready(space_id) -> Report` — the geospatial teeth: does the space
     have instructions + example SQL that alias geometry as `*_geojson` via `ST_ASGEOJSON`
     so answers render on a map? Suggest the missing example-SQL/instructions.
   - `create_genie_space(tables, warehouse, ...) -> space_id` — OPTIONAL upsert for when no
     space exists, with the same geospatial-aware defaults (auto-draft geometry instructions,
     inject `ST_ASGEOJSON` example SQL, pull UC column comments). Thin wrapper over the
     **preview** Genie API — the volatile part; keep it optional/quarantined.

**P2 — Generate the DAB yaml (the user's #2).**
   - `generate_dab_yaml(resources) -> databricks.yml + app.yaml` — generate the bundle for a
     map app from a declared resource list (warehouse, genie-space, app name), resolving
     names → ids; a `deploy` convenience over the SDK/CLI. Largely template + SDK calls, but
     the geospatial value-add is baking in the correct app resources + the map-readiness
     preflight as a deploy gate ("won't deploy a space/table that won't render").

**Later — Serving helpers (travels with the layer DSL).**
   - Range-request / signed-URL serving for COG / PMTiles / MVT to the browser — the "how
     does a render-kind reach the map" concern from [[genie-map-layer-dsl-vision]]. Belongs
     with the DSL work, not this initiative.

Build order: **P0 → P1 → P2** (P1/P2 both lean on P0's readiness checks as their gate).

## Process (how to build it without over-building)

- **Extract from the example — do not build the framework first.** Genie Map's hand-built
  DAB (plan Task 12) and Genie-space curation (plan Task 18) ARE the reference
  implementation. Ship Genie Map by hand, then extract the helpers that generalize *exactly*
  that manual work — validated against two real uses (Genie Map + the vapor-eyes DAB) before
  anything is shipped. Speculative scaffolding built ahead of a consumer models the wrong
  thing.
- **Shape the MVP for clean extraction (cheap, do now):** keep the manual DAB resource list
  and Genie table/instruction set **declarative** (data, not imperative glue) in Tasks 12/18
  so lifting them into `generate_dab_yaml` (P2) / `configure_genie_app` (P1) later is
  mechanical. Task 12/18 already carry extraction notes to this effect.

## Placement — decision deferred (tradeoffs captured)

Where these eventually live is **left open** until Genie Map is built and the API surface is
validated. Options:

| Option | Pros | Cons |
|---|---|---|
| **A. Optional extra in the wheel** — a driver-side `gbx.deploy` (or `gbx.mapping`) module gated behind `geobrix[deploy]`, mirroring how `gbx.sample`/`stac`/`earthdata` downloaders already ship as extra-gated, driver-side Python helper classes. | Shippable, versioned, docs-prominent, reusable across examples + customers. Precedent exists (GeoBrix already ships non-spatial helper classes for examples). | Preview Genie API churn becomes a GeoBrix support burden; risks scope drift toward "deployment framework"; another extra to maintain across the 3 resolving envs ([[light-ci-lock-completeness]], [[new-feature-dep-and-tier-checklist]]). |
| **B. Examples-layer companion only** — helpers live under `apps/` or `notebooks/examples/` as reusable-but-unshipped scaffolding. | Lower support burden; no wheel bloat; free to track the preview API loosely. | Not a product surface; less docs-prominent; customers can't `pip install` it. |
| **C. Split** (likely landing zone) — ship the **differentiated, stable** map-readiness helpers (P0) + the app-wiring/readiness-check parts of P1/P2 (`configure_genie_app`, `check_genie_space_map_ready`, `generate_dab_yaml`) as `geobrix[deploy]` in the wheel; keep the **preview-API** `create_genie_space` as an examples-layer companion until the Genie API stabilizes. | Ships the defensible, low-churn value; quarantines the volatile preview-API surface. | Two homes to explain. |

Recommendation to revisit at decision time: **C**. Ship map-readiness (P0) plus the
stable app-wiring + yaml-generation helpers (P1 `configure_genie_app` /
`check_genie_space_map_ready`, P2 `generate_dab_yaml`) in the wheel under an optional extra;
hold the preview-API `create_genie_space` in the examples layer until the Genie API firms up.
Whatever ships in the wheel goes out as **v0.4.2**.

## Docs prominence (if this exists)

Headline narrative: **"from gold tables to a live, Genie-queryable map in a few notebook
cells"** — `check_map_readiness` → `make_map_ready` → `configure_genie_app` (against an
existing space) → `generate_dab_yaml` → deploy. Genie Map and vapor-eyes are the worked
examples. This is a strong on-ramp story and reinforces the complement-to-native positioning.

## Guardrails (per repo conventions)

- Any wheel changes ship as **v0.4.2** (version bump → also re-run release-pill PNG
  generators, see [[release_pill_regeneration.md]]).
- New deps (e.g. a heavier `databricks-sdk` pin) go behind the `[deploy]` extra and must be
  pinned across all resolving envs + hashed locks ([[new-feature-dep-and-tier-checklist]],
  [[light-ci-lock-completeness]]).
- Driver-side only; no Spark/executor coupling (mirrors the sample/stac/earthdata helpers).
- User-facing docs voice — no internal planning vocabulary.
