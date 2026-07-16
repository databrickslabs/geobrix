# Genie Map App — Phase 0 + Phase 1 (MVP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt the `isaac_work/genie_map` kepler.gl + `@databricks/appkit` prototype into a deployable Databricks App (`apps/genie_map/`) that renders the vapor-eyes methane gold data — H3 hexagon layers (CH4 hotspots + wells density), wells/plume point layers, and a curated Genie NLP path — with implementer + slide-ware storytelling artifacts.

**Architecture:** Copy the prototype into `apps/genie_map/`, replace its build-time `VITE_*` taxi canonical-schema with a static **layer registry** (`config/datasets/vapor-eyes.ts` + per-layer SQL templates) featuring **density-aware dynamic H3 resolution**, point every layer at Lakeflow gold `geospatial_docs.vapor_eyes_lf`, add one new gold MV (`wells_enriched_latest`) to the vapor-eyes SDP, curate a Genie Space, and deploy via a DAB bundle wired to `gbx:app:*` commands.

**Tech Stack:** React 18 + TypeScript + kepler.gl 3.2.5; `@databricks/appkit` 0.41.6 (Node/Express); Vite 6; pnpm@10, node>=20; Databricks Asset Bundles (DAB); Lakeflow Declarative Pipeline (`pyspark.pipelines`); Databricks-native `st_*`/`h3_*` SQL.

## Global Constraints

- **App location:** new top-level `apps/genie_map/` on branch `apps/genie-map` (cut from `examples/vapor-eyes`, which carries the SDP + spec this depends on).
- **Data spine:** all gold tables in `geospatial_docs.vapor_eyes_lf`. Map-facing geometry is native `GEOMETRY` at **SRID 4326** — never raw WKB or H3 cell ids. Re-tag geometry that round-trips through an MV with `st_setsrid(..., 4326)` (SRID-0 is silently unrendered).
- **Exec context:** workspace `e2-demo-field-eng`, CLI profile **`oauth-fe`** (the default profile's PAT is dead). SQL warehouse = GeoBrix **`82e587bd93c6cbcf`**.
- **SDP gold conventions** (match `notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py`): `@dp.materialized_view`; no driver `.collect()`/`.rdd`/`spark.conf.set` (use window/crossJoin-broadcast); current well inventory = `wells_shl` filtered `__END_AT IS NULL`; no GeoBrix SQL needed in gold (pure native `st_*`/`h3_*`).
- **`gbx:*` command pattern:** each command is a `.md` + `.sh` pair under `scripts/commands/`, `.sh` sources `common.sh`, supports `--help`/`--log`, resolves `SCRIPT_DIR`/`PROJECT_ROOT` via `$SCRIPT_DIR/../..`, exits non-zero on failure. Fix the command, never work around it.
- **User-facing voice:** no internal planning vocabulary (no "wave N", no subagent/dispatch references) in anything under `apps/genie_map/docs/` or `docs/docs/`. Behavior, not process.
- **No aliases / single canonical name** per repo policy.
- **Serving model** (`DATABRICKS_SERVING_ENDPOINT_NAME`) is baked at Vite build time into `__LLM_MODEL__` — changing it requires a rebuild, not just runtime config. Document, don't fight it.
- **Commits:** frequent, one deliverable each; end commit messages with `Co-authored-by: Isaac`.

## File Structure

**New app (copied then modified from `isaac_work/genie_map/kepler-demo/`):**
```
apps/genie_map/
├── README.md                              # quickstart (Task 15)
├── app.yaml                               # Databricks Apps runtime (Task 3, edited)
├── package.json, pnpm-lock.yaml           # copied verbatim (Task 1)
├── vite.config.ts, tsconfig*.json         # copied verbatim (Task 1)
├── .env.example                           # renamed from taxi.env.example, vapor-eyes values (Task 3)
├── shared/types.ts                        # + registry types (Task 4)
├── config/queries/
│   ├── hotspot_h3.sql                     # Task 6 (cell-sourced dynamic H3, coarsen-only)
│   ├── wells_h3.sql                       # Task 8 (point-sourced dynamic H3, refine+coarsen)
│   ├── plume_points.sql                   # Task 7 (replaces point_data.sql)
│   └── wells_points.sql                   # Task 9
├── client/src/
│   ├── config/datasets/vapor-eyes.ts      # THE layer registry (Task 5)
│   ├── config/datasets/index.ts           # active-dataset selector (Task 5)
│   ├── config/h3-layer-config.ts          # generalized factory (Task 4)
│   ├── config/point-layer-config.ts       # converted to factory (Task 4)
│   ├── hooks/useLayerData.ts              # generic registry-driven hook (Task 6)
│   ├── hooks/useViewportBounds.ts         # copied verbatim
│   ├── hooks/useLayerVisibility.ts        # copied verbatim
│   ├── tools/databricks/genie-tool.ts     # widen geometry detection (Task 11)
│   ├── tools/databricks/types.ts          # copied (parseGeoJsonFromRows reused)
│   └── App.tsx                            # registry-driven layer wiring (Task 10)
├── bundle/databricks.yml                  # DAB bundle (Task 12)
└── docs/
    ├── BUILD.md                           # implementer narrative (Task 14, accrues)
    └── diagrams/*.py + *.png              # slide-ware (Task 13)
```

**SDP (existing pipeline, gold layer):**
```
notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py   # + 1 MV (Task 16)
notebooks/examples/vapor-eyes/lakeflow/tests/validate/                     # + MV validator
```

**`gbx:*` commands:**
```
scripts/commands/gbx-app-dev.{md,sh}       # Task 12
scripts/commands/gbx-app-deploy.{md,sh}    # Task 12
```

---

## Task ordering rationale

Tasks 1–15 are the **app + deploy + docs** track. Tasks 16 + 18 are the **SDP gold MV / Genie Space** track (Task 17 was merged into 16 — see below). The SDP track is independent of the app track and can run in parallel; the app's wells layers (Tasks 8, 9) *consume* the `wells_enriched_latest` MV from Task 16, so **run 16 before verifying 8–9 against live data** — but the app code (SQL templates + hooks) can be written against the documented MV schema first (TDD-style) and verified once the MV materializes. The Genie Space (Task 18) depends on all gold tables existing.

**Dynamic H3 (density-aware) — cross-cutting note.** Per spec §4 "Dynamic H3", H3 resolution is chosen at query time from **zoom (a max-resolution ceiling) AND density (a target of ~300 on-screen cells)**, not zoom alone. Two source modes: `ch4_hotspots` is **cell-sourced** (coarsen-only from `hotspot_latest.h3_cellid` via `h3_toparent`; never finer than native res 6); `well_density` is **point-sourced** (refine *and* coarsen from `wells_enriched_latest` points via `h3_longlatash3`). The originally-planned fixed `wells_h3_density_latest` MV is **dropped** (old Task 17 removed); `wells_enriched_latest` (Task 16) is the single wells source for both the wells H3 layer and the wells point layer.

---

### Task 1: Copy prototype into `apps/genie_map/` and cut the branch

**Files:**
- Create branch `apps/genie-map` from `examples/vapor-eyes`
- Create: `apps/genie_map/**` (copy of `isaac_work/genie_map/kepler-demo/`, minus taxi-specific + dead files)

**Interfaces:**
- Produces: the working tree that all later tasks edit. No code interface.

- [ ] **Step 1: Cut the branch**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
git checkout examples/vapor-eyes && git pull --ff-only
git checkout -b apps/genie-map
```

- [ ] **Step 2: Copy the app, excluding node_modules/dist and dead taxi files**

```bash
mkdir -p apps/genie_map
rsync -a --exclude node_modules --exclude dist --exclude '.env' --exclude '.env.local' \
  /Users/mjohns/isaac_work/genie_map/kepler-demo/ apps/genie_map/
# Remove dead-weight SQL (superseded duplicate + taxi templates replaced later)
rm -f apps/genie_map/config/queries/chart_operators.sql
# Rename env template
git -C apps/genie_map mv taxi.env.example .env.example 2>/dev/null || mv apps/genie_map/taxi.env.example apps/genie_map/.env.example
```

- [ ] **Step 3: Verify the tree copied and taxi notebooks were NOT dragged in**

Run: `ls apps/genie_map && ls apps/genie_map/config/queries && test ! -d apps/genie_map/notebooks && echo "clean"`
Expected: app files present; `config/queries/` has `h3_aggregation.sql point_data.sql chart_groups.sql` (chart_operators removed; taxi templates removed in Task 6/7); prints `clean`.

- [ ] **Step 4: Add an app-scoped `.gitignore` note and commit the raw copy**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
git add apps/genie_map
git commit -m "chore(genie-map): copy kepler-demo prototype into apps/genie_map

Verbatim copy of isaac_work/genie_map/kepler-demo minus node_modules,
dist, the dead chart_operators.sql, and the taxi env template (renamed
to .env.example). Subsequent tasks retarget it onto vapor-eyes gold.

Co-authored-by: Isaac"
```

---

### Task 2: Establish local build baseline (green before changes)

**Files:**
- Modify: none (verification only)

**Interfaces:**
- Produces: confidence that the copy builds, so later failures are attributable to our changes.

- [ ] **Step 1: Install dependencies**

Run: `cd apps/genie_map && pnpm install --frozen-lockfile`
Expected: install completes; `node_modules/` created. (If lockfile mismatch, run `pnpm install` and note it in BUILD.md.)

- [ ] **Step 2: Type-check + build the client and server**

Run: `cd apps/genie_map && pnpm build`
Expected: `build:server` (tsc) + `build:client` (vite) complete; `dist/server/index.js` and `dist/client/` produced. A missing `.env`/`VITE_DATASET_TABLE` is fine at build time — it fails only at query time.

- [ ] **Step 3: Record the baseline in BUILD.md (create it)**

Create `apps/genie_map/docs/BUILD.md` with a "Baseline" section noting: source prototype, versions built against, that build is green with no env. (This file accrues through Task 14.)

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/docs/BUILD.md
git commit -m "docs(genie-map): record green build baseline of copied app

Co-authored-by: Isaac"
```

---

### Task 3: Retarget env template + app.yaml to vapor-eyes

**Files:**
- Modify: `apps/genie_map/.env.example`
- Modify: `apps/genie_map/app.yaml`

**Interfaces:**
- Produces: the env contract (`DATABRICKS_*` + one `VITE_*` for the active dataset id + mapbox token) that Task 5's registry and Task 12's bundle read.

- [ ] **Step 1: Rewrite `.env.example` for vapor-eyes**

Replace the taxi dataset block. New contents:

```dotenv
# --- Databricks connection (local dev) ---
DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com
DATABRICKS_TOKEN=
DATABRICKS_WAREHOUSE_ID=82e587bd93c6cbcf
DATABRICKS_GENIE_SPACE_ID=
DATABRICKS_SERVING_ENDPOINT_NAME=databricks-gpt-5-2
VITE_MAPBOX_TOKEN=
PORT=3000

# --- Active dataset (selects a registry entry in client/src/config/datasets) ---
VITE_ACTIVE_DATASET=vapor-eyes
```

The per-layer schema is no longer env-driven — it lives in the registry (Task 5). Only the active-dataset *id* is env-selectable.

- [ ] **Step 2: Update `app.yaml` comment + keep resource wiring**

`app.yaml` already injects `sql-warehouse-id` and `genie-space-id` via `valueFrom` and hardcodes the serving endpoint. Add `VITE_ACTIVE_DATASET` note as a comment (it is baked at build time, not injected at runtime, so it belongs to the build, not app.yaml). Verify the file still reads:

```yaml
command:
  - node
  - dist/server/index.js
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse-id
  - name: DATABRICKS_GENIE_SPACE_ID
    valueFrom: genie-space-id
  - name: DATABRICKS_SERVING_ENDPOINT_NAME
    value: databricks-gpt-5-2
```

- [ ] **Step 3: Verify**

Run: `cat apps/genie_map/.env.example && echo '---' && cat apps/genie_map/app.yaml`
Expected: vapor-eyes values; warehouse `82e587bd93c6cbcf`; `VITE_ACTIVE_DATASET=vapor-eyes`.

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/.env.example apps/genie_map/app.yaml
git commit -m "feat(genie-map): retarget env + app.yaml to vapor_eyes_lf warehouse

Co-authored-by: Isaac"
```

---

### Task 4: Registry types + generalize the layer-config factories

**Files:**
- Modify: `apps/genie_map/shared/types.ts` (add registry interfaces)
- Modify: `apps/genie_map/client/src/config/h3-layer-config.ts` (parameterize palette/fields)
- Modify: `apps/genie_map/client/src/config/point-layer-config.ts` (convert static object → factory)
- Test: `apps/genie_map/client/src/config/__tests__/layer-config.test.ts`

**Interfaces:**
- Consumes: existing `createH3LayerConfig(options: H3LayerConfigOptions)` (h3-layer-config.ts:35).
- Produces:
  - `LayerDef`, `DatasetConfig` types (in `shared/types.ts`) — shapes below.
  - `createH3LayerConfig(options)` extended with `colorField`/`palette`/`tooltipFields`.
  - `createPointLayerConfig(options: PointLayerConfigOptions)` — new factory returning a kepler `addDataToMap` config, analogous to `createH3LayerConfig`.

- [ ] **Step 1: Write the failing test for registry types + point factory**

Create `apps/genie_map/client/src/config/__tests__/layer-config.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { createH3LayerConfig } from '../h3-layer-config';
import { createPointLayerConfig } from '../point-layer-config';

describe('layer config factories', () => {
  it('H3 factory honors a custom color field and dataset id', () => {
    const cfg = createH3LayerConfig({
      datasetId: 'ch4_hotspots', hexField: 'hex', valueField: 'ch4_max',
      label: 'CH4 Hotspots', enable3d: true,
    }) as any;
    const layer = cfg.config.visState.layers[0];
    expect(layer.config.dataId).toBe('ch4_hotspots');
    expect(layer.visualChannels.colorField.name).toBe('ch4_max');
    expect(layer.config.columns.hex_id).toBe('hex');
  });

  it('point factory returns a point layer bound to the given dataset id + coords', () => {
    const cfg = createPointLayerConfig({
      datasetId: 'wells', label: 'Wells',
      latField: 'latitude', lngField: 'longitude',
      tooltipFields: ['record_id', 'operator'],
    }) as any;
    const layer = cfg.config.visState.layers[0];
    expect(layer.type).toBe('point');
    expect(layer.config.dataId).toBe('wells');
    expect(layer.config.columns.lat).toBe('latitude');
    expect(layer.config.columns.lng).toBe('longitude');
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd apps/genie_map && pnpm vitest run client/src/config/__tests__/layer-config.test.ts`
Expected: FAIL — `createPointLayerConfig` is not exported.

- [ ] **Step 3: Add registry types to `shared/types.ts`**

Append:

```ts
export type LayerKind = 'h3' | 'point';
export type H3Source = 'cells' | 'points';

// Dynamic, density-aware H3 config (spec §4). Resolution is picked at query time
// from BOTH zoom (a max-res ceiling) AND density (target on-screen cell count).
export interface H3ResConfig {
  source: H3Source;         // 'cells' → coarsen-only via h3_toparent (from a stored cell col);
                            // 'points' → refine+coarsen via h3_longlatash3 (from lon/lat)
  cellIdCol?: string;       // source==='cells': the stored H3 cell column, e.g. 'h3_cellid'
  nativeRes?: number;       // source==='cells': stored resolution = hard finest ceiling
  lonCol?: string;          // source==='points'
  latCol?: string;          // source==='points'
  minRes: number;           // coarsest resolution ever rendered
  maxRes: number;           // finest resolution allowed (points may exceed a cell's nativeRes)
  zoomResBreaks: number[];  // exactly 4 ascending zoom thresholds
  resByBreak: number[];     // exactly 5 resolutions (per zoom band); each <= maxRes
  aggExpr: string;          // child aggregation, e.g. 'MAX(ch4_max)' or 'COUNT(*)'
  targetCells?: number;     // density target (default 300); coarsen when in-view count exceeds it
}

export interface LayerDef {
  id: string;                     // kepler dataId + layer key, e.g. 'ch4_hotspots'
  kind: LayerKind;
  label: string;
  queryName: string;              // key into config/queries (file name without .sql)
  hexField?: string;              // kind==='h3': the string hex column returned by SQL
  h3?: H3ResConfig;               // kind==='h3': dynamic-resolution config (required for h3)
  valueField: string;            // color/size metric column
  lngField?: string;              // kind==='point'
  latField?: string;             // kind==='point'
  tooltipFields: string[];
  palette?: string;               // kepler named colorRange, default 'Global Warming'
  enable3d?: boolean;
  zoomVisible: { min: number; max: number };   // hard visibility band: min <= z < max
  fadeBand?: [number, number];    // optional opacity ramp across [start,end] for smooth swap
}

export interface DatasetConfig {
  id: string;                     // matches VITE_ACTIVE_DATASET
  displayName: string;
  genieSpaceAlias: string;        // 'default' server-side; label only
  defaultViewport: { longitude: number; latitude: number; zoom: number };
  layers: LayerDef[];
}
```

- [ ] **Step 4: Extend `createH3LayerConfig` for palette + tooltip fields**

In `h3-layer-config.ts`, widen `H3LayerConfigOptions` with `palette?: string` and `tooltipFields?: string[]`, default `palette='Global Warming'`, and use them in the returned config's `visConfig.colorRange` name lookup and `interactionConfig.tooltip.fieldsToShow`. Keep the existing `colorField`/`sizeField` = `valueField` behavior.

- [ ] **Step 5: Convert `point-layer-config.ts` to a factory**

Replace the static `POINT_LAYER_CONFIG` object with:

```ts
export interface PointLayerConfigOptions {
  datasetId: string;
  label: string;
  latField: string;
  lngField: string;
  tooltipFields: string[];
  radius?: number;
  color?: [number, number, number];
}

export function createPointLayerConfig(options: PointLayerConfigOptions) {
  const { datasetId, label, latField, lngField, tooltipFields,
          radius = 20, color = [255, 195, 0] } = options;
  return {
    version: 'v1',
    config: { visState: { filters: [], layers: [{
      id: `point-layer-${datasetId}`, type: 'point',
      config: { dataId: datasetId, label, color,
        columns: { lat: latField, lng: lngField, altitude: null },
        isVisible: true,
        visConfig: { radius, fixedRadius: false, opacity: 0.8, outline: false,
          thickness: 2, strokeColor: null, colorRange: GLOBAL_WARMING,
          strokeColorRange: GLOBAL_WARMING, radiusRange: [0, 50], filled: true },
        textLabel: [] },
      visualChannels: { colorField: null, colorScale: 'quantile',
        strokeColorField: null, strokeColorScale: 'quantile',
        sizeField: null, sizeScale: 'linear' } }],
      interactionConfig: { tooltip: { fieldsToShow: { [datasetId]: tooltipFields },
        enabled: true }, brush: { size: 0.5, enabled: false },
        geocoder: { enabled: false } },
      layerBlending: 'normal', splitMaps: [] } },
  };
}
```

(Define `GLOBAL_WARMING` as the existing 6-color array already present in the file.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd apps/genie_map && pnpm vitest run client/src/config/__tests__/layer-config.test.ts`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add apps/genie_map/shared/types.ts apps/genie_map/client/src/config
git commit -m "feat(genie-map): registry types + parameterized layer factories

Adds LayerDef/DatasetConfig types and a createPointLayerConfig factory
(mirrors createH3LayerConfig) so layer styling/fields are per-dataset.

Co-authored-by: Isaac"
```

---

### Task 5: The vapor-eyes layer registry + active-dataset selector

**Files:**
- Create: `apps/genie_map/client/src/config/datasets/vapor-eyes.ts`
- Create: `apps/genie_map/client/src/config/datasets/index.ts`
- Test: `apps/genie_map/client/src/config/datasets/__tests__/registry.test.ts`

**Interfaces:**
- Consumes: `DatasetConfig`, `LayerDef` (Task 4).
- Produces:
  - `vaporEyes: DatasetConfig` (the one shipped config).
  - `getActiveDataset(): DatasetConfig` — reads `import.meta.env.VITE_ACTIVE_DATASET`, defaults `'vapor-eyes'`, throws on unknown id.
  - `DATASETS: Record<string, DatasetConfig>`.

- [ ] **Step 1: Write the failing test**

Create `registry.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { vaporEyes } from '../vapor-eyes';
import { getActiveDataset, DATASETS } from '../index';

describe('vapor-eyes registry', () => {
  it('declares the four MVP layers', () => {
    const ids = vaporEyes.layers.map((l) => l.id);
    expect(ids).toEqual(['ch4_hotspots', 'well_density', 'wells', 'plumes']);
  });
  it('ch4_hotspots is a cell-sourced H3 layer capped at native res 6', () => {
    const l = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    expect(l.kind).toBe('h3');
    expect(l.queryName).toBe('hotspot_h3');
    expect(l.valueField).toBe('ch4_max');
    expect(l.h3!.source).toBe('cells');
    expect(l.h3!.nativeRes).toBe(6);
    expect(l.h3!.maxRes).toBe(6);          // never finer than S5P native footprint
    expect(l.h3!.zoomResBreaks).toHaveLength(4);
    expect(l.h3!.resByBreak).toHaveLength(5);
  });
  it('well_density is a point-sourced H3 layer that can refine past res 6', () => {
    const l = vaporEyes.layers.find((x) => x.id === 'well_density')!;
    expect(l.h3!.source).toBe('points');
    expect(l.h3!.lonCol).toBe('longitude');
    expect(l.h3!.maxRes).toBeGreaterThan(6);
    expect(l.queryName).toBe('wells_h3');
  });
  it('wells H3 and wells points share the wells_enriched source + overlap-swap', () => {
    const density = vaporEyes.layers.find((x) => x.id === 'well_density')!;
    const wells = vaporEyes.layers.find((x) => x.id === 'wells')!;
    // ~1-level overlap band: density fades out where wells fades in.
    expect(density.zoomVisible.max).toBeGreaterThan(wells.zoomVisible.min);
    expect(density.fadeBand).toBeDefined();
  });
  it('ch4 hexes and plumes coexist (plumes appear on zoom-in, hexes stay)', () => {
    const ch4 = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    const plumes = vaporEyes.layers.find((x) => x.id === 'plumes')!;
    expect(ch4.zoomVisible.max).toBe(24);   // ch4 stays visible at all zooms
    expect(plumes.zoomVisible.min).toBeGreaterThan(0); // plumes only on zoom-in
  });
  it('getActiveDataset defaults to vapor-eyes', () => {
    expect(getActiveDataset().id).toBe('vapor-eyes');
    expect(DATASETS['vapor-eyes']).toBe(vaporEyes);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd apps/genie_map && pnpm vitest run client/src/config/datasets/__tests__/registry.test.ts`
Expected: FAIL — modules not found.

- [ ] **Step 3: Write `vapor-eyes.ts`**

```ts
import type { DatasetConfig } from '@shared/types';

const T = (name: string) => `geospatial_docs.vapor_eyes_lf.${name}`;

export const vaporEyes: DatasetConfig = {
  id: 'vapor-eyes',
  displayName: 'Vapor-Eyes — Permian Basin Methane',
  genieSpaceAlias: 'default',
  // Delaware Basin (full AOI center, from SDP _config bbox -104.5,30.8,-101.0,33.0)
  defaultViewport: { longitude: -102.75, latitude: 31.9, zoom: 8 },
  layers: [
    // CH4 hexes: cell-sourced, coarsen-ONLY (never finer than S5P native res 6).
    // Always-on wide-area context; density heuristic keeps it readable at low zoom.
    { id: 'ch4_hotspots', kind: 'h3', label: 'CH₄ Hotspots (latest)',
      queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
      tooltipFields: ['hex', 'ch4_max', 'ch4_mean', 'n_obs'],
      palette: 'Global Warming', enable3d: true,
      h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6,
            minRes: 2, maxRes: 6, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [3, 4, 5, 6, 6], aggExpr: 'MAX(ch4_max)', targetCells: 300 },
      zoomVisible: { min: 0, max: 24 } },
    // Well density: point-sourced, refine (finer on zoom-in) AND coarsen (only if dense).
    // Owns low/mid zoom; fades out over [11,12] as the wells point layer fades in.
    { id: 'well_density', kind: 'h3', label: 'Well Density (H3)',
      queryName: 'wells_h3', hexField: 'hex', valueField: 'well_count',
      tooltipFields: ['hex', 'well_count', 'operator_count'],
      palette: 'Uber Viz Sequential', enable3d: true,
      h3: { source: 'points', lonCol: 'longitude', latCol: 'latitude',
            minRes: 3, maxRes: 9, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [4, 5, 6, 7, 9], aggExpr: 'COUNT(*)', targetCells: 300 },
      zoomVisible: { min: 0, max: 12 }, fadeBand: [11, 12] },
    // Wells points: fade in over [11,12] — ~1-level overlap with well_density.
    { id: 'wells', kind: 'point', label: 'Wells',
      queryName: 'wells_points', valueField: 'well_count',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'operator', 'field', 'county', 'play_name'],
      zoomVisible: { min: 11, max: 24 }, fadeBand: [11, 12] },
    // EMIT plumes: coexist with the CH4 hex screen; appear once zoomed in to resolve sources.
    { id: 'plumes', kind: 'point', label: 'EMIT Plumes',
      queryName: 'plume_points', valueField: 'max_conc_ppmm',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'max_conc_ppmm', 'lead_operator', 'lead_county'],
      zoomVisible: { min: 9, max: 24 } },
  ],
};

export const VAPOR_EYES_TABLES = {
  hotspot: T('hotspot_latest'),
  wellsEnriched: T('wells_enriched_latest'),  // feeds BOTH well_density (H3) and wells (points)
  plumes: T('plume_leaderboard_latest'),
};
```

- [ ] **Step 4: Write `index.ts`**

```ts
import type { DatasetConfig } from '@shared/types';
import { vaporEyes } from './vapor-eyes';

export const DATASETS: Record<string, DatasetConfig> = { 'vapor-eyes': vaporEyes };

export function getActiveDataset(): DatasetConfig {
  const id = (import.meta.env.VITE_ACTIVE_DATASET as string) || 'vapor-eyes';
  const ds = DATASETS[id];
  if (!ds) throw new Error(`Unknown VITE_ACTIVE_DATASET '${id}'. Known: ${Object.keys(DATASETS).join(', ')}`);
  return ds;
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `cd apps/genie_map && pnpm vitest run client/src/config/datasets/__tests__/registry.test.ts`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add apps/genie_map/client/src/config/datasets
git commit -m "feat(genie-map): vapor-eyes layer registry + active-dataset selector

Four MVP layers with density-aware dynamic H3: cell-sourced ch4_hotspots
(coarsen-only, capped at native res 6) + point-sourced well_density
(refine+coarsen). Visibility choreography: wells H3<->points overlap-swap,
CH4 hexes + plumes coexist. getActiveDataset() is the seam for helios.

Co-authored-by: Isaac"
```

---

### Task 6: `hotspot_h3.sql` template + generic `useLayerData` hook

**Files:**
- Create: `apps/genie_map/config/queries/hotspot_h3.sql`
- Delete: `apps/genie_map/config/queries/h3_aggregation.sql`
- Create: `apps/genie_map/client/src/hooks/useLayerData.ts`
- Test: `apps/genie_map/client/src/hooks/__tests__/useLayerData.test.ts`

**Interfaces:**
- Consumes: `useKeplerDataset<TRow>` (useKeplerDataset.ts:74), `sql` from `@databricks/appkit-ui/js`, `LayerDef`, `createH3LayerConfig`/`createPointLayerConfig`, `ViewportBounds`.
- Produces: `useLayerData(layer: LayerDef, bounds: ViewportBounds | null): { data: unknown[]; isLoading: boolean; error: Error | null }` — resolves the SQL params + kepler layerConfig from the `LayerDef.kind` and delegates to `useKeplerDataset`.

- [ ] **Step 1: Write `hotspot_h3.sql` (cell-sourced, density-aware coarsen-only)**

Reads `hotspot_latest` (native res-6 cells). A CTE picks the zoom-ceiling resolution
(clamped to `native_res` — never finer), then coarsens further by
`floor(log₇(in_view_count / target_cells))` `h3_toparent` steps when the data is dense.
Sparse data → 0 coarsening steps → stays at res 6. Children are re-aggregated:

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; zoom_level INT ;
--         zoom_break_1..4 INT ; res_1..5 INT ; min_res INT ; native_res INT ;
--         target_cells INT ; table_name STRING via IDENTIFIER()
WITH in_view AS (
  SELECT h3_cellid, ch4_max, ch4_mean, n_obs
  FROM IDENTIFIER(:table_name)
  WHERE center_lon BETWEEN :x_min AND :x_max
    AND center_lat BETWEEN :y_min AND :y_max
    AND ch4_max IS NOT NULL
),
zoom_ceiling AS (
  SELECT CASE
    WHEN :zoom_level <= :zoom_break_1 THEN :res_1
    WHEN :zoom_level <= :zoom_break_2 THEN :res_2
    WHEN :zoom_level <= :zoom_break_3 THEN :res_3
    WHEN :zoom_level <= :zoom_break_4 THEN :res_4
    ELSE :res_5 END AS zc
),
counted AS (SELECT COUNT(*) AS n FROM in_view),
target_res AS (
  -- ceiling capped at native_res; density subtracts coarsening levels (each ≈ ÷7);
  -- floored at min_res. Sparse (n <= target) → levels = 0 → stays at ceiling.
  SELECT GREATEST(:min_res,
           LEAST(zc.zc, :native_res)
           - GREATEST(0, CAST(FLOOR(LOG(7.0, GREATEST(c.n, 1) / CAST(:target_cells AS DOUBLE))) AS INT))
         ) AS res
  FROM zoom_ceiling zc CROSS JOIN counted c
)
SELECT
  h3_h3tostring(h3_toparent(v.h3_cellid, t.res)) AS hex,
  CAST(MAX(v.ch4_max)  AS DOUBLE) AS ch4_max,
  CAST(AVG(v.ch4_mean) AS DOUBLE) AS ch4_mean,
  CAST(SUM(v.n_obs)    AS DOUBLE) AS n_obs
FROM in_view v CROSS JOIN target_res t
GROUP BY h3_toparent(v.h3_cellid, t.res)
```

(kepler's `hexagonId` layer draws the hexagon from the H3 string id itself, so returning a
coarser *parent* id renders a correctly-sized hex — no `hex_geom` needed on this path.)

- [ ] **Step 2: Delete the taxi template**

```bash
git -C /Users/mjohns/IdeaProjects/geobrix rm apps/genie_map/config/queries/h3_aggregation.sql
```

- [ ] **Step 3: Write the failing test for `useLayerData` param resolution**

The pure param-building logic is unit-testable. Create `useLayerData.ts` exporting
`buildLayerParams(layer, bounds, tableName)` and test that H3 layers get the dynamic-res
params and point layers get only bbox+table:

```ts
import { describe, it, expect } from 'vitest';
import { buildLayerParams } from '../useLayerData';
import type { LayerDef } from '@shared/types';

const bounds = { x_min: -103, x_max: -102, y_min: 31, y_max: 32, zoom_level: 8 };
const h3Layer: LayerDef = { id: 'ch4_hotspots', kind: 'h3', label: 'x',
  queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max', tooltipFields: [],
  h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6, minRes: 2, maxRes: 6,
        zoomResBreaks: [5, 7, 9, 11], resByBreak: [3, 4, 5, 6, 6],
        aggExpr: 'MAX(ch4_max)', targetCells: 300 },
  zoomVisible: { min: 0, max: 24 } };
const pointLayer: LayerDef = { id: 'plumes', kind: 'point', label: 'x',
  queryName: 'plume_points', valueField: 'max_conc_ppmm', lngField: 'longitude',
  latField: 'latitude', tooltipFields: ['record_id'], zoomVisible: { min: 9, max: 24 } };

describe('buildLayerParams', () => {
  it('returns null when bounds are null', () => {
    expect(buildLayerParams(h3Layer, null, 't')).toBeNull();
  });
  it('emits the dynamic-H3 params for an H3 layer', () => {
    const p = buildLayerParams(h3Layer, bounds, 'db.sch.hotspot_latest') as any;
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
    expect(p.zoom_level).toBeDefined();
    expect(p.zoom_break_1).toBeDefined();
    expect(p.res_1).toBeDefined();
    expect(p.res_5).toBeDefined();
    expect(p.native_res).toBeDefined();   // cells source carries native_res
    expect(p.target_cells).toBeDefined();
  });
  it('emits only bbox+table for a point layer', () => {
    const p = buildLayerParams(pointLayer, bounds, 'db.sch.plume_leaderboard_latest') as any;
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
    expect(p.zoom_break_1).toBeUndefined();
  });
});
```

- [ ] **Step 4: Run to verify it fails**

Run: `cd apps/genie_map && pnpm vitest run client/src/hooks/__tests__/useLayerData.test.ts`
Expected: FAIL — `buildLayerParams` not exported.

- [ ] **Step 5: Implement `useLayerData.ts`**

```ts
import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { useKeplerDataset } from './useKeplerDataset';
import { createH3LayerConfig } from '../config/h3-layer-config';
import { createPointLayerConfig } from '../config/point-layer-config';
import type { LayerDef, ViewportBounds } from '@shared/types';

export function buildLayerParams(
  layer: LayerDef, bounds: ViewportBounds | null, tableName: string,
): Record<string, unknown> | null {
  if (!bounds || !tableName) return null;
  const base: Record<string, unknown> = {
    x_min: sql.double(bounds.x_min), x_max: sql.double(bounds.x_max),
    y_min: sql.double(bounds.y_min), y_max: sql.double(bounds.y_max),
    table_name: sql.string(tableName),
  };
  if (layer.kind !== 'h3' || !layer.h3) return base;

  const h = layer.h3;
  const p: Record<string, unknown> = {
    ...base,
    zoom_level: sql.number(bounds.zoom_level),
    zoom_break_1: sql.number(h.zoomResBreaks[0]), zoom_break_2: sql.number(h.zoomResBreaks[1]),
    zoom_break_3: sql.number(h.zoomResBreaks[2]), zoom_break_4: sql.number(h.zoomResBreaks[3]),
    res_1: sql.number(h.resByBreak[0]), res_2: sql.number(h.resByBreak[1]),
    res_3: sql.number(h.resByBreak[2]), res_4: sql.number(h.resByBreak[3]),
    res_5: sql.number(h.resByBreak[4]),
    min_res: sql.number(h.minRes),
    target_cells: sql.number(h.targetCells ?? 300),
  };
  if (h.source === 'cells') p.native_res = sql.number(h.nativeRes ?? h.maxRes);
  else p.max_res = sql.number(h.maxRes);   // points source: finer allowed than any stored cell
  return p;
}

export function useLayerData(layer: LayerDef, bounds: ViewportBounds | null, tableName: string) {
  const params = useMemo(() => buildLayerParams(layer, bounds, tableName), [layer, bounds, tableName]);

  const layerConfig = useMemo(() => (
    layer.kind === 'h3'
      ? createH3LayerConfig({ datasetId: layer.id, hexField: layer.hexField ?? 'hex',
          valueField: layer.valueField, label: layer.label, enable3d: layer.enable3d ?? true,
          palette: layer.palette, tooltipFields: layer.tooltipFields })
      : createPointLayerConfig({ datasetId: layer.id, label: layer.label,
          latField: layer.latField!, lngField: layer.lngField!, tooltipFields: layer.tooltipFields })
  ), [layer]);

  const fields = useMemo(() => (
    layer.kind === 'h3'
      ? [{ name: layer.hexField ?? 'hex', type: 'string' }, { name: layer.valueField, type: 'real' }]
      : [{ name: 'longitude', type: 'real' }, { name: 'latitude', type: 'real' },
         ...layer.tooltipFields.map((f) => ({ name: f, type: 'string' }))]
  ), [layer]);

  return useKeplerDataset<Record<string, unknown>>({
    queryName: layer.queryName, params,
    transformRows: (raw) => raw as Record<string, unknown>[],
    toKeplerRow: (row) => fields.map((f) => (row as any)[f.name]),
    fields, datasetId: layer.id, datasetLabel: layer.label, layerConfig,
  });
}
```

- [ ] **Step 6: Run tests to verify pass**

Run: `cd apps/genie_map && pnpm vitest run client/src/hooks/__tests__/useLayerData.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add apps/genie_map/config/queries apps/genie_map/client/src/hooks/useLayerData.ts apps/genie_map/client/src/hooks/__tests__
git commit -m "feat(genie-map): density-aware dynamic hotspot_h3 SQL + useLayerData hook

Cell-sourced coarsen-only H3: zoom sets a res ceiling (capped at native
res 6), density coarsens further via h3_toparent only when the in-view
cell count exceeds ~300. buildLayerParams emits the dynamic-res params for
H3 layers, bbox-only for points.

Co-authored-by: Isaac"
```

---

### Task 7: `plume_points.sql` template

**Files:**
- Create: `apps/genie_map/config/queries/plume_points.sql`
- Delete: `apps/genie_map/config/queries/point_data.sql`

**Interfaces:**
- Consumes: `useLayerData` (Task 6) via the `plumes` LayerDef.
- Produces: SQL returning `longitude, latitude, record_id, max_conc_ppmm, lead_operator, lead_county`.

- [ ] **Step 1: Write `plume_points.sql`**

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  lon_max AS longitude,
  lat_max AS latitude,
  CAST(plume_id AS STRING) AS record_id,
  CAST(max_conc_ppmm AS DOUBLE) AS max_conc_ppmm,
  lead_operator, lead_county
FROM IDENTIFIER(:table_name)
WHERE lon_max BETWEEN :x_min AND :x_max
  AND lat_max BETWEEN :y_min AND :y_max
LIMIT 10000
```

(Uses `plume_leaderboard_latest`'s `lon_max`/`lat_max` — see `gold_analytics.py:82-85`.)

- [ ] **Step 2: Delete the taxi template**

```bash
git -C /Users/mjohns/IdeaProjects/geobrix rm apps/genie_map/config/queries/point_data.sql
```

- [ ] **Step 3: Verify SQL parses (syntax-only lint)**

Run: `grep -c 'IDENTIFIER(:table_name)' apps/genie_map/config/queries/plume_points.sql`
Expected: `1`. (Live execution verified in Task 19.)

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/config/queries
git commit -m "feat(genie-map): plume_points SQL over plume_leaderboard_latest

Co-authored-by: Isaac"
```

---

### Task 8: `wells_h3.sql` template (point-sourced, refine + coarsen)

**Files:**
- Create: `apps/genie_map/config/queries/wells_h3.sql`

**Interfaces:**
- Consumes: `wells_enriched_latest` MV (Task 16) — the **points** source. `useLayerData` via `well_density` LayerDef (`h3.source==='points'`).
- Produces: SQL returning `hex, well_count, operator_count`.

- [ ] **Step 1: Write `wells_h3.sql`**

Unlike the cell-sourced hotspot query, this aggregates the **well points on the fly** via
`h3_longlatash3(lon, lat, res)` — so it can go *finer as you zoom in* (up to `max_res`),
not just coarser. Same density heuristic: zoom sets the ceiling, density lowers it toward
`min_res` only when the in-view well count is dense. Two-pass: bin at the zoom ceiling to
estimate density, then re-bin at the density-adjusted resolution.

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; zoom_level INT ; zoom_break_1..4 INT ;
--         res_1..5 INT ; min_res INT ; max_res INT ; target_cells INT ;
--         table_name STRING via IDENTIFIER()
WITH in_view AS (
  SELECT longitude, latitude, operator
  FROM IDENTIFIER(:table_name)
  WHERE longitude BETWEEN :x_min AND :x_max
    AND latitude  BETWEEN :y_min AND :y_max
    AND longitude IS NOT NULL AND latitude IS NOT NULL
),
zoom_ceiling AS (
  SELECT LEAST(:max_res, CASE
    WHEN :zoom_level <= :zoom_break_1 THEN :res_1
    WHEN :zoom_level <= :zoom_break_2 THEN :res_2
    WHEN :zoom_level <= :zoom_break_3 THEN :res_3
    WHEN :zoom_level <= :zoom_break_4 THEN :res_4
    ELSE :res_5 END) AS zc
),
-- Estimate density at the ceiling resolution (distinct cells occupied in view).
ceiling_cells AS (
  SELECT COUNT(DISTINCT h3_longlatash3(longitude, latitude, (SELECT zc FROM zoom_ceiling))) AS n
  FROM in_view
),
target_res AS (
  SELECT GREATEST(:min_res,
           (SELECT zc FROM zoom_ceiling)
           - GREATEST(0, CAST(FLOOR(LOG(7.0, GREATEST(c.n, 1) / CAST(:target_cells AS DOUBLE))) AS INT))
         ) AS res
  FROM ceiling_cells c
)
SELECT
  h3_h3tostring(h3_longlatash3(v.longitude, v.latitude, t.res)) AS hex,
  CAST(COUNT(*)                    AS DOUBLE) AS well_count,
  CAST(COUNT(DISTINCT v.operator)  AS DOUBLE) AS operator_count
FROM in_view v CROSS JOIN target_res t
GROUP BY h3_longlatash3(v.longitude, v.latitude, t.res)
```

- [ ] **Step 2: Verify**

Run: `grep -c 'h3_longlatash3' apps/genie_map/config/queries/wells_h3.sql`
Expected: `3`.

- [ ] **Step 3: Commit**

```bash
git add apps/genie_map/config/queries/wells_h3.sql
git commit -m "feat(genie-map): point-sourced dynamic wells_h3 SQL over wells_enriched_latest

Aggregates well points via h3_longlatash3 at a zoom+density-driven
resolution — refines finer on zoom-in (up to max_res 9), coarsens only
when wells are genuinely dense. Single wells source, no fixed density MV.

Co-authored-by: Isaac"
```

---

### Task 9: `wells_points.sql` template

**Files:**
- Create: `apps/genie_map/config/queries/wells_points.sql`

**Interfaces:**
- Consumes: `wells_enriched_latest` MV (Task 16). `useLayerData` via `wells` LayerDef.
- Produces: SQL returning `longitude, latitude, record_id, operator, field, county, play_name`.

- [ ] **Step 1: Write `wells_points.sql`**

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  longitude,
  latitude,
  CAST(api AS STRING) AS record_id,
  operator, field, county_name AS county, play_name
FROM IDENTIFIER(:table_name)
WHERE longitude BETWEEN :x_min AND :x_max
  AND latitude  BETWEEN :y_min AND :y_max
LIMIT 10000
```

- [ ] **Step 2: Verify**

Run: `grep -c 'longitude' apps/genie_map/config/queries/wells_points.sql`
Expected: `2`.

- [ ] **Step 3: Commit**

```bash
git add apps/genie_map/config/queries/wells_points.sql
git commit -m "feat(genie-map): wells_points SQL over wells_enriched_latest MV

Co-authored-by: Isaac"
```

---

### Task 10: Rewire `App.tsx` to drive layers from the registry

**Files:**
- Modify: `apps/genie_map/client/src/App.tsx`
- Delete: `apps/genie_map/client/src/hooks/useH3AggregationData.ts`
- Delete: `apps/genie_map/client/src/hooks/usePointData.ts`

**Interfaces:**
- Consumes: `getActiveDataset` (Task 5), `useLayerData` (Task 6), `VAPOR_EYES_TABLES` (Task 5), `useViewportBounds`, `useLayerVisibility` (`LayerRule`).
- Produces: the rendered app; no exported code interface.

- [ ] **Step 1: Delete the two taxi-specific data hooks**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
git rm apps/genie_map/client/src/hooks/useH3AggregationData.ts apps/genie_map/client/src/hooks/usePointData.ts
```

- [ ] **Step 2: Rewrite the layer wiring in `App.tsx`**

Replace the taxi `LAYER_RULES` block (App.tsx:115-118) and the two hook calls (App.tsx:135-146) with registry-driven wiring. Map each `LayerDef` to a table (via `VAPOR_EYES_TABLES` keyed by `layer.id`), call `useLayerData` per layer, build `LAYER_RULES` from `zoomVisible`:

```tsx
import { getActiveDataset, VAPOR_EYES_TABLES } from './config/datasets';
import { useLayerData } from './hooks/useLayerData';
import type { LayerRule } from './hooks/useLayerVisibility';

const DATASET = getActiveDataset();
const TABLE_BY_LAYER: Record<string, string> = {
  ch4_hotspots: VAPOR_EYES_TABLES.hotspot,
  well_density: VAPOR_EYES_TABLES.wellsEnriched,  // well_density H3 aggregates well points
  wells:        VAPOR_EYES_TABLES.wellsEnriched,  // ...same source as the wells point layer
  plumes:       VAPOR_EYES_TABLES.plumes,
};
const LAYER_RULES: LayerRule[] = DATASET.layers.map((l) => ({
  layerId: l.kind === 'h3' ? `h3-layer-${l.id}` : `point-layer-${l.id}`,
  activeWhen: (z: number) => z >= l.zoomVisible.min && z < l.zoomVisible.max,
}));
```

Inside `App()`, replace the two hook calls with a loop that is React-hooks-safe (fixed-length registry → stable call order):

```tsx
const { bounds, onViewStateChange } = useViewportBounds();
// Registry length is compile-time fixed, so per-layer hook calls keep stable order.
DATASET.layers.forEach((layer) => {
  useLayerData(layer, bounds, TABLE_BY_LAYER[layer.id]);
});
useLayerVisibility(bounds?.zoom_level ?? null, LAYER_RULES);
```

(If a linter flags hooks-in-loop, replace with explicit per-layer calls — the registry is fixed at 4; unroll them. Add a comment either way.) Set the initial kepler viewport from `DATASET.defaultViewport`. Remove the now-dead `useFilterState`/AnalyticsDashboard props that referenced taxi `h3Data`/`pointData` if they break the build; keep panels that still compile.

- [ ] **Step 3: Type-check + build**

Run: `cd apps/genie_map && pnpm build`
Expected: build green. Fix any dangling taxi imports (dataset-config constants, `H3_LAYER_ID`, `POINT_LAYER_ID`) revealed by tsc — replace references with registry-derived ids.

- [ ] **Step 4: Run the full unit test suite**

Run: `cd apps/genie_map && pnpm vitest run`
Expected: PASS (Tasks 4/5/6 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/genie_map/client/src/App.tsx apps/genie_map/client/src/hooks
git commit -m "feat(genie-map): drive kepler layers from the vapor-eyes registry

Replaces the two taxi-specific data hooks + hardcoded LAYER_RULES with a
registry loop (useLayerData per LayerDef, LAYER_RULES from zoomVisible).

Co-authored-by: Isaac"
```

---

### Task 11: Widen Genie geometry-column detection

**Files:**
- Modify: `apps/genie_map/client/src/tools/databricks/genie-tool.ts` (findGeometryColumn, line 113-118)
- Test: `apps/genie_map/client/src/tools/databricks/__tests__/genie-geom.test.ts`

**Interfaces:**
- Consumes: nothing new.
- Produces: `findGeometryColumn(columns)` recognizing curated vapor-eyes geometry column names.

- [ ] **Step 1: Write the failing test**

```ts
import { describe, it, expect } from 'vitest';
import { findGeometryColumn } from '../genie-tool';

describe('findGeometryColumn', () => {
  it('matches curated ST_ASGEOJSON alias names', () => {
    expect(findGeometryColumn(['operator', 'geojson'])).toBe('geojson');
    expect(findGeometryColumn(['hex_geojson', 'x'])).toBe('hex_geojson');
    expect(findGeometryColumn(['geometry'])).toBe('geometry');
    expect(findGeometryColumn(['geom_geojson'])).toBe('geom_geojson');
    expect(findGeometryColumn(['operator', 'ch4_max'])).toBeUndefined();
  });
});
```

(Requires exporting `findGeometryColumn` — add `export` if not already.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd apps/genie_map && pnpm vitest run client/src/tools/databricks/__tests__/genie-geom.test.ts`
Expected: FAIL — not exported / `geom_geojson` handled but assertion on export missing.

- [ ] **Step 3: Widen the matcher**

```ts
export function findGeometryColumn(columns: string[]): string | undefined {
  return columns.find((col) => {
    const lower = col.toLowerCase();
    return lower.includes('geojson') || lower === 'geometry' || lower === 'geom';
  });
}
```

(Curated Genie SQL always aliases geometry to a `*_geojson` name via `ST_ASGEOJSON`, so the substring match is the primary path; `geometry`/`geom` are fallbacks.)

- [ ] **Step 4: Run tests to verify pass**

Run: `cd apps/genie_map && pnpm vitest run client/src/tools/databricks/__tests__/genie-geom.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/genie_map/client/src/tools/databricks/genie-tool.ts apps/genie_map/client/src/tools/databricks/__tests__
git commit -m "feat(genie-map): widen Genie geometry-column detection for *_geojson aliases

Co-authored-by: Isaac"
```

---

### Task 12: DAB bundle + `gbx:app:dev` / `gbx:app:deploy` commands

**Files:**
- Create: `apps/genie_map/bundle/databricks.yml`
- Create: `scripts/commands/gbx-app-dev.md`, `scripts/commands/gbx-app-dev.sh`
- Create: `scripts/commands/gbx-app-deploy.md`, `scripts/commands/gbx-app-deploy.sh`

**Interfaces:**
- Consumes: `common.sh` helpers (`print_banner`, `resolve_log_path`).
- Produces: two runnable commands; a deployable bundle.

> **Extraction note** (see `2026-07-16-geobrix-deploy-helpers-vision.md`): keep the bundle's
> resource set (warehouse, genie-space, app name) **declarative** — a simple named list, not
> imperative glue — so a future `scaffold_map_app(resources)` helper can be lifted from this
> hand-built bundle mechanically. Don't build the helper now; just don't entangle the data.

- [ ] **Step 1: Write the DAB bundle**

`apps/genie_map/bundle/databricks.yml`:

```yaml
bundle:
  name: genie-map

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://e2-demo-field-eng.cloud.databricks.com

resources:
  apps:
    genie_map:
      name: genie-map
      description: "Genie Map — vapor-eyes methane on GeoBrix"
      source_code_path: ../
      resources:
        - name: sql-warehouse-id
          sql_warehouse:
            id: 82e587bd93c6cbcf
            permission: CAN_USE
        - name: genie-space-id
          genie_space:
            id: ${var.genie_space_id}
            permission: CAN_RUN

variables:
  genie_space_id:
    description: "Curated vapor-eyes Genie Space id (from Task 18)"
    default: ""
```

(If the DAB `genie_space` resource type is unavailable in the installed CLI, fall back to injecting `genie-space-id` as an app secret/env and note it in BUILD.md — verified live in Task 20.)

- [ ] **Step 2: Write `gbx-app-dev.sh`**

```bash
#!/bin/bash
# gbx:app:dev - Run Genie Map locally (pnpm dev)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
APP_DIR="$PROJECT_ROOT/apps/genie_map"
show_help() { cat <<EOF
$(print_banner "🗺️  GeoBrix: Genie Map dev server")
Run the Genie Map app locally with hot reload (client :5173, server :3000).
USAGE: bash scripts/commands/gbx-app-dev.sh [--help]
NOTES: requires apps/genie_map/.env (copy from .env.example). Runs pnpm install if needed.
EOF
exit 0; }
[ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] && show_help
cd "$APP_DIR" || exit 1
[ -f .env ] || { echo "❌ apps/genie_map/.env missing — copy .env.example"; exit 1; }
[ -d node_modules ] || pnpm install
exec pnpm dev
```

- [ ] **Step 3: Write `gbx-app-deploy.sh`**

```bash
#!/bin/bash
# gbx:app:deploy - Build + deploy Genie Map to Databricks Apps via DAB
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
APP_DIR="$PROJECT_ROOT/apps/genie_map"
PROFILE="oauth-fe"
LOG_ARG=""
show_help() { cat <<EOF
$(print_banner "🚀 GeoBrix: Deploy Genie Map")
Build the app (pnpm build) and deploy to Databricks Apps via the DAB bundle.
USAGE: bash scripts/commands/gbx-app-deploy.sh [--profile <p>] [--log <path>] [--help]
OPTIONS: --profile (default oauth-fe), --log, --help
EOF
exit 0; }
while [ $# -gt 0 ]; do case "$1" in
  --help|-h) show_help;;
  --profile) PROFILE="$2"; shift 2;;
  --log) LOG_ARG="$2"; shift 2;;
  *) echo "Unknown option: $1"; exit 1;;
esac; done
cd "$APP_DIR" || exit 1
[ -d node_modules ] || pnpm install
pnpm build || exit 1
cd "$APP_DIR/bundle" || exit 1
databricks bundle deploy --profile "$PROFILE" || exit 1
databricks bundle run genie_map --profile "$PROFILE"
```

- [ ] **Step 4: Write the two `.md` registrations**

Each `.md`: short title, 1-2 sentence description, `USAGE`, options, one example — matching the existing `gbx-docker-attach.md` shape.

- [ ] **Step 5: Make executable + verify `--help`**

```bash
chmod +x scripts/commands/gbx-app-dev.sh scripts/commands/gbx-app-deploy.sh
bash scripts/commands/gbx-app-dev.sh --help && bash scripts/commands/gbx-app-deploy.sh --help
```
Expected: both print banners and exit 0.

- [ ] **Step 6: Commit**

```bash
git add apps/genie_map/bundle scripts/commands/gbx-app-dev.* scripts/commands/gbx-app-deploy.*
git commit -m "feat(genie-map): DAB bundle + gbx:app:dev / gbx:app:deploy commands

Co-authored-by: Isaac"
```

---

### Task 13: Slide-ware diagrams (sources authored; batch-rendered)

**Files:**
- Create: `apps/genie_map/docs/diagrams/genie-map.py` (generator, mirrors `resources/images/generators/vapor-eyes.py` conventions)
- Create (rendered, when online): `apps/genie_map/docs/diagrams/*.png`

**Interfaces:**
- Consumes: the vapor-eyes accent palette + Chrome-render SVG→PNG→PIL-crop pipeline.
- Produces: 4 diagram PNGs referenced by BUILD.md + the docs page.

- [ ] **Step 1: Read the existing generator conventions**

Read `resources/images/generators/vapor-eyes.py` (THEMES dict, render helper) so the new generator matches palette + output naming.

- [ ] **Step 2: Author `genie-map.py` with four diagram specs**

`genie-map-architecture` (User → client/server → {warehouse←gold, Genie Space}); `genie-map-two-paths` (viewport vs NLP); `genie-map-lineage` (GeoBrix/vapor-eyes gold → the new `wells_enriched_latest` MV → map layers); `genie-map-registry` (one config → many layers, helios as future plug-in). Consider a fifth `genie-map-dynamic-h3` explainer (zoom+density → resolution; cell-coarsen vs point-refine) — it's a strong slide. Reuse the vapor-eyes accent progression.

- [ ] **Step 3: Batch-render (online)**

Run the generator per the vapor-eyes generator's render path; produce the 4 PNGs.
Expected: 4 PNGs in `apps/genie_map/docs/diagrams/`, visually consistent with vapor-eyes diagrams.

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/docs/diagrams
git commit -m "docs(genie-map): slide-ware diagrams (architecture, two-paths, lineage, registry)

Co-authored-by: Isaac"
```

---

### Task 14: `BUILD.md` implementer narrative + reproduce-it runbook

**Files:**
- Modify: `apps/genie_map/docs/BUILD.md` (started in Task 2)

**Interfaces:**
- Produces: the technical-implementer artifact (§9a of the spec).

- [ ] **Step 1: Write the narrative sections**

Sections: (1) What we adapted (prototype → vapor-eyes); (2) The layer-registry contract (`LayerDef`/`DatasetConfig`/`H3ResConfig`, how to add a layer, how helios plugs in later); (3) The **density-aware dynamic H3** design (why zoom-only regressed on sparse data; cell-coarsen via `h3_toparent` vs point-refine via `h3_longlatash3`; the target-cell-count heuristic) and the new `wells_enriched_latest` MV and *why* (single wells source for both H3 + points, basin/county joins); (4) Genie Space curation decisions; (5) Deploy wiring (`gbx:app:*`, DAB); (6) Gotchas with fixes (SRID-0 re-tag, `*_geojson` detection, `__LLM_MODEL__` build-time bake, `oauth-fe` profile). No internal planning vocabulary.

- [ ] **Step 2: Write the reproduce-it runbook**

Ordered, verifiable steps: rerun SDP for the 2 MVs (Task 18) → create/curate Genie Space (Task 18) → set bundle `genie_space_id` var → `gbx:app:deploy`. Each step states its verification.

- [ ] **Step 3: Embed provenance**

Reference the Task-19/20 screenshots (working viewport H3, wells layers, an NL query rendering) and the actual SQL templates.

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/docs/BUILD.md
git commit -m "docs(genie-map): implementer build narrative + reproduce-it runbook

Co-authored-by: Isaac"
```

---

### Task 15: `README.md` quickstart + docs-site example page

**Files:**
- Create: `apps/genie_map/README.md`
- Create: `docs/docs/examples/genie-map.mdx`

**Interfaces:**
- Produces: the app quickstart + the site page embedding Task 13 diagrams.

- [ ] **Step 1: Write `README.md`**

Quickstart: prerequisites (vapor_eyes_lf gold materialized, a curated Genie Space id, mapbox token), `cp .env.example .env`, `gbx:app:dev`, `gbx:app:deploy`. Link to `docs/BUILD.md` for the deep dive.

- [ ] **Step 2: Write `docs/docs/examples/genie-map.mdx`**

Short user-facing page: what Genie Map is, the two paths, embed the 4 diagrams, link into the vapor-eyes story. Enforce user-facing voice (no internal vocabulary).

- [ ] **Step 3: Voice check**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+|subagent|dispatch" docs/docs/examples/genie-map.mdx apps/genie_map/README.md`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add apps/genie_map/README.md docs/docs/examples/genie-map.mdx
git commit -m "docs(genie-map): README quickstart + docs-site example page

Co-authored-by: Isaac"
```

---

### Task 16: New gold MV `wells_enriched_latest` (basin/play + county/state)

> This is the **single** new wells MV. The originally-planned fixed
> `wells_h3_density_latest` MV was dropped (see the Dynamic-H3 note in "Task ordering
> rationale"): the wells H3 layer aggregates *these points* on the fly at a
> zoom+density-driven resolution (Task 8's `wells_h3.sql`), so `wells_enriched_latest`
> feeds both the wells point layer AND the wells H3 layer.

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py` (append MV)
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/validate/test_wells_gold.py` (new)

**Interfaces:**
- Consumes: `wells_shl` (SCD2), `ref_shale_plays` (`play_name`, `play_geom`), `ref_counties` (`county_name`, `state_fp`, `geoid`, `county_geom`).
- Produces: MV `wells_enriched_latest` with `api, operator, lease, field, well_url, longitude, latitude, well_geom_native (GEOMETRY 4326), play_name, county_name, state_fp, geoid, county_rrc`. (Point columns are aliased `longitude`/`latitude` so the app's `wells_points.sql` and the point-sourced `wells_h3.sql` read them directly.)

- [ ] **Step 1: Write the failing validator test**

Add `tests/validate/test_wells_gold.py` (validators in this repo run against a
materialized dev pipeline; follow the sibling `tests/validate/` pattern):

```python
def test_wells_enriched_latest_schema(spark):
    df = spark.read.table("geospatial_docs.vapor_eyes_lf.wells_enriched_latest")
    cols = set(df.columns)
    assert {"api", "operator", "longitude", "latitude",
            "play_name", "county_name", "state_fp", "geoid"} <= cols
    # one row per api (multi-play containment deduped to a single deterministic play)
    assert df.count() == df.select("api").distinct().count()
    # map-facing geometry re-tagged 4326 (SRID-0 silently unrendered)
    srid = df.selectExpr("st_srid(well_geom_native) AS s").first()["s"]
    assert srid == 4326
```

- [ ] **Step 2: Run to verify it fails**

Run (once the dev pipeline is materialized): the repo's validate command against this test.
Expected: FAIL — table does not exist yet.

- [ ] **Step 3: Append the MV**

```python
@dp.materialized_view(
    name="wells_enriched_latest",
    comment="Current well inventory spatially tagged with shale play + county/state (map-ready points)",
)
def wells_enriched_latest():
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    spark = SparkSession.getActiveSession()

    wells = (
        spark.read.table("wells_shl").filter(F.col("__END_AT").isNull())
        .select(
            "api", "operator", "lease", "field", "well_url",
            F.col("county").alias("county_rrc"),
            F.expr("st_x(st_geomfromwkb(well_geom))").alias("longitude"),
            F.expr("st_y(st_geomfromwkb(well_geom))").alias("latitude"),
            F.expr("st_setsrid(st_geomfromwkb(well_geom), 4326)").alias("well_geom_native"),
        )
    )
    plays = spark.read.table("ref_shale_plays").select("play_name", "play_geom")
    counties = spark.read.table("ref_counties").select(
        "county_name", "state_fp", "geoid", "county_geom"
    )

    # Play: a well may fall in multiple/zero plays. Keep one deterministic play
    # (first by play_name) via a windowed row_number; NULL when outside all plays.
    with_play = (
        wells.join(plays, F.expr("st_contains(play_geom, well_geom_native)"), "left")
        .withColumn(
            "_pr",
            F.row_number().over(Window.partitionBy("api").orderBy(F.col("play_name").asc_nulls_last())),
        )
        .filter("_pr = 1").drop("_pr", "play_geom")
    )
    # County expected unique per point.
    with_county = (
        with_play.join(counties, F.expr("st_contains(county_geom, well_geom_native)"), "left")
        .drop("county_geom")
    )
    return with_county.select(
        "api", "operator", "lease", "field", "well_url", "county_rrc",
        "longitude", "latitude", "well_geom_native",
        "play_name", "county_name", "state_fp", "geoid",
    )
```

- [ ] **Step 4: Verify (materialize + run test)**

Deploy + run the vapor-eyes SDP dev target (existing bundle) and run the validator.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py notebooks/examples/vapor-eyes/lakeflow/tests/validate/test_wells_gold.py
git commit -m "feat(vapor-eyes): wells_enriched_latest gold MV with play + county/state joins

One row per well tagged with shale play (first-containing, deterministic)
and authoritative county/state from ref_counties; SRID 4326 point.

Co-authored-by: Isaac"
```

---

### Task 18: Materialize gold + curate the vapor-eyes Genie Space

**Files:**
- Modify: none (workspace-side operation; record the space id in BUILD.md + bundle var)

**Interfaces:**
- Consumes: all gold tables incl. the new `wells_enriched_latest` MV.
- Produces: a Genie Space id (recorded for Task 12's `genie_space_id` var + `.env`).

> **Extraction note** (see `2026-07-16-geobrix-deploy-helpers-vision.md`): keep the space's
> table list + curated instructions/example-SQL **declarative** (data, not one-off glue) so a
> future `create_genie_space(tables, warehouse, …)` helper can be lifted from this manual
> curation. Don't build the helper now.

- [ ] **Step 1: Deploy + run the vapor-eyes SDP so the new MV materializes**

Run the existing vapor-eyes Lakeflow bundle (`-p oauth-fe`) to update the pipeline with Task 16 (`wells_enriched_latest`). Verify the MV exists and is non-empty via a warehouse query.

- [ ] **Step 2: Create the Genie Space**

Create a Genie Space (workspace UI or API) over `geospatial_docs.vapor_eyes_lf`, adding tables: `hotspot_latest`, `plume_leaderboard_latest`, `wells_enriched_latest`, `plume_candidate_wells`, `ref_shale_plays`, `ref_counties`, `operator_intensity_latest`, `detections_by_county`, `emissions_by_play`. (If a space already exists, reuse its id — the goal is wiring the app to *a* space, not necessarily creating one.)

- [ ] **Step 3: Add instructions + example SQL that emit `*_geojson` geometry**

Curate: column descriptions; join hints (wells↔plumes via `plume_candidate_wells`; ↔basin via `st_contains(play_geom, ...)`; ↔county via `st_contains(county_geom, ...)`); the concentration-led framing (rank by `max_conc_ppmm`, never sum emission rates). Add example NL→SQL pairs whose SELECT aliases geometry as `hex_geojson`/`plume_geojson` via `ST_ASGEOJSON(...)` so the app renders answers. Example prompts: "well density in Loving County, TX"; "operators with the most wells in the Delaware Basin"; "highest-concentration plumes and their nearest operator".

- [ ] **Step 4: Smoke-test the space**

Ask one geometry prompt and one aggregate prompt in the space; confirm the geometry answer returns a `*_geojson` column.

- [ ] **Step 5: Record the space id**

Put the id in `apps/genie_map/bundle/databricks.yml` `genie_space_id` default + note in `apps/genie_map/.env.example` (leave blank there, real value in local `.env`) and BUILD.md runbook.

- [ ] **Step 6: Commit the recorded id**

```bash
git add apps/genie_map/bundle/databricks.yml apps/genie_map/docs/BUILD.md
git commit -m "chore(genie-map): record curated vapor-eyes Genie Space id

Co-authored-by: Isaac"
```

---

### Task 19: Live smoke — viewport SQL templates against `vapor_eyes_lf`

**Files:**
- Modify: none (verification; capture provenance into BUILD.md)

**Interfaces:**
- Consumes: the four SQL templates + materialized gold.
- Produces: evidence each layer query returns rows with the expected columns.

- [ ] **Step 1: Run each SQL template on the warehouse**

For each of `hotspot_h3.sql`, `wells_h3.sql`, `plume_points.sql`, `wells_points.sql`: substitute a Delaware Basin bbox + the fully-qualified table name (and, for the two H3 queries, the dynamic-res params — pick a low-zoom and a high-zoom case), run on warehouse `82e587bd93c6cbcf` (`-p oauth-fe`, or the databricks-query skill). Verify each returns rows and the columns the hooks expect (`hex`+metric, or `longitude/latitude`+tooltips), and that the H3 queries return **coarser hexes at low zoom / finer at high zoom** (dynamic resolution working).

- [ ] **Step 2: Capture results into BUILD.md**

Record row counts + a sample row per query as provenance.

- [ ] **Step 3: Commit**

```bash
git add apps/genie_map/docs/BUILD.md
git commit -m "docs(genie-map): capture live SQL-template smoke results

Co-authored-by: Isaac"
```

---

### Task 20: Deploy + end-to-end demo verification

**Files:**
- Modify: none (verification; capture screenshots for BUILD.md/diagrams)

**Interfaces:**
- Consumes: everything.
- Produces: a deployed app + screenshots proving the MVP paths.

- [ ] **Step 1: Set local `.env` and run locally**

`cp apps/genie_map/.env.example apps/genie_map/.env`, fill token + mapbox + genie space id, `gbx:app:dev`. Confirm the map renders CH4 hotspots at low zoom, well-density H3, and wells/plume points at high zoom.

- [ ] **Step 2: Exercise the Genie NLP path**

Ask "well density in Loving County, TX" and "highest-concentration plumes and their nearest operator"; confirm at least one renders a layer via the `*_geojson` path.

- [ ] **Step 3: Deploy**

Run `bash scripts/commands/gbx-app-deploy.sh --profile oauth-fe`. Confirm the app comes up in the workspace and the deployed URL renders.

- [ ] **Step 4: Capture screenshots + finalize BUILD.md provenance**

Screenshot each working path; embed in BUILD.md; verify diagrams (Task 13) render.

- [ ] **Step 5: Commit**

```bash
git add apps/genie_map/docs
git commit -m "docs(genie-map): end-to-end demo verification + screenshots

Co-authored-by: Isaac"
```

---

## Follow-on (separate plans, not this MVP)

- **Phase 2 — PMTiles:** consume the SDP's PMTiles fanout shards + light overview as a MapLibre/deck.gl vector layer. New plan.
- **Phase 3+ — Raster/EMIT overlay; helios as a selectable Genie Space + dataset config** (proves the registry seam); multi-layer selection UI. New plan(s).

## Self-Review notes

- **Spec coverage:** §1 goals → Tasks 1–20; §3 data spine → registry (5) + SQL (6–9); §4 registry + dynamic H3 → Tasks 4–6, 8; §5 gold MV → Task 16; §6 Genie Space → Task 18; §7 packaging/deploy → Tasks 1,3,12; §8 phasing → this plan = P0+P1, follow-on noted; §9 storytelling → Tasks 13 (slide-ware), 14 (BUILD.md), 15 (README/site), with provenance capture in 19–20; §9 constraint (render batched online) honored in Task 13. All spec sections mapped.
- **Type consistency:** `LayerDef`/`DatasetConfig` (+ `H3ResConfig`) defined in Task 4, consumed in 5/6/10; `createPointLayerConfig`/`createH3LayerConfig` signatures consistent across 4/6; `buildLayerParams`/`useLayerData` signatures consistent 6/10; the dynamic-H3 SQL params emitted by `buildLayerParams` (Task 6: `zoom_level`, `zoom_break_1..4`, `res_1..5`, `min_res`, `native_res`/`max_res`, `target_cells`) match the `:param` names consumed in `hotspot_h3.sql` (Task 6) and `wells_h3.sql` (Task 8); `wells_enriched_latest` columns (Task 16: `longitude`, `latitude`, `operator`, `county_name`, `play_name`, `api`) match the reads in `wells_h3.sql` (Task 8) and `wells_points.sql` (Task 9). Registry query names (`hotspot_h3`, `wells_h3`, `plume_points`, `wells_points`) match the SQL file names in Tasks 6–9.
- **Placeholder scan:** no TBD/TODO; every code step shows code; SQL/bundle/command bodies are complete. One documented fallback (DAB `genie_space` resource type) is a real contingency with a stated alternative, not a placeholder.
- **Known live-verify dependency:** Tasks 8/9 SQL and Task 10 wiring are written against the documented MV schema (16/17); their live correctness is verified in Task 19 after Task 18 materializes gold — ordering noted at top.
