# Genie Map App — Phase 0 + Phase 1 (MVP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt the `isaac_work/genie_map` kepler.gl + `@databricks/appkit` prototype into a deployable Databricks App (`apps/genie_map/`) that renders the vapor-eyes methane gold data — H3 hexagon layers (CH4 hotspots + wells density), wells/plume point layers, and a curated Genie NLP path — with implementer + slide-ware storytelling artifacts.

**Architecture:** Copy the prototype into `apps/genie_map/`, replace its build-time `VITE_*` taxi canonical-schema with a static **layer registry** (`config/datasets/vapor-eyes.ts` + per-layer SQL templates), point every layer at Lakeflow gold `geospatial_docs.vapor_eyes_lf`, add two new gold MVs (`wells_h3_density_latest`, `wells_enriched_latest`) to the vapor-eyes SDP, curate a Genie Space, and deploy via a DAB bundle wired to `gbx:app:*` commands.

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
│   ├── hotspot_h3.sql                     # Task 6 (net-new, replaces h3_aggregation.sql)
│   ├── wells_h3_density.sql               # Task 8
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
notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py   # + 2 MVs (Task 16, 17)
notebooks/examples/vapor-eyes/lakeflow/tests/validate/                     # + MV validators
```

**`gbx:*` commands:**
```
scripts/commands/gbx-app-dev.{md,sh}       # Task 12
scripts/commands/gbx-app-deploy.{md,sh}    # Task 12
```

---

## Task ordering rationale

Tasks 1–15 are the **app + deploy + docs** track. Tasks 16–18 are the **SDP gold MV** track. The SDP track (16–18) is independent of the app track and can run in parallel; the app's wells layers (Tasks 8, 9) *consume* the MVs from Tasks 16–17, so **run 16–17 before verifying 8–9 against live data** — but the app code (SQL templates + hooks) can be written against the documented MV schema first (TDD-style) and verified once the MVs materialize. The Genie Space (Task 18) depends on all gold tables existing.

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

export interface LayerDef {
  id: string;                     // kepler dataId + layer key, e.g. 'ch4_hotspots'
  kind: LayerKind;
  label: string;
  queryName: string;              // key into config/queries (file name without .sql)
  hexField?: string;              // kind==='h3': the string hex column returned by SQL
  valueField: string;            // color/size metric column
  lngField?: string;              // kind==='point'
  latField?: string;             // kind==='point'
  tooltipFields: string[];
  palette?: string;               // kepler named colorRange, default 'Global Warming'
  enable3d?: boolean;
  zoomVisible: { min: number; max: number };  // activeWhen: min <= z < max
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
  it('ch4_hotspots is an H3 layer keyed on hex/ch4_max', () => {
    const l = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    expect(l.kind).toBe('h3');
    expect(l.queryName).toBe('hotspot_h3');
    expect(l.valueField).toBe('ch4_max');
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
    { id: 'ch4_hotspots', kind: 'h3', label: 'CH₄ Hotspots (latest)',
      queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
      tooltipFields: ['hex', 'ch4_max', 'ch4_mean', 'n_obs'],
      palette: 'Global Warming', enable3d: true,
      zoomVisible: { min: 0, max: 12 } },
    { id: 'well_density', kind: 'h3', label: 'Well Density (H3)',
      queryName: 'wells_h3_density', hexField: 'hex', valueField: 'well_count',
      tooltipFields: ['hex', 'well_count', 'operator_count'],
      palette: 'Uber Viz Sequential', enable3d: true,
      zoomVisible: { min: 0, max: 12 } },
    { id: 'wells', kind: 'point', label: 'Wells',
      queryName: 'wells_points', valueField: 'well_count',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'operator', 'field', 'county', 'play_name'],
      zoomVisible: { min: 12, max: 24 } },
    { id: 'plumes', kind: 'point', label: 'EMIT Plumes',
      queryName: 'plume_points', valueField: 'max_conc_ppmm',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'max_conc_ppmm', 'lead_operator', 'lead_county'],
      zoomVisible: { min: 12, max: 24 } },
  ],
};

export const VAPOR_EYES_TABLES = {
  hotspot: T('hotspot_latest'),
  wellDensity: T('wells_h3_density_latest'),
  wellsEnriched: T('wells_enriched_latest'),
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
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add apps/genie_map/client/src/config/datasets
git commit -m "feat(genie-map): vapor-eyes layer registry + active-dataset selector

Four MVP layers (ch4_hotspots, well_density H3; wells, plumes points)
over vapor_eyes_lf gold. getActiveDataset() is the seam for helios later.

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

- [ ] **Step 1: Write `hotspot_h3.sql`**

Reads `hotspot_latest` directly (already H3-aggregated in gold). The hex column is derived from the stored `h3_cellid` via `h3_h3tostring`; viewport filter uses the stored `center_lon/lat` (cheap point-in-bbox, no polygon build needed since gold is pre-aggregated):

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  h3_h3tostring(h3_cellid) AS hex,
  CAST(ch4_max  AS DOUBLE) AS ch4_max,
  CAST(ch4_mean AS DOUBLE) AS ch4_mean,
  CAST(n_obs    AS DOUBLE) AS n_obs
FROM IDENTIFIER(:table_name)
WHERE center_lon BETWEEN :x_min AND :x_max
  AND center_lat BETWEEN :y_min AND :y_max
  AND ch4_max IS NOT NULL
```

- [ ] **Step 2: Delete the taxi template**

```bash
git -C /Users/mjohns/IdeaProjects/geobrix rm apps/genie_map/config/queries/h3_aggregation.sql
```

- [ ] **Step 3: Write the failing test for `useLayerData` param resolution**

The hook's pure param-building logic is unit-testable by extracting it. Create `useLayerData.ts` exporting a pure helper `buildLayerParams(layer, bounds, tableName)` and test it:

```ts
import { describe, it, expect } from 'vitest';
import { buildLayerParams } from '../useLayerData';
import type { LayerDef } from '@shared/types';

const bounds = { x_min: -103, x_max: -102, y_min: 31, y_max: 32, zoom_level: 8 };
const h3Layer: LayerDef = { id: 'ch4_hotspots', kind: 'h3', label: 'x',
  queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
  tooltipFields: [], zoomVisible: { min: 0, max: 12 } };

describe('buildLayerParams', () => {
  it('returns null when bounds are null', () => {
    expect(buildLayerParams(h3Layer, null, 't')).toBeNull();
  });
  it('builds bbox + table params for an H3 layer', () => {
    const p = buildLayerParams(h3Layer, bounds, 'geospatial_docs.vapor_eyes_lf.hotspot_latest') as any;
    expect(p).not.toBeNull();
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
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
  return {
    x_min: sql.double(bounds.x_min), x_max: sql.double(bounds.x_max),
    y_min: sql.double(bounds.y_min), y_max: sql.double(bounds.y_max),
    table_name: sql.string(tableName),
  };
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
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add apps/genie_map/config/queries apps/genie_map/client/src/hooks/useLayerData.ts apps/genie_map/client/src/hooks/__tests__
git commit -m "feat(genie-map): hotspot_h3 SQL + generic registry-driven useLayerData hook

Replaces taxi h3_aggregation.sql (viewport-adaptive re-aggregation)
with a direct read of the pre-aggregated hotspot_latest gold MV.

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

### Task 8: `wells_h3_density.sql` template

**Files:**
- Create: `apps/genie_map/config/queries/wells_h3_density.sql`

**Interfaces:**
- Consumes: `wells_h3_density_latest` MV (Task 16). `useLayerData` via `well_density` LayerDef.
- Produces: SQL returning `hex, well_count, operator_count`.

- [ ] **Step 1: Write `wells_h3_density.sql`**

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  h3_h3tostring(h3_cellid) AS hex,
  CAST(well_count     AS DOUBLE) AS well_count,
  CAST(operator_count AS DOUBLE) AS operator_count
FROM IDENTIFIER(:table_name)
WHERE center_lon BETWEEN :x_min AND :x_max
  AND center_lat BETWEEN :y_min AND :y_max
  AND well_count IS NOT NULL
```

- [ ] **Step 2: Verify**

Run: `grep -c 'h3_h3tostring' apps/genie_map/config/queries/wells_h3_density.sql`
Expected: `1`.

- [ ] **Step 3: Commit**

```bash
git add apps/genie_map/config/queries/wells_h3_density.sql
git commit -m "feat(genie-map): wells_h3_density SQL over wells_h3_density_latest MV

Co-authored-by: Isaac"
```

---

### Task 9: `wells_points.sql` template

**Files:**
- Create: `apps/genie_map/config/queries/wells_points.sql`

**Interfaces:**
- Consumes: `wells_enriched_latest` MV (Task 17). `useLayerData` via `wells` LayerDef.
- Produces: SQL returning `longitude, latitude, record_id, operator, field, county, play_name`.

- [ ] **Step 1: Write `wells_points.sql`**

```sql
-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  well_lon AS longitude,
  well_lat AS latitude,
  CAST(api AS STRING) AS record_id,
  operator, field, county_name AS county, play_name
FROM IDENTIFIER(:table_name)
WHERE well_lon BETWEEN :x_min AND :x_max
  AND well_lat BETWEEN :y_min AND :y_max
LIMIT 10000
```

- [ ] **Step 2: Verify**

Run: `grep -c 'well_lon' apps/genie_map/config/queries/wells_points.sql`
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
  well_density: VAPOR_EYES_TABLES.wellDensity,
  wells:        VAPOR_EYES_TABLES.wellsEnriched,
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

`genie-map-architecture` (User → client/server → {warehouse←gold, Genie Space}); `genie-map-two-paths` (viewport vs NLP); `genie-map-lineage` (GeoBrix/vapor-eyes gold → the 2 new wells MVs → map layers); `genie-map-registry` (one config → many layers, helios as future plug-in). Reuse the vapor-eyes accent progression.

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

Sections: (1) What we adapted (prototype → vapor-eyes); (2) The layer-registry contract (`LayerDef`/`DatasetConfig`, how to add a layer, how helios plugs in later); (3) The two new gold MVs and *why* (wells-as-H3 requirement, basin/county joins); (4) Genie Space curation decisions; (5) Deploy wiring (`gbx:app:*`, DAB); (6) Gotchas with fixes (SRID-0 re-tag, `*_geojson` detection, `__LLM_MODEL__` build-time bake, `oauth-fe` profile). No internal planning vocabulary.

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

### Task 16: New gold MV `wells_h3_density_latest`

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py` (append MV)
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/validate/test_wells_gold.py` (new)

**Interfaces:**
- Consumes: `wells_shl` (SCD2), `cfg(spark)` from `_config` (has `h3_res`).
- Produces: MV `wells_h3_density_latest` with columns `h3_cellid (bigint), well_count, operator_count, center_lon, center_lat, hex_geom (GEOMETRY 4326)`.

- [ ] **Step 1: Write the failing validator test**

Add to `tests/validate/test_wells_gold.py` a test asserting the MV exists with the expected columns and non-null `hex_geom` at SRID 4326 (validators in this repo run against a materialized dev pipeline; follow the pattern in the sibling `tests/validate/` files):

```python
def test_wells_h3_density_latest_schema(spark):
    df = spark.read.table("geospatial_docs.vapor_eyes_lf.wells_h3_density_latest")
    cols = set(df.columns)
    assert {"h3_cellid", "well_count", "operator_count",
            "center_lon", "center_lat", "hex_geom"} <= cols
    assert df.filter("hex_geom IS NULL").count() == 0
    srid = df.selectExpr("st_srid(hex_geom) AS s").first()["s"]
    assert srid == 4326
```

- [ ] **Step 2: Run to verify it fails**

Run (once the dev pipeline is materialized): the repo's validate command against this test.
Expected: FAIL — table does not exist yet.

- [ ] **Step 3: Append the MV to `gold_analytics.py`**

```python
@dp.materialized_view(
    name="wells_h3_density_latest",
    comment="Current TX RRC well inventory aggregated to H3 cells (map-ready hexagons)",
)
def wells_h3_density_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    c = cfg(spark)
    res = c["h3_res"]
    # Current SCD2 version of the well inventory.
    wells = spark.read.table("wells_shl").filter(F.col("__END_AT").isNull())

    celled = wells.withColumn(
        "h3_cellid",
        F.expr(f"h3_longlatash3(st_x(st_geomfromwkb(well_geom)), "
               f"st_y(st_geomfromwkb(well_geom)), {res})"),
    )
    agg = celled.groupBy("h3_cellid").agg(
        F.count("api").alias("well_count"),
        F.countDistinct("operator").alias("operator_count"),
    )
    return agg.select(
        "h3_cellid", "well_count", "operator_count",
        F.expr("st_x(st_geomfromwkb(h3_boundaryaswkb(h3_cellid)))").alias("center_lon"),
        F.expr("st_y(st_geomfromwkb(h3_boundaryaswkb(h3_cellid)))").alias("center_lat"),
        F.expr("st_geomfromwkb(h3_boundaryaswkb(h3_cellid), 4326)").alias("hex_geom"),
    )
```

(Uses native `h3_longlatash3` / `h3_boundaryaswkb`, matching `hotspot_latest`'s pattern at `gold_analytics.py:167`. `h3_res` default is 6 per `_config.py`.)

- [ ] **Step 4: Verify (materialize the pipeline update, then run the test)**

Deploy + run the vapor-eyes SDP dev target (existing bundle) and run the validator.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py notebooks/examples/vapor-eyes/lakeflow/tests/validate/test_wells_gold.py
git commit -m "feat(vapor-eyes): wells_h3_density_latest gold MV for Genie Map well layer

Aggregates the current SCD2 well inventory to H3 cells (well_count,
operator_count, map-ready hex_geom at SRID 4326).

Co-authored-by: Isaac"
```

---

### Task 17: New gold MV `wells_enriched_latest` (basin/play + county/state)

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py` (append MV)
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/validate/test_wells_gold.py` (extend)

**Interfaces:**
- Consumes: `wells_shl` (SCD2), `ref_shale_plays` (`play_name`, `play_geom`), `ref_counties` (`county_name`, `state_fp`, `geoid`, `county_geom`).
- Produces: MV `wells_enriched_latest` with `api, operator, lease, field, well_url, well_lon, well_lat, well_geom_native (GEOMETRY 4326), play_name, county_name, state_fp, geoid, county_rrc`.

- [ ] **Step 1: Extend the validator test**

```python
def test_wells_enriched_latest_schema(spark):
    df = spark.read.table("geospatial_docs.vapor_eyes_lf.wells_enriched_latest")
    cols = set(df.columns)
    assert {"api", "operator", "well_lon", "well_lat",
            "play_name", "county_name", "state_fp", "geoid"} <= cols
    # one row per api (dedup on multi-play containment held)
    assert df.count() == df.select("api").distinct().count()
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — table missing.

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
            F.expr("st_x(st_geomfromwkb(well_geom))").alias("well_lon"),
            F.expr("st_y(st_geomfromwkb(well_geom))").alias("well_lat"),
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
        "well_lon", "well_lat", "well_geom_native",
        "play_name", "county_name", "state_fp", "geoid",
    )
```

- [ ] **Step 4: Verify (materialize + run tests)**

Expected: PASS (both wells tests).

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
- Consumes: all gold tables incl. the two new MVs.
- Produces: a Genie Space id (recorded for Task 12's `genie_space_id` var + `.env`).

- [ ] **Step 1: Deploy + run the vapor-eyes SDP so the two new MVs materialize**

Run the existing vapor-eyes Lakeflow bundle (`-p oauth-fe`) to update the pipeline with Tasks 16/17. Verify both MVs exist and are non-empty via a warehouse query.

- [ ] **Step 2: Create the Genie Space**

Create a Genie Space (workspace UI or API) over `geospatial_docs.vapor_eyes_lf`, adding tables: `hotspot_latest`, `plume_leaderboard_latest`, `wells_h3_density_latest`, `wells_enriched_latest`, `plume_candidate_wells`, `ref_shale_plays`, `ref_counties`, `operator_intensity_latest`, `detections_by_county`, `emissions_by_play`.

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

For each of `hotspot_h3.sql`, `wells_h3_density.sql`, `plume_points.sql`, `wells_points.sql`: substitute a Delaware Basin bbox + the fully-qualified table name, run on warehouse `82e587bd93c6cbcf` (`-p oauth-fe`, or the databricks-query skill). Verify each returns rows and the columns the hooks expect (`hex`+metric, or `longitude/latitude`+tooltips).

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

- **Spec coverage:** §1 goals → Tasks 1–20; §3 data spine → registry (5) + SQL (6–9); §4 registry → Tasks 4–5; §5 gold MVs → Tasks 16–17; §6 Genie Space → Task 18; §7 packaging/deploy → Tasks 1,3,12; §8 phasing → this plan = P0+P1, follow-on noted; §9 storytelling → Tasks 13 (slide-ware), 14 (BUILD.md), 15 (README/site), with provenance capture in 19–20; §9 constraint (render batched online) honored in Task 13. All spec sections mapped.
- **Type consistency:** `LayerDef`/`DatasetConfig` defined in Task 4, consumed in 5/6/10; `createPointLayerConfig`/`createH3LayerConfig` signatures consistent across 4/6; `buildLayerParams`/`useLayerData` signatures consistent 6/10; MV column names in 16/17 match the SQL templates in 8/9 (`h3_cellid`→`h3_h3tostring`→`hex`; `well_lon/well_lat`→`longitude/latitude`; `county_name`→`county`).
- **Placeholder scan:** no TBD/TODO; every code step shows code; SQL/bundle/command bodies are complete. One documented fallback (DAB `genie_space` resource type) is a real contingency with a stated alternative, not a placeholder.
- **Known live-verify dependency:** Tasks 8/9 SQL and Task 10 wiring are written against the documented MV schema (16/17); their live correctness is verified in Task 19 after Task 18 materializes gold — ordering noted at top.
