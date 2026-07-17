# Genie Map — setup runbook

How to stand up Genie Map in a workspace, end to end. It is designed to be
**repeatable**: most of it is declared in the DAB bundle (`databricks.yml`) and a
committed Genie-space definition; the few steps the platform can't automate are
called out explicitly and are one command or one copy/paste each.

**Legend:** 🤖 automated (bundle/script) · ✋ manual (secret value, UI curation, or
a heavy run you trigger).

## Prerequisites

- A Databricks workspace with **app capacity** and a **Pro/Serverless SQL warehouse**.
- Databricks CLI ≥ 0.229, authenticated: `databricks auth login --host <url> --profile <p>`.
- Node ≥ 20 + pnpm 10 (only for local dev; the app builds on-compute when deployed).
- The vapor-eyes gold schema must exist and be populated (the Lakeflow SDP under
  `notebooks/examples/vapor-eyes/lakeflow/` produces it — see that bundle's README).

## Configure for your workspace

Copy the override template and fill in your workspace host, profile, and warehouse:

```bash
cd apps/genie_map
cp databricks.override.yml.example databricks.override.yml   # git-ignored
# edit host / profile / warehouse_id
```

If your gold tables live in a catalog/schema other than
`serverless_stable_genie_map_catalog.vapor_eyes_lf`, also update:
- `VITE_GOLD_CATALOG` / `VITE_GOLD_SCHEMA` in `app.yaml` (client build), and
- the table identifiers in `genie_space.geniespace.json` (must stay sorted).

## Steps

### 1. 🤖 Deploy the bundle (app + Genie Space + resources)

```bash
databricks bundle deploy -p <profile>          # from apps/genie_map/
```

This creates/updates: the **Databricks App**, its **OAuth scopes**
(`dashboards.genie`, `sql`, `serving.serving-endpoints`), its **resource grants**
(warehouse `CAN_USE`, serving endpoint `CAN_QUERY`, the Genie space `CAN_RUN`), and
the **Genie Space** itself (table set from `genie_space.geniespace.json` + the guardrail
description). The app is wired to the space it just created — no space id to paste.

### 2. 🤖 Build + start the app

```bash
databricks apps start genie-map -p <profile>   # if compute is stopped
bash ../../scripts/commands/gbx-app-deploy.sh --profile <profile>
```

`gbx:app:deploy` builds the client on-compute and starts the server. (First deploy:
create → start compute → deploy, in that order — the command handles the build/deploy.)

### 3. 🤖 Grant the app SP data access + check secrets

```bash
bash ../../scripts/commands/gbx-app-setup.sh --profile <profile>
```

`gbx:app:setup` grants the app's **service principal** `USE CATALOG` / `USE SCHEMA` /
`SELECT` on the gold schema (the bundle can't, without taking ownership of a schema the
pipeline manages), verifies the UC secrets, and prints the manual Genie step. Idempotent.

### 4. ✋ Create the UC secrets (values only)

Only if `gbx:app:setup` reported them missing. The **values** are secret, so you set them:

```bash
databricks secrets-uc create-secret earthdata_token     <catalog> <schema> "<TOKEN>" -p <profile>
databricks secrets-uc create-secret carbon_mapper_token <catalog> <schema> "<TOKEN>" -p <profile>
```

(Needed by the pipeline downloads, not the app itself.)

### 5. ✋ Curate the Genie Space in the UI

The bundle set the table list + a short description; the **rich instructions and example
SQL** are pasted in the Genie editor (the API doesn't reliably take them). Open the
**"Vapor-Eyes — Permian Methane (Genie Map)"** space and paste the two blocks from
[`GENIE-SPACE.md`](./GENIE-SPACE.md): **block A** into the *Instructions* tab, **block B**
as *Example SQL queries*. This is what makes natural-language answers reliable (e.g. it
stops Genie from filtering by a non-existent "Permian" play).

### 6. ✋ Re-authorize (first user only)

Because step 1 may have added the `dashboards.genie` OAuth scope, the **first** user opens
the app in a fresh session and accepts the consent prompt so their token carries the new
scope. (A cached pre-scope token yields `Invalid scope, required scopes: genie`.)

## Verify

- Map renders CH₄ hotspots / wells / plumes across zoom (viewport SQL path).
- The AI panel names the configured space and answers a data question, rendering a
  `*_geojson` result as a map layer (Genie path).
- `databricks apps logs genie-map -p <profile>` shows no `INSUFFICIENT_PERMISSIONS` or
  `Invalid scope` errors.

## What lives where

| Concern | Where | Auto? |
|---|---|---|
| App, scopes, warehouse/serving/genie resources | `databricks.yml` | 🤖 |
| Genie Space + table set + guardrail description | `databricks.yml` + `genie_space.geniespace.json` | 🤖 |
| Client build config (catalog/model baked in) | `app.yaml` | 🤖 |
| App SP catalog/schema grants | `gbx:app:setup` | 🤖 (script) |
| UC secret **values** | operator | ✋ |
| Genie instructions + example SQL | `GENIE-SPACE.md` → UI | ✋ (copy/paste) |
| First-user OAuth re-consent | operator | ✋ |
