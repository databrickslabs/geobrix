# Vapor-Eyes dashboard: parameterize catalog/schema (stop hardcoding)

## Problem

The vapor-eyes AI/BI dashboard
(`notebooks/examples/vapor-eyes/lakeflow/dashboards/vapor_eyes_lf.lvdash.json`) hardcodes
`geospatial_docs.vapor_eyes_lf.` as the catalog/schema prefix in all **12** of its dataset
queries. Every other part of the example is catalog/schema-parameterized via the bundle
variables `var.catalog` / `var.schema` (the pipeline reads them from `spark.conf`, see
`transformations/_config.py`), and the deploy override
(`databricks.override.yml`) already repoints those vars per workspace.

But Databricks Asset Bundles upload a `.lvdash.json` **verbatim** — they do NOT perform
`${var.x}` substitution inside the dashboard file. So the dashboard ignores `var.catalog`.
An end user who follows the README, sets their own `catalog`/`schema` in an override, and
deploys gets a dashboard whose 12 widgets all fail with "catalog `geospatial_docs` not
found" (the FEVM metastore, for instance, does not permit a `geospatial_docs` catalog).
This is a correctness bug in a **public** example — everything deploys except the
dashboard, which silently breaks.

## Fix (DAB-native, no build-time templating)

Databricks dashboards support a **default catalog/schema** for datasets: queries that omit
the `catalog.schema` prefix inherit it. DAB exposes this on the dashboard resource as
`dataset_catalog` / `dataset_schema`, and those fields accept bundle variables (verified
against the installed CLI's `databricks bundle schema` — `resources.Dashboard` includes
both `dataset_catalog` and `dataset_schema`).

Two coordinated edits:

1. **`dashboards/vapor_eyes_lf.lvdash.json`** — strip the `geospatial_docs.vapor_eyes_lf.`
   prefix from all 12 dataset queries, leaving unqualified table names. Example:
   `SELECT * FROM geospatial_docs.vapor_eyes_lf.cm_monitoring_status`
   → `SELECT * FROM cm_monitoring_status`
   (queries with trailing `ORDER BY` / `LIMIT` keep those clauses; only the table
   reference changes). All 12 are simple single-table `SELECT *` — no joins, no
   cross-catalog references, one catalog prefix in the whole file — so the strip is
   mechanical and safe.

2. **`lakeflow/databricks.yml`** — on `resources.dashboards.vapor_eyes_lf_dashboard`, add:
   ```yaml
   dataset_catalog: ${var.catalog}
   dataset_schema: ${var.schema}
   ```
   `var.catalog` / `var.schema` already exist (defaults `geospatial_docs` /
   `vapor_eyes_lf`), so committed behavior is unchanged for the reference deployment, and
   any override (including the FEVM `serverless_stable_genie_map_catalog`) now drives the
   dashboard the same way it already drives the pipeline.

Net effect: one consistent parameterization story across pipeline + dashboard; the public
example deploys cleanly for any user who repoints `catalog`/`schema`.

## Scope

- **In scope:** the 12 dataset query strings in the lvdash.json + the 2 new yaml fields.
- **Out of scope:** widget/layout/page structure (39 widgets across 4 pages — untouched),
  and other example artifacts (the pipeline/notebooks already parameterize correctly; a
  broader audit is deferred).

## Verification

1. Static: after the edit, `grep -c geospatial_docs vapor_eyes_lf.lvdash.json` → `0`;
   `databricks bundle validate` for the FEVM profile passes.
2. Live: deploy the dashboard to the FEVM workspace (`genie-map-env`, override
   `catalog: serverless_stable_genie_map_catalog`), open it, confirm the widgets resolve
   and return rows against the FEVM catalog (all 12 tables already exist there).
3. Reference-default sanity: the committed defaults still resolve to
   `geospatial_docs.vapor_eyes_lf` (no behavior change for that deployment).

## Risks

- If any dataset actually needed a cross-catalog reference, prefix-stripping would break
  it. Confirmed not the case — all 12 target the one gold schema.
- `dataset_catalog`/`dataset_schema` apply as the DEFAULT only to unqualified queries; any
  future dataset that re-adds a fully-qualified name would bypass it. Note this in the
  dashboard's neighboring docs if relevant (deferred; out of scope here).
