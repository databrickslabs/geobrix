# gbx:app:setup

One-time, idempotent setup for the Genie Map app covering the pieces the DAB
bundle cannot declare: grants the app's service principal access to the gold
catalog/schema, verifies the required Unity Catalog secrets exist, and prints
the manual Genie-Space UI curation step.

Run **after** `gbx:app:deploy` has created the app.

## Usage

```bash
bash scripts/commands/gbx-app-setup.sh [OPTIONS]
```

## Options

- `--profile <p>` — CLI profile of the app's workspace (required)
- `--app <name>` — app name (default `genie-map`)
- `--warehouse <id>` — SQL warehouse id (default: `DATABRICKS_WAREHOUSE_ID` in `databricks.env`)
- `--catalog <c>` — gold catalog (default: `VITE_GOLD_CATALOG` in `databricks.env`)
- `--schema <s>` — gold schema (default: `VITE_GOLD_SCHEMA` in `databricks.env`, else `vapor_eyes_lf`)
- `--help` — show help and exit

## Example

```bash
bash scripts/commands/gbx-app-setup.sh --profile <profile>
```

## What it does / doesn't

Automates: the app-SP UC grants (`USE CATALOG` / `USE SCHEMA` / `SELECT`) and a
secret-existence check. Does **not** automate: creating the UC secret values,
the pipeline run, or the Genie-Space instruction/example-SQL curation (pasted in
the UI — see `examples/apps/genie_map/docs/GENIE-SPACE.md`). Full sequence in
`examples/apps/genie_map/docs/SETUP.md`.
