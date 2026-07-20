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

- `--profile <p>` — CLI profile (default `genie-map-env`)
- `--app <name>` — app name (default `genie-map`)
- `--warehouse <id>` — SQL warehouse id
- `--catalog <c>` / `--schema <s>` — gold catalog/schema
- `--help` — show help and exit

## Example

```bash
bash scripts/commands/gbx-app-setup.sh --profile genie-map-env
```

## What it does / doesn't

Automates: the app-SP UC grants (`USE CATALOG` / `USE SCHEMA` / `SELECT`) and a
secret-existence check. Does **not** automate: creating the UC secret values,
the pipeline run, or the Genie-Space instruction/example-SQL curation (pasted in
the UI — see `apps/genie_map/docs/GENIE-SPACE.md`). Full sequence in
`apps/genie_map/docs/SETUP.md`.
