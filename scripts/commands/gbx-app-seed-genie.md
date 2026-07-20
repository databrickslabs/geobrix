# gbx:app:seed-genie

Seed the Genie Map app's Genie Space with its curated **instructions** and
**example SQL queries** from the tracked source of truth
(`apps/genie_map/docs/GENIE-SPACE.md`, Paste blocks A and B), via the Genie
spaces update API — no manual UI pasting.

The DAB `genie_space` resource re-applies only the **table set**
(`genie_space.geniespace.json`) on every `gbx:app:deploy`, which **wipes** any
instructions/examples curated in the space. Run this command after each deploy
to restore them. It is idempotent (each instruction/example gets a
content-derived id, so re-runs converge and satisfy the API's sorted-by-id
requirement).

## Usage

```bash
bash scripts/commands/gbx-app-seed-genie.sh --profile <p> [OPTIONS]
```

## Options

- `--profile <p>` — CLI profile of the workspace you deployed to (**required**)
- `--space-id <id>` — Genie Space id (default: resolved from `databricks bundle summary`)
- `--warehouse <id>` — SQL warehouse id (default: resolved from `databricks bundle summary`)
- `--dry-run` — parse and print what would be written; do not PATCH
- `--log <path>` — tee output to `test-logs/<path>`
- `--help`, `-h` — show help

By default the Genie Space id and warehouse are read from `databricks bundle
summary` for the given profile — the space this bundle deployed to that
workspace — so the command follows your deploy target wherever it is (the shared
`oauth-fe` workspace is perpetually at app capacity, so deploys go elsewhere).

## Examples

```bash
# Re-seed after a deploy (resolves the space from the bundle)
bash scripts/commands/gbx-app-seed-genie.sh --profile genie-map-env

# Preview what would be written (no changes)
bash scripts/commands/gbx-app-seed-genie.sh --profile genie-map-env --dry-run
```
