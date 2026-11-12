# Deploy Genie Map

Build the app (`pnpm build`) and deploy to Databricks Apps via the DAB bundle.

## Usage

```bash
bash scripts/commands/gbx-app-deploy.sh [OPTIONS]
```

## Options

- `--profile <p>` - Databricks CLI profile of the target workspace (required)
- `--log <path>` - Tee output to a log file (filename → `test-logs/<name>`)
- `--help`, `-h` - Display help message

## Examples

```bash
# Build + deploy to the workspace of a profile
bash scripts/commands/gbx-app-deploy.sh --profile <profile>

# With a log
bash scripts/commands/gbx-app-deploy.sh --profile <profile> --log deploy.log
```

## Notes

- Requires `examples/apps/genie_map/databricks.env` (copy from `databricks.env.example` and fill it in).
- The env file is sourced into the shell so the build picks up the vars.
- Runs `pnpm install` automatically if `node_modules` is missing.
- Deploys the bundle at `examples/apps/genie_map/databricks.yml` (app root), then runs the app.
