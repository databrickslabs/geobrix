# Deploy Genie Map

Build the app (`pnpm build`) and deploy to Databricks Apps via the DAB bundle.

## Usage

```bash
bash scripts/commands/gbx-app-deploy.sh [OPTIONS]
```

## Options

- `--profile <p>` - Databricks CLI profile (default: `oauth-fe`)
- `--log <path>` - Tee output to a log file (filename → `test-logs/<name>`)
- `--help`, `-h` - Display help message

## Examples

```bash
# Build + deploy with the default profile
bash scripts/commands/gbx-app-deploy.sh

# Deploy with an explicit profile and log
bash scripts/commands/gbx-app-deploy.sh --profile oauth-fe --log deploy.log
```

## Notes

- Requires `apps/genie_map/databricks.env` (copy from `databricks.env.example` and fill it in).
- The env file is sourced into the shell so the build picks up the vars.
- Runs `pnpm install` automatically if `node_modules` is missing.
- Deploys the bundle in `apps/genie_map/bundle`, then runs the app.
