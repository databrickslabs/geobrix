# Run Genie Map (dev)

Run the Genie Map app locally with hot reload (client :5173, server :3000).

## Usage

```bash
bash scripts/commands/gbx-app-dev.sh [OPTIONS]
```

## Options

- `--help`, `-h` - Display help message

## Examples

```bash
# Start the local dev server
bash scripts/commands/gbx-app-dev.sh
```

## Notes

- Requires `apps/genie_map/databricks.env` (copy from `databricks.env.example` and fill it in).
- The env file is sourced into the shell so Vite's `loadEnv('')` and the server pick up the vars.
- Runs `pnpm install` automatically if `node_modules` is missing.
- Ctrl+C to stop.
