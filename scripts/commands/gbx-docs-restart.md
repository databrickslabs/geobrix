# Restart Documentation Server

Restarts Docusaurus documentation server

## Usage

```bash
bash scripts/commands/gbx-docs-restart.sh [OPTIONS]
```

## Options

- `--skip-build` - Skip npm build, serve existing build
- `--port <number>` - Custom port (default: 3000). A missing or non-numeric value exits 2 with the usage text.
- `--force` - Replace the server even if another checkout started it. Default: refuse.
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Restart with rebuild
bash scripts/commands/gbx-docs-restart.sh

# Restart without rebuild
bash scripts/commands/gbx-docs-restart.sh --skip-build

# Restart on custom port
bash scripts/commands/gbx-docs-restart.sh --port 3001
```

## Notes

- Stops only the server on `--port`, never other ports
- Refuses a server started from another checkout unless `--force`
- Starts new server with specified options
- Combines `gbx:docs:stop --port` + `gbx:docs:start`
