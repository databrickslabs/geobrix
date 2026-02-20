# Restart Documentation Server

Restarts Docusaurus documentation server

## Usage

```bash
bash .cursor/commands/gbx-docs-restart.sh [OPTIONS]
```

## Options

- `--skip-build` - Skip npm build, serve existing build
- `--port <number>` - Custom port (default: 3000)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Restart with rebuild
bash .cursor/commands/gbx-docs-restart.sh

# Restart without rebuild
bash .cursor/commands/gbx-docs-restart.sh --skip-build

# Restart on custom port
bash .cursor/commands/gbx-docs-restart.sh --port 3001
```

## Notes

- Stops existing server (all ports)
- Starts new server with specified options
- Combines `gbx:docs:stop` + `gbx:docs:start`
