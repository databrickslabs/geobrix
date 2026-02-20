# Start Documentation Server

Starts Docusaurus documentation server with live rebuild

## Usage

```bash
bash .cursor/commands/gbx-docs-start.sh [OPTIONS]
```

## Options

- `--skip-build` - Skip npm build, serve existing build
- `--port <number>` - Custom port (default: 3000)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Build and serve docs
bash .cursor/commands/gbx-docs-start.sh

# Serve without rebuild
bash .cursor/commands/gbx-docs-start.sh --skip-build

# Use custom port
bash .cursor/commands/gbx-docs-start.sh --port 3001

# Build and log output
bash .cursor/commands/gbx-docs-start.sh --log docs-server.log
```

## Notes

- Default port: 3000
- Checks if server already running
- Stores PID in `/tmp/docusaurus-<port>.pid`
- Access at: `http://localhost:<port>`
- Stop with: `gbx:docs:stop`
