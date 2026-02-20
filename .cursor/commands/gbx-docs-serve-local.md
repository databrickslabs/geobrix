# Serve Documentation Locally

Build (optional) and run `npm run serve` to serve the static Docusaurus site locally.

## Usage

```bash
bash .cursor/commands/gbx-docs-serve-local.sh [OPTIONS]
```

## Options

- `--skip-build` - Skip npm build; serve existing build only
- `--port <number>` - Custom port (default: 3000)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Build and serve docs
bash .cursor/commands/gbx-docs-serve-local.sh

# Serve existing build without rebuilding
bash .cursor/commands/gbx-docs-serve-local.sh --skip-build

# Use custom port
bash .cursor/commands/gbx-docs-serve-local.sh --port 3001

# Build and log output
bash .cursor/commands/gbx-docs-serve-local.sh --log docs-serve.log
```

## Notes

- **Requires any existing docs server to be stopped first** (run `gbx:docs:stop` if needed).
- By default runs `npm run build` then `npm run serve`.
- Default port: 3000.
- Access at: `http://localhost:<port>`.
- Stop with: `gbx:docs:stop`.
- Uses same PID/log files as other docs commands so stop works across all of them.
