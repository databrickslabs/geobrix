# Docs Development (Hot Reload)

Start the Docusaurus **development** server so edits to docs trigger automatic browser refresh.

## Usage

```bash
bash .cursor/commands/gbx-docs-dev.sh [OPTIONS]
```

## Options

- `--port <number>` - Custom port (default: 3000)
- `--no-stop-first` - If port is in use, do not stop existing server (default: stop first, then start)
- `--log <path>` - Write output to log file
- `--help` - Display help message

## When to use

- **gbx-docs-dev** – Use while editing docs. Runs `npm run start` (dev server). **Dynamic refresh**: changing MDX, JS, or CSS reloads the browser.
- **gbx-docs-serve-local** / **gbx-docs-restart** – Use to preview the production build. Runs `npm run build` then `npm run serve`. No hot reload; rebuild required to see changes.

## Examples

```bash
# Start dev server with hot reload
bash .cursor/commands/gbx-docs-dev.sh

# Custom port (by default, stops existing server on 3000 if in use)
bash .cursor/commands/gbx-docs-dev.sh --port 3001
```

## Notes

- By default, if port is in use the command runs `gbx:docs:stop` then starts the dev server. Use `--no-stop-first` to disable that and fail if the port is busy.
- Uses the same PID file as other docs commands, so `gbx:docs:stop` stops the dev server too.
- Access at: http://localhost:3000 (or your port).
