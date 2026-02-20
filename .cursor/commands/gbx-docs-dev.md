# Docs Development (Hot Reload)

Start the Docusaurus **development** server so edits to docs trigger automatic browser refresh.

## Usage

```bash
bash .cursor/commands/gbx-docs-dev.sh [OPTIONS]
```

## Options

- `--port <number>` - Custom port (default: 3000)
- `--log <path>` - Write output to log file
- `--help` - Display help message

## When to use

- **gbx-docs-dev** – Use while editing docs. Runs `npm run start` (dev server). **Dynamic refresh**: changing MDX, JS, or CSS reloads the browser.
- **gbx-docs-serve-local** / **gbx-docs-restart** – Use to preview the production build. Runs `npm run build` then `npm run serve`. No hot reload; rebuild required to see changes.

## Examples

```bash
# Start dev server with hot reload
bash .cursor/commands/gbx-docs-dev.sh

# Custom port
bash .cursor/commands/gbx-docs-dev.sh --port 3001
```

## Notes

- Requires any existing docs server to be stopped first (`gbx:docs:stop`).
- Uses the same PID file as other docs commands, so `gbx:docs:stop` stops the dev server too.
- Access at: http://localhost:3000 (or your port).
