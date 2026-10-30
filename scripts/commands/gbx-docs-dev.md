# Docs Development (Hot Reload)

Start the Docusaurus **development** server so edits to docs trigger automatic browser refresh.

## Usage

```bash
bash scripts/commands/gbx-docs-dev.sh [OPTIONS]
```

## Options

- `--port <number>` - Custom port (default: 3000)
- `--no-stop-first` - If the port is in use, fail instead of replacing the server. Default: replace it.
- `--force` - Replace the server even if another checkout started it. Default: refuse.
- `--log <path>` - Write output to log file
- `--help` - Display help message

## When to use

- **gbx-docs-dev** – Use while editing docs. Runs `npm run start` (dev server). **Dynamic refresh**: changing MDX, JS, or CSS reloads the browser.
- **gbx-docs-serve-local** / **gbx-docs-restart** – Use to preview the production build. Runs `npm run build` then `npm run serve`. No hot reload; rebuild required to see changes.

## Examples

```bash
# Start dev server with hot reload
bash scripts/commands/gbx-docs-dev.sh

# Custom port (by default, replaces the server on that port if it is yours)
bash scripts/commands/gbx-docs-dev.sh --port 3001
```

## Notes

- If the port is in use, the command runs `gbx:docs:stop --port <number>`, then starts. Other ports are never touched.
- If another checkout started the server on that port, the command refuses and prints the `--force` command.
- It uses the same PID file as other docs commands, so `gbx:docs:stop --port <number>` stops it.
- Access at: http://localhost:3000 (or your port).
