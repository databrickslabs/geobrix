# Docs Static Build (Offline Zip)

Build the documentation with relative paths and optionally create a zip for offline/local viewing.

## Usage

```bash
bash .cursor/commands/gbx-docs-static-build.sh [OPTIONS]
```

## Options

- `--output <path>` - Folder for the zip file (default: `resources/static`)
- `--skip-zip` - Run only `npm run build:static-zip`; do not create the zip
- `--log <path>` - Write build output to a log file
- `--help` - Display help message

## When to use

- **gbx-docs-static-build** – Build docs for static zip (relative baseUrl + hash router + relativized paths). By default zips to `resources/static/geobrix-docs-<version>.zip`; use `--output <path>` to override.
- **gbx-docs-serve-local** – Build (normal baseUrl) and serve locally with `npm run serve`.
- **gbx-docs-dev** – Dev server with hot reload; no zip.

## Examples

```bash
# Build and zip to resources/static/geobrix-docs-<version>.zip
bash .cursor/commands/gbx-docs-static-build.sh

# Zip to a custom folder
bash .cursor/commands/gbx-docs-static-build.sh --output ./docs-build   # or any path; zip name uses version from docs/package.json

# Build only (no zip)
bash .cursor/commands/gbx-docs-static-build.sh --skip-zip

# Build and log output
bash .cursor/commands/gbx-docs-static-build.sh --log docs-static-build.log
```

## Notes

- Runs `npm run build:static-zip` in `docs/` (sets `DOCS_STATIC_ZIP=1`). Uses `@someok/docusaurus-plugin-relative-paths` to convert absolute paths to relative in HTML/CSS; a post-step relativizes JS and injects a file:// redirect for the hash router so the home page loads when opening `index.html` from a folder.
- Zip filename is `geobrix-docs-<version>.zip` where `<version>` is read from `docs/package.json` at build time.
- Unzipped folder can be opened by double-clicking `index.html`; site uses hash routing so body content loads when opened via file://.
