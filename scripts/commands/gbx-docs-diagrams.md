# Docs Diagrams

Regenerates the GeoBrix-owned HTML diagrams and exports a light and a dark PNG of each. Today that is the RasterX function-categories diagram on the RasterX Function Reference page.

## Usage

```bash
bash scripts/commands/gbx-docs-diagrams.sh [OPTIONS] [name ...]
```

## Options

- `name` - One diagram to rebuild, such as `rasterx-function-categories`. Default: all.
- `--html-only` - Write the HTML only and skip the PNG export. Needs no Chrome.
- `--theme <light|dark|both>` - The themes to export. Default: `both`.
- `--log <path>` - Write output to a log file (filename, relative or absolute path).
- `--help` - Show the help.

## Examples

```bash
# Rebuild every diagram after a function is added, renamed or moved on its docs page
gbx:docs:diagrams

# Check the HTML only, with no browser
gbx:docs:diagrams --html-only rasterx-function-categories
```

## How it works

- The generator `resources/images/generators/<name>.py` reads the function manifest and the docs page. It fills `resources/images/generators/diagrams/function-categories.html.tmpl` and inlines `diagrams.css`.
- `resources/images/generators/diagrams/export_diagrams.py` opens the HTML in the system Google Chrome, through `uvx --with playwright`. It writes `<name>-light.png` and `<name>-dark.png` next to the HTML.
- The docs page shows the PNGs with `ThemedImage`, so each site theme gets its own export.
- Commit the HTML and both PNGs. `docs/scripts/check-diagram-coverage.py` (QC `diagram-coverage`) reads the committed HTML.

## Limits

- Runs on the host, not in Docker. It needs `python3`, and for the export `uv` and Google Chrome.
- The diagrams copied in from the Databricks Geospatial Hub (`resources/images/diagrams/overview/`) are not built here. Copy their current exports from that project.
