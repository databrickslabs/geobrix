# Docs Diagrams

Regenerates the GeoBrix-owned HTML diagrams and exports a light and a dark PNG of each. Two diagrams: the RasterX function categories and the raster-genie example architecture. Either can also export a 16:9 slide PNG.

## Usage

```bash
bash scripts/commands/gbx-docs-diagrams.sh [OPTIONS] [name ...]
```

## Options

- `name` - One diagram to rebuild: `rasterx-function-categories` or `raster-genie-architecture`. Default: all.
- `--html-only` - Write the HTML only and skip the PNG export. Needs no Chrome.
- `--theme <light|dark|both>` - The themes to export. Default: `both`.
- `--slide` - Also export a 1920 x 1080 (16:9) slide PNG per theme: `<name>-slide-light.png`, `<name>-slide-dark.png`. Default: off.
- `--log <path>` - Write output to a log file (filename, relative or absolute path).
- `--help` - Show the help.

## Examples

```bash
# Rebuild every diagram after a function is added, renamed or moved on its docs page
gbx:docs:diagrams

# Check the HTML only, with no browser
gbx:docs:diagrams --html-only rasterx-function-categories

# Rebuild the raster-genie diagram with its light, dark and slide PNGs
gbx:docs:diagrams --slide raster-genie-architecture
```

## How it works

- The generator `resources/images/generators/<name>.py` reads its sources. The RasterX diagram reads the function manifest and the docs page. The raster-genie diagram reads `FUNCTIONS` in `examples/notebooks/raster-genie/genie/agent_spec.py` and the questions in `run_questions.py`. It fills `resources/images/generators/diagrams/function-categories.html.tmpl` and inlines `diagrams.css`.
- `resources/images/generators/diagrams/export_diagrams.py` opens the HTML in the system Google Chrome, through `uvx --with playwright`. It writes `<name>-light.png` and `<name>-dark.png` next to the HTML.
- With `--slide`, the exporter frames the diagram in 1920 x 1080 and leaves off the intro paragraph.
- The docs page shows the PNGs with `ThemedImage`, so each site theme gets its own export.
- Commit the HTML and the PNGs. `docs/scripts/check-diagram-coverage.py` (QC `diagram-coverage`) reads the committed RasterX HTML.
- `python/geobrix/test/genie/test_raster_genie_diagram.py` checks the raster-genie pills, chips and freshness.

## Limits

- Runs on the host, not in Docker. It needs `python3`, and for the export `uv` and Google Chrome.
- The diagrams copied in from the Databricks Geospatial Hub (`resources/images/diagrams/overview/`) are not built here. Copy their current exports from that project.
