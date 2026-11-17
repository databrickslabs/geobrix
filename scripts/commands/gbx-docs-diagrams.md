# Docs Diagrams

Regenerates the GeoBrix-owned HTML diagrams and exports a light and a dark PNG of each. Two diagrams: the RasterX function categories and the raster-genie example architecture. Either can also export a 16:9 slide PNG and animated GIFs.

## Usage

```bash
bash scripts/commands/gbx-docs-diagrams.sh [OPTIONS] [name ...]
```

## Options

- `name` - One diagram to rebuild: `rasterx-function-categories` or `raster-genie-architecture`. Default: all.
- `--html-only` - Write the HTML only and skip the PNG export. Needs no Chrome.
- `--theme <light|dark|both>` - The themes to export. Default: `both`.
- `--slide` - Also export the full-bleed 16:9 slide PNG at 2560 x 1440: `<name>-slide-light.png`, `<name>-slide-dark.png`. Default: off.
- `--gif` - Also export animated GIFs: `<name>-light.gif`, `<name>-dark.gif`, and with `--slide` the slide GIFs. Default: off.
- `--log <path>` - Write output to a log file (filename, relative or absolute path).
- `--help` - Show the help.

## Examples

```bash
# Rebuild every diagram after a function is added, renamed or moved on its docs page
gbx:docs:diagrams

# Check the HTML only, with no browser
gbx:docs:diagrams --html-only rasterx-function-categories

# Rebuild the raster-genie diagram with PNGs, slide PNGs and GIFs
gbx:docs:diagrams --slide --gif raster-genie-architecture
```

## How it works

- The generator `resources/images/generators/<name>.py` reads its sources. The RasterX diagram reads the function manifest and the docs page. The raster-genie diagram reads `FUNCTIONS` in `examples/notebooks/raster-genie/genie/agent_spec.py` and the questions in `run_questions.py`. It fills `resources/images/generators/diagrams/function-categories.html.tmpl` and inlines `diagrams.css`.
- `resources/images/generators/diagrams/export_diagrams.py` opens the HTML in the system Google Chrome, through `uvx --with playwright`. It writes `<name>-light.png` and `<name>-dark.png` next to the HTML.
- The exporter pauses the SMIL and CSS animations and steps them to exact times. The PNG and the first GIF frame are both the frame at 3.0 s.
- A GIF is 12 fps over a 4.8 s loop (58 frames) at 1730 px wide. A slide GIF is 15 fps (72 frames) at 2560 x 1440. ffmpeg encodes both with one palette and Sierra dithering.
- `--gif` runs `uvx` with `imageio-ffmpeg` added. Every animation period must divide 4.8 s so the loop closes.
- A slide fits the diagram to the frame on its own stage colour, like the Geospatial Hub slide exports.
- `diagrams.css` holds the motion classes (`reveal`, `draw`, `pulse`, `particles`) and the reduced-motion guard.
- The docs page shows the PNGs with `ThemedImage`, so each site theme gets its own export.
- Commit the HTML and the PNGs. `docs/scripts/check-diagram-coverage.py` (QC `diagram-coverage`) reads the committed RasterX HTML.
- `python/geobrix/test/genie/test_raster_genie_diagram.py` checks the raster-genie pills, chips and freshness.

## Limits

- Runs on the host, not in Docker. It needs `python3`, and for the export `uv` and Google Chrome.
- With `--html-only`, `--slide` and `--gif` print a note and write nothing.
- The diagrams copied in from the Databricks Geospatial Hub (`resources/images/diagrams/overview/`) are not built here. Copy their current exports from that project.
