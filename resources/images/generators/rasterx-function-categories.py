#!/usr/bin/env python3
"""Generate the RasterX function-categories diagram (HTML source for the PNG exports).

The data comes from two places, never a hand-kept function list:

  - the function manifest (python/geobrix/src/databricks/labs/gbx/manifest/functions.json):
    the registered ``gbx_rst_*`` set, and each function's levels (JVM, Unity Catalog);
  - the RasterX Function Reference page (docs/docs/api/raster-functions.mdx): each
    ``## <category>`` section and the ``### rst_<name>`` functions under it.

Each card is one page category. Its pills are the registered functions in that section.
The script fails when a registered function is on no page section, or when the page has a
category that CATEGORIES (below) does not describe.

Run it through the command, which also exports the PNGs:

    bash scripts/commands/gbx-docs-diagrams.sh

or on its own (stdlib only) to rewrite just the HTML:

    python3 resources/images/generators/rasterx-function-categories.py [OUT.html]

The HTML fills resources/images/generators/diagrams/function-categories.html.tmpl and inlines
diagrams.css (light and dark themes). docs/scripts/check-diagram-coverage.py reads the HTML.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from html import escape
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
PAGE = ROOT / "docs/docs/api/raster-functions.mdx"
TEMPLATES = Path(__file__).resolve().parent / "diagrams"
OUT = ROOT / "resources/images/diagrams/rasterx/rasterx-function-categories.html"

# --- Category display: title, one-line description, colour group, keyed by the page heading ---------------
# Headings are matched without their {#anchor}. Function lists are NOT kept here; they come from the page.

BLUE, PURPLE, GOLD, GREEN, CORAL = "#9fc3d6", "#c8b6e2", "#e8c46a", "#98c9b8", "#ff8a73"

LEGEND = [
    (BLUE, "load &amp; inspect"),
    (PURPLE, "tile &amp; aggregate"),
    (GOLD, "grid cells"),
    (GREEN, "terrain, spectral &amp; point clouds"),
    (CORAL, "transform, convert &amp; analyze"),
]

CATEGORIES = {
    "Accessor Functions": ("Accessors", "Size, georeference, CRS, bands and statistics", BLUE),
    "Aggregator Functions": ("Aggregators", "Merge, combine and rasterize tiles in GROUP BY", PURPLE),
    "Constructor Functions": ("Constructors", "Tiles from files, bytes, bands, points or shapes", BLUE),
    "LiDAR / Point-Cloud DSM": ("LiDAR &amp; Point Clouds", "Bin points into surface and canopy-height models", GREEN),
    "Generator Functions": ("Generators", "Split a tile into tiles, bands or grid cells", PURPLE),
    "Grid Functions (H3)": ("H3 Grid", "Raster values to H3 cells", GOLD),
    "Grid Functions (quadbin)": ("Quadbin Grid", "Raster values to CARTO quadbin cells", GOLD),
    "Grid Functions (BNG)": ("BNG Grid", "Raster values to British National Grid cells", GOLD),
    "Grid Functions (Custom Grid)": ("Custom Grid", "Raster values to the cells of your own grid", GOLD),
    "Operations": ("Operations", "Clip, resample, reproject, combine, map algebra, coordinates and XYZ tiles", CORAL),
    "Vector↔raster bridge": ("Vector ↔ Raster", "Geometries to tiles and tiles to polygons", CORAL),
    "Terrain Analysis": ("Terrain Analysis", "Slope, aspect, hillshade and relief from a DEM", GREEN),
    "Spectral Indices": ("Spectral Indices", "Vegetation, water, burn and land-cover indices", GREEN),
    "Pixel ops + extraction": ("Pixel Ops &amp; Extraction", "Bands, NoData fill, thresholds, samples, histograms and overviews", CORAL),
    "Analysis": ("Analysis", "Contours, proximity, viewshed, COG output and GDAL commands", CORAL),
}

# --- Layout (SVG units; the reference grid is 4 columns of 289.5 on a 1240-wide stage) ---------------------

W = 1240
MARGIN = 20
NCOLS = 4
COL_W = 289.5
COL_GAP = 14
CARD_GAP = 14
CARDS_TOP = 98
PAD_X = 14
PILL_H, PILL_ROW, PILL_GAP = 21, 27, 5
MONO_W, SANS_W, LABEL_W = 6.9, 6.1, 7.4  # per-character widths for 11.5px mono, 11.5px sans, 12px bold labels
DESC_CHARS = 48

# The surfaces a user calls, as the page's "How to read the tabs" table shows them.
# Each value is a list of (text, mono) runs.
SURFACES = [
    ("SQL", [("gbx_rst_*", True), (" after ", False), ("register(spark)", True)]),
    ("Python", [("from databricks.labs.gbx.rasterx import functions as rx", True)]),
    ("Unity Catalog SQL", [("<catalog>.<schema>.gbx_rst_*", True)]),
]


@dataclass
class Category:
    title: str
    desc: str
    color: str
    fns: list[str]


def registered() -> dict[str, dict]:
    """Registered rst_ names (gbx_ prefix dropped) -> manifest row."""
    rows = json.loads(MANIFEST.read_text())["functions"]
    return {r["sql_name"][len("gbx_"):]: r for r in rows if r["sql_name"].startswith("gbx_rst_")}


def page_sections(text: str) -> list[tuple[str, list[str]]]:
    """[(heading, [rst_name, ...]), ...] in page order; code fences skipped, {#anchors} dropped."""
    sections: list[tuple[str, list[str]]] = []
    fence = False
    for line in text.splitlines():
        if line.startswith("```"):
            fence = not fence
        if fence:
            continue
        if m := re.match(r"^## (.+?)\s*(\{#[^}]*\})?\s*$", line):
            sections.append((m.group(1), []))
        elif (m := re.match(r"^### (rst_[a-z0-9_]+)\b", line)) and sections:
            sections[-1][1].append(m.group(1))
    return sections


def categories(reg: dict[str, dict]) -> list[Category]:
    out, placed, unknown = [], {}, []
    for heading, names in page_sections(PAGE.read_text()):
        fns = sorted(n for n in names if n in reg)
        if not fns:
            continue  # a section of Python helpers only (not registered SQL functions)
        if heading not in CATEGORIES:
            unknown.append(heading)
            continue
        for n in fns:
            placed.setdefault(n, []).append(heading)
        title, desc, color = CATEGORIES[heading]
        out.append(Category(title, desc, color, fns))
    errors = []
    if unknown:
        errors.append(f"page categories with no CATEGORIES entry: {unknown}")
    if missing := sorted(set(reg) - set(placed)):
        errors.append(f"registered functions on no {PAGE.name} section: {missing}")
    if twice := {n: h for n, h in placed.items() if len(h) > 1}:
        errors.append(f"functions in more than one section: {twice}")
    if errors:
        sys.exit("rasterx-function-categories: " + "; ".join(errors))
    return out


# --- SVG ----------------------------------------------------------------------------------------------------


def wrap(text: str, width: int) -> list[str]:
    lines, cur = [], ""
    for word in text.split():
        if cur and len(cur) + 1 + len(word) > width:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}" if cur else word
    return lines + [cur] if cur else lines


def pill_rows(fns: list[str], max_w: float) -> list[list[tuple[float, float, str]]]:
    rows, cur, x = [], [], 0.0
    for f in fns:
        w = round(len(f) * MONO_W + 12, 1)
        if cur and x + PILL_GAP + w > max_w:
            rows.append(cur)
            cur, x = [], 0.0
        px = x + PILL_GAP if cur else 0.0
        cur.append((px, w, f))
        x = px + w
    return rows + [cur] if cur else rows


def card_height(c: Category) -> float:
    lines = len(wrap(c.desc, DESC_CHARS))
    rows = len(pill_rows(c.fns, COL_W - 2 * PAD_X))
    pills_top = 45 + 15 * (lines - 1) + 13
    return pills_top + (rows - 1) * PILL_ROW + PILL_H + 16


def masonry(cats: list[Category]) -> list[list[tuple[float, Category]]]:
    """Balance the columns: tallest card first, each into the shortest column so far.

    Inside a column the cards then follow the legend order (then page order), and the
    columns are ordered by their first card, so the colour groups read left to right.
    """
    group = {color: i for i, (color, _) in enumerate(LEGEND)}
    rank = {id(c): (group[c.color], i) for i, c in enumerate(cats)}
    cols: list[list[Category]] = [[] for _ in range(NCOLS)]
    heights = [0.0] * NCOLS
    for c in sorted(cats, key=lambda c: (-card_height(c), rank[id(c)])):
        i = heights.index(min(heights))
        cols[i].append(c)
        heights[i] += card_height(c) + CARD_GAP
    cols = sorted((sorted(col, key=lambda c: rank[id(c)]) for col in cols), key=lambda col: rank[id(col[0])])
    placed = []
    for col in cols:
        y, out = float(CARDS_TOP), []
        for c in col:
            out.append((y, c))
            y += card_height(c) + CARD_GAP
        placed.append(out)
    return placed


def render_card(x: float, y: float, c: Category, cid: str) -> tuple[str, str]:
    h = card_height(c)
    n = str(len(c.fns))
    bw = round(14 + 7.2 * len(n), 1)
    bx = round(x + COL_W - PAD_X - bw, 1)
    clip = f'<clipPath id="{cid}"><rect x="{x}" y="{y}" width="{COL_W}" height="{h}" rx="12"/></clipPath>'
    out = [
        f'<g class="node" data-category="{c.title}">'
        f'<rect class="box" x="{x}" y="{y}" width="{COL_W}" height="{h}" rx="12" fill="url(#card)" '
        f'stroke="{c.color}" stroke-opacity=".55" stroke-width="1.3" filter="url(#shd)"/>',
        f'  <rect x="{x}" y="{y}" width="{COL_W}" height="4" fill="{c.color}" clip-path="url(#{cid})"/>',
        f'  <text x="{x + PAD_X}" y="{y + 27}" font-size="15" font-weight="700" fill="#f9f7f4">{c.title}</text>',
        f'  <g class="badge"><rect x="{bx}" y="{y + 13}" width="{bw}" height="20" rx="10" fill="{c.color}"/>'
        f'<text x="{round(bx + bw / 2, 2)}" y="{y + 27}" font-size="11.5" font-weight="800" fill="#0f1e24" '
        f'text-anchor="middle">{n}</text></g>',
    ]
    lines = wrap(c.desc, DESC_CHARS)
    for i, line in enumerate(lines):
        out.append(f'  <text x="{x + PAD_X}" y="{y + 45 + 15 * i}" font-size="11" fill="#a7b6ba">{line}</text>')
    py = y + 45 + 15 * (len(lines) - 1) + 13
    out.append(f'  <g class="mono" font-size="11.5" fill="{c.color}" text-anchor="middle">')
    for r, row in enumerate(pill_rows(c.fns, COL_W - 2 * PAD_X)):
        ry = py + r * PILL_ROW
        cells = []
        for px, w, name in row:
            rx = round(x + PAD_X + px, 1)
            cells.append(
                f'<g class="pill" data-fn="{name}"><rect x="{rx}" y="{ry}" width="{w}" height="{PILL_H}" rx="10.5" '
                f'fill="#0f1e24" fill-opacity=".55" stroke="{c.color}" stroke-opacity=".4"/>'
                f'<text x="{round(rx + w / 2, 2)}" y="{ry + 14.5}">{name}</text></g>'
            )
        out.append("    " + "".join(cells))
    out.append("  </g></g>")
    return "\n".join(out), clip


def render_surfaces(kicker: str) -> str:
    out = [
        '<g class="reveal r1">',
        '  <circle class="pulse" cx="24" cy="20" r="3.5" fill="#ff8a73"/>',
        f'  <text x="34" y="24" font-size="10.5" fill="#86999f" letter-spacing="1.6">{kicker}</text>',
    ]
    x = float(MARGIN)
    for label, runs in SURFACES:
        lw = round(len(label) * LABEL_W + 20, 1)
        tw = sum(len(t) * (MONO_W if mono else SANS_W) for t, mono in runs)
        cw = round(lw + 12 + tw + 12, 2)
        spans = "".join(
            (f'<tspan class="mono" fill="#f3d98f">{escape(t)}</tspan>' if mono else f'<tspan fill="#a7b6ba">{escape(t)}</tspan>')
            for t, mono in runs
        )
        out.append(
            f'  <rect x="{x}" y="34" width="{cw}" height="30" rx="8" fill="#0f1e24" stroke="#2b4650"/>'
            f'<rect x="{x}" y="34" width="{lw}" height="30" rx="8" fill="#ff8a73" fill-opacity=".14"/>'
            f'<text x="{round(x + lw / 2, 2)}" y="53.5" font-size="12" font-weight="700" fill="#ff8a73" '
            f'text-anchor="middle">{label}</text>'
        )
        out.append(f'  <text x="{round(x + lw + 12, 2)}" y="53.5" font-size="11.5" xml:space="preserve">{spans}</text>')
        x = round(x + cw + 12, 2)
    out.append("</g>")
    if x - 12 > W - MARGIN:
        sys.exit(f"rasterx-function-categories: the surfaces bar is {x - 12:.0f} wide; the stage is {W - MARGIN}")
    return "\n".join(out)


def render(reg: dict[str, dict], cats: list[Category]) -> str:
    total = sum(len(c.fns) for c in cats)
    n_jvm = sum("L1" in reg[f]["levels"] for c in cats for f in c.fns)
    n_uc = sum("L4" in reg[f]["levels"] for c in cats for f in c.fns)
    cols = masonry(cats)
    bottom = max(y + card_height(c) for col in cols for y, c in col)
    h = round(bottom + 44)

    cards, clips = [], []
    k = 0
    for ci, col in enumerate(cols):
        cards.append(f'<g class="reveal r{min(ci + 1, 4)}">')
        for y, c in col:
            k += 1
            svg, clip = render_card(round(MARGIN + ci * (COL_W + COL_GAP), 1), y, c, f"c{k}")
            cards.append(svg)
            clips.append(clip)
        cards.append("</g>")

    aria = (
        f"The {total} RasterX functions in {len(cats)} categories, each card listing its function names: "
        + ", ".join(f"{c.title} {len(c.fns)}" for c in cats)
        + ". Call them from SQL after register(spark), from Python with "
        "from databricks.labs.gbx.rasterx import functions as rx, and from Unity Catalog SQL."
    )
    svg = f"""  <svg viewBox="0 0 {W} {h}" role="img" aria-label="{aria}">
    <defs>
      <radialGradient id="ga" cx="18%" cy="20%" r="45%"><stop offset="0" stop-color="#9fc3d6" stop-opacity=".12"/><stop offset="1" stop-color="#9fc3d6" stop-opacity="0"/></radialGradient>
      <radialGradient id="gb" cx="82%" cy="70%" r="50%"><stop offset="0" stop-color="#ff3621" stop-opacity=".12"/><stop offset=".6" stop-color="#ff3621" stop-opacity=".03"/><stop offset="1" stop-color="#ff3621" stop-opacity="0"/></radialGradient>
      <linearGradient id="card" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#1f3841"/><stop offset="1" stop-color="#16282f"/></linearGradient>
      <filter id="glow" x="-400%" y="-400%" width="900%" height="900%"><feGaussianBlur stdDeviation="5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
      <filter id="shd" x="-10%" y="-10%" width="120%" height="140%"><feDropShadow dx="0" dy="6" stdDeviation="8" flood-color="#000" flood-opacity=".35"/></filter>
      <pattern id="grat" width="62" height="62" patternUnits="userSpaceOnUse"><path d="M62 0H0V62" fill="none" stroke="#2b4650" stroke-width=".6" opacity=".55"/></pattern>
      {chr(10).join("      " + c for c in clips).strip()}
    </defs>
    <rect width="{W}" height="{h}" fill="url(#grat)" opacity=".45"/>
    <rect width="{W}" height="{h}" fill="url(#ga)"/><rect width="{W}" height="{h}" fill="url(#gb)"/>
{render_surfaces("CALL THEM FROM SQL, PYTHON &amp; UNITY CATALOG SQL")}
    <path id="scan" d="M{MARGIN},80 H{W - MARGIN}" fill="none" stroke="#2b4650" stroke-width="1" stroke-dasharray="2 5"/>
{chr(10).join(cards)}
    <g class="particles">
      <circle r="3" fill="#fff" filter="url(#glow)"><animateMotion dur="4.8s" repeatCount="indefinite"><mpath href="#scan"/></animateMotion></circle>
      <circle r="2.4" fill="#ffd2c9"><animateMotion dur="4.8s" begin="2.4s" repeatCount="indefinite"><mpath href="#scan"/></animateMotion></circle>
    </g>
    <text x="{MARGIN}" y="{h - 16}" font-size="12" fill="#a7b6ba"><tspan fill="#ff8a73" font-weight="700">{len(cats)}</tspan> categories · <tspan fill="#ff8a73" font-weight="700">{total}</tspan> functions</text>
    <text x="{W - MARGIN}" y="{h - 16}" font-size="11" fill="#86999f" text-anchor="end">Source: GeoBrix function manifest · RasterX Function Reference</text>
  </svg>"""

    sub = (
        f"The {total} RasterX functions on the Function Reference page, in {len(cats)} groups: "
        "load and inspect, tile and aggregate, grid cells, and transform and analyze. "
        "Every function runs at the Python level on any compute. "
        f"With the JAR on dedicated compute, GeoBrix runs {n_jvm} of them at the faster JVM level. "
        f"{n_uc} of them also install as Unity Catalog functions for SQL warehouses."
    )
    legend = "".join(f'<span><i style="background:{c}"></i>{label}</span>' for c, label in LEGEND)
    return Template((TEMPLATES / "function-categories.html.tmpl").read_text()).substitute(
        generator="resources/images/generators/rasterx-function-categories.py",
        title_text="GeoBrix RasterX functions",
        css=(TEMPLATES / "diagrams.css").read_text().strip(),
        kicker="GeoBrix · RasterX functions",
        title_lead=f"{total} RasterX functions,",
        title_span="raster processing for Databricks.",
        sub=sub,
        svg=svg,
        legend=legend,
    )


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
    reg = registered()
    cats = categories(reg)
    out.write_text(render(reg, cats))
    print(f"wrote {out.relative_to(ROOT) if out.is_relative_to(ROOT) else out}: "
          f"{len(cats)} categories, {sum(len(c.fns) for c in cats)} functions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
