#!/usr/bin/env python3
"""Generate the raster-genie example diagram (HTML source for the PNG exports).

One picture of the chain the example builds: sources, the raster data-engineering step that lands master COGs,
the FILE table, the GeoBrix Unity Catalog functions and the Genie Agent. It is the executive-brief view of
examples/notebooks/raster-genie/.

The pills are never a hand-kept list. They come from the example itself:

  - the function pills: ``FUNCTIONS`` in examples/notebooks/raster-genie/genie/agent_spec.py, the set
    attached to the agent. FUNCTION_GROUPS (below) only sorts them into read / inspect / stats / indices /
    map algebra / terrain. The script fails when an attached function fits no group;
  - the question chips Q1 and Q3: the questions run_questions.py asks the two agents (Q2 and Q4 are the
    planned statewide questions of the design).

No timing is written. The agent timings live in test-logs/ (not committed), so a number here could not be
rebuilt. The two forms of the agent carry the labels "on the fly" and "precomputed".

Run it through the command, which also exports the PNGs:

    bash scripts/commands/gbx-docs-diagrams.sh raster-genie-architecture

or on its own (stdlib only) to rewrite just the HTML:

    python3 resources/images/generators/raster-genie-architecture.py [OUT.html]

The HTML fills resources/images/generators/diagrams/function-categories.html.tmpl (a title, a paragraph, one
SVG and a legend) and inlines diagrams.css, so both themes come from one source.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from html import escape
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples/notebooks/raster-genie"
AGENT_SPEC = EXAMPLE / "genie/agent_spec.py"
RUN_QUESTIONS = EXAMPLE / "genie/run_questions.py"
TEMPLATES = Path(__file__).resolve().parent / "diagrams"
OUT = ROOT / "resources/images/diagrams/raster-genie/raster-genie-architecture.html"

BLUE, PURPLE, GOLD, GREEN, CORAL = "#9fc3d6", "#c8b6e2", "#e8c46a", "#98c9b8", "#ff8a73"
INK, MUTED, FAINT, STAGE, LINE = "#f9f7f4", "#a7b6ba", "#86999f", "#0f1e24", "#2b4650"

# --- The function groups: read by name pattern, listed by what each one does -------------------------------

FUNCTION_GROUPS = [
    ("read", {"fromfile", "clip", "merge"}),
    ("inspect", {"width", "height", "pixelwidth", "pixelheight", "srid", "boundingbox", "numbands", "metadata"}),
    ("stats", {"avg", "min", "max", "median", "summary", "pixelcount", "histogram", "stats_partial"}),
    ("indices", {"ndvi", "nbr", "savi", "ndwi", "evi", "index"}),
    ("map algebra", {"mapalgebra"}),
    ("terrain", {"slope", "aspect", "hillshade", "tpi", "tri", "roughness"}),
]
HERO = "fromfile"

# --- The questions: Q1 and Q3 are the ones the example asks; Q2 and Q4 are the planned statewide ones -------

QUESTIONS = [  # (id, audience, text or None to read from run_questions.py, in this example)
    ("Q1", "data product owner", None, True),
    ("Q2", "ag lender / insurer", "How did vegetation health on cropland in Lubbock County change through the 2024 growing season vs 2023?", False),
    ("Q3", "insurer / emergency mgmt", None, True),
    ("Q4", "operator / regulator", "Which wells in the Permian focus area show new bare-ground clearing nearby since 2023, and which are on steep terrain?", False),
]

# --- Layout (SVG units) ------------------------------------------------------------------------------------

W = 1240
MARGIN = 20
GAP = 42
PAD = 14
COLS = [224, 214, 252, 384]  # sources, land, FILE table, functions
MONO_W, SANS_W = 6.6, 5.9  # per-character widths for 11px mono and 11px sans
PILL_H, PILL_ROW, PILL_GAP = 21, 26, 5
GLOW = ' filter="url(#glow)"'


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def attached_functions() -> list[str]:
    """The short rst names (no catalog, schema or gbx_rst_ prefix) attached to the agent, in order."""
    fns = load(AGENT_SPEC, "raster_genie_agent_spec").FUNCTIONS
    return [f.rsplit(".", 1)[-1] for f in fns]


def questions() -> list[tuple[str, str, str, bool]]:
    src = RUN_QUESTIONS.read_text()
    out = []
    for qid, audience, text, live in QUESTIONS:
        if text is None:
            m = re.search(rf'^{qid} = "(.+)"$', src, re.M)
            if not m:
                sys.exit(f"raster-genie-architecture: no {qid} in {RUN_QUESTIONS.name}")
            text = m.group(1)
        out.append((qid, audience, text, live))
    return out


def groups(rst: list[str]) -> list[tuple[str, list[str]]]:
    """[(group, [function, ...])] in FUNCTION_GROUPS order; fails when an attached function fits no group."""
    names = [r[len("gbx_rst_"):] for r in rst]
    known = set().union(*(m for _, m in FUNCTION_GROUPS))
    if stray := [n for n in names if n not in known]:
        sys.exit(f"raster-genie-architecture: attached functions with no group in FUNCTION_GROUPS: {stray}")
    return [(g, [n for n in names if n in members]) for g, members in FUNCTION_GROUPS]


# --- SVG helpers -------------------------------------------------------------------------------------------


def wrap(text: str, width: float, cw: float = 5.5) -> list[str]:
    lines, cur = [], ""
    for word in text.split():
        if cur and (len(cur) + 1 + len(word)) * cw > width:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}" if cur else word
    return lines + [cur] if cur else lines


def pill_w(name: str) -> float:
    return round(len(name) * MONO_W + 12, 1)


def pill_rows(names: list[str], max_w: float) -> list[list[tuple[float, float, str]]]:
    rows, cur, x = [], [], 0.0
    for n in names:
        w = pill_w(n)
        if cur and x + PILL_GAP + w > max_w:
            rows.append(cur)
            cur, x = [], 0.0
        px = x + PILL_GAP if cur else 0.0
        cur.append((px, w, n))
        x = px + w
    return rows + [cur] if cur else rows


def mono_pills(x: float, y: float, names: list[str], max_w: float, color: str, *, attr: dict[str, str] | None = None,
               hero: str | None = None, dashed: bool = False) -> tuple[str, float]:
    """Mono pills wrapped to max_w. Returns (svg, height). attr maps a name to its data-fn value."""
    out = [f'<g class="mono" font-size="11" fill="{color}" text-anchor="middle">']
    rows = pill_rows(names, max_w)
    for r, row in enumerate(rows):
        ry = y + r * PILL_ROW
        for px, w, n in row:
            rx = round(x + px, 1)
            data = f' data-fn="{attr[n]}"' if attr else ""
            hot = n == hero
            bold = ' font-weight="700"' if hot else ""
            fill = f'fill="{color}" fill-opacity=".22"' if hot else f'fill="{STAGE}" fill-opacity=".55"'
            stroke = f'stroke="{color}" stroke-opacity="{".95" if hot else ".4"}"' + (' stroke-width="1.4"' if hot else "")
            if dashed:
                stroke += ' stroke-dasharray="3 3"'
            out.append(
                f'<g class="pill"{data}><rect x="{rx}" y="{ry}" width="{w}" height="{PILL_H}" rx="10.5" {fill} {stroke}/>'
                f'<text x="{round(rx + w / 2, 2)}" y="{ry + 14.5}"{bold}>{n}</text></g>'
            )
    out.append("</g>")
    return "".join(out), (len(rows) - 1) * PILL_ROW + PILL_H


def card(x: float, y: float, w: float, h: float, color: str, cid: str, num: str | None, title: str,
         sub_lines: list[str], *, dashed: bool = False) -> str:
    stroke = (f'stroke="{LINE}" stroke-width="1.3" stroke-dasharray="5 4"' if dashed
              else f'stroke="{color}" stroke-opacity=".55" stroke-width="1.3"')
    out = [
        f'<clipPath id="{cid}"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12"/></clipPath>',
        f'<rect class="box" x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="url(#card)" {stroke} filter="url(#shd)"/>',
    ]
    if not dashed:
        out.append(f'<rect x="{x}" y="{y}" width="{w}" height="4" fill="{color}" clip-path="url(#{cid})"/>')
    tx = x + PAD
    if num:
        out.append(
            f'<circle cx="{tx + 10}" cy="{y + 27}" r="11" fill="{color}"/>'
            f'<text x="{tx + 10}" y="{y + 31.5}" font-size="12" font-weight="800" fill="{STAGE}" text-anchor="middle">{num}</text>'
        )
        tx += 28
    out.append(f'<text x="{tx}" y="{y + 32}" font-size="15" font-weight="700" fill="{INK}">{title}</text>')
    for i, line in enumerate(sub_lines):
        out.append(f'<text x="{x + PAD}" y="{y + 54 + 14 * i}" font-size="11" fill="{MUTED}">{line}</text>')
    return "".join(out)


def label(x: float, y: float, text: str, color: str = FAINT) -> str:
    return f'<text x="{x}" y="{y}" font-size="9.5" font-weight="700" letter-spacing="1.3" fill="{color}">{text}</text>'


def arrow(x1: float, x2: float, y: float, rid: str) -> str:
    return (
        f'<path id="{rid}" d="M{x1},{y} H{x2 - 3}" fill="none" stroke="{CORAL}" stroke-width="2" stroke-linecap="round"/>'
        f'<path d="M{x2 - 9},{y - 5.5} L{x2},{y} L{x2 - 9},{y + 5.5} Z" fill="{CORAL}"/>'
    )


def down_arrow(x: float, y1: float, y2: float, rid: str, text: str) -> str:
    return (
        f'<path id="{rid}" d="M{x},{y1} V{y2 - 3}" fill="none" stroke="{CORAL}" stroke-width="2" stroke-linecap="round"/>'
        f'<path d="M{x - 5.5},{y2 - 9} L{x},{y2} L{x + 5.5},{y2 - 9} Z" fill="{CORAL}"/>'
        f'<text x="{x + 12}" y="{round((y1 + y2) / 2 + 4, 1)}" font-size="11" fill="{MUTED}">{text}</text>'
    )


# --- The five blocks ---------------------------------------------------------------------------------------

SOURCES_NOW = [
    ("Sentinel-2 L2A", "Planetary Computer, 10 m imagery"),
    ("WorldPop", "population, people per pixel"),
    ("NIFC", "fire perimeters"),
    ("TIGER", "county boundaries"),
]
SOURCES_PLANNED = [
    ("USGS 3DEP", "elevation"),
    ("ESA WorldCover", "land cover"),
    ("TX RRC", "wells"),
]


def sources_card(x: float, y: float, w: float, h: float) -> str:
    out = [card(x, y, w, h, BLUE, "k1", "1", "Sources", ["Open data, fetched by code"])]
    cy = y + 82
    out.append(label(x + PAD, cy, "IN THIS EXAMPLE"))
    cy += 18
    for name, detail in SOURCES_NOW:
        out.append(
            f'<circle cx="{x + PAD + 4}" cy="{cy - 4}" r="3.5" fill="{BLUE}"/>'
            f'<text x="{x + PAD + 16}" y="{cy}" font-size="12" font-weight="700" fill="{INK}">{name}</text>'
            f'<text x="{x + PAD + 16}" y="{cy + 14}" font-size="10.5" fill="{MUTED}">{detail}</text>'
        )
        cy += 40
    cy += 6
    out.append(label(x + PAD, cy, "PLANNED, STATEWIDE"))
    cy += 18
    out.append('<g opacity=".6">')
    for name, detail in SOURCES_PLANNED:
        out.append(
            f'<circle cx="{x + PAD + 4}" cy="{cy - 4}" r="3.2" fill="none" stroke="{LINE}" stroke-width="1.2"/>'
            f'<text x="{x + PAD + 16}" y="{cy}" font-size="11.5" font-weight="700" fill="{INK}">{name}</text>'
            f'<text x="{x + PAD + 16 + 104}" y="{cy}" font-size="10.5" fill="{MUTED}">{detail}</text>'
        )
        cy += 27
    out.append("</g>")
    return "".join(out)


def land_card(x: float, y: float, w: float, h: float) -> str:
    out = [card(x, y, w, h, PURPLE, "k2", "2", "Land", ["The raster data-engineering step"])]
    iw = w - 2 * PAD
    cy = y + 80
    for i, (head, detail) in enumerate([("Download + validate", "one scene or file at a time"),
                                         ("Write master COGs", "cog_gbx, one COG per scene"),
                                         ("Register in the table", "a thin pipeline, nothing copied")]):
        out.append(
            f'<rect x="{x + PAD}" y="{cy}" width="{iw}" height="44" rx="9" fill="{STAGE}" fill-opacity=".55" '
            f'stroke="{PURPLE}" stroke-opacity=".5"/>'
            f'<text x="{x + PAD + 12}" y="{cy + 19}" font-size="12" font-weight="700" fill="{INK}">{head}</text>'
            f'<text x="{x + PAD + 12}" y="{cy + 34}" font-size="10.5" fill="{MUTED}">{detail}</text>'
        )
        if i < 2:
            cx = x + w / 2
            out.append(
                f'<path d="M{cx},{cy + 46} V{cy + 58}" stroke="{PURPLE}" stroke-width="1.6"/>'
                f'<path d="M{cx - 4.5},{cy + 54} L{cx},{cy + 60} L{cx + 4.5},{cy + 54} Z" fill="{PURPLE}"/>'
            )
        cy += 62
    cy += 6
    out.append(label(x + PAD, cy, "EACH COG IS WRITTEN WITH"))
    cy += 12
    items = ["512 px tiles", "overviews", "compression", "nodata set", "cloud mask at 10 m (SCL)"]
    px, py = 0.0, cy
    for t in items:
        tw = round(len(t) * SANS_W + 14, 1)
        if px and px + tw > iw:
            px, py = 0.0, py + 26
        out.append(
            f'<rect x="{x + PAD + px}" y="{py}" width="{tw}" height="21" rx="10.5" fill="{PURPLE}" fill-opacity=".14" '
            f'stroke="{PURPLE}" stroke-opacity=".55"/>'
            f'<text x="{round(x + PAD + px + tw / 2, 1)}" y="{py + 14.5}" font-size="10.5" fill="{PURPLE}" text-anchor="middle">{t}</text>'
        )
        px += tw + 5
    return "".join(out)


def file_card(x: float, y: float, w: float, h: float) -> str:
    out = [card(x, y, w, h, GOLD, "k3", "3", "FILE table", ["Metadata columns plus a FILE column"])]
    iw = w - 2 * PAD
    cy = y + 76
    out.append(label(x + PAD, cy, "RASTERS, ONE ROW PER MASTER COG"))
    cy += 10
    svg, ht = mono_pills(x + PAD, cy, ["asset_id", "product", "tier", "acquired_date", "footprint"], iw, GOLD)
    out.append(svg)
    cy += ht + PILL_GAP + 3
    out.append(
        f'<g class="mono" font-size="11" text-anchor="middle"><g class="pill"><rect x="{x + PAD}" y="{cy}" width="{iw}" height="26" rx="13" '
        f'fill="{GOLD}" fill-opacity=".22" stroke="{GOLD}" stroke-opacity=".95" stroke-width="1.4"/>'
        f'<text x="{x + PAD + 40}" y="{cy + 17}" fill="{GOLD}" font-weight="700">file</text>'
        f'<text x="{x + PAD + iw - 74}" y="{cy + 17}" fill="{GOLD}" font-size="10">FILE column</text></g></g>'
        f'<text x="{x + PAD + 4}" y="{cy + 41}" font-size="10.5" fill="{MUTED}">points at the master COG, nothing is copied</text>'
    )
    cy += 60
    out.append(label(x + PAD, cy, "ALONGSIDE"))
    cy += 10
    svg, ht = mono_pills(x + PAD, cy, ["aois", "s2_scene_assets", "coverage"], iw, GOLD)
    out.append(svg)
    cy += ht + 22
    out.append(label(x + PAD, cy, "TWO QUALITY TIERS"))
    cy += 10
    hw = (iw - 6) / 2
    out.append(
        f'<rect x="{x + PAD}" y="{cy}" width="{hw}" height="38" rx="8" fill="none" stroke="{LINE}" stroke-width="1.2" stroke-dasharray="4 3" opacity=".7"/>'
        f'<text x="{x + PAD + 9}" y="{cy + 16}" font-size="11.5" font-weight="700" fill="{INK}" opacity=".7">Standard</text>'
        f'<text x="{x + PAD + 9}" y="{cy + 30}" font-size="10" fill="{MUTED}" opacity=".7">all of Texas</text>'
        f'<text x="{x + PAD + hw - 8}" y="{cy + 16}" font-size="9" fill="{FAINT}" text-anchor="end">planned</text>'
        f'<rect x="{x + PAD + hw + 6}" y="{cy}" width="{hw}" height="38" rx="8" fill="{GOLD}" fill-opacity=".14" stroke="{GOLD}" stroke-opacity=".7"/>'
        f'<text x="{x + PAD + hw + 15}" y="{cy + 16}" font-size="11.5" font-weight="700" fill="{INK}">Enhanced</text>'
        f'<text x="{x + PAD + hw + 15}" y="{cy + 30}" font-size="10" fill="{MUTED}">focus areas</text>'
    )
    return "".join(out)


def functions_card(x: float, y: float, w: float, h: float, grouped: list[tuple[str, list[str]]]) -> str:
    out = [card(x, y, w, h, GREEN, "k4", "4", "GeoBrix Unity Catalog functions", [])]
    out.append(
        f'<text x="{x + PAD}" y="{y + 54}" font-size="11" fill="{MUTED}"><tspan class="mono" fill="{GREEN}" font-weight="700">gbx_rst_fromfile</tspan>'
        " reads only the COG blocks a clip needs.</text>"
        f'<text x="{x + PAD}" y="{y + 68}" font-size="11" fill="{MUTED}">Plain SQL on any SQL warehouse. Names start with '
        f'<tspan class="mono" fill="{GREEN}">gbx_rst_</tspan>.</text>'
    )
    label_w = 86
    cy = y + 92
    for gname, fns in grouped:
        planned = not fns
        out.append(
            f'<text x="{x + PAD}" y="{cy + 14.5}" font-size="10.5" font-weight="700" letter-spacing=".6" '
            f'fill="{FAINT if planned else MUTED}">{gname}</text>'
        )
        if planned:
            out.append(
                f'<g opacity=".6"><rect x="{x + PAD + label_w}" y="{cy}" width="104" height="{PILL_H}" rx="10.5" fill="none" '
                f'stroke="{LINE}" stroke-width="1.2" stroke-dasharray="3 3"/>'
                f'<text x="{x + PAD + label_w + 52}" y="{cy + 14.5}" font-size="10.5" fill="{MUTED}" text-anchor="middle">planned</text></g>'
            )
            cy += PILL_ROW + 8
            continue
        svg, ht = mono_pills(x + PAD + label_w, cy, fns, w - 2 * PAD - label_w, GREEN,
                             attr={n: f"gbx_rst_{n}" for n in fns}, hero=HERO)
        out.append(svg)
        cy += ht + 8 + 2
    return "".join(out)


def functions_height(grouped: list[tuple[str, list[str]]], w: float) -> float:
    label_w = 86
    cy = 92.0
    for _, fns in grouped:
        if not fns:
            cy += PILL_ROW + 8
            continue
        rows = pill_rows(fns, w - 2 * PAD - label_w)
        cy += (len(rows) - 1) * PILL_ROW + PILL_H + 10
    return cy + 4


def chain(x: float, y: float, names: list[str], max_w: float, color: str) -> tuple[str, float]:
    """Small mono pills joined by chevrons, wrapped to max_w. Returns (svg, height)."""
    out, px, py = [f'<g class="mono" font-size="10" fill="{color}" text-anchor="middle">'], 0.0, y
    for k, n in enumerate(names):
        w = round(len(n) * 6.0 + 12, 1)
        if px and px + 14 + w > max_w:
            px, py = 0.0, py + 25
        if px:
            out.append(f'<text x="{round(x + px + 7, 1)}" y="{py + 14}" fill="{FAINT}" font-size="12">\u203a</text>')
            px += 14
        out.append(
            f'<rect x="{round(x + px, 1)}" y="{py}" width="{w}" height="19" rx="9.5" fill="{STAGE}" fill-opacity=".55" '
            f'stroke="{color}" stroke-opacity=".5"/><text x="{round(x + px + w / 2, 1)}" y="{py + 13.5}">{n}</text>'
        )
        px += w
    out.append("</g>")
    return "".join(out), py - y + 19


def genie_band(x: float, y: float, w: float, qs: list[tuple[str, str, str, bool]], rst: list[str]) -> tuple[str, float]:
    """The Genie Agent band: the agent and its two forms, the headline questions, the later Genie Map."""
    fw = 236  # one form
    qx, qw = x + PAD + 2 * fw + 12 + 18, 452
    text_w = qw - 136
    chip_h = [max(14 * len(wrap(t, text_w)) + 18, 54) for _, _, t, _ in qs]
    h = 56 + sum(chip_h) + 7 * (len(qs) - 1) + 16
    out = [card(x, y, w, h, CORAL, "k5", "5", "Genie Agent", [], dashed=False)]
    out.append(
        f'<rect x="{x + 152}" y="{y + 17}" width="82" height="21" rx="10.5" fill="{CORAL}" fill-opacity=".16" stroke="{CORAL}" stroke-opacity=".6"/>'
        f'<text x="{x + 193}" y="{y + 31.5}" font-size="10.5" font-weight="700" fill="{CORAL}" text-anchor="middle">Agent mode</text>'
    )
    for i, line in enumerate(wrap("Ask in plain English. The agent composes the functions into one SQL statement per answer.", 2 * fw)):
        out.append(f'<text x="{x + PAD}" y="{y + 54 + 14 * i}" font-size="11" fill="{MUTED}">{line}</text>')
    # two forms, side by side
    fy = y + 84
    fh = h - 84 - 16
    names = [n[len("gbx_rst_"):] for n in rst]
    live_chain = [n for n in ("fromfile", "nbr", "mapalgebra", "merge", "histogram") if n in names]
    forms = [
        (x + PAD, CORAL, "Live: GeoBrix composes", "on the fly", "Reads the COGs and runs the whole chain on each question:", live_chain,
         None, "any area, window or index"),
        (x + PAD + fw + 12, GOLD, "Precomputed summary", "precomputed", "Reads a saved summary table for the heavy answer:", ["burn_severity_summary"],
         "The same functions answer everything else.", "the repeated headline answer"),
    ]
    out.append(f'<clipPath id="k5f0"><rect x="{x + PAD}" y="{fy}" width="{fw}" height="{fh}" rx="10"/></clipPath>')
    out.append(f'<clipPath id="k5f1"><rect x="{x + PAD + fw + 12}" y="{fy}" width="{fw}" height="{fh}" rx="10"/></clipPath>')
    for k, (fx, color, head, tag, body, steps, tail, best) in enumerate(forms):
        out.append(
            f'<rect x="{fx}" y="{fy}" width="{fw}" height="{fh}" rx="10" fill="{STAGE}" fill-opacity=".5" stroke="{color}" stroke-opacity=".6" stroke-width="1.3"/>'
            f'<rect x="{fx}" y="{fy}" width="5" height="{fh}" fill="{color}" clip-path="url(#k5f{k})"/>'
            f'<text x="{fx + 18}" y="{fy + 24}" font-size="13" font-weight="700" fill="{INK}">{head}</text>'
        )
        tw = round(len(tag) * 6.1 + 18, 1)
        out.append(
            f'<rect x="{fx + 18}" y="{fy + 34}" width="{tw}" height="20" rx="10" fill="{color}" fill-opacity=".2" stroke="{color}" stroke-opacity=".7"/>'
            f'<text x="{round(fx + 18 + tw / 2, 1)}" y="{fy + 48}" font-size="10.5" font-weight="700" fill="{color}" text-anchor="middle">{tag}</text>'
        )
        ty = fy + 74
        for line in wrap(body, fw - 36):
            out.append(f'<text x="{fx + 18}" y="{ty}" font-size="11" fill="{MUTED}">{line}</text>')
            ty += 14
        svg, ht = chain(fx + 18, ty + 4, steps, fw - 36, color)
        out.append(svg)
        ty += ht + 22
        if tail:
            for line in wrap(tail, fw - 36):
                out.append(f'<text x="{fx + 18}" y="{ty}" font-size="11" fill="{MUTED}">{line}</text>')
                ty += 14
        out.append(
            f'<line x1="{fx + 18}" x2="{fx + fw - 14}" y1="{fy + fh - 46}" y2="{fy + fh - 46}" stroke="{LINE}" stroke-width="1"/>'
            + label(fx + 18, fy + fh - 28, "BEST FOR")
            + f'<text x="{fx + 18}" y="{fy + fh - 12}" font-size="11" fill="{INK}">{best}</text>'
        )
    # headline questions
    out.append(label(qx, y + 34, "HEADLINE QUESTIONS", FAINT))
    cy = y + 50
    for (qid, audience, text, live), ch in zip(qs, chip_h):
        color = CORAL if live else LINE
        op = "" if live else ' opacity=".62"'
        stroke = (f'stroke="{CORAL}" stroke-opacity=".6" stroke-width="1.3"' if live
                  else f'stroke="{LINE}" stroke-width="1.2" stroke-dasharray="4 3"')
        out.append(
            f'<g class="qchip" data-q="{qid}"{op}><rect x="{qx}" y="{cy}" width="{qw}" height="{ch}" rx="9" '
            f'fill="{STAGE}" fill-opacity=".5" {stroke}/>'
            f'<text x="{qx + 14}" y="{cy + 21}" font-size="13" font-weight="800" fill="{CORAL if live else MUTED}">{qid}</text>'
        )
        for i, line in enumerate(wrap(audience, 96, 5.4)):
            out.append(f'<text x="{qx + 14}" y="{cy + 34 + 11 * i}" font-size="9.5" fill="{FAINT}">{escape(line)}</text>')
        for i, line in enumerate(wrap(text, text_w)):
            out.append(f'<text x="{qx + 126}" y="{cy + 20 + 14 * i}" font-size="11" fill="{INK}">{escape(line)}</text>')
        out.append("</g>")
        cy += ch + 7
    # later: Genie Map
    lx = qx + qw + 16
    lw = x + w - PAD - lx
    out.append(
        f'<g class="later"><rect x="{lx}" y="{y + 16}" width="{lw}" height="{h - 32}" rx="12" fill="none" stroke="{LINE}" stroke-width="1.3" stroke-dasharray="5 4"/>'
        f'<g opacity=".7"><text x="{lx + 14}" y="{y + 42}" font-size="9.5" font-weight="700" letter-spacing="1.3" fill="{FAINT}">LATER</text>'
        f'<text x="{lx + 14}" y="{y + 64}" font-size="14" font-weight="700" fill="{INK}">Genie Map</text>'
    )
    for i, line in enumerate(wrap("selects a Genie Agent, then answers on a map", lw - 28)):
        out.append(f'<text x="{lx + 14}" y="{y + 84 + 14 * i}" font-size="11" fill="{MUTED}">{line}</text>')
    mx, my = lx + 14, y + 128
    out.append(
        f'<path d="M{mx},{my + 6} L{mx + 22},{my} L{mx + 44},{my + 6} L{mx + 66},{my} V{my + 34} L{mx + 44},{my + 40} L{mx + 22},{my + 34} L{mx},{my + 40} Z" '
        f'fill="none" stroke="{LINE}" stroke-width="1.4" stroke-linejoin="round"/>'
        f'<path d="M{mx + 22},{my} V{my + 34} M{mx + 44},{my + 6} V{my + 40}" stroke="{LINE}" stroke-width="1" stroke-dasharray="2 3"/>'
        "</g></g>"
    )
    return "".join(out), h


# --- Assemble ----------------------------------------------------------------------------------------------


def render() -> str:
    rst = attached_functions()
    grouped = groups(rst)
    qs = questions()

    top = 18
    h1 = max(round(functions_height(grouped, COLS[3])), 340)
    xs = [MARGIN]
    for cw in COLS[:-1]:
        xs.append(xs[-1] + cw + GAP)
    row = [
        sources_card(xs[0], top, COLS[0], h1),
        land_card(xs[1], top, COLS[1], h1),
        file_card(xs[2], top, COLS[2], h1),
        functions_card(xs[3], top, COLS[3], h1, grouped),
    ]
    arrows = []
    for i in range(3):
        arrows.append(arrow(xs[i] + COLS[i] + 6, xs[i + 1] - 6, top + 27, f"flow{i}"))
    gy = top + h1 + 44
    band, bh = genie_band(MARGIN, gy, W - 2 * MARGIN, qs, rst)
    drop = [
        down_arrow(xs[2] + COLS[2] / 2, top + h1 + 6, gy - 6, "drop0", "tables the agent reads"),
        down_arrow(xs[3] + COLS[3] / 2, top + h1 + 6, gy - 6, "drop1", "functions attached to the agent"),
    ]
    height = round(gy + bh + 44)
    n_fn = len(rst)
    aria = (
        "The raster-genie example from sources to answers. Sources: Sentinel-2 L2A, WorldPop, NIFC fire perimeters and "
        "TIGER counties in this example, with USGS 3DEP, ESA WorldCover and TX RRC wells planned. Land writes master COGs "
        "with 512 pixel tiles, overviews, compression and a cloud mask at 10 m. The FILE table rasters holds metadata "
        f"columns and a FILE column, next to aois, s2_scene_assets and coverage. {n_fn} GeoBrix Unity Catalog functions "
        "are attached to the Genie Agent, which answers in two forms, live and precomputed. Genie Map is planned."
    )
    particles = "".join(
        f'<circle r="2.6" fill="{"#fff" if k % 2 == 0 else "#ffd2c9"}"{GLOW if k % 2 == 0 else ""}>'
        f'<animateMotion dur="2.4s" begin="{k * 0.6}s" repeatCount="indefinite"><mpath href="#{pid}"/></animateMotion></circle>'
        for k, pid in enumerate(["flow0", "flow1", "flow2", "drop0", "drop1"])
    )
    svg = f"""  <svg viewBox="0 0 {W} {height}" role="img" aria-label="{escape(aria)}">
    <defs>
      <radialGradient id="ga" cx="18%" cy="20%" r="45%"><stop offset="0" stop-color="#9fc3d6" stop-opacity=".12"/><stop offset="1" stop-color="#9fc3d6" stop-opacity="0"/></radialGradient>
      <radialGradient id="gb" cx="82%" cy="70%" r="50%"><stop offset="0" stop-color="#ff3621" stop-opacity=".12"/><stop offset=".6" stop-color="#ff3621" stop-opacity=".03"/><stop offset="1" stop-color="#ff3621" stop-opacity="0"/></radialGradient>
      <linearGradient id="card" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#1f3841"/><stop offset="1" stop-color="#16282f"/></linearGradient>
      <filter id="glow" x="-400%" y="-400%" width="900%" height="900%"><feGaussianBlur stdDeviation="5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
      <filter id="shd" x="-10%" y="-10%" width="120%" height="140%"><feDropShadow dx="0" dy="6" stdDeviation="8" flood-color="#000" flood-opacity=".35"/></filter>
      <pattern id="grat" width="62" height="62" patternUnits="userSpaceOnUse"><path d="M62 0H0V62" fill="none" stroke="#2b4650" stroke-width=".6" opacity=".55"/></pattern>
    </defs>
    <rect width="{W}" height="{height}" fill="url(#grat)" opacity=".45"/>
    <rect width="{W}" height="{height}" fill="url(#ga)"/><rect width="{W}" height="{height}" fill="url(#gb)"/>
    <g class="reveal r1">{row[0]}</g>
    <g class="reveal r2">{row[1]}</g>
    <g class="reveal r3">{row[2]}</g>
    <g class="reveal r4">{row[3]}</g>
    <g class="reveal r2">{"".join(arrows)}</g>
    <g class="reveal r3">{"".join(drop)}</g>
    <g class="reveal r4">{band}</g>
    <g class="particles">{particles}</g>
    <text x="{MARGIN}" y="{height - 16}" font-size="12" fill="{MUTED}"><tspan fill="{CORAL}" font-weight="700">{n_fn}</tspan> GeoBrix functions attached to the agent · <tspan fill="{CORAL}" font-weight="700">2</tspan> forms of the agent</text>
    <text x="{W - MARGIN}" y="{height - 16}" font-size="11" fill="{FAINT}" text-anchor="end">Source: examples/notebooks/raster-genie</text>
  </svg>"""

    sub = (
        "Land master COGs in a FILE table, read them with GeoBrix functions in Unity Catalog SQL, "
        "and ask a Genie Agent in plain English. The agent composes the functions into one SQL statement per answer."
    )
    legend = (
        f'<span><i style="background:{BLUE}"></i>sources</span>'
        f'<span><i style="background:{PURPLE}"></i>land</span>'
        f'<span><i style="background:{GOLD}"></i>FILE table</span>'
        f'<span><i style="background:{GREEN}"></i>functions</span>'
        f'<span><i style="background:{CORAL}"></i>agent and data flow</span>'
        "<span>solid: in this example · dashed: planned</span>"
    )
    return Template((TEMPLATES / "function-categories.html.tmpl").read_text()).substitute(
        generator="resources/images/generators/raster-genie-architecture.py",
        title_text="Raster is ready for Genie and SQL warehouses",
        css=(TEMPLATES / "diagrams.css").read_text().strip(),
        kicker="GeoBrix · raster-genie example",
        title_lead="Raster is ready for",
        title_span="Genie and SQL warehouses.",
        sub=sub,
        svg=svg,
        legend=legend,
    )


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render())
    print(f"wrote {out.relative_to(ROOT) if out.is_relative_to(ROOT) else out}: {len(attached_functions())} function pills")
    return 0


if __name__ == "__main__":
    sys.exit(main())
