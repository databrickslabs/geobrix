#!/usr/bin/env python3
"""Generate conceptual slide-ware diagrams for the Genie Map app.

Reuses the vapor-eyes / helios / eo-series primitive library (shared card,
stripe, chip, arrow, glyph and header/footer helpers) so these sit visually
alongside the vapor-eyes notebook diagrams — same blue/orange/teal/violet
accent progression, same fonts, same background.

Unlike the linear four-stage notebook diagrams, these are free-form layouts
(branches, side-by-side lanes, fan-outs) built from the same primitives via a
flexible `node()` box and a point-to-point `parrow()`.

Renders SVGs here; turn them into PNGs with headless Chrome, then crop:

    python3 resources/images/generators/genie-map.py
    for n in architecture two-paths lineage registry dynamic-h3; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1520,760 \
        --screenshot=resources/images/diagrams/genie-map/genie-map-$n.png \
        resources/images/diagrams/genie-map/genie-map-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/genie-map/genie-map-*.png'):
        img = Image.open(p).convert('RGB')
        bbox = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
"""
import importlib.util
import math
import os

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_HERE, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


eo = _load("eo_series", "eo-series.py")
ve = _load("vapor_eyes_gen", "vapor-eyes.py")
hel = _load("helios_gen", "helios.py")

# Shared primitives / constants
text, chip, arrow, esc = eo.text, eo.chip, eo.arrow, eo.esc
card, top_stripe, mono = eo.card, eo.top_stripe, eo.mono
_wrap_text = eo._wrap_text
C_INK, C_MUTED, C_MUTED_2, C_MUTED_3, C_BORDER = (
    eo.C_INK, eo.C_MUTED, eo.C_MUTED_2, eo.C_MUTED_3, eo.C_BORDER
)
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD
FOOTER_H = eo.FOOTER_H

# vapor-eyes accent progression (blue -> orange -> teal -> violet -> rose)
THEMES = ve.THEMES
BLUE, ORANGE, TEAL, VIOLET, ROSE = (THEMES[i] for i in (1, 2, 3, 4, 5))


# --- generic drawing helpers --------------------------------------------------

def parrow(x1, y1, x2, y2, color, *, head=9, dash=None, width=2.4):
    """Straight arrow from (x1,y1) to (x2,y2)."""
    ang = math.atan2(y2 - y1, x2 - x1)
    ex, ey = x2 - head * math.cos(ang), y2 - head * math.sin(ang)
    d = f' stroke-dasharray="{dash}"' if dash else ""
    lx = x2 - head * math.cos(ang) + head / 1.8 * math.cos(ang + math.pi / 2)
    ly = y2 - head * math.sin(ang) + head / 1.8 * math.sin(ang + math.pi / 2)
    rx = x2 - head * math.cos(ang) + head / 1.8 * math.cos(ang - math.pi / 2)
    ry = y2 - head * math.sin(ang) + head / 1.8 * math.sin(ang - math.pi / 2)
    return (
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{ex:.1f}" y2="{ey:.1f}" '
        f'stroke="{color}" stroke-width="{width}" stroke-linecap="round"{d}/>'
        f'<polygon points="{x2:.1f},{y2:.1f} {lx:.1f},{ly:.1f} {rx:.1f},{ry:.1f}" '
        f'fill="{color}"/>'
    )


def wrap_left(x, y, max_w, s, *, size=11, fill=C_MUTED, line_h=15, max_lines=3):
    char_w = size * 0.55
    max_chars = max(8, int(max_w / char_w))
    words, lines, cur = s.split(), [], ""
    for w in words:
        test = (cur + " " + w).strip()
        if len(test) > max_chars and cur:
            lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)
    return [text(x, y + i * line_h, ln, size=size, fill=fill)
            for i, ln in enumerate(lines[:max_lines])]


def node(x, y, w, h, title, subtitle="", *, accent, tint, chip_text=None,
         glyph=None, glyph_h=54, dashed=False, muted=False, title_size=15):
    """Flexible rounded-card node: top stripe, optional glyph, title, chip, caption."""
    fill = "#F7F9FB" if muted else "#FFFFFF"
    if dashed:
        border = f'stroke="{accent}" stroke-width="1.8" stroke-dasharray="6 5"'
    else:
        border = f'stroke="{C_BORDER}" stroke-width="1"'
    out = [f'<rect x="{x}" y="{y}" rx="12" ry="12" width="{w}" height="{h}" '
           f'fill="{fill}" {border} filter="url(#card-shadow)"/>']
    out.append(top_stripe(x, y, w, accent, r=12))
    ty = y + 24
    if glyph:
        out.append(glyph(x + w / 2, y + 14 + glyph_h / 2, accent, tint))
        ty = y + 14 + glyph_h + 22
    out.append(text(x + w / 2, ty, title, size=title_size, weight=800,
                    fill=(C_MUTED if muted else C_INK), anchor="middle"))
    cy2 = ty + 4
    if chip_text:
        cs, cw = chip(0, 0, chip_text, fg=accent, bg=tint, mono_font=True)
        out.append(f'<g transform="translate({x + (w - cw) / 2:.1f}, {cy2 + 8})">{cs}</g>')
        cy2 += 32
    if subtitle:
        out.extend(_wrap_text(x + 12, cy2 + 18, w - 24, subtitle, size=11.5,
                              fill=(C_MUTED_2 if muted else C_MUTED)))
    return "".join(out)


def inner_panel(x, y, w, h, title, subtitle, theme):
    a, t = theme["accent"], theme["tint"]
    out = [f'<rect x="{x}" y="{y}" rx="9" ry="9" width="{w}" height="{h}" '
           f'fill="{t}" stroke="{a}" stroke-width="1.4"/>',
           f'<rect x="{x}" y="{y}" width="6" height="{h}" rx="3" ry="3" fill="{a}"/>',
           text(x + 20, y + 26, title, size=14, weight=800, fill=C_INK)]
    out.extend(wrap_left(x + 20, y + 47, w - 34, subtitle, size=11, fill=C_MUTED))
    return "".join(out)


def col_label(x, y, s, color):
    return text(x, y, s, size=11, weight=700, fill=color, letter_spacing="1.4")


# --- custom compact glyphs ----------------------------------------------------

def g_user(cx, cy, color, tint):
    return (
        f'<circle cx="{cx}" cy="{cy - 14}" r="13" fill="{tint}" stroke="{color}" '
        f'stroke-width="2.2"/>'
        f'<path d="M {cx - 22} {cy + 22} a 22 20 0 0 1 44 0 Z" fill="{tint}" '
        f'stroke="{color}" stroke-width="2.2" stroke-linejoin="round"/>'
    )


def g_kepler_map(cx, cy, color, tint):
    """A small deck.gl-style map: rounded frame with a hex and point layer."""
    w, h = 108, 76
    x, y = cx - w / 2, cy - h / 2
    out = [f'<rect x="{x}" y="{y}" rx="8" ry="8" width="{w}" height="{h}" '
           f'fill="{tint}" stroke="{color}" stroke-width="2"/>']
    # two hexes
    for hx, hy, R in [(cx - 24, cy - 6, 15), (cx - 6, cy + 14, 12)]:
        pts = " ".join(
            f"{hx + R * math.cos(math.radians(60 * i - 90)):.1f},"
            f"{hy + R * math.sin(math.radians(60 * i - 90)):.1f}" for i in range(6))
        out.append(f'<polygon points="{pts}" fill="{color}" fill-opacity="0.28" '
                   f'stroke="{color}" stroke-width="1.6"/>')
    # points
    for dx, dy in [(20, -18), (34, 4), (16, 22), (30, 24)]:
        out.append(f'<circle cx="{cx + dx}" cy="{cy + dy}" r="3.4" fill="{color}"/>')
    return "".join(out)


def g_genie(cx, cy, color, tint):
    """A chat bubble with a sparkle — the natural-language answer."""
    w, h = 92, 60
    x, y = cx - w / 2, cy - h / 2 - 6
    out = [
        f'<path d="M {x + 12} {y} H {x + w - 12} Q {x + w} {y} {x + w} {y + 12} '
        f'V {y + h - 12} Q {x + w} {y + h} {x + w - 12} {y + h} '
        f'H {x + 30} L {x + 16} {y + h + 16} L {x + 18} {y + h} '
        f'H {x + 12} Q {x} {y + h} {x} {y + h - 12} V {y + 12} '
        f'Q {x} {y} {x + 12} {y} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
    ]
    # sparkle
    sx, sy = cx + 6, cy - 4
    out.append(
        f'<path d="M {sx} {sy - 14} L {sx + 4} {sy - 4} L {sx + 14} {sy} '
        f'L {sx + 4} {sy + 4} L {sx} {sy + 14} L {sx - 4} {sy + 4} '
        f'L {sx - 14} {sy} L {sx - 4} {sy - 4} Z" fill="{color}"/>'
    )
    out.append(f'<circle cx="{sx - 20}" cy="{sy + 8}" r="2.6" fill="{color}"/>')
    return "".join(out)


def g_warehouse(cx, cy, color, tint):
    """A database cylinder — the SQL warehouse."""
    w, rx, ry = 78, 39, 12
    top, bot = cy - 30, cy + 30
    out = [
        f'<path d="M {cx - rx} {top} V {bot} A {rx} {ry} 0 0 0 {cx + rx} {bot} '
        f'V {top}" fill="{tint}" stroke="{color}" stroke-width="2"/>',
        f'<ellipse cx="{cx}" cy="{top}" rx="{rx}" ry="{ry}" fill="{tint}" '
        f'stroke="{color}" stroke-width="2"/>',
    ]
    for i in range(1, 3):
        yy = top + i * 20
        out.append(f'<path d="M {cx - rx} {yy} A {rx} {ry} 0 0 0 {cx + rx} {yy}" '
                   f'fill="none" stroke="{color}" stroke-width="1.4" '
                   f'stroke-opacity="0.5"/>')
    return "".join(out)


def g_tables(cx, cy, color, tint, *, label="gold"):
    return eo.g_delta_table(cx, cy, color, tint, label=label)


def g_coarse_hex(cx, cy, color, tint):
    return eo.g_hex_grid(cx, cy, color, tint, R=26, rows=2, cols=2)


def g_fine_hex(cx, cy, color, tint):
    # Sized to fit the card's glyph_h (~96px): R=13, 4 rows ≈ 84px tall, so it
    # stays inside the card and clear of the title. (eo.g_dense_hex_grid is R=14
    # x 7 rows ≈ 154px — it overflows this card, so we don't use it here.)
    return eo.g_hex_grid(cx, cy, color, tint, R=13, rows=4, cols=6)


def g_points(cx, cy, color, tint):
    pts = [(-30, -14), (-6, -22), (18, -10), (34, 8), (6, 6),
           (-22, 16), (14, 24), (-2, -4)]
    return "".join(f'<circle cx="{cx + dx}" cy="{cy + dy}" r="5" fill="{tint}" '
                   f'stroke="{color}" stroke-width="2"/>' for dx, dy in pts)


# --- canvas scaffold ----------------------------------------------------------

def canvas(badge, title, subtitle, name, theme, body, chips, note):
    accent, tint = theme["accent"], theme["tint"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">',
        '<defs>'
        '<filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" '
        'flood-opacity="0.08"/></filter>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0" stop-color="#FAFBFC"/>'
        '<stop offset="1" stop-color="#F1F4F8"/></linearGradient></defs>',
        f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>',
        ve.render_header_generic(badge, title, subtitle, f"Genie Map  ·  {name}", accent),
        body,
        ve.render_footer_generic(chips, accent, tint, note),
        "</svg>",
    ]
    return "\n".join(parts)


NOTE = "databrickslabs/geobrix  ·  Genie Map  ·  geospatial_docs.vapor_eyes_lf"


# --- diagram bodies -----------------------------------------------------------

def body_architecture():
    out = []
    # User
    out.append(node(66, 300, 156, 150, "User",
                    "Pans the map, or asks a question in plain English",
                    accent=BLUE["accent"], tint=BLUE["tint"], glyph=g_user,
                    glyph_h=56, title_size=17))
    # Genie Map app (hero, center)
    gx, gy, gw, gh = 420, 236, 372, 268
    out.append(f'<rect x="{gx}" y="{gy}" rx="14" ry="14" width="{gw}" height="{gh}" '
               f'fill="#FFFFFF" stroke="{C_BORDER}" stroke-width="1" '
               f'filter="url(#card-shadow)"/>')
    out.append(top_stripe(gx, gy, gw, BLUE["accent"], r=14))
    out.append(text(gx + gw / 2, gy + 34, "Genie Map app", size=19, weight=800,
                    fill=C_INK, anchor="middle"))
    out.append(inner_panel(gx + 22, gy + 52, gw - 44, 90, "kepler.gl client",
                           "deck.gl layers · viewport bounds · layer visibility", BLUE))
    out.append(inner_panel(gx + 22, gy + 156, gw - 44, 90, "Databricks AppKit server",
                           "analytics · Genie · model serving proxy", BLUE))
    # backends
    out.append(node(1006, 214, 412, 136, "SQL warehouse",
                    "Deterministic viewport SQL over the gold tables",
                    accent=TEAL["accent"], tint=TEAL["tint"],
                    chip_text="vapor_eyes_lf gold", title_size=17))
    out.append(node(1006, 398, 412, 136, "Genie Space",
                    "Natural-language questions answered as map geometry",
                    accent=VIOLET["accent"], tint=VIOLET["tint"],
                    chip_text="ST_ASGEOJSON(...) → *_geojson", title_size=17))
    # arrows
    out.append(parrow(224, 375, 414, 372, BLUE["accent"]))
    out.append(parrow(796, 356, 1000, 282, TEAL["accent"]))
    out.append(parrow(796, 388, 1000, 466, VIOLET["accent"]))
    return "".join(out)


def body_two_paths():
    out = []
    lanes = [
        dict(x=54, w=660, theme=BLUE, head="Deterministic viewport path",
             good="Precise, repeatable, instant on every pan and zoom",
             nodes=[
                 ("Map move", "The user pans or zooms; the client captures the new viewport bounds and zoom", g_kepler_map),
                 ("Parameterized Spark SQL", "One templated query per layer, bound to the bbox and a zoom-and-density resolution", None),
                 ("H3 cells + point layers", "Hexagon and point layers render straight from the returned rows — no geometry round-trip", g_coarse_hex),
             ]),
        dict(x=766, w=660, theme=VIOLET, head="Natural-language path",
             good="Exploratory — answers ad-hoc questions the templates never anticipated",
             nodes=[
                 ("Question", "“Which operators sit nearest the strongest plumes?” — typed in the assistant panel", None),
                 ("Genie Space", "Genie plans SQL over the curated gold tables and returns a result set", g_genie),
                 ("Geometry answer", "A column aliased *_geojson (ST_ASGEOJSON) is recognized as drawable geometry", None),
                 ("Rendered layer", "The answer becomes a map layer alongside the deterministic ones", g_kepler_map),
             ]),
    ]
    # Fixed vertical band shared by both lanes so they align despite differing
    # node counts: lane 168..616, header 46, nodes 236..556, good-for strip below.
    lane_top, lane_h = 168, 448
    node_top, node_band = 236, 322
    for L in lanes:
        a, t = L["theme"]["accent"], L["theme"]["tint"]
        lx, lw = L["x"], L["w"]
        # lane panel
        out.append(f'<rect x="{lx}" y="{lane_top}" rx="16" ry="16" width="{lw}" '
                   f'height="{lane_h}" fill="{t}" fill-opacity="0.35" '
                   f'stroke="{a}" stroke-width="1.4"/>')
        # lane header
        out.append(f'<rect x="{lx}" y="{lane_top}" rx="16" ry="16" width="{lw}" height="46" fill="{a}"/>')
        out.append(f'<rect x="{lx}" y="{lane_top + 26}" width="{lw}" height="20" fill="{a}"/>')
        out.append(text(lx + lw / 2, lane_top + 29, L["head"], size=16, weight=800,
                        fill="#FFFFFF", anchor="middle"))
        # nodes stacked — size to fill the shared band regardless of count
        nw = lw - 120
        nx = lx + (lw - nw) / 2
        n = len(L["nodes"])
        gap = 22
        nh = (node_band - (n - 1) * gap) / n
        centers = []
        for i, (ti, sub, gl) in enumerate(L["nodes"]):
            yy = node_top + i * (nh + gap)
            out.append(node(nx, yy, nw, nh, ti, sub, accent=a, tint=t,
                            glyph=None, title_size=14))
            centers.append((yy, yy + nh))
        for i in range(len(centers) - 1):
            out.append(parrow(lx + lw / 2, centers[i][1] + 3,
                              lx + lw / 2, centers[i + 1][0] - 3, a))
        # "good for" strip inside lane, aligned across both lanes
        gy = node_top + node_band + 20
        out.extend(wrap_left(lx + 30, gy, lw - 60, "Good for:  " + L["good"],
                             size=12, fill=a, line_h=15))
    return "".join(out)


def body_lineage():
    out = []
    cA, cB, cC = 52, 566, 1122
    wA, wB, wC = 300, 340, 306
    out.append(col_label(cA + 4, 190, "VAPOR-EYES GOLD", C_MUTED_3))
    out.append(col_label(cB + 4, 190, "MAP-READY VIEWS", C_MUTED_3))
    out.append(col_label(cC + 4, 190, "MAP LAYERS", C_MUTED_3))

    # sources
    src = [
        (206, "Hotspot cells", "Per-cell CH₄ max from the wide-area TROPOMI / EMIT screen"),
        (330, "Plume complexes", "EMIT plume outlines with JPL emission-rate and concentration"),
        (454, "Well inventory + reference", "Well history (as-of-now), shale plays, county / state boundaries"),
    ]
    src_y = {}
    for y, ti, sub in src:
        out.append(node(cA, y, wA, 96, ti, sub, accent=TEAL["accent"],
                        tint=TEAL["tint"], title_size=14))
        src_y[ti] = y + 48

    # MVs
    out.append(node(cB, 206, wB, 116, "wells_enriched_latest",
                    "One row per well: play + county tagged, map-ready point, SRID 4326",
                    accent=ORANGE["accent"], tint=ORANGE["tint"],
                    chip_text="new gold MV", title_size=16))
    out.append(node(cB, 346, wB, 84, "hotspot_latest",
                    "Latest per-cell CH₄ hotspot surface",
                    accent=TEAL["accent"], tint=TEAL["tint"], title_size=15))
    out.append(node(cB, 452, wB, 84, "plume_leaderboard_latest",
                    "Ranked plumes by max concentration",
                    accent=VIOLET["accent"], tint=VIOLET["tint"], title_size=15))
    mv_y = {"wells": 206 + 58, "hot": 346 + 42, "plume": 452 + 42}

    # layers
    lay = [
        (196, "ch4_hotspots", "H3 · hotspot_latest", TEAL, "hot"),
        (290, "well_density", "H3 · wells_enriched_latest", ORANGE, "wells"),
        (384, "wells", "point · wells_enriched_latest", ORANGE, "wells"),
        (478, "plumes", "point · plume_leaderboard_latest", VIOLET, "plume"),
    ]
    lay_y = {}
    for y, ti, sub, th, key in lay:
        out.append(node(cC, y, wC, 78, ti, sub, accent=th["accent"],
                        tint=th["tint"], title_size=15))
        lay_y[ti] = (y + 39, key)

    # source -> MV arrows
    out.append(parrow(cA + wA, src_y["Hotspot cells"], cB, mv_y["hot"], TEAL["accent"]))
    out.append(parrow(cA + wA, src_y["Plume complexes"], cB, mv_y["plume"], VIOLET["accent"]))
    out.append(parrow(cA + wA, src_y["Well inventory + reference"], cB, mv_y["wells"], ORANGE["accent"]))

    # MV -> layer arrows (wells_enriched_latest feeds TWO layers)
    theme_by_key = {"hot": TEAL, "wells": ORANGE, "plume": VIOLET}
    mv_out_x = cB + wB
    for ti, (yc, key) in lay_y.items():
        src_yy = mv_y[key]
        out.append(parrow(mv_out_x, src_yy, cC, yc, theme_by_key[key]["accent"]))
    # emphasis note on the shared MV
    out.append(text(cB + wB / 2, 176, "wells_enriched_latest feeds both the density hexes and the well points",
                    size=11, weight=600, fill=ORANGE["accent"], anchor="middle"))
    return "".join(out)


def body_registry():
    out = []
    # seam label
    out.append(text(60, 190, "ONE SEAM:  getActiveDataset()  reads  VITE_ACTIVE_DATASET",
                    size=12, weight=700, fill=ORANGE["accent"], letter_spacing="0.6"))

    # DatasetConfig (active) + helios (future)
    dcx, dcw = 52, 292
    out.append(node(dcx, 236, dcw, 132, "DatasetConfig",
                    "One typed config declares a list of LayerDefs and the active dataset",
                    accent=ORANGE["accent"], tint=ORANGE["tint"],
                    chip_text="vapor-eyes · active", title_size=17))
    out.append(node(dcx, 430, dcw, 120, "DatasetConfig",
                    "A second dataset plugs into the same seam — no app code changes",
                    accent=C_MUTED_2, tint="#EEF1F4",
                    chip_text="helios · future plug-in", title_size=17,
                    dashed=True, muted=True))

    # LayerDefs
    ldx, ldw = 556, 330
    lds = [
        (196, "LayerDef  ch4_hotspots", "kind: h3 · palette · zoomVisible · H3ResConfig"),
        (290, "LayerDef  well_density", "kind: h3 · point-sourced · dynamic resolution"),
        (384, "LayerDef  wells", "kind: point · tooltip columns · palette"),
        (478, "LayerDef  plumes", "kind: point · metric max_conc_ppmm"),
    ]
    ld_y = []
    for y, ti, sub in lds:
        out.append(node(ldx, y, ldw, 78, ti, sub, accent=ORANGE["accent"],
                        tint=ORANGE["tint"], title_size=14))
        ld_y.append(y + 39)

    # rendered layers (right)
    rlx, rlw = 1128, 300
    kinds = ["H3 hexagon layer", "H3 hexagon layer", "point layer", "point layer"]
    for i, (y, k) in enumerate(zip([196, 290, 384, 478], kinds)):
        out.append(node(rlx, y, rlw, 78, "kepler layer", k,
                        accent=ORANGE["accent"], tint=ORANGE["tint"],
                        glyph=None, title_size=14))

    # fan-out arrows from active DatasetConfig to each LayerDef
    dc_out = (dcx + dcw, 236 + 66)
    for yc in ld_y:
        out.append(parrow(dc_out[0], dc_out[1], ldx, yc, ORANGE["accent"]))
    # LayerDef -> rendered layer
    for i, yc in enumerate(ld_y):
        out.append(parrow(ldx + ldw, yc, rlx, [196, 290, 384, 478][i] + 39,
                          ORANGE["accent"]))
    # helios (future) dashed arrows toward the same LayerDef column
    out.append(parrow(dcx + dcw, 430 + 60, ldx - 6, 500, C_MUTED_2, dash="6 5"))
    out.append(text(ldx + ldw / 2, 574, "…its own LayerDefs, same downstream machinery",
                    size=11, weight=600, fill=C_MUTED_2, anchor="middle"))
    return "".join(out)


def body_dynamic_h3():
    out = []
    # zoom axis band
    ax0, ax1, ay = 120, 1360, 210
    out.append(text(PAD, 186, "ZOOM SETS A CEILING", size=11, weight=700,
                    fill=C_MUTED_3, letter_spacing="1.4"))
    out.append(parrow(ax0, ay, ax1, ay, BLUE["accent"], width=3))
    out.append(text(ax0, ay + 26, "zoomed out", size=12, fill=C_MUTED))
    out.append(text(ax1, ay + 26, "zoomed in", size=12, fill=C_MUTED, anchor="end"))
    steps = [("res 4", 0.12), ("res 5", 0.37), ("res 6", 0.62), ("res 7", 0.87)]
    for lbl, f in steps:
        x = ax0 + f * (ax1 - ax0)
        out.append(f'<circle cx="{x:.0f}" cy="{ay}" r="6" fill="{BLUE["accent"]}"/>')
        out.append(text(x, ay - 16, lbl, size=12, weight=700,
                        fill=BLUE["accent"], anchor="middle"))

    # density strip
    dsy = 262
    out.append(f'<rect x="{PAD}" y="{dsy}" rx="12" ry="12" '
               f'width="{CANVAS_W - 2 * PAD}" height="52" '
               f'fill="{ROSE["tint"]}" stroke="{ROSE["accent"]}" stroke-width="1.4"/>')
    out.append(text(PAD + 22, dsy + 24, "DENSITY LOWERS IT ONLY WHEN CROWDED",
                    size=11, weight=700, fill=ROSE["accent"], letter_spacing="1.0"))
    out.append(text(PAD + 22, dsy + 42,
                    "If in-view cells exceed the target (~300), coarsen further by "
                    "floor(log₇(count / target)) H3 parent steps. Sparse data stays at the ceiling.",
                    size=12, fill=C_MUTED))

    # two source-mode panels
    py, ph = 340, 262
    lw = (CANVAS_W - 2 * PAD - 40) / 2
    out.append(node(PAD, py, lw, ph, "Cell-sourced  ·  ch4_hotspots",
                    "Gold MV stores cells at the satellite's native resolution. Coarsen "
                    "with h3_toparent when dense — but capped at native res, so it never "
                    "invents precision the instrument lacks.",
                    accent=TEAL["accent"], tint=TEAL["tint"], glyph=g_coarse_hex,
                    glyph_h=96, chip_text="h3_toparent · coarsen only", title_size=17))
    out.append(node(PAD + lw + 40, py, lw, ph, "Point-sourced  ·  well_density",
                    "Query bins raw well coordinates with h3_longlatash3 at the chosen "
                    "resolution, so it can refine finer as you zoom in — as well as "
                    "coarsen when the view is crowded.",
                    accent=BLUE["accent"], tint=BLUE["tint"], glyph=g_fine_hex,
                    glyph_h=96, chip_text="h3_longlatash3 · coarsen or refine",
                    title_size=17))
    return "".join(out)


DIAGRAMS = {
    "architecture": dict(
        badge="GM", theme=BLUE,
        title="How the Genie Map fits together",
        subtitle="One kepler.gl + AppKit app over two Databricks back ends",
        body=body_architecture,
        chips=["kepler.gl", "Databricks AppKit", "SQL warehouse", "Genie Space",
               "vapor_eyes_lf"],
    ),
    "two-paths": dict(
        badge="GM", theme=BLUE,
        title="Two ways to put geometry on the map",
        subtitle="A deterministic viewport path and a natural-language path, side by side",
        body=body_two_paths,
        chips=["useLayerData", "parameterized SQL", "h3_longlatash3", "Genie",
               "ST_ASGEOJSON"],
    ),
    "lineage": dict(
        badge="GM", theme=TEAL,
        title="From methane gold to map layers",
        subtitle="vapor-eyes gold tables → map-ready views → the four shipped layers",
        body=body_lineage,
        chips=["wells_enriched_latest", "hotspot_latest", "plume_leaderboard_latest",
               "SRID 4326"],
    ),
    "registry": dict(
        badge="GM", theme=ORANGE,
        title="The layer registry is the extension point",
        subtitle="One DatasetConfig declares many LayerDefs — swap the dataset, keep the app",
        body=body_registry,
        chips=["DatasetConfig", "LayerDef", "getActiveDataset", "VITE_ACTIVE_DATASET"],
    ),
    "dynamic-h3": dict(
        badge="GM", theme=VIOLET,
        title="Density-aware dynamic H3 resolution",
        subtitle="Zoom sets a ceiling; density lowers it; cell-sourced coarsens, point-sourced refines",
        body=body_dynamic_h3,
        chips=["zoomResBreaks", "resByBreak", "h3_toparent", "h3_longlatash3",
               "log₇(count/target)"],
    ),
}


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "genie-map")
    os.makedirs(out_dir, exist_ok=True)
    for name, d in DIAGRAMS.items():
        svg = canvas(d["badge"], d["title"], d["subtitle"], name.replace("-", " ").title(),
                     d["theme"], d["body"](), d["chips"], NOTE)
        path = os.path.join(out_dir, f"genie-map-{name}.svg")
        with open(path, "w") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
