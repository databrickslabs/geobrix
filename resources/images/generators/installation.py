#!/usr/bin/env python3
"""Generate the Installation page diagrams (landing page, DCS, init script).

Three free-form diagrams for docs/docs/installation.mdx and its two sub-pages:

    installation-paths        the three install paths, picked by compute + tier
    installation-dcs          GeoBrix container image -> dedicated cluster -> tiers
    installation-init-script  isolated VOL_DIR -> geobrix-gdal-init.sh -> x86 cluster

Reuses the eo-series / vapor-eyes / genie-map primitive library (card, stripe,
chip, node, arrow, header/footer helpers) and the file-gbx-flow Volume glyph,
so these sit visually alongside the other GeoBrix diagrams — same palette,
same fonts, same background.

Re-render after editing this script (writes SVG, then PNG via headless Chrome
when it is installed; pass --svg-only to skip the PNG step):

    python3 resources/images/generators/installation.py

The PNG step is equivalent to, per diagram:

    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1480,720 \\
        --screenshot=resources/images/diagrams/installation/installation-<name>.png \\
        resources/images/diagrams/installation/installation-<name>.svg

followed by a crop to the SVG bounds (2x).
"""
import importlib.util
import os
import shutil
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_HERE, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


eo = _load("eo_series", "eo-series.py")
ve = _load("vapor_eyes_gen", "vapor-eyes.py")
gm = _load("genie_map_gen", "genie-map.py")
fgf = _load("file_gbx_flow_gen", "file-gbx-flow.py")

# Shared primitives / constants
text, chip, esc, mono = eo.text, eo.chip, eo.esc, eo.mono
top_stripe = eo.top_stripe
parrow, node, wrap_left, col_label = gm.parrow, gm.node, gm.wrap_left, gm.col_label
C_INK, C_MUTED, C_MUTED_2, C_MUTED_3, C_BORDER = (
    eo.C_INK, eo.C_MUTED, eo.C_MUTED_2, eo.C_MUTED_3, eo.C_BORDER
)
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD

# vapor-eyes accent progression (blue -> orange -> teal -> violet -> rose)
THEMES = ve.THEMES
BLUE, ORANGE, TEAL, VIOLET, ROSE = (THEMES[i] for i in (1, 2, 3, 4, 5))

# Tier colors, used consistently across all three diagrams.
LIGHT_TIER = BLUE
HEAVY_TIER = ORANGE

NOTE = "databrickslabs/geobrix  ·  Installation"
OUT_DIR = os.path.join(_HERE, "..", "diagrams", "installation")


# --- small helpers -------------------------------------------------------------

def chip_at(x, y, s, theme, *, mono_font=True, h=24):
    svg, w = chip(x, y, s, fg=theme["accent"], bg=theme["tint"], mono_font=mono_font, h=h)
    return svg, w


def centered_chip(cx, y, s, theme, *, mono_font=True, h=24):
    _, w = chip(0, 0, s, mono_font=mono_font, h=h)
    svg, _ = chip_at(cx - w / 2, y, s, theme, mono_font=mono_font, h=h)
    return svg


def tier_chips(x, y, tiers):
    """Row of tier chips ("Lightweight" / "Heavyweight") starting at x."""
    out, cx = [], x
    for t in tiers:
        theme = LIGHT_TIER if t == "Lightweight" else HEAVY_TIER
        svg, w = chip_at(cx, y, t, theme, mono_font=False, h=26)
        out.append(svg)
        cx += w + 10
    return "".join(out), cx - x - 10


def label_line(x, y, label, value, *, color=C_MUTED_3, size=12.5, max_w=380, line_h=17):
    """Small-caps label on its own line, value wrapped beneath it."""
    out = [text(x, y, label, size=10.5, weight=700, fill=color, letter_spacing="1.2")]
    out.extend(wrap_left(x, y + line_h + 1, max_w, value, size=size, fill=C_INK,
                         line_h=line_h, max_lines=3))
    return "".join(out)


def bullet(x, y, s, color, *, size=12.5, max_w=360, line_h=17, fill=C_INK):
    out = [f'<circle cx="{x + 4}" cy="{y - 4}" r="3.2" fill="{color}"/>']
    out.extend(wrap_left(x + 16, y, max_w - 16, s, size=size, fill=fill,
                         line_h=line_h, max_lines=2))
    return "".join(out)


def strip(x, y, w, h, theme, title, body):
    a, t = theme["accent"], theme["tint"]
    return "".join([
        f'<rect x="{x}" y="{y}" rx="12" ry="12" width="{w}" height="{h}" '
        f'fill="{t}" stroke="{a}" stroke-width="1.4"/>',
        text(x + 20, y + 22, title, size=11, weight=700, fill=a, letter_spacing="1.0"),
        text(x + 20, y + 41, body, size=12.5, fill=C_INK),
    ])


def panel(x, y, w, h, *, accent, title=None, title_size=16, fill="#FFFFFF"):
    out = [f'<rect x="{x}" y="{y}" rx="14" ry="14" width="{w}" height="{h}" '
           f'fill="{fill}" stroke="{C_BORDER}" stroke-width="1" filter="url(#card-shadow)"/>',
           top_stripe(x, y, w, accent, r=14)]
    if title:
        out.append(text(x + w / 2, y + 32, title, size=title_size, weight=800,
                        fill=C_INK, anchor="middle"))
    return "".join(out)


# --- glyphs (centered on cx, cy) -------------------------------------------------

def g_wheel(cx, cy, color, tint):
    """A Python wheel package: a parcel with a 'whl' label."""
    w, h, d = 74, 52, 16
    x, y = cx - w / 2, cy - h / 2 + d / 2
    return (
        f'<path d="M {x} {y} L {x + d} {y - d} H {x + w + d} L {x + w} {y} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
        f'<path d="M {x + w} {y} L {x + w + d} {y - d} V {y + h - d} L {x + w} {y + h} Z" '
        f'fill="{color}" fill-opacity="0.25" stroke="{color}" stroke-width="2" '
        f'stroke-linejoin="round"/>'
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="#FFFFFF" '
        f'stroke="{color}" stroke-width="2"/>'
        f'<text x="{x + w / 2}" y="{y + h / 2 + 6}" text-anchor="middle" '
        f'font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16" '
        f'font-weight="800" fill="{color}">.whl</text>'
    )


def g_container(cx, cy, color, tint):
    """A container image: three stacked layers."""
    w, lh, gap = 96, 16, 5
    out = []
    labels = ["", "", ""]
    for i, _ in enumerate(labels):
        y = cy - 30 + i * (lh + gap)
        fill = color if i == 0 else tint
        op = "0.9" if i == 0 else "1"
        out.append(f'<rect x="{cx - w / 2}" y="{y}" rx="4" ry="4" width="{w}" '
                   f'height="{lh}" fill="{fill}" fill-opacity="{op}" stroke="{color}" '
                   f'stroke-width="2"/>')
    # base plate
    out.append(f'<line x1="{cx - w / 2 - 8}" y1="{cy + 36}" x2="{cx + w / 2 + 8}" '
               f'y2="{cy + 36}" stroke="{color}" stroke-width="2.4" stroke-linecap="round"/>')
    return "".join(out)


def g_script(cx, cy, color, tint):
    """A shell script: a terminal window with a prompt."""
    w, h = 92, 64
    x, y = cx - w / 2, cy - h / 2
    return (
        f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="{h}" fill="{tint}" '
        f'stroke="{color}" stroke-width="2"/>'
        f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="14" fill="{color}"/>'
        f'<rect x="{x}" y="{y + 8}" width="{w}" height="6" fill="{color}"/>'
        f'<path d="M {x + 14} {y + 28} L {x + 26} {y + 37} L {x + 14} {y + 46}" '
        f'fill="none" stroke="{color}" stroke-width="2.6" stroke-linecap="round" '
        f'stroke-linejoin="round"/>'
        f'<line x1="{x + 32}" y1="{y + 47}" x2="{x + 52}" y2="{y + 47}" stroke="{color}" '
        f'stroke-width="2.6" stroke-linecap="round"/>'
    )


def g_cluster(cx, cy, color, tint):
    """A cluster: one driver box over three worker boxes."""
    out = [f'<rect x="{cx - 22}" y="{cy - 34}" rx="4" ry="4" width="44" height="22" '
           f'fill="{color}" fill-opacity="0.9" stroke="{color}" stroke-width="2"/>']
    for dx in (-44, 0, 44):
        out.append(f'<rect x="{cx + dx - 18}" y="{cy + 10}" rx="4" ry="4" width="36" '
                   f'height="22" fill="{tint}" stroke="{color}" stroke-width="2"/>')
        out.append(f'<line x1="{cx}" y1="{cy - 12}" x2="{cx + dx}" y2="{cy + 10}" '
                   f'stroke="{color}" stroke-width="1.6" stroke-opacity="0.7"/>')
    return "".join(out)


def g_jar(cx, cy, color, tint):
    """A JAR archive: a document with a 'jar' label."""
    return (fgf.glyph_file(cx, cy, color, tint, s=46)
            + f'<text x="{cx - 2}" y="{cy - 2}" text-anchor="middle" '
              f'font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
              f'font-size="10" font-weight="800" fill="{color}">.jar</text>')


# --- canvas scaffold -----------------------------------------------------------

def canvas(badge, title, subtitle, pill, theme, body, chips, label="ARTIFACTS"):
    accent, tint = theme["accent"], theme["tint"]
    return "\n".join([
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
        ve.render_header_generic(badge, title, subtitle, pill, accent),
        body,
        ve.render_footer_generic(chips, accent, tint, NOTE, label=label),
        "</svg>",
    ])


# --- diagram bodies -----------------------------------------------------------

PATHS = [
    dict(theme=BLUE, glyph=g_wheel, letter="a",
         title="WHL-only (Lightweight)", chip="geobrix[light_env6]",
         branch="Lightweight tier only", badge="RECOMMENDED",
         runs="Serverless · standard access mode · Lakeflow pipelines · classic clusters · ARM",
         install="One wheel with a runtime-pinned extra, from a Volume or the GitHub release",
         extra="No JAR, no init script, no native GDAL bundle",
         tiers=["Lightweight"]),
    dict(theme=TEAL, glyph=g_container, letter="b",
         title="Databricks Container Service", chip="GeoBrix container image",
         branch="Heavyweight · Graviton (arm64)", badge="PREFERRED",
         runs="Dedicated cluster · Graviton (arm64) · DBR 19",
         install="A GeoBrix image (native GDAL + the wheel), plus the JAR as a cluster library",
         extra="Not yet covered: x86 (amd64), DBR 17.3 / 18",
         tiers=["Lightweight", "Heavyweight"]),
    dict(theme=VIOLET, glyph=g_script, letter="c",
         title="Init Script (Legacy)", chip="geobrix-gdal-init.sh",
         branch="Heavyweight · classic x86", badge=None,
         runs="Classic x86 cluster · DBR 17.3 / 18 / 19",
         install="Init script, GDAL tarball + .sha256 sidecar on a Volume; wheel as a cluster library",
         extra="Still supported where the DCS image doesn't reach yet",
         tiers=["Heavyweight"]),
]


def body_paths():
    out = []
    # decision pill
    qw, qh, qy = 640, 44, 140
    qx = (CANVAS_W - qw) / 2
    out.append(f'<rect x="{qx}" y="{qy}" rx="22" ry="22" width="{qw}" height="{qh}" '
               f'fill="{C_INK}"/>')
    out.append(text(CANVAS_W / 2, qy + 28, "Which tier do you need, and on which compute?",
                    size=16, weight=700, fill="#FFFFFF", anchor="middle"))

    n, gap = len(PATHS), 30
    cw = (CANVAS_W - 2 * PAD - (n - 1) * gap) / n
    cy0, ch = 248, 372
    for i, p in enumerate(PATHS):
        a, t = p["theme"]["accent"], p["theme"]["tint"]
        x = PAD + i * (cw + gap)
        mid = x + cw / 2
        # branch arrow + label
        out.append(parrow(qx + qw * (i + 0.5) / n, qy + qh + 2, mid, cy0 - 4, a, width=2.2))
        lw = len(p["branch"]) * 6.9 + 22
        out.append(f'<rect x="{mid - lw / 2 + (i - 1) * 40}" y="{qy + qh + 14}" rx="11" '
                   f'ry="11" width="{lw}" height="22" fill="#FFFFFF" stroke="{a}" '
                   f'stroke-width="1.2"/>')
        out.append(text(mid + (i - 1) * 40, qy + qh + 29, p["branch"], size=12, weight=700,
                        fill=a, anchor="middle"))
        # card
        out.append(panel(x, cy0, cw, ch, accent=a))
        if p["badge"]:
            bw = len(p["badge"]) * 8.4 + 24
            out.append(f'<rect x="{x + cw - bw - 14}" y="{cy0 + 16}" rx="10" ry="10" '
                       f'width="{bw}" height="20" fill="{a}"/>')
            out.append(text(x + cw - bw / 2 - 14, cy0 + 30, p["badge"], size=10.5,
                            weight=800, fill="#FFFFFF", anchor="middle",
                            letter_spacing="1.0"))
        # letter disc
        out.append(f'<circle cx="{x + 32}" cy="{cy0 + 34}" r="15" fill="{a}"/>')
        out.append(text(x + 32, cy0 + 40, p["letter"], size=16, weight=900,
                        fill="#FFFFFF", anchor="middle"))
        out.append(p["glyph"](mid, cy0 + 56, a, t))
        out.append(text(mid, cy0 + 118, p["title"], size=19, weight=800, fill=C_INK,
                        anchor="middle"))
        out.append(centered_chip(mid, cy0 + 130, p["chip"], p["theme"]))
        ly = cy0 + 184
        out.append(label_line(x + 24, ly, "RUNS ON", p["runs"], max_w=cw - 48,
                              size=14, line_h=19))
        out.append(label_line(x + 24, ly + 72, "YOU INSTALL", p["install"], max_w=cw - 48,
                              size=14, line_h=19))
        out.extend(wrap_left(x + 24, ly + 142, cw - 48, p["extra"], size=13, fill=C_MUTED_2))
        tsvg, _ = tier_chips(x + 24, cy0 + ch - 40, p["tiers"])
        out.append(text(x + cw - 24, cy0 + ch - 22, "TIERS", size=10.5, weight=700,
                        fill=C_MUTED_3, anchor="end", letter_spacing="1.2"))
        out.append(tsvg)
    return "".join(out)


def body_dcs():
    out = []
    top, h = 150, 368
    # --- column 1: the image -------------------------------------------------
    x1, w1 = PAD, 420
    out.append(col_label(x1 + 4, top - 8, "1 · CONTAINER IMAGE", C_MUTED_3))
    out.append(panel(x1, top, w1, h, accent=TEAL["accent"], title="GeoBrix image"))
    out.append(centered_chip(x1 + w1 / 2, top + 44, "verified tag  0.5.3-env6-arm", TEAL))
    layers = [
        ("GeoBrix wheel  ·  [light_env6]", "Installed into the image's Python", BLUE),
        ("GDAL 3.11.4 + Java (JNI) bindings", "Native GDAL the heavyweight JAR loads", ORANGE),
        ("databricksruntime/environment:v6-standard-arm", "Databricks environment 6 base (arm64)", TEAL),
    ]
    ly = top + 88
    for title, sub, th in layers:
        a, t = th["accent"], th["tint"]
        out.append(f'<rect x="{x1 + 20}" y="{ly}" rx="9" ry="9" width="{w1 - 40}" '
                   f'height="76" fill="{t}" stroke="{a}" stroke-width="1.4"/>')
        out.append(f'<rect x="{x1 + 20}" y="{ly}" width="6" height="76" rx="3" ry="3" fill="{a}"/>')
        out.append(mono(x1 + 40, ly + 30, title, size=12.5, weight=700, fill=C_INK))
        out.append(text(x1 + 40, ly + 54, sub, size=12.5, fill=C_MUTED))
        ly += 88

    # --- column 2: the dedicated cluster ---------------------------------------
    x2, w2 = 536, 420
    out.append(col_label(x2 + 4, top - 8, "2 · DEDICATED CLUSTER", C_MUTED_3))
    out.append(panel(x2, top, w2, h, accent=TEAL["accent"], title="Dedicated cluster"))
    out.append(g_cluster(x2 + w2 / 2, top + 84, TEAL["accent"], TEAL["tint"]))
    rows = [
        ("ACCESS MODE", "Dedicated  ·  DBR 19  ·  Graviton (arm64)"),
        ("DOCKER IMAGE URL", "The GeoBrix image"),
        ("SPARK CONFIG", "spark.databricks.unityCatalog.volumes.enabled true"),
        ("LIBRARY", "GeoBrix JAR as a cluster library"),
    ]
    ry = top + 150
    for lbl, val in rows:
        out.append(text(x2 + 26, ry, lbl, size=10.5, weight=700, fill=C_MUTED_3,
                        letter_spacing="1.2"))
        if lbl == "SPARK CONFIG":
            out.append(mono(x2 + 26, ry + 19, val, size=11.5, weight=600, fill=C_INK))
        else:
            out.append(text(x2 + 26, ry + 19, val, size=13, weight=600, fill=C_INK))
        ry += 52
    # JAR -> cluster (from below)
    jx, jy = x2 + w2 / 2, top + h + 62
    out.append(f'<rect x="{jx - 150}" y="{jy - 26}" rx="12" ry="12" width="300" height="52" '
               f'fill="#FFFFFF" stroke="{HEAVY_TIER["accent"]}" stroke-width="1.4" '
               f'stroke-dasharray="6 5"/>')
    out.append(g_jar(jx - 112, jy, HEAVY_TIER["accent"], HEAVY_TIER["tint"]))
    out.append(text(jx - 80, jy - 4, "GeoBrix JAR", size=13, weight=800, fill=C_INK))
    out.append(text(jx - 80, jy + 14, "/scala-jars/user is not loaded on dedicated",
                    size=11, fill=C_MUTED))
    out.append(parrow(jx, jy - 28, jx, top + h + 4, HEAVY_TIER["accent"], width=2.2))

    # image -> cluster arrow
    out.append(parrow(x1 + w1 + 6, top + h / 2, x2 - 6, top + h / 2, TEAL["accent"]))
    out.append(text((x1 + w1 + x2) / 2, top + h / 2 - 12, "image", size=11, weight=700,
                    fill=TEAL["accent"], anchor="middle"))

    # --- column 3: tiers that run ----------------------------------------------
    x3, w3 = 1036, CANVAS_W - PAD - 1036
    out.append(col_label(x3 + 4, top - 8, "3 · TIERS THAT RUN", C_MUTED_3))
    tiers = [
        (LIGHT_TIER, "Lightweight", "pyrx · pyvx · pygx and the *_gbx readers",
         "No %pip step: the wheel is in the image"),
        (HEAVY_TIER, "Heavyweight", "rasterx · vectorx · gridx (gbx_* on the JVM)",
         "Read /Volumes with *_gbx readers, then heavy functions"),
    ]
    th_h, ty = 150, top
    for th, name, l1, l2 in tiers:
        a, t = th["accent"], th["tint"]
        out.append(panel(x3, ty, w3, th_h, accent=a))
        out.append(f'<circle cx="{x3 + 34}" cy="{ty + 40}" r="14" fill="{a}"/>')
        out.append(f'<path d="M {x3 + 27} {ty + 40} L {x3 + 32} {ty + 46} L {x3 + 42} {ty + 34}" '
                   f'fill="none" stroke="#FFFFFF" stroke-width="3" stroke-linecap="round" '
                   f'stroke-linejoin="round"/>')
        out.append(text(x3 + 58, ty + 46, name, size=18, weight=800, fill=C_INK))
        out.extend(wrap_left(x3 + 24, ty + 82, w3 - 48, l1, size=13, fill=C_INK))
        out.extend(wrap_left(x3 + 24, ty + 116, w3 - 48, l2, size=12, fill=C_MUTED_2))
        ty += th_h + 24
    out.append(parrow(x2 + w2 + 6, top + th_h / 2, x3 - 6, top + th_h / 2, LIGHT_TIER["accent"]))
    out.append(parrow(x2 + w2 + 6, top + th_h * 1.5 + 24, x3 - 6, top + th_h * 1.5 + 24,
                      HEAVY_TIER["accent"]))

    # --- not-covered strip -----------------------------------------------------
    sx, sy = x3, top + 2 * th_h + 48
    sw, sh = w3, 122
    out.append(f'<rect x="{sx}" y="{sy}" rx="12" ry="12" width="{sw}" height="{sh}" '
               f'fill="#F7F9FB" stroke="{C_MUTED_3}" stroke-width="1.2" stroke-dasharray="6 5"/>')
    out.append(text(sx + 18, sy + 24, "NOT COVERED BY THIS SETUP", size=10.5, weight=700,
                    fill=C_MUTED_2, letter_spacing="1.2"))
    out.extend(wrap_left(sx + 18, sy + 46, sw - 36,
                         "Standard access mode (Beta): lightweight functions only; "
                         "Python DataSource readers fail; no heavyweight tier.",
                         size=12, fill=C_MUTED, line_h=16))
    out.extend(wrap_left(sx + 18, sy + 92, sw - 36,
                         "Not yet covered: x86 (amd64) images and DBR 17.3 / 18.",
                         size=12, fill=C_MUTED, line_h=16))
    return "".join(out)


def body_init_script():
    out = []
    top, h = 150, 386
    # --- column 1: isolated VOL_DIR --------------------------------------------
    x1, w1 = PAD, 470
    out.append(col_label(x1 + 4, top - 8, "1 · STAGE ON A VOLUME", C_MUTED_3))
    out.append(panel(x1, top, w1, h, accent=VIOLET["accent"]))
    out.append(fgf.glyph_volume(x1 + 44, top + 42, VIOLET["accent"], VIOLET["tint"], s=40))
    out.append(text(x1 + 76, top + 38, "VOL_DIR", size=18, weight=800, fill=C_INK))
    out.append(text(x1 + 76, top + 58, "An isolated Unity Catalog Volume folder", size=12.5,
                    fill=C_MUTED))
    files = [
        ("geobrix-gdal-artifacts-v<version>-noble.tar.gz",
         "The GDAL tarball (artifacts or platform: one, not both)"),
        ("geobrix-gdal-artifacts-v<version>-noble.tar.gz.sha256",
         "Its sidecar: the hash the script verifies at start"),
        ("geobrix-<version>-jar-with-dependencies.jar",
         "At most ONE JAR (used with the platform tarball)"),
    ]
    fy = top + 88
    for name, sub in files:
        out.append(f'<rect x="{x1 + 20}" y="{fy}" rx="9" ry="9" width="{w1 - 40}" height="70" '
                   f'fill="{VIOLET["tint"]}" stroke="{VIOLET["accent"]}" stroke-width="1.2"/>')
        out.append(fgf.glyph_file(x1 + 44, fy + 35, VIOLET["accent"], "#FFFFFF", s=34))
        out.append(mono(x1 + 70, fy + 29, name, size=11.5, weight=700, fill=C_INK))
        out.append(text(x1 + 70, fy + 51, sub, size=12, fill=C_MUTED))
        fy += 82
    out.append(text(x1 + w1 / 2, top + h - 20, "Nothing else: never a shared folder of artifacts",
                    size=12.5, weight=700, fill=ROSE["accent"], anchor="middle"))

    # --- column 2: init script ---------------------------------------------------
    x2, w2 = 556, 420
    out.append(col_label(x2 + 4, top - 8, "2 · INIT SCRIPT AT CLUSTER START", C_MUTED_3))
    out.append(panel(x2, top, w2, h, accent=VIOLET["accent"]))
    out.append(g_script(x2 + 66, top + 54, VIOLET["accent"], VIOLET["tint"]))
    out.append(mono(x2 + 126, top + 50, "geobrix-gdal-init.sh", size=15, weight=800, fill=C_INK))
    out.append(text(x2 + 126, top + 70, "Versioned per release; edit VOL_DIR only",
                    size=12, fill=C_MUTED))
    steps = [
        "Check the architecture is amd64 (fails fast on ARM)",
        "Verify the tarball against the .sha256 sidecar",
        "Extract and verify the SHA256SUMS manifest",
        "dpkg -i the bundled GDAL / PROJ / GEOS .debs",
        "pip install the bundled GDAL wheels (no PyPI)",
        "Copy libgdalalljni.so and the JAR into place",
    ]
    sy = top + 116
    for i, s in enumerate(steps, 1):
        out.append(f'<circle cx="{x2 + 36}" cy="{sy - 5}" r="11" fill="{VIOLET["accent"]}"/>')
        out.append(text(x2 + 36, sy - 1, str(i), size=11.5, weight=800, fill="#FFFFFF",
                        anchor="middle"))
        out.append(text(x2 + 58, sy, s, size=12.5, fill=C_INK))
        sy += 42

    # --- column 3: classic x86 cluster -----------------------------------------
    x3, w3 = 1056, CANVAS_W - PAD - 1056
    out.append(col_label(x3 + 4, top - 8, "3 · CLASSIC X86 CLUSTER", C_MUTED_3))
    out.append(panel(x3, top, w3, h, accent=VIOLET["accent"], title="Classic cluster"))
    out.append(g_cluster(x3 + w3 / 2, top + 86, VIOLET["accent"], VIOLET["tint"]))
    rows = [
        ("RUNTIME", "DBR 17.3 / 18 / 19 (Ubuntu 24.04)"),
        ("INSTANCE TYPE", "Intel or AMD x86_64, not ARM"),
        ("LIBRARY", "GeoBrix wheel as a cluster library"),
    ]
    ry = top + 152
    for lbl, val in rows:
        out.append(text(x3 + 24, ry, lbl, size=10.5, weight=700, fill=C_MUTED_3,
                        letter_spacing="1.2"))
        out.append(text(x3 + 24, ry + 19, val, size=13, weight=600, fill=C_INK))
        ry += 52
    tsvg, _ = tier_chips(x3 + 24, top + h - 48, ["Heavyweight"])
    out.append(tsvg)
    out.append(text(x3 + w3 - 24, top + h - 30, "TIER", size=10.5, weight=700,
                    fill=C_MUTED_3, anchor="end", letter_spacing="1.2"))

    # arrows
    out.append(parrow(x1 + w1 + 6, top + h / 2, x2 - 6, top + h / 2, VIOLET["accent"]))
    out.append(parrow(x2 + w2 + 6, top + h / 2, x3 - 6, top + h / 2, VIOLET["accent"]))

    # isolation strip
    out.append(strip(PAD, top + h + 18, CANVAS_W - 2 * PAD, 52, ROSE,
                     "WHY VOL_DIR STAYS ISOLATED",
                     "The script glob-copies every geobrix-*-jar-with-dependencies.jar it finds "
                     "there: a mixed folder gives a nondeterministic classpath."))
    return "".join(out)


DIAGRAMS = {
    "paths": dict(
        badge="GB", theme=BLUE,
        title="Three ways to install GeoBrix",
        subtitle="Pick the path that matches the tier you need and the compute you run on",
        pill="Installation  ·  Paths",
        body=body_paths,
        chips=["geobrix-<version>-py3-none-any.whl",
               "geobrix-<version>-jar-with-dependencies.jar", "geobrix-gdal-init.sh"],
    ),
    "dcs": dict(
        badge="GB", theme=TEAL,
        title="GeoBrix on Databricks Container Service",
        subtitle="The preferred heavyweight packaging: one image carries native GDAL and the wheel",
        pill="Installation  ·  Container Service",
        body=body_dcs,
        chips=["environment:v6-standard-arm", "GDAL 3.11.4", "geobrix[light_env6]",
               "jar-with-dependencies.jar"],
    ),
    "init-script": dict(
        badge="GB", theme=VIOLET,
        title="Init Script (Legacy)",
        subtitle="The older heavyweight route, still supported where the DCS image doesn't reach yet",
        pill="Installation  ·  Init Script",
        body=body_init_script,
        chips=["geobrix-gdal-init.sh", "*-noble.tar.gz", "*.tar.gz.sha256",
               "geobrix-*-jar-with-dependencies.jar"],
    ),
}

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def to_png(svg_path, png_path):
    """Rasterize with headless Chrome at 2x, then crop to the SVG bounds."""
    chrome = CHROME if os.path.exists(CHROME) else shutil.which("google-chrome")
    if not chrome:
        print("headless Chrome not found; skipped PNG for", svg_path)
        return
    subprocess.run([chrome, "--headless", "--disable-gpu", "--hide-scrollbars",
                    "--force-device-scale-factor=2",
                    f"--window-size={CANVAS_W},{CANVAS_H}",
                    f"--screenshot={png_path}", svg_path],
                   check=True, capture_output=True)
    from PIL import Image
    img = Image.open(png_path).convert("RGB")
    img.crop((0, 0, min(img.width, CANVAS_W * 2), min(img.height, CANVAS_H * 2))).save(png_path)
    print(f"wrote {png_path}")


def main(argv):
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, d in DIAGRAMS.items():
        svg = canvas(d["badge"], d["title"], d["subtitle"], d["pill"], d["theme"],
                     d["body"](), d["chips"])
        svg_path = os.path.abspath(os.path.join(OUT_DIR, f"installation-{name}.svg"))
        with open(svg_path, "w") as f:
            f.write(svg)
        print(f"wrote {svg_path}")
        if "--svg-only" not in argv:
            to_png(svg_path, svg_path[:-4] + ".png")


if __name__ == "__main__":
    main(sys.argv[1:])
