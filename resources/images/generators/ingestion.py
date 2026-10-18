#!/usr/bin/env python3
"""Generate the Ingestion Pipelines docs diagrams, one per page.

Pages (docs/docs/ingestion/): overview, lakeflow-connector, lakeflow-sdp,
auto-loader and read-paths. Each diagram is a free-form layout built from the
genie-map primitives (``parrow``) and, through them, the eo-series /
vapor-eyes card, chip, header and footer helpers, so these sit visually
alongside the other GeoBrix diagrams: same palette, fonts, background and
1480x720 canvas.

A route keeps its color on every page: Lakeflow connector = blue, Auto Loader
ingestion (``declare_ingest``) = orange, Lakeflow SDP = teal, ``read_paths`` =
violet, bad records = rose.

Text is laid out with a width estimate and wrapped; a title or paragraph that
would not fit its box raises ``ValueError`` here instead of overflowing in the
PNG. Re-check the PNGs by eye after an edit all the same.

Renders SVGs here; turn them into PNGs with headless Chrome, then crop:

    python3 resources/images/generators/ingestion.py
    for n in overview lakeflow-connector lakeflow-sdp auto-loader read-paths; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1520,760 \
        --screenshot=resources/images/diagrams/ingestion/ingestion-$n.png \
        resources/images/diagrams/ingestion/ingestion-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/ingestion/ingestion-*.png'):
        img = Image.open(p).convert('RGB')
        bg = Image.new('RGB', img.size, (255, 255, 255))
        bbox = ImageChops.difference(img, bg).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
"""
import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_HERE, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gm = _load("genie_map_gen", "genie-map.py")
eo, ve = gm.eo, gm.ve

# Shared primitives / constants
text, chip, esc, top_stripe = eo.text, eo.chip, eo.esc, eo.top_stripe
parrow = gm.parrow
C_INK, C_MUTED, C_MUTED_2, C_MUTED_3, C_BORDER = (
    eo.C_INK,
    eo.C_MUTED,
    eo.C_MUTED_2,
    eo.C_MUTED_3,
    eo.C_BORDER,
)
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD

# One color per route, the same on every page.
BLUE, ORANGE, TEAL, VIOLET, ROSE = gm.BLUE, gm.ORANGE, gm.TEAL, gm.VIOLET, gm.ROSE
CONNECTOR, AUTOLOADER, SDP, READ_PATHS, BAD = BLUE, ORANGE, TEAL, VIOLET, ROSE
SLATE = {"accent": C_MUTED_2, "tint": "#EEF1F4"}

SANS = "Inter, -apple-system, system-ui, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
NOTE = "databrickslabs/geobrix  ·  Ingestion Pipelines"


# --- text measurement and rich (backtick = code) text ------------------------


def _advance(s, size, *, code=False, weight=400):
    """Rough rendered width of ``s``, about 10% over what Chrome draws (measured
    on these PNGs), so an estimate that fits renders."""
    if code:
        return len(s) * size * 0.95 * 0.61
    per = 0.52 if weight < 600 else (0.55 if weight < 800 else 0.56)
    return len(s) * size * per


def _tokens(s):
    """``s`` split into (text, is_code, space_before) tokens. Backticks mark code;
    a code span is one token, so it never wraps."""
    toks, ended_space = [], True
    for i, seg in enumerate(s.split("`")):
        if not seg:
            continue
        if i % 2 == 1:
            toks.append((seg, True, ended_space))
        else:
            for j, piece in enumerate(seg.split(" ")):
                if piece:
                    toks.append((piece, False, j > 0 or ended_space))
        ended_space = seg.endswith(" ")
    return toks


def _words(s):
    """Tokens grouped into wrap units (a token without a space before it sticks)."""
    words = []
    for tok in _tokens(s):
        if words and not tok[2]:
            words[-1].append(tok)
        else:
            words.append([tok])
    return words


def _word_w(word, size, weight):
    return sum(_advance(t, size, code=c, weight=weight) for t, c, _ in word)


def _line_svg(x, y, line, *, size, fill, weight, anchor, code_fill):
    spans = []
    for k, word in enumerate(line):
        if k > 0:
            spans.append("<tspan> </tspan>")
        for tok, code, _ in word:
            if code:
                spans.append(
                    f'<tspan font-family="{MONO}" font-size="{size * 0.95:.1f}" '
                    f'font-weight="600" fill="{code_fill}">{esc(tok)}</tspan>'
                )
            else:
                spans.append(f"<tspan>{esc(tok)}</tspan>")
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="{SANS}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}" '
        f'style="white-space: pre">{"".join(spans)}</text>'
    )


def para(
    x,
    y,
    w,
    s,
    *,
    size=13,
    fill=C_MUTED,
    weight=400,
    line_h=None,
    max_lines=4,
    anchor="start",
    code_fill=C_INK,
):
    """Wrapped rich text; returns (svg, n_lines). Raises if it needs more lines."""
    line_h = line_h or round(size * 1.32)
    lines, cur, cur_w = [], [], 0.0
    space = _advance(" ", size, weight=weight)
    for word in _words(s):
        ww = _word_w(word, size, weight)
        if ww > w:
            raise ValueError(f"word {word!r} is wider than {w}px")
        if cur and cur_w + space + ww > w:
            lines.append(cur)
            cur, cur_w = [word], ww
        else:
            cur_w += (space if cur else 0) + ww
            cur.append(word)
    if cur:
        lines.append(cur)
    if len(lines) > max_lines:
        raise ValueError(f"{s!r} needs {len(lines)} lines in {w}px (max {max_lines})")
    svg = "".join(
        _line_svg(
            x,
            y + i * line_h,
            ln,
            size=size,
            fill=fill,
            weight=weight,
            anchor=anchor,
            code_fill=code_fill,
        )
        for i, ln in enumerate(lines)
    )
    return svg, len(lines)


def fit_text(x, y, w, s, *, size, weight=800, fill=C_INK, mono=False, anchor="start"):
    """A one-line title that must fit ``w``."""
    width = _advance(s, size, code=mono, weight=weight)
    if width > w:
        raise ValueError(f"title {s!r} (~{width:.0f}px) is wider than {w}px")
    if mono:
        return eo.mono(x, y, s, size=size, weight=700, fill=fill, anchor=anchor)
    return text(x, y, s, size=size, weight=weight, fill=fill, anchor=anchor)


# --- boxes ---------------------------------------------------------------------


def box(x, y, w, h, accent, *, dashed=False, muted=False, r=12, stripe=True):
    fill = "#F7F9FB" if muted else "#FFFFFF"
    if dashed:
        border = f'stroke="{accent}" stroke-width="1.8" stroke-dasharray="6 5"'
    else:
        border = f'stroke="{C_BORDER}" stroke-width="1"'
    out = (
        f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
        f'fill="{fill}" {border} filter="url(#card-shadow)"/>'
    )
    if stripe and not dashed:
        out += top_stripe(x, y, w, accent, r=r)
    return out


def tcard(
    x,
    y,
    w,
    h,
    title,
    body="",
    *,
    theme,
    mono_title=False,
    chip_txt=None,
    title_size=17,
    body_size=14,
    max_lines=4,
    align="start",
    muted=False,
    dashed=False,
    pad=16,
):
    """Card: title, optional chip under it, wrapped body. Raises on overflow."""
    a, t = theme["accent"], theme["tint"]
    out = [box(x, y, w, h, a, dashed=dashed, muted=muted)]
    inner = w - 2 * pad
    tx = x + pad if align == "start" else x + w / 2
    ty = y + 28
    out.append(
        fit_text(
            tx,
            ty,
            inner,
            title,
            size=title_size,
            mono=mono_title,
            fill=C_MUTED_2 if muted else C_INK,
            anchor=align,
        )
    )
    by = ty + 21
    if chip_txt:
        cs, cw = chip(0, 0, chip_txt, fg=a, bg=t, mono_font=True)
        if cw > inner:
            raise ValueError(f"chip {chip_txt!r} is wider than {inner}px")
        cx = x + pad if align == "start" else x + (w - cw) / 2
        out.append(f'<g transform="translate({cx:.1f}, {ty + 10})">{cs}</g>')
        by = ty + 10 + 22 + 21
    paras = [body] if isinstance(body, str) else list(body)
    line_h = round(body_size * 1.32)
    bottom = by
    for k, b in enumerate(p for p in paras if p):
        svg, n = para(
            tx,
            by,
            inner,
            b,
            size=body_size,
            max_lines=max_lines,
            anchor=align,
            fill=C_MUTED_3 if muted else C_MUTED,
        )
        out.append(svg)
        bottom = by + (n - 1) * line_h + 5
        by += n * line_h + 10
    if bottom > y + h - 6:
        raise ValueError(f"card {title!r}: body ends at {bottom}, box at {y + h}")
    return "".join(out)


def question(x, y, w, h, s):
    """A decision box: a "?" badge and the question."""
    out = [
        f'<rect x="{x}" y="{y}" rx="{h / 2:.0f}" ry="{h / 2:.0f}" width="{w}" '
        f'height="{h}" fill="#FFFFFF" stroke="{C_MUTED_3}" stroke-width="1.4" '
        f'filter="url(#card-shadow)"/>',
        f'<circle cx="{x + 34}" cy="{y + h / 2}" r="17" fill="{C_INK}"/>',
        text(
            x + 34,
            y + h / 2 + 6,
            "?",
            size=18,
            weight=900,
            fill="#FFFFFF",
            anchor="middle",
        ),
    ]
    size, line_h = 15, 19
    inner = w - 76
    svg, n = para(x + 62, 0, inner, s, size=size, weight=700, fill=C_INK, max_lines=2)
    top = y + h / 2 - (n - 1) * line_h / 2 + 5
    svg, _ = para(
        x + 62,
        top,
        inner,
        s,
        size=size,
        weight=700,
        fill=C_INK,
        line_h=line_h,
        max_lines=2,
    )
    out.append(svg)
    return "".join(out)


def pill(cx, cy, s, color, *, bg="#FFFFFF"):
    """A small label centered on (cx, cy), for arrows."""
    _, w = chip(0, 0, s, mono_font=True)
    svg, _ = chip(cx - w / 2, cy - 11, s, fg=color, bg=bg, border=color, mono_font=True)
    return svg


def elbow(points, color, *, dash=None):
    """Polyline arrow through ``points``; the head is on the last segment."""
    d = f' stroke-dasharray="{dash}"' if dash else ""
    out = []
    for (x1, y1), (x2, y2) in zip(points[:-2], points[1:-1]):
        out.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="2.4" stroke-linecap="round"{d}/>'
        )
    (x1, y1), (x2, y2) = points[-2], points[-1]
    out.append(parrow(x1, y1, x2, y2, color, dash=dash))
    return "".join(out)


def col_label(x, y, s, color=C_MUTED_3, anchor="start"):
    return text(
        x, y, s, size=12, weight=700, fill=color, letter_spacing="1.4", anchor=anchor
    )


def g_folder(x, y, color, tint, *, w=26, h=20):
    """A folder icon with its top-left corner at (x, y)."""
    return (
        f'<path d="M {x} {y + 3} Q {x} {y} {x + 3} {y} H {x + w * 0.38} '
        f"L {x + w * 0.5} {y + 4} H {x + w - 3} Q {x + w} {y + 4} {x + w} {y + 7} "
        f"V {y + h - 3} Q {x + w} {y + h} {x + w - 3} {y + h} H {x + 3} "
        f'Q {x} {y + h} {x} {y + h - 3} Z" fill="{tint}" stroke="{color}" '
        f'stroke-width="1.8" stroke-linejoin="round"/>'
    )


def g_file(x, y, color, tint, *, w=18, h=22):
    """A page icon with a folded corner, top-left corner at (x, y)."""
    f = 6
    return (
        f'<path d="M {x} {y} H {x + w - f} L {x + w} {y + f} V {y + h} H {x} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="1.8" stroke-linejoin="round"/>'
        f'<path d="M {x + w - f} {y} V {y + f} H {x + w}" fill="none" '
        f'stroke="{color}" stroke-width="1.4"/>'
    )


# --- canvas --------------------------------------------------------------------


def _footer_fits(chips, note, label):
    used = PAD + 130 + sum(int(len(c) * 7.0) + 24 + 8 for c in chips)
    note_x = CANVAS_W - PAD - len(note) * 11 * 0.56
    if used + 24 > note_x:
        raise ValueError(f"footer chips {chips} run into the note {note!r}")


def canvas(title, subtitle, page, theme, body, chips, note=NOTE, label="KEY FUNCTIONS"):
    _footer_fits(chips, note, label)
    series = f"Ingestion Pipelines  ·  {page}"
    if (
        _advance(title, 28, weight=800)
        > CANVAS_W - 2 * PAD - 78 - len(series) * 6.6 - 48
    ):
        raise ValueError(f"header title {title!r} runs into the series pill")
    accent, tint = theme["accent"], theme["tint"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: {SANS};">',
        "<defs>"
        '<filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" '
        'flood-opacity="0.08"/></filter>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0" stop-color="#FAFBFC"/>'
        '<stop offset="1" stop-color="#F1F4F8"/></linearGradient></defs>',
        f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>',
        ve.render_header_generic("IP", title, subtitle, series, accent),
        body,
        ve.render_footer_generic(chips, accent, tint, note, label=label),
        "</svg>",
    ]
    return "\n".join(parts)


# --- diagram bodies ------------------------------------------------------------


def body_overview():
    """The route chooser: three questions, four routes, connector first."""
    out = []
    rows, rh = [168, 284, 400, 516], 100
    centers = [r + rh / 2 for r in rows]
    # Start, and what every route gives you.
    out.append(
        tcard(
            36,
            168,
            250,
            100,
            "New files on a Volume",
            "vector, LiDAR, photos, rasters, NetCDF, PMTiles",
            theme=SLATE,
            max_lines=2,
        )
    )
    ex, ey, ew, eh = 36, 286, 250, 330
    out.append(box(ex, ey, ew, eh, C_MUTED_2))
    out.append(fit_text(ex + 16, ey + 32, ew - 32, "Every route", size=17))
    bullets = [
        "One row per feature, point, image or tile",
        "Lineage on every row: `_source_path`, `_source_mtime`",
        "The same light readers and formats",
        "Pure Python: no JAR, runs on Serverless",
    ]
    by = ey + 64
    for b in bullets:
        out.append(f'<circle cx="{ex + 21}" cy="{by - 5}" r="3.4" fill="{C_MUTED_2}"/>')
        svg, n = para(ex + 32, by, ew - 48, b, size=14, max_lines=3)
        out.append(svg)
        by += n * 18 + 14
    if by - 14 > ey + eh - 8:
        raise ValueError("'Every route' panel overflows")

    # Questions (left) and routes (right).
    qx, qw, qh = 326, 430, 80
    rx, rw = 846, 598
    q1y, q2y = centers[0] - qh / 2, centers[1] - qh / 2
    q3y = (centers[2] + centers[3]) / 2 - qh / 2
    out.append(
        question(
            qx,
            q1y,
            qw,
            qh,
            "Does an admin govern the folder, and is appending new " "files enough?",
        )
    )
    out.append(
        question(
            qx,
            q2y,
            qw,
            qh,
            "Files overwritten? Need history, quarantine, or very " "large folders?",
        )
    )
    out.append(question(qx, q3y, qw, qh, "Do you build your own Lakeflow pipeline?"))
    out.append(parrow(286 + 2, centers[0], qx - 4, centers[0], C_MUTED_2))

    routes = [
        (
            CONNECTOR,
            "Lakeflow connector",
            "geobrix",
            "A Unity Catalog connection to the folder, then one entry per table in "
            "a pipeline source file. Append-only.",
        ),
        (
            AUTOLOADER,
            "Auto Loader ingestion",
            "declare_ingest",
            "One call in a Lakeflow pipeline: `scd2`, `replace` or `append` for "
            "overwritten files, and bad records set aside.",
        ),
        (
            SDP,
            "Lakeflow SDP pipeline",
            "binaryFile → read_paths",
            "Bronze: an Auto Loader inventory. Silver: `read_paths` reads the "
            "listed files. As in the production examples.",
        ),
        (
            READ_PATHS,
            "read_paths",
            "notebooks · jobs",
            "You write the file listing or the stream, and where the rows go.",
        ),
    ]
    for (theme, title, chip_txt, body), y in zip(routes, rows):
        a, t = theme["accent"], theme["tint"]
        out.append(box(rx, y, rw, rh, a))
        out.append(fit_text(rx + 20, y + 42, 250, title, size=18))
        cs, cw = chip(0, 0, chip_txt, fg=a, bg=t, mono_font=True, h=24)
        if cw > 250:
            raise ValueError(f"chip {chip_txt!r} too wide")
        out.append(f'<g transform="translate({rx + 20}, {y + 56})">{cs}</g>')
        svg, n = para(rx + 286, 0, rw - 306, body, size=14, max_lines=4)
        top = y + rh / 2 - (n - 1) * 19 / 2 + 5
        svg, _ = para(rx + 286, top, rw - 306, body, size=14, line_h=19, max_lines=4)
        out.append(svg)

    # yes / no arrows
    yes, no = "#2F8F5B", C_MUTED_2
    qr = qx + qw
    out.append(parrow(qr + 2, centers[0], rx - 4, centers[0], CONNECTOR["accent"]))
    out.append(pill((qr + rx) / 2, centers[0], "yes", yes))
    out.append(parrow(qr + 2, centers[1], rx - 4, centers[1], AUTOLOADER["accent"]))
    out.append(pill((qr + rx) / 2, centers[1], "yes", yes))
    q3c = q3y + qh / 2
    out.append(parrow(qr + 2, q3c - 8, rx - 4, centers[2], SDP["accent"]))
    out.append(pill((qr + rx) / 2, (q3c - 8 + centers[2]) / 2, "yes", yes))
    out.append(parrow(qr + 2, q3c + 8, rx - 4, centers[3], READ_PATHS["accent"]))
    out.append(pill((qr + rx) / 2, (q3c + 8 + centers[3]) / 2, "no", no))
    mid = qx + qw / 2
    out.append(parrow(mid, q1y + qh + 2, mid, q2y - 4, no))
    out.append(pill(mid + 34, (q1y + qh + q2y) / 2, "no", no))
    out.append(parrow(mid, q2y + qh + 2, mid, q3y - 4, no))
    out.append(pill(mid + 34, (q2y + qh + q3y) / 2, "no", no))
    return "".join(out)


def body_connector():
    """Volume folder -> connection -> one table per entry -> reader -> tables."""
    out = []
    A, B, C, D, E = (36, 290), (376, 230), (656, 220), (916, 230), (1186, 258)
    out.append(col_label(A[0] + 4, 164, "LANDING FOLDER"))
    out.append(col_label(B[0] + 4, 164, "CONNECTION"))
    out.append(col_label(C[0] + 4, 164, "ONE TABLE PER ENTRY"))
    out.append(col_label(D[0] + 4, 164, "READER"))
    out.append(col_label(E[0] + 4, 164, "APPEND-ONLY TABLES"))
    a, t = CONNECTOR["accent"], CONNECTOR["tint"]
    rows, rh = [204, 300, 396, 492], 84
    cy = [r + rh / 2 for r in rows]

    # A: the folder, one entry per row.
    ay, ah = 180, 396
    out.append(box(A[0], ay, A[1], ah, a))
    out.append(g_folder(A[0] + 16, ay + 14, a, t, w=24, h=18))
    out.append(
        fit_text(
            A[0] + 48,
            ay + 29,
            A[1] - 64,
            "/Volumes/main/geo/landing",
            size=14,
            mono=True,
        )
    )
    entries = [
        ("boroughs/", "2024.geojson, 2025.geojson", True, False),
        ("Zones 2024.gpkg", "one file, one dataset", False, False),
        ("scenes/", "a.tif, b.tif", True, False),
        ("_archive/", "ignored: starts with _", True, True),
    ]
    tree_x = A[0] + 28
    out.append(
        f'<line x1="{tree_x}" y1="{ay + 36}" x2="{tree_x}" y2="{cy[3]}" '
        f'stroke="{C_MUTED_3}" stroke-width="1.4"/>'
    )
    for (name, sub, folder, ignored), c in zip(entries, cy):
        color = C_MUTED_3 if ignored else a
        tint = "#EEF1F4" if ignored else t
        out.append(
            f'<line x1="{tree_x}" y1="{c}" x2="{tree_x + 14}" y2="{c}" '
            f'stroke="{C_MUTED_3}" stroke-width="1.4"/>'
        )
        if folder:
            out.append(g_folder(tree_x + 18, c - 9, color, tint, w=22, h=17))
        else:
            out.append(g_file(tree_x + 20, c - 11, color, tint))
        out.append(
            fit_text(
                tree_x + 48,
                c - 2,
                A[1] - 92,
                name,
                size=15,
                mono=True,
                fill=C_MUTED_3 if ignored else C_INK,
            )
        )
        svg, _ = para(
            tree_x + 48,
            c + 19,
            A[1] - 92,
            sub,
            size=13,
            fill=C_MUTED_3 if ignored else C_MUTED_2,
            max_lines=1,
        )
        out.append(svg)

    # B: the connection.
    by, bh = rows[0], rows[3] + rh - rows[0]
    out.append(
        tcard(
            B[0],
            by,
            B[1],
            bh,
            "UC connection",
            [
                "`root_path` is the landing folder: each folder and each recognized "
                "file directly under it is a table, and every table path must stay "
                "inside it.",
                "`externalOptionsAllowList` names the table options a pipeline may "
                "pass.",
                "`USE CONNECTION` decides who builds pipelines on it.",
            ],
            theme=CONNECTOR,
            chip_txt="COMMUNITY · geobrix",
            max_lines=8,
        )
    )
    out.append(parrow(A[0] + A[1] + 2, by + bh / 2, B[0] - 4, by + bh / 2, a))

    # C, D, E per entry.
    tables = [
        (
            "boroughs",
            "a folder: its files, read recursively",
            "geojson",
            "inferred from `.geojson`",
            "features, appended",
        ),
        (
            "zones_2024",
            "named after the file, in lower case",
            "gpkg",
            "inferred from `.gpkg`",
            "features, appended",
        ),
        (
            "scenes",
            "a folder of rasters",
            "cog",
            "`format: cog` (a `.tif` infers `gtiff`)",
            "raster tiles, appended",
        ),
    ]
    for (name, why, reader, how, rows_txt), y, c in zip(tables, rows, cy):
        out.append(parrow(B[0] + B[1] + 2, c, C[0] - 4, c, a))
        out.append(
            tcard(
                C[0],
                y,
                C[1],
                rh,
                name,
                why,
                theme=CONNECTOR,
                mono_title=True,
                max_lines=2,
            )
        )
        out.append(parrow(C[0] + C[1] + 2, c, D[0] - 4, c, a))
        out.append(
            tcard(
                D[0],
                y,
                D[1],
                rh,
                reader,
                how,
                theme=CONNECTOR,
                mono_title=True,
                max_lines=2,
            )
        )
        out.append(parrow(D[0] + D[1] + 2, c, E[0] - 4, c, a))
        out.append(
            tcard(
                E[0],
                y,
                E[1],
                rh,
                name,
                rows_txt,
                theme=CONNECTOR,
                mono_title=True,
                max_lines=2,
            )
        )
    out.append(
        tcard(
            C[0],
            rows[3],
            C[1],
            rh,
            "no table",
            "`_` and `.` names are ignored",
            theme=SLATE,
            muted=True,
            dashed=True,
            max_lines=2,
        )
    )
    out.append(
        tcard(
            E[0],
            rows[3],
            E[1],
            rh,
            "Every row",
            "the reader's columns + `_source_path`, `_source_mtime`",
            theme=SLATE,
            muted=True,
            max_lines=2,
        )
    )

    svg, _ = para(
        CANVAS_W / 2,
        606,
        CANVAS_W - 2 * PAD,
        "Each update appends the rows of the files that are new since the last one: "
        "an overwritten file is appended again, an unreadable file fails the update.",
        size=14,
        fill=C_MUTED,
        anchor="middle",
        max_lines=1,
    )
    out.append(svg)
    return "".join(out)


def body_sdp():
    """Bronze inventory -> read_paths -> gold, and the two production examples."""
    out = []
    L, LAND, BR, SI, GO = (36, 150), (206, 220), (466, 290), (796, 330), (1166, 278)
    out.append(col_label(LAND[0] + 4, 164, "LANDING"))
    out.append(col_label(BR[0] + 4, 164, "BRONZE  ·  LIST THE FILES"))
    out.append(col_label(SI[0] + 4, 164, "SILVER  ·  READ THE FILES"))
    out.append(col_label(GO[0] + 4, 164, "GOLD"))
    a = SDP["accent"]
    lanes = [
        (
            "The pattern",
            "",
            180,
            130,
            [
                ("Volume folder", "new files land under `/Volumes/...`", None, False),
                (
                    "Streaming table",
                    "lists each new file: its path and modification time, not its "
                    "bytes",
                    "cloudFiles · binaryFile",
                    False,
                ),
                (
                    "read_paths",
                    "reads the listed files with a light reader: rows + lineage",
                    "read_paths(inventory, format)",
                    True,
                ),
                (
                    "Materialized views",
                    "aggregates, H3 grids, map-ready tables",
                    None,
                    False,
                ),
            ],
        ),
        (
            "Wireless Coverage",
            "USGS 3DEP LiDAR",
            326,
            122,
            [
                ("LiDAR tiles", "staged `.laz` files on a Volume", None, False),
                ("laz_inventory", "streaming table over the `.laz` files", None, True),
                (
                    "wc_lidar_pts",
                    "streaming table: each update reads only newly inventoried tiles "
                    "with `read_paths`",
                    None,
                    True,
                ),
                (
                    "H3 grids + viewsheds",
                    "materialized views over the surfaces; a job task after the "
                    "pipeline adds tower viewsheds",
                    None,
                    False,
                ),
            ],
        ),
        (
            "Vapor-Eyes",
            "methane detection",
            464,
            122,
            [
                (
                    "Five raw sources",
                    "satellite products and well records",
                    None,
                    False,
                ),
                (
                    "Bronze inventories",
                    "one Auto Loader streaming table per source",
                    None,
                    False,
                ),
                (
                    "Detection + attribution",
                    "materialized views: `read_paths` reads the inventoried satellite "
                    "files",
                    None,
                    False,
                ),
                (
                    "Map-ready gold",
                    "tables, PMTiles and an AI/BI dashboard",
                    None,
                    False,
                ),
            ],
        ),
    ]
    cols = [LAND, BR, SI, GO]
    for lane, sub, y, h, cards in lanes:
        hero = not sub
        tag_fill = a if hero else "#FFFFFF"
        out.append(
            f'<rect x="{L[0]}" y="{y}" rx="12" ry="12" width="{L[1]}" height="{h}" '
            f'fill="{tag_fill}" stroke="{a}" stroke-width="1.4"/>'
        )
        if hero:
            svg, _ = para(
                L[0] + L[1] / 2,
                y + h / 2 + 6,
                L[1] - 20,
                lane,
                size=17,
                weight=800,
                fill="#FFFFFF",
                anchor="middle",
                max_lines=1,
            )
            out.append(svg)
        else:
            svg, n = para(
                L[0] + L[1] / 2,
                y + h / 2 - 6,
                L[1] - 20,
                lane,
                size=16,
                weight=800,
                fill=C_INK,
                anchor="middle",
                max_lines=2,
                line_h=20,
            )
            out.append(svg)
            svg, _ = para(
                L[0] + L[1] / 2,
                y + h / 2 + 16 + (n - 1) * 20,
                L[1] - 16,
                sub,
                size=13,
                fill=a,
                anchor="middle",
                max_lines=1,
            )
            out.append(svg)
        for (title, body, chip_txt, mono_title), (cx, cw) in zip(cards, cols):
            out.append(
                tcard(
                    cx,
                    y,
                    cw,
                    h,
                    title,
                    body,
                    theme=SDP,
                    chip_txt=chip_txt,
                    mono_title=mono_title,
                    max_lines=3,
                )
            )
        for (x1, w1), (x2, _) in zip(cols[:-1], cols[1:]):
            out.append(parrow(x1 + w1 + 2, y + h / 2, x2 - 4, y + h / 2, a))

    svg, _ = para(
        CANVAS_W / 2,
        612,
        CANVAS_W - 2 * PAD,
        "A streaming table over Auto Loader reads only the new files at each update; "
        "a materialized view that calls GeoBrix Python UDFs is recomputed in full.",
        size=14,
        fill=C_MUTED,
        anchor="middle",
        max_lines=1,
    )
    out.append(svg)
    return "".join(out)


def body_auto_loader():
    """declare_ingest: the stream, the change handling and the side tables."""
    out = []
    A, B, C, D, E = (36, 180), (246, 200), (486, 270), (796, 300), (1136, 308)
    b1, b2, b3, b4 = (164, 128), (316, 92), (430, 92), (544, 80)
    a, bad = AUTOLOADER["accent"], BAD["accent"]

    def mid(band):
        return band[0] + band[1] / 2

    def bottom(band):
        return band[0] + band[1]

    out.append(
        tcard(
            A[0],
            b1[0],
            A[1],
            b1[1],
            "Landing folder",
            "`source_dir` on a Volume; `glob` picks the files",
            theme=AUTOLOADER,
        )
    )
    out.append(
        tcard(
            B[0],
            b1[0],
            B[1],
            b1[1],
            "Auto Loader",
            "`cloudFiles` `binaryFile` stream; an overwritten file is "
            "delivered again",
            theme=AUTOLOADER,
        )
    )
    out.append(
        tcard(
            C[0],
            b1[0],
            C[1],
            b1[1],
            "<t>_file_events",
            "`scd2`, `replace`: one row per delivery, with the SHA-256 "
            "`_source_hash`",
            theme=AUTOLOADER,
            mono_title=True,
        )
    )
    out.append(
        tcard(
            D[0],
            b1[0],
            D[1],
            b1[1],
            "foreach_batch_sink",
            "skips content already stored, and reads the newest delivery "
            "of each file with `read_paths`",
            theme=AUTOLOADER,
            mono_title=True,
        )
    )
    out.append(
        tcard(
            E[0],
            b1[0],
            E[1],
            b1[1],
            "target",
            "the rows. `scd2`: each new version is appended. `replace`: an "
            "atomic `replaceWhere` swaps a file's rows",
            theme=AUTOLOADER,
            mono_title=True,
        )
    )
    for (x1, w1), (x2, _) in zip([A, B, C, D], [B, C, D, E]):
        out.append(parrow(x1 + w1 + 2, mid(b1), x2 - 4, mid(b1), a))

    # Quarantine: staged, bad records first.
    out.append(
        tcard(
            D[0],
            b2[0],
            D[1],
            b2[1],
            "Scratch staging",
            "`quarantine`: each micro-batch is read once, into " "`scratch_dir`",
            theme=BAD,
            max_lines=2,
        )
    )
    out.append(
        tcard(
            E[0],
            b2[0],
            E[1],
            b2[1],
            "<t>_bad_records",
            "what cannot be read, appended before the rows",
            theme=BAD,
            mono_title=True,
            max_lines=2,
        )
    )
    dx = D[0] + D[1] / 2
    out.append(parrow(dx, bottom(b1) + 2, dx, b2[0] - 4, bad))
    out.append(parrow(D[0] + D[1] + 2, mid(b2), E[0] - 4, mid(b2), bad))

    # History (scd2).
    out.append(
        tcard(
            C[0],
            b3[0],
            C[1],
            b3[1],
            "<t>_file_versions",
            "`scd2`: AUTO CDC SCD type 2, history on `_source_hash`",
            theme=AUTOLOADER,
            mono_title=True,
            max_lines=2,
        )
    )
    out.append(
        tcard(
            E[0],
            b3[0],
            E[1],
            b3[1],
            "<target>_current",
            "`scd2`: a view of each file's latest readable version",
            theme=AUTOLOADER,
            mono_title=True,
            max_lines=2,
        )
    )
    cx = C[0] + C[1] / 2
    out.append(parrow(cx, bottom(b1) + 2, cx, b3[0] - 4, a))
    out.append(pill(cx, (bottom(b1) + b3[0]) / 2, "scd2", a))
    out.append(parrow(C[0] + C[1] + 2, mid(b3), E[0] - 4, mid(b3), a, dash="6 5"))
    svg, _ = para(
        D[0] + D[1] / 2,
        mid(b3) - 12,
        E[0] - C[0] - C[1] - 16,
        "joined with the rows of `target`",
        size=13,
        fill=a,
        anchor="middle",
        max_lines=1,
        code_fill=a,
    )
    out.append(svg)

    # append: a plain streaming table.
    out.append(
        tcard(
            C[0],
            b4[0],
            E[0] + E[1] - C[0],
            b4[1],
            "target  (append)",
            "a plain streaming table: `read_paths` over the Auto Loader "
            "stream appends every delivery. No hash and no bad-records "
            "table: bad records are skipped with a warning.",
            theme=AUTOLOADER,
            mono_title=True,
            max_lines=2,
        )
    )
    bx = B[0] + B[1] / 2
    out.append(elbow([(bx, bottom(b1) + 2), (bx, mid(b4)), (C[0] - 4, mid(b4))], a))
    out.append(pill(bx, (bottom(b1) + mid(b4)) / 2, "append", a))

    # on_change key.
    ky, kh = b2[0], bottom(b3) - b2[0]
    out.append(box(A[0], ky, A[1], kh, C_MUTED_2))
    out.append(fit_text(A[0] + 14, ky + 30, A[1] - 28, "on_change", size=16, mono=True))
    yy = ky + 58
    for line in (
        "`scd2` (default): every content version",
        "`replace`: latest content only",
        "`append`: every delivery",
    ):
        svg, n = para(A[0] + 14, yy, A[1] - 28, line, size=13.5, max_lines=2)
        out.append(svg)
        yy += n * 18 + 12
    if yy - 12 > ky + kh - 6:
        raise ValueError("on_change key overflows")
    return "".join(out)


def body_read_paths():
    """A paths DataFrame -> one light reader per file -> rows with lineage."""
    out = []
    a, t = READ_PATHS["accent"], READ_PATHS["tint"]
    # A: the input.
    ax, ay, aw, ah = 36, 168, 250, 304
    out.append(box(ax, ay, aw, ah, a))
    out.append(fit_text(ax + 16, ay + 32, aw - 32, "Paths DataFrame", size=18))
    svg, _ = para(ax + 16, ay + 56, aw - 32, "one row per file", size=14, max_lines=1)
    out.append(svg)
    for i, (head, body) in enumerate(
        [
            ("Batch", "a `binaryFile` or file listing"),
            ("Stream", "an Auto Loader stream, with `schema_path`"),
        ]
    ):
        py = ay + 74 + i * 84
        out.append(
            f'<rect x="{ax + 14}" y="{py}" rx="9" ry="9" width="{aw - 28}" '
            f'height="74" fill="{t}" stroke="{a}" stroke-width="1.2"/>'
        )
        out.append(fit_text(ax + 28, py + 24, aw - 56, head, size=15))
        svg, _ = para(ax + 28, py + 46, aw - 56, body, size=13.5, max_lines=2)
        out.append(svg)
    cx = ax + 16
    for c in ("path", "modificationTime"):
        cs, cw = chip(cx, ay + ah - 40, c, fg=a, bg=t, mono_font=True, h=24)
        out.append(cs)
        cx += cw + 8

    # Readers: one task per file, or plan + read.
    ux, uy, uw, uh = 336, 168, 500, 112
    out.append(
        tcard(
            ux,
            uy,
            uw,
            uh,
            "Vector · LiDAR · EXIF · NetCDF",
            "one task reads each file whole; `num_partitions` spreads the "
            "files across tasks",
            theme=READ_PATHS,
            max_lines=2,
        )
    )
    out.append(col_label(ux + 4, 310, "RASTER · PMTILES: TWO STAGES", a))
    ly, lh = 324, 148
    out.append(
        tcard(
            336,
            ly,
            230,
            lh,
            "1 · Plan",
            "raster: each file's tile windows (`tileSize`); PMTiles: "
            "groups of tiles",
            theme=READ_PATHS,
        )
    )
    out.append(
        tcard(
            606,
            ly,
            230,
            lh,
            "2 · Read",
            "spread across tasks with `num_partitions`, each window read " "on its own",
            theme=READ_PATHS,
        )
    )
    out.append(parrow(566 + 2, ly + lh / 2, 606 - 4, ly + lh / 2, a))
    out.append(parrow(ax + aw + 2, ay + ah / 2 - 30, ux - 4, uy + uh / 2, a))
    out.append(parrow(ax + aw + 2, ay + ah / 2 + 30, 336 - 4, ly + lh / 2, a))

    # on_error.
    oy, oh = 506, 104
    out.append(col_label(ax + 4, oy + 26, "WHAT CANNOT BE READ", BAD["accent"]))
    svg, _ = para(
        ax + 4,
        oy + 50,
        aw - 8,
        "`on_error` decides: it fails the read, or it is skipped",
        size=14,
        max_lines=3,
    )
    out.append(svg)
    out.append(
        tcard(
            336,
            oy,
            240,
            oh,
            'on_error="fail"',
            "the default: an error fails the read, naming the file",
            theme=BAD,
            mono_title=True,
            title_size=15,
            max_lines=3,
        )
    )
    out.append(
        tcard(
            596,
            oy,
            240,
            oh,
            'on_error="skip"',
            "bad records are skipped with a warning; the rest is read",
            theme=BAD,
            mono_title=True,
            title_size=15,
            max_lines=3,
        )
    )

    # Rows with lineage.
    rx, ry, rw, rh = 886, 168, 558, 304
    out.append(box(rx, ry, rw, rh, a))
    out.append(fit_text(rx + 18, ry + 36, rw - 36, "Rows", size=19))
    svg, _ = para(
        rx + 18,
        ry + 62,
        rw - 36,
        "the reader's columns, one row per feature, point, image or tile, "
        "plus two lineage columns",
        size=14,
        max_lines=2,
    )
    out.append(svg)
    tx, ty = rx + 18, ry + 120
    for dx, h in [(0, "column"), (176, "type"), (296, "meaning")]:
        out.append(col_label(tx + dx, ty, h.upper()))
    out.append(
        f'<line x1="{tx}" y1="{ty + 10}" x2="{rx + rw - 18}" y2="{ty + 10}" '
        f'stroke="{C_BORDER}" stroke-width="1.4"/>'
    )
    lineage = [
        ("_source_path", "STRING", "the file, as a bare `/Volumes/...` path"),
        ("_source_mtime", "TIMESTAMP", "its modification time"),
    ]
    for i, (col, typ, meaning) in enumerate(lineage):
        yy = ty + 40 + i * 54
        out.append(fit_text(tx, yy, 168, col, size=15, mono=True, fill=a))
        out.append(fit_text(tx + 176, yy, 112, typ, size=14, mono=True, fill=C_MUTED))
        svg, _ = para(tx + 296, yy, rw - 36 - 296, meaning, size=14, max_lines=2)
        out.append(svg)
    svg, _ = para(
        tx,
        ry + rh - 22,
        rw - 36,
        "`lineage=False` leaves them out.",
        size=13.5,
        fill=C_MUTED_2,
        max_lines=1,
    )
    out.append(svg)
    out.append(parrow(ux + uw + 2, uy + uh / 2, rx - 4, uy + uh / 2, a))
    out.append(parrow(606 + 230 + 2, ly + lh / 2, rx - 4, ly + lh / 2, a))

    out.append(
        tcard(
            rx,
            oy,
            rw,
            oh,
            "Where the rows go is yours",
            "write a table, or return them from a pipeline table. "
            "`declare_ingest` and the connector read files with the same "
            "code.",
            theme=SLATE,
            max_lines=3,
        )
    )
    return "".join(out)


DIAGRAMS = {
    "overview": dict(
        title="Choose an ingestion route",
        subtitle="Keep tables in sync with a landing folder: from a governed "
        "connector to your own notebook code",
        page="Overview",
        theme=CONNECTOR,
        body=body_overview,
        chips=["geobrix", "declare_ingest", "cloudFiles", "read_paths"],
    ),
    "lakeflow-connector": dict(
        title="Lakeflow connector: a folder becomes tables",
        subtitle="One Unity Catalog connection to a landing folder; every update "
        "appends the rows of new files",
        page="Lakeflow connector",
        theme=CONNECTOR,
        body=body_connector,
        chips=["root_path", "path", "format", "file_glob", "layer", "mode"],
        label="KEY OPTIONS",
    ),
    "lakeflow-sdp": dict(
        title="Lakeflow SDP: inventory, read, build",
        subtitle="Bronze lists the files, silver reads them with read_paths, gold "
        "builds the products, as in the two production examples",
        page="Lakeflow SDP",
        theme=SDP,
        body=body_sdp,
        chips=[
            "cloudFiles",
            "binaryFile",
            "read_paths",
            "@dp.table",
            "@dp.materialized_view",
        ],
    ),
    "auto-loader": dict(
        title="declare_ingest: Auto Loader ingestion",
        subtitle="One call declares the file stream, what an overwritten file does, "
        "and where bad records go",
        page="Auto Loader ingestion",
        theme=AUTOLOADER,
        body=body_auto_loader,
        chips=[
            "declare_ingest",
            "cloudFiles",
            "foreach_batch_sink",
            "create_auto_cdc_flow",
        ],
    ),
    "read-paths": dict(
        title="read_paths: one light reader per file",
        subtitle="Paths in (batch or stream), rows with lineage out; rasters and "
        "PMTiles are planned first, then read in parallel",
        page="read_paths",
        theme=READ_PATHS,
        body=body_read_paths,
        chips=["read_paths", "schema_path", "num_partitions", "on_error", "lineage"],
    ),
}


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "ingestion")
    os.makedirs(out_dir, exist_ok=True)
    for name, d in DIAGRAMS.items():
        svg = canvas(
            d["title"],
            d["subtitle"],
            d["page"],
            d["theme"],
            d["body"](),
            d["chips"],
            label=d.get("label", "KEY FUNCTIONS"),
        )
        path = os.path.join(out_dir, f"ingestion-{name}.svg")
        with open(path, "w") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
