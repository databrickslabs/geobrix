#!/usr/bin/env python3
"""Generate the diagrams of the Formats / Zarr page (docs/docs/formats/zarr.mdx).

Five diagrams, each an SVG plus a PNG under resources/images/diagrams/zarr/:

    store-kinds          the four kinds of Zarr store and where their bytes and rows live
    write-commit         a write in two phases: files first, then one atomic commit
    concurrent-writers   three ways two writers meet: disjoint, shared metadata, same chunk
    retention-timeline   which files a vacuum keeps and which it deletes, over time
    chunk-vs-shard-rows  chunk rows over a shard row, and what optimize folds back

Re-render after editing (the PNGs come from headless Chrome and are auto-cropped):

    python3 resources/images/generators/zarr-diagrams.py

Set CHROME to the browser binary if it is not at the macOS default; --svg-only skips the PNGs.
"""

import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.normpath(os.path.join(HERE, "..", "diagrams", "zarr"))
CHROME = os.environ.get(
    "CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
)

# --- Palette (shared with the other GeoBrix diagrams) -----------------------------------

INK, INK2 = "#0F1B2A", "#1B3139"
MUTED, MUTED2, MUTED3 = "#3F4D5E", "#5A6878", "#7A8794"
BORDER = "#E5E7EB"
# What things are, never reused for anything else:
BASE, BASE_T = "#5A6878", "#EEF1F5"  # a store's own objects (never modified)
PACK, PACK_T = "#1F6FB5", "#E3EEF8"  # immutable pack files GeoBrix writes
TABLE, TABLE_T = "#7A5AA6", "#ECE5F5"  # the manifest, a Delta table
OK, OK_T = "#2F8F4E", "#DCEFE0"  # committed, visible, live
BAD, BAD_T = "#D2441F", "#FCE9E2"  # conflict, failure, deleted
KEEP, KEEP_T = "#0F8E8B", "#E4F3F2"  # tagged, protected
WAIT, WAIT_T = "#B7791F", "#FBF1DC"  # in flight, recent

SANS = "Inter, -apple-system, system-ui, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"


# --- SVG helpers ------------------------------------------------------------------------


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(
    x, y, s, *, size=13, weight=400, fill=INK, anchor="start", family=SANS, ls=None
):
    extra = f' letter-spacing="{ls}"' if ls else ""
    return (
        f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{extra}>{esc(s)}</text>'
    )


def mono(x, y, s, **kw):
    kw.setdefault("size", 12)
    kw.setdefault("weight", 500)
    return text(x, y, s, family=MONO, **kw)


def wrap(s, width_chars):
    """Greedy word wrap on an average glyph width; returns the lines."""
    lines, cur = [], ""
    for word in s.split():
        if cur and len(cur) + 1 + len(word) > width_chars:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        lines.append(cur)
    return lines


def para(
    x, y, s, width_px, *, size=13, weight=400, fill=MUTED, lh=None, anchor="start"
):
    """Wrapped text; returns (svg, height used)."""
    lh = lh or size * 1.42
    lines = wrap(s, max(8, int(width_px / (size * 0.53))))
    out = [
        text(x, y + i * lh, line, size=size, weight=weight, fill=fill, anchor=anchor)
        for i, line in enumerate(lines)
    ]
    return "".join(out), len(lines) * lh


def rect(
    x, y, w, h, *, fill="#FFFFFF", stroke=BORDER, r=10, sw=1, dash=None, shadow=False
):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    f = ' filter="url(#shadow)"' if shadow else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" ry="{r}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{sw}"{d}{f}/>'
    )


def line(x1, y1, x2, y2, *, color=MUTED2, w=2, dash=None, arrow=False):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    m = f' marker-end="url(#arrow-{color[1:]})"' if arrow else ""
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{w}"{d}{m}/>'


def path(d, *, color=MUTED2, w=2, dash=None, arrow=False, fill="none"):
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    m = f' marker-end="url(#arrow-{color[1:]})"' if arrow else ""
    return f'<path d="{d}" fill="{fill}" stroke="{color}" stroke-width="{w}"{ds}{m}/>'


def pill(
    x, y, label, *, fg=INK, bg="#F1F4F8", h=24, size=12, mono_font=False, border=None
):
    cw = (0.6 if mono_font else 0.56) * size
    w = int(len(label) * cw) + 22
    fam = MONO if mono_font else SANS
    b = f' stroke="{border}" stroke-width="1"' if border else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{h / 2:.0f}" fill="{bg}"{b}/>'
        f'<text x="{x + w / 2}" y="{y + h / 2 + size * 0.36:.1f}" text-anchor="middle" '
        f'font-family="{fam}" font-size="{size}" font-weight="700" fill="{fg}">{esc(label)}</text>',
        w,
    )


def badge(x, y, n, color):
    return (
        f'<circle cx="{x}" cy="{y}" r="13" fill="{color}"/>'
        f'<text x="{x}" y="{y + 5}" text-anchor="middle" font-family="{SANS}" font-size="14" '
        f'font-weight="800" fill="#FFFFFF">{n}</text>'
    )


def file_glyph(x, y, color, tint, *, s=26, label=None, dash=None, locked=False):
    """A small document with a folded corner; `locked` adds a padlock."""
    d = f' stroke-dasharray="{dash}"' if dash else ""
    fold = s * 0.28
    out = [
        f'<path d="M {x} {y} H {x + s - fold} L {x + s} {y + fold} V {y + s * 1.25} H {x} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"{d}/>',
        f'<path d="M {x + s - fold} {y} V {y + fold} H {x + s}" fill="none" stroke="{color}" stroke-width="1.5"/>',
    ]
    if label:
        out.append(
            text(
                x + s / 2,
                y + s * 0.95,
                label,
                size=9,
                weight=700,
                fill=color,
                anchor="middle",
            )
        )
    if locked:
        lx, ly = x + s - 9, y + s * 1.25 - 3
        out.append(
            f'<rect x="{lx}" y="{ly}" width="14" height="11" rx="2" fill="{INK2}"/>'
            f'<path d="M {lx + 3} {ly} v -4 a 4 4 0 0 1 8 0 v 4" fill="none" stroke="{INK2}" stroke-width="2"/>'
        )
    return "".join(out)


def table_glyph(x, y, w, rows, color=TABLE, tint=TABLE_T, *, head=None, rh=17):
    """A Delta table: a header bar and `rows` row lines."""
    h = 26 + rows * rh
    out = [
        rect(x, y, w, h, fill=tint, stroke=color, r=8, sw=2),
        f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="8" fill="{color}"/>',
        f'<rect x="{x}" y="{y + 12}" width="{w}" height="10" fill="{color}"/>',
    ]
    if head:
        out.append(text(x + 10, y + 16, head, size=11, weight=700, fill="#FFFFFF"))
    for i in range(rows):
        yy = y + 26 + i * rh
        out.append(line(x + 8, yy + rh - 3, x + w - 8, yy + rh - 3, color=color, w=1))
    return "".join(out), h


def svg_open(w, h):
    markers = "".join(
        f'<marker id="arrow-{c[1:]}" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="7" '
        f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>'
        for c in (MUTED2, PACK, TABLE, OK, BAD, KEEP, WAIT, INK2, BASE)
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'style="font-family: {SANS};"><defs>{markers}'
        '<filter id="shadow" x="-5%" y="-5%" width="110%" height="115%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="5" flood-color="#0F1B2A" flood-opacity="0.08"/></filter>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FAFBFC"/>'
        '<stop offset="1" stop-color="#F1F4F8"/></linearGradient>'
        '<pattern id="hatch" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">'
        f'<line x1="0" y1="0" x2="0" y2="7" stroke="{BAD}" stroke-width="2" stroke-opacity="0.45"/></pattern>'
        "</defs>"
        f'<rect width="{w}" height="{h}" fill="url(#bg)"/>'
    )


def header(w, title, subtitle):
    return text(40, 60, title, size=28, weight=800) + text(
        40, 88, subtitle, size=14, fill=MUTED
    )


def footer(w, h, note):
    return text(w - 40, h - 22, note, size=11, fill=MUTED3, anchor="end")


# --- 1. store kinds ---------------------------------------------------------------------


def store_kinds():
    W, H = 1500, 800
    out = [
        svg_open(W, H),
        header(
            W,
            "Four kinds of Zarr store",
            "Where the bytes live, where the rows live, and who may write",
        ),
    ]
    cw, gap, top = 335, 26, 120
    cards = [
        dict(
            title="Plain store",
            sub="Written by xarray, a vendor or any Zarr tool",
            manifest=None,
            base="read as they are",
            overlay=None,
            rows=[
                ("Bytes", "the store's own objects"),
                ("Manifest", "none: no versions, no tags"),
                ("GeoBrix", "reads and samples it; never writes"),
            ],
        ),
        dict(
            title="Registered, read-only",
            sub="gbx_zarr_register(path, table)",
            manifest="table",
            base="listed once, not copied",
            overlay=None,
            rows=[
                ("Bytes", "the store's own objects"),
                ("Manifest", "one row per object, with its identity"),
                ("GeoBrix", "reads; detects outside changes (sync)"),
            ],
        ),
        dict(
            title="Registered, writable",
            sub="gbx_zarr_register(..., writable=True)",
            manifest="table",
            base="never written",
            overlay="Overlay root",
            rows=[
                ("Bytes", "the store's objects plus an overlay of packs"),
                ("Manifest", "overlay rows win over the store's rows"),
                ("GeoBrix", "writes go to the overlay; publish is opt-in"),
            ],
        ),
        dict(
            title="Managed store",
            sub='gbx_zarr_write(..., mode="create")',
            manifest="table",
            base=None,
            overlay="Data root",
            rows=[
                ("Bytes", "pack files GeoBrix wrote under the root"),
                ("Manifest", "every chunk and shard, every version"),
                ("GeoBrix", "anyone with MODIFY on the table writes"),
            ],
        ),
    ]
    for i, c in enumerate(cards):
        x = 40 + i * (cw + gap)
        h = 560
        out.append(rect(x, top, cw, h, shadow=True, r=14))
        accent = [BASE, TABLE, TABLE, PACK][i]
        out.append(
            f'<rect x="{x}" y="{top}" width="{cw}" height="6" rx="3" fill="{accent}"/>'
        )
        out.append(text(x + 20, top + 36, c["title"], size=19, weight=800))
        out.append(mono(x + 20, top + 58, c["sub"], size=10.5, fill=MUTED2, weight=500))
        # manifest zone
        zy = top + 92
        mh = 26 + 3 * 17
        if c["manifest"]:
            out.append(
                table_glyph(x + 20, zy, cw - 40, 3, head="Manifest table (Delta)")[0]
            )
        else:
            out.append(
                rect(
                    x + 20,
                    zy,
                    cw - 40,
                    mh,
                    fill="#FFFFFF",
                    stroke=MUTED3,
                    r=8,
                    sw=1.5,
                    dash="5 4",
                )
            )
            out.append(
                text(
                    x + cw / 2,
                    zy + 46,
                    "no manifest table",
                    size=13,
                    weight=600,
                    fill=MUTED3,
                    anchor="middle",
                )
            )
        # files zone
        fy = zy + mh + 40
        fh = 206
        out.append(
            rect(
                x + 20, fy, cw - 40, fh, fill="#FBFCFD", stroke=BORDER, r=10, dash="4 4"
            )
        )
        out.append(
            text(
                x + 32,
                fy + fh - 12,
                "FILES ON A VOLUME",
                size=10,
                weight=800,
                fill=MUTED3,
                ls="1.2",
            )
        )
        gy = fy + 22
        groups = []  # (left, width, color, tint, title, note, kind)
        if c["base"] is not None:
            groups.append(
                (x + 32, 128, BASE, BASE_T, "Objects of the store", c["base"], "base")
            )
        if c["overlay"]:
            if c["base"] is not None:
                groups.append(
                    (
                        x + 32 + 140,
                        128,
                        PACK,
                        PACK_T,
                        c["overlay"],
                        "immutable packs",
                        "pack",
                    )
                )
            else:
                groups.append(
                    (
                        x + 32 + 40,
                        220,
                        PACK,
                        PACK_T,
                        c["overlay"],
                        "immutable packs",
                        "pack",
                    )
                )
        for left, gw, col, tint, title, note, kind in groups:
            out.append(rect(left, gy, gw, 150, fill=tint, stroke=col, r=8, sw=1.5))
            n = 6 if (kind == "base" or gw > 200) else 3
            cols = 3
            span = gw - 16
            step = span / cols
            for k in range(n):
                bx = left + 8 + (k % cols) * step + (step - 26) / 2
                by = gy + 12 + (k // cols) * 44
                out.append(
                    file_glyph(
                        bx,
                        by,
                        col,
                        "#FFFFFF",
                        s=26,
                        label="pack" if kind == "pack" else None,
                        locked=(kind == "base" and c["overlay"] and k == 5),
                    )
                )
            out.append(
                text(
                    left + gw / 2,
                    gy + 118,
                    title,
                    size=10.5,
                    weight=700,
                    fill=col,
                    anchor="middle",
                )
            )
            out.append(
                text(
                    left + gw / 2, gy + 133, note, size=10, fill=MUTED2, anchor="middle"
                )
            )
            if kind == "pack":
                out.append(
                    text(
                        left + gw / 2,
                        gy + 146,
                        "+ owner ledger",
                        size=9,
                        fill=MUTED3,
                        anchor="middle",
                    )
                )
            # arrow from the manifest to the top of the group
            if c["manifest"]:
                tx = left + gw / 2
                sx = min(max(tx, x + 60), x + cw - 60)
                out.append(
                    path(
                        f"M {sx} {zy + mh} C {sx} {zy + mh + 22}, {tx} {gy - 24}, {tx} {gy - 2}",
                        color=col,
                        w=2,
                        arrow=True,
                    )
                )
        # facts
        ry = fy + fh + 30
        for label, val in c["rows"]:
            out.append(
                text(
                    x + 20,
                    ry,
                    label.upper(),
                    size=10,
                    weight=800,
                    fill=MUTED3,
                    ls="1.2",
                )
            )
            tv, th2 = para(x + 98, ry, val, cw - 120, size=12, fill=INK2)
            out.append(tv)
            ry += max(22, th2 + 6)
    out.append(rect(40, 700, W - 80, 62, fill=TABLE_T, stroke=TABLE, r=10, sw=1.5))
    out.append(text(60, 726, "Branches", size=13, weight=800, fill=TABLE))
    nt, _ = para(
        140,
        726,
        "Many tables can register one store. Each registration keeps its own history, grants and overlay root, and sees the store plus only its own writes; "
        "there is no merge between them. A root (an overlay root or a data root) has exactly one owner, decided by a Delta transaction in its _gbx_owner table.",
        W - 200,
        size=12,
        fill=INK2,
    )
    out.append(nt)
    out.append(footer(W, H, "GeoBrix · Zarr stores"))
    out.append("</svg>")
    return "\n".join(out), W, H


# --- 2. write and commit ---------------------------------------------------------------


def write_commit():
    W, H = 1500, 800
    out = [
        svg_open(W, H),
        header(
            W,
            "A write is two phases",
            "Files first, invisible; then one atomic commit makes them a version",
        ),
    ]
    lanes = [
        ("Driver", INK2),
        ("Executor tasks", PACK),
        ("Volume", BASE),
        ("Manifest (Delta)", TABLE),
    ]
    lx, lw, ly0, lh = 40, W - 80, 150, 118
    for i, (name, col) in enumerate(lanes):
        y = ly0 + i * lh
        out.append(rect(lx, y, lw, lh - 10, fill="#FFFFFF", stroke=BORDER, r=10))
        out.append(
            f'<rect x="{lx}" y="{y}" width="8" height="{lh - 10}" rx="4" fill="{col}"/>'
        )
        out.append(
            text(lx + 22, y + 26, name.upper(), size=11, weight=800, fill=col, ls="1.2")
        )
    split = 900
    x0 = 210
    out.append(
        f'<rect x="{x0}" y="112" width="{split - x0 - 6}" height="28" rx="8" fill="{PACK_T}"/>'
    )
    out.append(
        text(
            (x0 + split) / 2,
            131,
            "PHASE 1 · FILES (distributed, nothing visible yet)",
            size=12,
            weight=800,
            fill=PACK,
            anchor="middle",
            ls="0.8",
        )
    )
    out.append(
        f'<rect x="{split + 6}" y="112" width="{W - 40 - split - 6}" height="28" rx="8" fill="{OK_T}"/>'
    )
    out.append(
        text(
            (split + 6 + W - 40) / 2,
            131,
            "PHASE 2 · COMMIT (driver, atomic)",
            size=12,
            weight=800,
            fill=OK,
            anchor="middle",
            ls="0.8",
        )
    )
    out.append(line(split, 108, split, ly0 + 4 * lh - 14, color=OK, w=2.4, dash="7 5"))

    def box(x, y, w, h, title, body, col, tint, n):
        s = [
            rect(x, y, w, h, fill=tint, stroke=col, r=9, sw=1.6),
            badge(x + 2, y + 2, n, col),
            text(x + 22, y + 24, title, size=13, weight=800, fill=col),
        ]
        t, _ = para(x + 14, y + 44, body, w - 28, size=11.5, fill=INK2, lh=15)
        return "".join(s) + t

    y1, y2, y3, y4 = ly0 + 18, ly0 + lh + 18, ly0 + 2 * lh + 18, ly0 + 3 * lh + 8
    out.append(
        box(
            x0 + 20,
            y1,
            330,
            88,
            "Plan against a base version",
            "Read the manifest at version V. Group the input by target shard and pick a write_id (32 hex characters).",
            INK2,
            "#EEF1F5",
            1,
        )
    )
    out.append(
        box(
            x0 + 20,
            y2,
            330,
            88,
            "Encode the chunks",
            "Fully covered chunks are encoded fresh. Partly covered ones are read at V, patched and re-encoded.",
            PACK,
            PACK_T,
            2,
        )
    )
    out.append(
        box(
            x0 + 380,
            y3,
            290,
            88,
            "One pack per task",
            "data/<array>/<write_id>/part-*.zpack, written once, sequentially, never modified again.",
            BASE,
            BASE_T,
            3,
        )
    )
    out.append(
        box(
            x0 + 380,
            y2,
            290,
            88,
            "Return small rows",
            "About 100 bytes per chunk: file, offset, length, checksum.",
            PACK,
            PACK_T,
            4,
        )
    )
    out.append(
        box(
            split + 56,
            y1,
            320,
            88,
            "Collect the rows",
            "The driver holds the manifest rows of the whole write.",
            INK2,
            "#EEF1F5",
            5,
        )
    )
    out.append(
        box(
            split + 24,
            y4,
            380,
            100,
            "One Delta MERGE",
            "Chunk rows with a compare-and-swap on each replaced key's write_id (and “still absent” for new keys), the changed array and coordinate rows, and a commit row keyed by write_id.",
            TABLE,
            TABLE_T,
            6,
        )
    )
    out.append(
        box(split + 424, y4, 136, 100, "Version V+1", "Visible whole.", OK, OK_T, 7)
    )
    cx2 = x0 + 20 + 165
    out.append(path(f"M {cx2} {y1 + 88} V {y2 - 2}", color=MUTED2, arrow=True))
    out.append(
        path(
            f"M {x0 + 350} {y2 + 44} C {x0 + 365} {y2 + 44}, {x0 + 355} {y3 + 44}, {x0 + 378} {y3 + 44}",
            color=PACK,
            arrow=True,
        )
    )
    out.append(path(f"M {x0 + 525} {y3} V {y2 + 90}", color=PACK, arrow=True))
    out.append(
        path(
            f"M {x0 + 670} {y2 + 44} C {x0 + 760} {y2 + 44}, {x0 + 760} {y1 + 44}, {split + 54} {y1 + 44}",
            color=PACK,
            arrow=True,
        )
    )
    out.append(
        path(
            f"M {split + 200} {y1 + 88} C {split + 200} {y1 + 200}, {split + 210} {y4 - 90}, {split + 210} {y4 - 2}",
            color=TABLE,
            arrow=True,
        )
    )
    out.append(
        path(f"M {split + 404} {y4 + 50} H {split + 422}", color=OK, w=3, arrow=True)
    )
    ny = ly0 + 4 * lh + 6
    out.append(rect(x0, ny, 640, 74, fill=BAD_T, stroke=BAD, r=9, sw=1.4))
    out.append(
        text(
            x0 + 16,
            ny + 26,
            "If anything fails before the MERGE",
            size=13,
            weight=800,
            fill=BAD,
        )
    )
    t, _ = para(
        x0 + 16,
        ny + 46,
        "No row was written, so nothing is visible and the version has not moved. The packs are unreferenced and the next vacuum removes them.",
        610,
        size=12,
        fill=INK2,
        lh=16,
    )
    out.append(t)
    out.append(rect(870, ny, 590, 74, fill=OK_T, stroke=OK, r=9, sw=1.4))
    out.append(
        text(
            886,
            ny + 26,
            "Run it again with the same write_id",
            size=13,
            weight=800,
            fill=OK,
        )
    )
    t, _ = para(
        886,
        ny + 46,
        "If that write already committed, the call returns its version and writes nothing, so a retried job cannot apply a write twice.",
        560,
        size=12,
        fill=INK2,
        lh=16,
    )
    out.append(t)
    out.append(footer(W, H, "GeoBrix · Zarr stores"))
    out.append("</svg>")
    return "\n".join(out), W, H


# --- 3. concurrent writers --------------------------------------------------------------


def concurrent_writers():
    W, H = 1500, 840
    out = [
        svg_open(W, H),
        header(
            W,
            "When two writers meet",
            "The manifest decides, row by row: different chunks land, shared metadata rebases, the same chunk conflicts",
        ),
    ]
    pw, gap, top, ph = 450, 25, 120, 670
    panels = [
        ("Different chunks", "Two writers touch different inner chunks", OK, OK_T),
        (
            "Shared metadata",
            "Two appends change the array's shape and a shared coordinate chunk",
            WAIT,
            WAIT_T,
        ),
        ("The same chunk", "Two writes change cells of one inner chunk", BAD, BAD_T),
    ]
    for i, (title, sub, col, tint) in enumerate(panels):
        x = 40 + i * (pw + gap)
        out.append(rect(x, top, pw, ph, shadow=True, r=14))
        out.append(
            f'<rect x="{x}" y="{top}" width="{pw}" height="6" rx="3" fill="{col}"/>'
        )
        out.append(text(x + 20, top + 36, title, size=19, weight=800))
        sv, _ = para(x + 20, top + 58, sub, pw - 40, size=12, fill=MUTED2)
        out.append(sv)
        rx, ax, bx = x + 62, x + 190, x + 340
        y10, y11, y12 = top + 160, top + 280, top + 470
        out.append(
            text(
                x + 20, top + 112, "MANIFEST", size=10, weight=800, fill=TABLE, ls="1.2"
            )
        )
        out.append(line(rx, top + 124, rx, y12 + 36, color=TABLE, w=3))
        for v, vy in (("v10", y10), ("v11", y11), ("v12", y12)):
            out.append(f'<circle cx="{rx}" cy="{vy}" r="9" fill="{TABLE}"/>')
            out.append(
                text(rx - 16, vy + 5, v, size=12, weight=800, fill=TABLE, anchor="end")
            )
        out.append(pill(ax - 38, top + 100, "writer A", fg="#FFFFFF", bg=PACK)[0])
        out.append(pill(bx - 38, top + 100, "writer B", fg="#FFFFFF", bg=KEEP)[0])
        out.append(line(ax, top + 126, ax, y12 + 36, color=PACK, w=1.5, dash="4 4"))
        out.append(line(bx, top + 126, bx, y12 + 36, color=KEEP, w=1.5, dash="4 4"))
        out.append(line(rx + 12, y10, bx, y10, color=MUTED3, w=1.5, dash="3 3"))
        out.append(
            text(
                (ax + bx) / 2,
                y10 + 20,
                "both read v10",
                size=11,
                fill=MUTED2,
                anchor="middle",
            )
        )
        out.append(text(ax + 8, y10 + 44, "write packs", size=11, fill=PACK))
        out.append(text(bx + 8, y10 + 44, "write packs", size=11, fill=KEEP))
        # A lands at v11
        out.append(path(f"M {ax} {y11} H {rx + 12}", color=PACK, w=2.6, arrow=True))
        out.append(
            text(rx + 24, y11 - 10, "A lands: v11", size=12, weight=700, fill=PACK)
        )
        if i == 0:
            out.append(path(f"M {bx} {y12} H {rx + 12}", color=KEEP, w=2.6, arrow=True))
            out.append(
                text(rx + 24, y12 - 10, "B lands: v12", size=12, weight=700, fill=KEEP)
            )
            note = (
                "Row-level concurrency",
                "Each writer's rows are different rows, so Delta lands both with no retry and no wait. Different chunks never contend.",
            )
        elif i == 1:
            bxx = bx - 80
            out.append(
                rect(bxx, y11 + 24, 180, 150, fill=WAIT_T, stroke=WAIT, r=8, sw=1.4)
            )
            out.append(
                text(
                    bxx + 12,
                    y11 + 44,
                    "B REBASES",
                    size=10,
                    weight=800,
                    fill=WAIT,
                    ls="1.2",
                )
            )
            steps = [
                "1  re-read the latest version",
                "2  check none of its data keys moved",
                "3  recompute the shape and the shared chunk",
                "4  retry the MERGE",
            ]
            yy = y11 + 62
            for s in steps:
                tt, hh = para(bxx + 12, yy, s, 156, size=11, fill=INK2, lh=13)
                out.append(tt)
                yy += hh + 6
            out.append(path(f"M {bx} {y12} H {rx + 12}", color=KEEP, w=2.6, arrow=True))
            out.append(
                text(rx + 24, y12 - 10, "B lands: v12", size=12, weight=700, fill=KEEP)
            )
            note = (
                "Metadata conflicts rebase",
                "B's data files are never rewritten. Up to 20 attempts, with a jittered wait capped at 16 s; each lost round means another writer landed.",
            )
        else:
            bxx = bx - 80
            out.append(
                rect(bxx, y11 + 24, 180, 128, fill=BAD_T, stroke=BAD, r=8, sw=1.6)
            )
            out.append(
                text(
                    bxx + 12,
                    y11 + 44,
                    "B's MERGE FAILS",
                    size=10,
                    weight=800,
                    fill=BAD,
                    ls="1.2",
                )
            )
            tt, _ = para(
                bxx + 12,
                y11 + 64,
                "Its compare-and-swap expected the chunk as v10 had it. A changed it.",
                156,
                size=11,
                fill=INK2,
                lh=14,
            )
            out.append(tt)
            out.append(
                mono(
                    bxx + 12,
                    y11 + 132,
                    "ZarrWriteConflict",
                    size=11,
                    fill=BAD,
                    weight=700,
                )
            )
            out.append(
                path(
                    f"M {bx} {y11 + 152} V {y12 + 8}",
                    color=BAD,
                    w=2,
                    dash="6 4",
                    arrow=True,
                )
            )
            out.append(
                f'<rect x="{bx - 92}" y="{y12 + 14}" width="184" height="24" rx="6" fill="#FFFFFF"/>'
            )
            out.append(
                text(
                    bx,
                    y12 + 31,
                    "nothing of B is visible",
                    size=12,
                    weight=700,
                    fill=BAD,
                    anchor="middle",
                )
            )
            note = (
                "A true overlap is an error",
                "The error names the keys and the winning write. B reads the region again and writes again; GeoBrix never merges two writers' cells silently.",
            )
        out.append(
            rect(
                x + 20, top + ph - 112, pw - 40, 92, fill=tint, stroke=col, r=9, sw=1.3
            )
        )
        out.append(text(x + 34, top + ph - 88, note[0], size=13, weight=800, fill=col))
        tt, _ = para(x + 34, top + ph - 68, note[1], pw - 68, size=12, fill=INK2, lh=16)
        out.append(tt)
    out.append(footer(W, H, "GeoBrix · Zarr stores"))
    out.append("</svg>")
    return "\n".join(out), W, H


# --- 4. retention timeline --------------------------------------------------------------


def retention_timeline():
    W, H = 1500, 830
    out = [
        svg_open(W, H),
        header(
            W,
            "What a vacuum keeps",
            "A file goes only when no readable version, no tag and no write in flight needs it",
        ),
    ]
    ax0, ax1, ay = 230, 1440, 168
    win_x = 740
    out.append(
        f'<rect x="{win_x}" y="{ay - 34}" width="{ax1 - win_x}" height="448" rx="10" fill="{PACK_T}" fill-opacity="0.55"/>'
    )
    out.append(
        text(
            (win_x + ax1) / 2,
            ay - 14,
            "retain window (default 7 days): every version here stays readable",
            size=12,
            weight=800,
            fill=PACK,
            anchor="middle",
        )
    )
    out.append(
        text(
            (ax0 + win_x) / 2,
            ay - 14,
            "older than retain",
            size=12,
            weight=800,
            fill=MUTED3,
            anchor="middle",
        )
    )
    out.append(line(ax0, ay + 12, ax1, ay + 12, color=INK2, w=2.5, arrow=True))
    out.append(text(ax1, ay + 34, "now", size=12, weight=800, fill=INK2, anchor="end"))
    out.append(text(ax0, ay + 34, "time →", size=12, fill=MUTED2))
    vx = [270, 390, 520, 640, 840, 990, 1140, 1300]
    for k, x in enumerate(vx):
        out.append(f'<circle cx="{x}" cy="{ay + 12}" r="8" fill="{TABLE}"/>')
        out.append(
            text(
                x, ay - 2, f"v{k + 1}", size=12, weight=800, fill=TABLE, anchor="middle"
            )
        )
    out.append(line(win_x, ay - 4, win_x, ay + 424, color=BAD, w=2.4, dash="7 5"))
    out.append(
        text(
            win_x - 8,
            ay + 442,
            "vacuum horizon",
            size=12,
            weight=800,
            fill=BAD,
            anchor="end",
        )
    )
    out.append(
        text(
            win_x - 8,
            ay + 458,
            "versions before it raise ZarrVersionUnavailable",
            size=11,
            fill=MUTED2,
            anchor="end",
        )
    )
    rows = [
        (
            "pack of v1's chunks",
            270,
            520,
            "superseded",
            BAD,
            "replaced before the horizon: deleted",
            True,
        ),
        (
            "pack of v2's chunks",
            390,
            900,
            "retained",
            PACK,
            "replaced at v5, inside the window: a readable version still uses it",
            False,
        ),
        (
            "pack of v3's chunks",
            520,
            1440,
            "live",
            OK,
            "a current row points at it",
            False,
        ),
        (
            "pack of v4's chunks",
            640,
            1440,
            "live",
            OK,
            "a current row points at it",
            False,
        ),
        (
            "tagged version",
            270,
            1440,
            "tagged",
            KEEP,
            "a tag row points at it: kept until the tag is removed",
            False,
        ),
        (
            "failed write",
            990,
            1440,
            "uncommitted",
            WAIT,
            "never committed: kept for retain, then deleted",
            False,
        ),
        (
            "write in flight",
            1380,
            1440,
            "recent",
            MUTED3,
            "under 1 hour old: always kept",
            False,
        ),
    ]
    by = ay + 62
    tints = {
        BAD: BAD_T,
        OK: OK_T,
        KEEP: KEEP_T,
        WAIT: WAIT_T,
        MUTED3: "#EEF1F5",
        PACK: PACK_T,
    }
    for k, (label, x0, x1, kind, col, why, gone) in enumerate(rows):
        y = by + k * 52
        out.append(
            text(ax0 - 14, y + 20, label, size=12, weight=700, fill=INK2, anchor="end")
        )
        out.append(
            rect(
                x0,
                y,
                x1 - x0,
                30,
                fill=tints[col],
                stroke=col,
                r=7,
                sw=1.8,
                dash="5 3" if gone else None,
            )
        )
        if gone:
            out.append(
                f'<rect x="{x0}" y="{y}" width="{x1 - x0}" height="30" rx="7" fill="url(#hatch)"/>'
            )
        out.append(text(x0 + 10, y + 20, kind, size=12, weight=800, fill=col))
        if x1 - x0 > 260:
            tx = max(x0 + 110, win_x + 16) if x0 < win_x < x1 else x0 + 110
            out.append(text(tx, y + 20, why, size=11.5, fill=INK2))
        elif k == 0:
            out.append(text(x1 + 12, y + 20, why, size=11.5, fill=INK2))
        else:
            out.append(text(x0 - 12, y + 20, why, size=11.5, fill=INK2, anchor="end"))
    py = by + 7 * 52 + 58
    out.append(
        rect(40, py, W - 80, 130, fill="#FFFFFF", stroke=BORDER, r=12, shadow=True)
    )
    out.append(
        text(
            60,
            py + 28,
            "The manifest table keeps its own history to match",
            size=14,
            weight=800,
        )
    )
    items = [
        ("geobrix.zarr.retain", "7 days", "how long old versions stay readable"),
        (
            "delta.deletedFileRetentionDuration",
            "at least retain",
            "never lowered below the store's retain",
        ),
        (
            "delta.logRetentionDuration",
            "max(30 days, retain)",
            "Delta history lasts at least as long",
        ),
        (
            "predictive optimization",
            "keeps at least 7 days",
            "Databricks may VACUUM the manifest table itself",
        ),
    ]
    cw = (W - 120) / 4
    for k, (key, val, note) in enumerate(items):
        x = 60 + k * cw
        out.append(
            mono(x, py + 58, key, size=11, fill=TABLE, weight=700)
            if k < 3
            else text(x, py + 58, key, size=12, weight=700, fill=TABLE)
        )
        out.append(text(x, py + 80, val, size=15, weight=800, fill=INK))
        tt, _ = para(x, py + 98, note, cw - 24, size=11.5, fill=MUTED, lh=14)
        out.append(tt)
    out.append(footer(W, H, "GeoBrix · Zarr stores"))
    out.append("</svg>")
    return "\n".join(out), W, H


# --- 5. chunk rows vs shard rows --------------------------------------------------------


def chunk_vs_shard_rows():
    W, H = 1500, 760
    out = [
        svg_open(W, H),
        header(
            W,
            "Chunk rows over a shard row",
            "Writes add small rows over a shard; optimize folds them back into one",
        ),
    ]
    pw, gap, top, ph = 420, 60, 120, 452
    x_start = (W - (3 * pw + 2 * gap)) // 2
    stages = [
        (
            "1  A store of shards",
            "One shard row per shard object, as in a store registered from outside",
            BASE,
        ),
        (
            "2  After appends and updates",
            "Each changed chunk is a chunk row in a small pack, laid over the shard",
            WAIT,
        ),
        ("3  After optimize", "One shard row again, pointing at a new pack", OK),
    ]
    colors = {
        "s0": (BASE, BASE_T),
        "c1": (WAIT, WAIT_T),
        "c2": (PACK, PACK_T),
        "s1": (OK, OK_T),
    }

    def cells(x, y, fills, tomb=()):
        s = []
        for k, key in enumerate(fills):
            cx, cy = x + (k % 4) * 78, y + (k // 4) * 70
            col, tint = colors[key]
            if k in tomb:
                col = BAD
            s.append(
                rect(
                    cx,
                    cy,
                    70,
                    62,
                    fill="#FFFFFF" if k in tomb else tint,
                    stroke=col,
                    r=7,
                    sw=2,
                    dash="5 3" if k in tomb else None,
                )
            )
            s.append(
                mono(cx + 35, cy + 28, f"c/0/{k}", size=11, fill=col, anchor="middle")
            )
            if k in tomb:
                s.append(
                    text(
                        cx + 35,
                        cy + 46,
                        "deleted",
                        size=10,
                        weight=700,
                        fill=BAD,
                        anchor="middle",
                    )
                )
        return "".join(s)

    def rows(x, y, w, items):
        s = [
            rect(
                x, y, w, 26 + len(items) * 22, fill=TABLE_T, stroke=TABLE, r=8, sw=1.6
            ),
            f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="8" fill="{TABLE}"/>',
            f'<rect x="{x}" y="{y + 12}" width="{w}" height="10" fill="{TABLE}"/>',
        ]
        s.append(
            text(
                x + 10,
                y + 16,
                "manifest rows of one array",
                size=11,
                weight=700,
                fill="#FFFFFF",
            )
        )
        for k, (kind, key, pack, col) in enumerate(items):
            yy = y + 40 + k * 22
            s.append(mono(x + 12, yy, kind, size=11, fill=col, weight=800))
            s.append(mono(x + 70, yy, key, size=11, fill=INK2))
            s.append(mono(x + 150, yy, pack, size=11, fill=col, weight=700))
        return "".join(s)

    for i, (title, sub, col) in enumerate(stages):
        x = x_start + i * (pw + gap)
        out.append(rect(x, top, pw, ph, shadow=True, r=14))
        out.append(
            f'<rect x="{x}" y="{top}" width="{pw}" height="6" rx="3" fill="{col}"/>'
        )
        out.append(text(x + 20, top + 36, title, size=19, weight=800))
        sv, _ = para(x + 20, top + 58, sub, pw - 40, size=12, fill=MUTED2)
        out.append(sv)
        if i == 0:
            items = [("shard", "c/0/0", "object A", BASE)]
            fills, tomb = ["s0"] * 8, ()
            reading = "A read opens one object and finds each inner chunk through the shard's index."
        elif i == 1:
            items = [
                ("shard", "c/0/0", "object A", BASE),
                ("chunk", "c/0/2", "pack B", WAIT),
                ("chunk", "c/0/5", "pack C", PACK),
                ("chunk", "c/0/6", "(deleted)", BAD),
            ]
            fills, tomb = ["s0", "s0", "c1", "s0", "s0", "c2", "c2", "s0"], (6,)
            reading = "A read lays the chunk rows over the shard's bytes. A deleted chunk reads as the fill value."
        else:
            items = [("shard", "c/0/0", "pack D", OK)]
            fills, tomb = ["s1"] * 8, ()
            reading = "The same cells in one object. The chunk rows and the deleted chunk are folded away."
        out.append(rows(x + 20, top + 92, pw - 40, items))
        gy = top + 92 + 26 + len(items) * 22 + 40
        out.append(
            text(
                x + 20,
                gy - 12,
                "THE SHARD'S INNER CHUNKS",
                size=10,
                weight=800,
                fill=MUTED3,
                ls="1.2",
            )
        )
        out.append(cells(x + 34, gy, fills, tomb))
        tt, _ = para(x + 20, gy + 164, reading, pw - 40, size=12.5, fill=INK2, lh=17)
        out.append(tt)
        if i < 2:
            ax = x + pw + 8
            out.append(
                path(
                    f"M {ax} {top + 215} H {ax + gap - 18}",
                    color=MUTED2,
                    w=3,
                    arrow=True,
                )
            )
            out.append(
                text(
                    ax + (gap - 18) / 2,
                    top + 200,
                    ["write", "optimize"][i],
                    size=11,
                    weight=700,
                    fill=MUTED2,
                    anchor="middle",
                )
            )
    sy = top + ph + 24
    out.append(rect(40, sy, W - 80, 108, fill="#FFFFFF", stroke=BORDER, r=12))
    msgs = [
        (
            "Why chunk rows exist",
            "An append or an update changes a few chunks. Writing each as its own row in a small pack keeps the write cheap: nothing is rewritten. A new store is chunk rows only.",
        ),
        (
            "What optimize does",
            "It copies each shard's bytes, without decoding them, into one new pack, and commits one shard row in place of the chunk rows, all under compare-and-swap.",
        ),
        (
            "When to run it",
            "After many small appends, especially on a Volume, where every object costs a round trip. A shard that changed while it ran is skipped and folded next time.",
        ),
    ]
    cw = (W - 120) / 3
    for k, (h1, body) in enumerate(msgs):
        x = 60 + k * cw
        out.append(text(x, sy + 28, h1, size=13, weight=800, fill=INK2))
        tt, _ = para(x, sy + 48, body, cw - 30, size=12, fill=MUTED, lh=16)
        out.append(tt)
    out.append(footer(W, H, "GeoBrix · Zarr stores"))
    out.append("</svg>")
    return "\n".join(out), W, H


DIAGRAMS = {
    "store-kinds": store_kinds,
    "write-commit": write_commit,
    "concurrent-writers": concurrent_writers,
    "retention-timeline": retention_timeline,
    "chunk-vs-shard-rows": chunk_vs_shard_rows,
}


def render_png(svg_path, png_path, w, h):
    """Rasterize with headless Chrome at 2x, then crop the white margin."""
    if not os.path.exists(CHROME):
        print(f"  no browser at {CHROME}: PNG skipped (set CHROME)")
        return False
    with tempfile.TemporaryDirectory() as profile:
        subprocess.run(
            [
                CHROME,
                "--headless",
                "--disable-gpu",
                "--hide-scrollbars",
                f"--user-data-dir={profile}",
                "--force-device-scale-factor=2",
                f"--window-size={w},{h}",
                f"--screenshot={png_path}",
                f"file://{svg_path}",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    from PIL import Image, ImageChops

    img = Image.open(png_path).convert("RGB")
    bbox = ImageChops.difference(
        img, Image.new("RGB", img.size, (255, 255, 255))
    ).getbbox()
    if bbox:
        img.crop(bbox).save(png_path)
    return True


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    only_svg = "--svg-only" in sys.argv
    names = [a for a in sys.argv[1:] if not a.startswith("--")] or list(DIAGRAMS)
    for name in names:
        svg, w, h = DIAGRAMS[name]()
        svg_path = os.path.join(OUT, f"{name}.svg")
        with open(svg_path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"wrote {svg_path}")
        if not only_svg:
            png_path = os.path.join(OUT, f"{name}.png")
            if render_png(svg_path, png_path, w, h):
                print(f"wrote {png_path}")
