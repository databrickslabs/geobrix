#!/usr/bin/env python3
"""Card-and-pill SVG building blocks for the function-palette infographics.

``vapor-eyes-lakeflow-functions.py`` renders its cards with these helpers. They came
from the first RasterX function-categories diagram, which now renders from an HTML
template instead (``rasterx-function-categories.py``).
"""
from dataclasses import dataclass, field

@dataclass
class Section:
    label: str
    fns: list

@dataclass
class Card:
    title: str
    subtitle: str
    color: str            # accent (header)
    tint: str             # light fill used for pill background
    sections: list = field(default_factory=list)
    fns: list = field(default_factory=list)   # if no sections, flat list


# --- Layout -------------------------------------------------------------------

PAD = 36
COL_GAP = 24
CARD_GAP = 22
TITLE_BLOCK_H = 110
CARD_W = 660
CANVAS_W = PAD * 2 + CARD_W * 2 + COL_GAP

CARD_PAD_X = 22
CARD_PAD_TOP = 22
CARD_HEAD_GAP = 14
SECTION_LABEL_H = 22
SECTION_GAP = 14
PILL_H = 28
PILL_GAP_X = 8
PILL_GAP_Y = 8
PILL_PAD_X = 12
CHAR_W = 8.0     # approx for 13px monospace (SF Mono / Menlo); tuned visually
PILL_FONT = 13

def pill_width(text):
    return max(int(len(text) * CHAR_W) + PILL_PAD_X * 2, 60)

def layout_pills(fns, max_w):
    """Return (rows, total_height) where rows = list of [(x, w, text), ...]."""
    rows = []
    cur = []
    cur_w = 0
    for f in fns:
        w = pill_width(f)
        needed = w if not cur else cur_w + PILL_GAP_X + w
        if cur and needed > max_w:
            rows.append(cur)
            cur = [(0, w, f)]
            cur_w = w
        else:
            x = 0 if not cur else cur_w + PILL_GAP_X
            cur.append((x, w, f))
            cur_w = needed
    if cur:
        rows.append(cur)
    h = len(rows) * PILL_H + max(0, len(rows) - 1) * PILL_GAP_Y
    return rows, h

def card_height(card):
    inner_w = CARD_W - 2 * CARD_PAD_X
    h = CARD_PAD_TOP + 28 + 6 + 16 + CARD_HEAD_GAP   # title + subtitle + gap
    if card.sections:
        for i, sec in enumerate(card.sections):
            if i > 0:
                h += SECTION_GAP
            h += SECTION_LABEL_H
            _, ph = layout_pills(sec.fns, inner_w)
            h += ph
    else:
        _, ph = layout_pills(card.fns, inner_w)
        h += ph
    h += CARD_PAD_TOP   # bottom padding
    return h

def column_height(cards):
    return sum(card_height(c) for c in cards) + (len(cards) - 1) * CARD_GAP

# --- Render -------------------------------------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_pill(x, y, w, text, color, tint):
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" rx="13" ry="13" '
        f'width="{w}" height="{PILL_H}" fill="{tint}" stroke="{color}" '
        f'stroke-opacity="0.35" stroke-width="1"/>'
        f'<text x="{x + w/2:.1f}" y="{y + PILL_H/2 + 4:.1f}" '
        f'text-anchor="middle" font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" '
        f'font-size="{PILL_FONT}" fill="{color}" font-weight="600">{esc(text)}</text>'
    )

def render_card(x, y, card):
    inner_w = CARD_W - 2 * CARD_PAD_X
    h = card_height(card)
    out = []

    out.append(
        f'<rect x="{x}" y="{y}" rx="14" ry="14" width="{CARD_W}" height="{h}" '
        f'fill="#FFFFFF" stroke="#E5E7EB" stroke-width="1" '
        f'filter="url(#card-shadow)"/>'
    )
    # Top accent stripe - rounded only on top corners
    r = 14
    stripe_h = 5
    out.append(
        f'<path d="M {x} {y + r} '
        f'A {r} {r} 0 0 1 {x + r} {y} '
        f'H {x + CARD_W - r} '
        f'A {r} {r} 0 0 1 {x + CARD_W} {y + r} '
        f'V {y + stripe_h} '
        f'H {x} Z" fill="{card.color}"/>'
    )

    # Title block
    tx = x + CARD_PAD_X
    ty = y + CARD_PAD_TOP + 18
    # Function count badge to the right
    total = len(card.fns) if not card.sections else sum(len(s.fns) for s in card.sections)
    badge_text = f"{total} fns"
    badge_w = int(len(badge_text) * 7) + 22
    badge_x = x + CARD_W - CARD_PAD_X - badge_w
    badge_y = y + CARD_PAD_TOP + 4
    out.append(
        f'<rect x="{badge_x}" y="{badge_y}" rx="11" ry="11" '
        f'width="{badge_w}" height="22" fill="{card.color}"/>'
        f'<text x="{badge_x + badge_w/2}" y="{badge_y + 15.5}" '
        f'text-anchor="middle" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{esc(badge_text)}</text>'
    )

    out.append(
        f'<text x="{tx}" y="{ty}" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="20" font-weight="800" fill="#1B3139">{esc(card.title)}</text>'
    )
    out.append(
        f'<text x="{tx}" y="{ty + 20}" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="12" fill="#5A6878">{esc(card.subtitle)}</text>'
    )

    cy = ty + 20 + CARD_HEAD_GAP + 8

    if card.sections:
        for i, sec in enumerate(card.sections):
            if i > 0:
                cy += SECTION_GAP
            out.append(
                f'<text x="{tx}" y="{cy + 14}" font-family="Inter, -apple-system, system-ui, sans-serif" '
                f'font-size="11" font-weight="700" fill="#7A8794" letter-spacing="1.2">'
                f'{esc(sec.label.upper())}</text>'
            )
            cy += SECTION_LABEL_H
            rows, ph = layout_pills(sec.fns, inner_w)
            for r_idx, row in enumerate(rows):
                row_y = cy + r_idx * (PILL_H + PILL_GAP_Y)
                for px, pw, name in row:
                    out.append(render_pill(tx + px, row_y, pw, name, card.color, card.tint))
            cy += ph
    else:
        rows, ph = layout_pills(card.fns, inner_w)
        for r_idx, row in enumerate(rows):
            row_y = cy + r_idx * (PILL_H + PILL_GAP_Y)
            for px, pw, name in row:
                out.append(render_pill(tx + px, row_y, pw, name, card.color, card.tint))

    return "\n".join(out), h

