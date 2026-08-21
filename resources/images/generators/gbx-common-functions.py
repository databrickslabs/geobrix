#!/usr/bin/env python3
"""Generate the GBX Common Functions architecture diagram SVG.

Three horizontal tiers — GENERIC (session-ful, top), FORMAT-SPECIFIC (middle),
SESSION-FREE CORE (bottom) — showing how the shared file_gbx base is layered and
where each boundary lies (FUSE vs FILE; generic vs format-specific).

Re-render after a change:

    python3 resources/images/generators/gbx-common-functions.py
    # then rasterize to PNG (used by docs/common-functions.mdx):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1400,900 \\
        --screenshot=resources/images/diagrams/rasterx/gbx-common-functions.png \\
        resources/images/diagrams/rasterx/gbx-common-functions.svg
    # then auto-crop whitespace:
    python3 -c "
from PIL import Image, ImageChops
p='resources/images/diagrams/rasterx/gbx-common-functions.png'
img=Image.open(p).convert('RGB')
bbox=ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
if bbox: img.crop(bbox).save(p)
"
"""
from textwrap import dedent

# --- Palette (shared with file-gbx-flow.py / rasterx-tile-structure.py) ---

C_INK     = "#0F1B2A"
C_INK_2   = "#1B3139"
C_MUTED   = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER  = "#E5E7EB"

# Lane accents (from the shared palette): READ = Source blue, WRITE = violet.
ACCENT_READ  = "#1F6FB5"; TINT_READ  = "#E3EEF8"   # blue — generic functions
ACCENT_WRITE = "#7A5AA6"; TINT_WRITE = "#ECE5F5"   # violet — format-specific

# FILE / governed-storage accent
C_FILE   = "#5060A8"; TINT_FILE = "#E7E9F6"

# FUSE floor — neutral slate
C_FUSE   = "#41546A"; TINT_FUSE = "#EAEEF3"

# No-gating: green = graceful auto-downgrade, red = explicit-FILE error
C_OK  = "#1E7D4B"; TINT_OK  = "#E0F2E9"
C_ERR = "#C0392B"; TINT_ERR = "#FBE7E4"

# --- Canvas -------------------------------------------------------------------

PAD      = 40
CANVAS_W = 1400
HEADER_H = 86

# Three tiers
TIER_GAP = 20
TIER1_H  = 220    # generic functions
TIER2_H  = 220    # format-specific
TIER3_H  = 130    # session-free core
PILL_OVERHANG = 24

# Stage columns
C1_X = 60;  C1_W = 320
C2_X = 440; C2_W = 380
C3_X = 880; C3_W = 460

# --- SVG helpers (shared conventions) -----------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def text(x, y, s, *, size=13, weight=400, fill=C_INK,
         family="Inter, -apple-system, system-ui, sans-serif",
         anchor="start", letter_spacing=None, opacity=None):
    ls = f' letter-spacing="{letter_spacing}"' if letter_spacing else ""
    op = f' opacity="{opacity}"' if opacity is not None else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{ls}{op}>'
            f'{esc(s)}</text>')

def mono(x, y, s, *, size=13, weight=500, fill=C_INK, anchor="start"):
    return text(x, y, s, size=size, weight=weight, fill=fill, anchor=anchor,
                family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace")

def card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True, dash=None):
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{ds}{flt}/>')

def top_stripe(x, y, w, color, *, r=14, h=5):
    return (f'<path d="M {x} {y + r} A {r} {r} 0 0 1 {x + r} {y} '
            f'H {x + w - r} A {r} {r} 0 0 1 {x + w} {y + r} V {y + h} H {x} Z" '
            f'fill="{color}"/>')

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False,
         h=22, size=12, pad_x=12):
    char_w = (0.60 if mono_font else 0.56) * size
    w = int(len(txt) * char_w) + pad_x * 2
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" width="{w}" '
           f'height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + 4}" text-anchor="middle" '
           f'font-family="{family}" font-size="{size}" font-weight="700" '
           f'fill="{fg}">{esc(txt)}</text>')
    return svg, w

def arrow_line(x1, y1, x2, y2, *, marker="arrow", color=C_MUTED_2, width=2.4, dash=None):
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{width}"{ds} marker-end="url(#{marker})"/>')

def arrow_curve(x1, y1, x2, y2, *, marker="arrow", color=C_MUTED_2, width=2.2, dash=None):
    mx = (x1 + x2) / 2
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<path d="M {x1:.1f} {y1:.1f} C {mx:.1f} {y1:.1f} {mx:.1f} {y2:.1f} '
            f'{x2:.1f} {y2:.1f}" fill="none" stroke="{color}" stroke-width="{width}"'
            f'{ds} marker-end="url(#{marker})"/>')

def lane_band(y, h, accent, tint, badge, descriptor):
    """Rounded lane container with accent pill overhanging the top."""
    out = [f'<rect x="{PAD}" y="{y}" rx="18" ry="18" width="{CANVAS_W - 2*PAD}" '
           f'height="{h}" fill="{tint}" fill-opacity="0.35" '
           f'stroke="{accent}" stroke-width="1.5"/>']
    out.append(f'<rect x="{PAD}" y="{y}" rx="18" ry="18" width="10" '
               f'height="{h}" fill="{accent}"/>')
    out.append(f'<rect x="{PAD + 5}" y="{y}" width="5" height="{h}" fill="{accent}"/>')
    pill, pw = chip(PAD + 22, y - PILL_OVERHANG, badge, fg="#FFFFFF", bg=accent,
                    h=30, size=15, pad_x=16)
    out.append(pill)
    out.append(mono(PAD + 22 + pw + 12, y - PILL_OVERHANG + 20, descriptor,
                    size=12, weight=600, fill=C_MUTED_2))
    return "".join(out)

# --- Render -------------------------------------------------------------------

def render():
    tier1_top = PAD + HEADER_H + 30
    tier2_top = tier1_top + TIER1_H + TIER_GAP + 30
    tier3_top = tier2_top + TIER2_H + TIER_GAP + 30
    canvas_h  = tier3_top + TIER3_H + PAD + 30

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {canvas_h}" '
        f'width="{CANVAS_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    ]
    parts.append(dedent(f'''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/><stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
          <marker id="arrow" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_MUTED_2}"/>
          </marker>
          <marker id="arrow-read" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_READ}"/>
          </marker>
          <marker id="arrow-write" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_WRITE}"/>
          </marker>
          <marker id="arrow-fuse" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_FUSE}"/>
          </marker>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 30, "GeoBrix · GBX Common Functions",
                      size=28, weight=800, fill=C_INK))
    parts.append(text(PAD, PAD + 56,
                      "One shared file-access base under every lightweight reader and writer — "
                      "generic session-ful functions on top, format-specific decoders/encoders "
                      "in the middle, session-free core floor below",
                      size=13.5, fill=C_MUTED))
    # Legend (top-right)
    lx = CANVAS_W - PAD - 360
    parts.append(f'<rect x="{lx}" y="{PAD + 12}" width="16" height="16" rx="3" '
                 f'fill="{TINT_FILE}" stroke="{C_FILE}" stroke-width="2"/>')
    parts.append(text(lx + 24, PAD + 25, "FILE — session-ful (managed / external)",
                      size=12, weight=600, fill=C_MUTED))
    parts.append(f'<rect x="{lx}" y="{PAD + 36}" width="16" height="16" rx="3" '
                 f'fill="{TINT_FUSE}" stroke="{C_FUSE}" stroke-width="2" stroke-dasharray="4 3"/>')
    parts.append(text(lx + 24, PAD + 49, "FUSE — always-available floor (no FILE column)",
                      size=12, weight=600, fill=C_MUTED))

    # ========================= TIER 1: GENERIC FUNCTIONS =========================
    parts.append(lane_band(tier1_top, TIER1_H, ACCENT_READ, TINT_READ,
                            "GENERIC", 'gbx_file_read / gbx_file_write  (session-ful, function layer)'))
    t1y = tier1_top + 22

    # --- READ box ---
    bx, bw, bh = C1_X + 14, C1_W - 28, 80
    parts.append(card(bx, t1y + 20, bw, bh, stroke=ACCENT_READ, shadow=False, r=10))
    parts.append(top_stripe(bx, t1y + 20, bw, ACCENT_READ, r=10))
    rc, rw = chip(bx + 12, t1y + 32, "gbx_file_read", fg=ACCENT_READ, bg=TINT_READ,
                  border=ACCENT_READ, mono_font=True, h=21, size=11)
    parts.append(rc)
    parts.append(text(bx + 12, t1y + 68, "spark, source, *, access=", size=10, weight=500, fill=C_MUTED))
    parts.append(mono(bx + 12 + 130, t1y + 68, '"auto"', size=10, weight=700, fill=ACCENT_READ))

    # returns badge
    rb, rw2 = chip(bx + 12, t1y + 80, "[path, size, file]", fg=C_FILE, bg=TINT_FILE,
                   border=C_FILE, mono_font=True, h=18, size=9.5, pad_x=8)
    parts.append(rb)

    # --- WRITE box ---
    bx2, bw2, bh2 = C1_X + 14, C1_W - 28, 80
    wy_box = t1y + 122
    parts.append(card(bx2, wy_box, bw2, bh2, stroke=ACCENT_WRITE, shadow=False, r=10))
    parts.append(top_stripe(bx2, wy_box, bw2, ACCENT_WRITE, r=10))
    wc, _ = chip(bx2 + 12, wy_box + 12, "gbx_file_write", fg=ACCENT_WRITE, bg=TINT_WRITE,
                 border=ACCENT_WRITE, mono_font=True, h=21, size=11)
    parts.append(wc)
    parts.append(text(bx2 + 12, wy_box + 48, "df, target, *, file_mode=", size=10, weight=500, fill=C_MUTED))
    parts.append(mono(bx2 + 12 + 140, wy_box + 48, '"auto"', size=10, weight=700, fill=ACCENT_WRITE))
    parts.append(text(bx2 + 12, wy_box + 65, "returns None (writes Delta table)", size=10, weight=500, fill=C_MUTED_2))

    # --- NO-GATING callout ---
    ngy = t1y + 20
    ngx = C2_X + 8
    ngw = CANVAS_W - ngx - PAD - 20
    ngh = 180
    parts.append(card(ngx, ngy, ngw, ngh, stroke=C_BORDER, shadow=True, r=12))
    parts.append(top_stripe(ngx, ngy, ngw, C_INK_2, r=12, h=5))

    parts.append(text(ngx + 22, ngy + 32, "The two boundaries", size=14, weight=800, fill=C_INK))

    # Boundary 1: FUSE vs FILE
    parts.append(text(ngx + 22, ngy + 58, "FUSE vs FILE", size=11, weight=700,
                      fill=C_MUTED, letter_spacing="0.8"))
    parts.append(text(ngx + 22, ngy + 76,
                      "DataSource readers/writers (spark.read.format / df.write.format) are",
                      size=10.5, weight=500, fill=C_MUTED))
    parts.append(text(ngx + 22, ngy + 92,
                      "FUSE-only (session-less on Connect). FILE-tier access lives here, in",
                      size=10.5, weight=500, fill=C_MUTED))
    parts.append(text(ngx + 22, ngy + 108,
                      "the function layer, where a SparkSession is present.", size=10.5,
                      weight=500, fill=C_MUTED))

    # Boundary 2: Generic vs Format-specific
    parts.append(text(ngx + 22, ngy + 130, "Generic vs format-specific", size=11, weight=700,
                      fill=C_MUTED, letter_spacing="0.8"))
    parts.append(text(ngx + 22, ngy + 148,
                      "gbx_file_read / gbx_file_write move path references. The format-specific",
                      size=10.5, weight=500, fill=C_MUTED))
    parts.append(text(ngx + 22, ngy + 164,
                      "decoders/encoders (rst_fromfile, vector_file_read) understand the payload.",
                      size=10.5, weight=500, fill=C_MUTED))

    # ========================= TIER 2: FORMAT-SPECIFIC =========================
    parts.append(lane_band(tier2_top, TIER2_H, ACCENT_WRITE, TINT_WRITE,
                            "FORMAT-SPECIFIC",
                            "rst_fromfile / vector_file_read / write_file_table  (decoders/encoders)"))
    t2y = tier2_top + 22

    # Raster decode
    rx_bx = C1_X + 14; rx_bw = (CANVAS_W - 2*PAD - 28) // 2 - 20; rx_bh = 80
    parts.append(card(rx_bx, t2y + 20, rx_bw, rx_bh, stroke=ACCENT_WRITE, shadow=False, r=10))
    parts.append(top_stripe(rx_bx, t2y + 20, rx_bw, ACCENT_WRITE, r=10))
    rc2, _ = chip(rx_bx + 12, t2y + 32, "rst_fromfile", fg=ACCENT_WRITE, bg=TINT_WRITE,
                  border=ACCENT_WRITE, mono_font=True, h=21, size=11)
    parts.append(rc2)
    parts.append(text(rx_bx + 12, t2y + 68, "path → tile struct  (raster decode)", size=10,
                      weight=500, fill=C_MUTED))
    parts.append(text(rx_bx + 12, t2y + 82, "virtual by default; pass materialize=True for bytes",
                      size=9.5, weight=500, fill=C_MUTED_2))

    # Vector read
    vr_bx = C1_X + 14 + rx_bw + 20; vr_bw = rx_bw; vr_bh = 80
    parts.append(card(vr_bx, t2y + 20, vr_bw, vr_bh, stroke=ACCENT_WRITE, shadow=False, r=10,
                      dash="5 4"))
    parts.append(top_stripe(vr_bx, t2y + 20, vr_bw, ACCENT_WRITE, r=10))
    vc2, _ = chip(vr_bx + 12, t2y + 32, "vector_file_read", fg=ACCENT_WRITE, bg=TINT_WRITE,
                  border=ACCENT_WRITE, mono_font=True, h=21, size=11)
    parts.append(vc2)
    parts.append(text(vr_bx + 12, t2y + 68, "spark, path, *, driver  → geometry rows",
                      size=10, weight=500, fill=C_MUTED))
    parts.append(text(vr_bx + 12, t2y + 82, "FILE-leveraged via mapInPandas + pyogrio",
                      size=9.5, weight=500, fill=C_MUTED_2))

    # Write side: write_file_table / vector_file_write
    wt_bx = C1_X + 14; wt_bw = rx_bw; wt_bh = 80
    parts.append(card(wt_bx, t2y + 122, wt_bw, wt_bh, stroke=C_FILE, shadow=False, r=10))
    parts.append(top_stripe(wt_bx, t2y + 122, wt_bw, C_FILE, r=10))
    wtc, _ = chip(wt_bx + 12, t2y + 134, "write_file_table", fg=C_FILE, bg=TINT_FILE,
                  border=C_FILE, mono_font=True, h=21, size=11)
    parts.append(wtc)
    parts.append(text(wt_bx + 12, t2y + 170, "raster write → FILE-column Delta table",
                      size=10, weight=500, fill=C_MUTED))
    parts.append(text(wt_bx + 12, t2y + 184, "MANAGED / EXTERNAL / FUSE modes",
                      size=9.5, weight=500, fill=C_MUTED_2))

    vw_bx = vr_bx; vw_bw = vr_bw; vw_bh = 80
    parts.append(card(vw_bx, t2y + 122, vw_bw, vw_bh, stroke=C_FILE, shadow=False, r=10,
                      dash="5 4"))
    parts.append(top_stripe(vw_bx, t2y + 122, vw_bw, C_FILE, r=10))
    vwc, _ = chip(vw_bx + 12, t2y + 134, "vector_file_write", fg=C_FILE, bg=TINT_FILE,
                  border=C_FILE, mono_font=True, h=21, size=11)
    parts.append(vwc)
    parts.append(text(vw_bx + 12, t2y + 170, "vector write → FILE-column Delta table",
                      size=10, weight=500, fill=C_MUTED))
    parts.append(text(vw_bx + 12, t2y + 184, "pyogrio-based, Connect-safe",
                      size=9.5, weight=500, fill=C_MUTED_2))

    # ========================= TIER 3: SESSION-FREE CORE =========================
    parts.append(lane_band(tier3_top, TIER3_H, C_FUSE, TINT_FUSE,
                            "SESSION-FREE CORE",
                            "list_local_files / enumerate_files / to_local_path  (no SparkSession required)"))
    t3y = tier3_top + 22

    funcs = [
        ("list_local_files",  "path, *, extensions=None  → list[str]", "sorted paths — FUSE only, every DataSource reader uses this"),
        ("enumerate_files",   "path, *, spark=None  → DataFrame | list[dict]", "FILE-capable when session present, FUSE fallback otherwise"),
        ("to_local_path",     "path → str",                                     "dbfs:/Volumes/... → /Volumes/... scheme normalization"),
    ]
    col_w = (CANVAS_W - 2 * PAD - 28) // len(funcs) - 10
    for i, (fn, sig, note) in enumerate(funcs):
        fx = C1_X + 14 + i * (col_w + 10)
        parts.append(card(fx, t3y + 14, col_w, 88, stroke=C_FUSE, shadow=False, r=10))
        parts.append(top_stripe(fx, t3y + 14, col_w, C_FUSE, r=10))
        fc, _ = chip(fx + 10, t3y + 26, fn, fg=C_FUSE, bg=TINT_FUSE, border=C_FUSE,
                     mono_font=True, h=20, size=10)
        parts.append(fc)
        parts.append(mono(fx + 10, t3y + 62, sig, size=9, weight=500, fill=C_MUTED))
        parts.append(text(fx + 10, t3y + 80, note, size=9, weight=500, fill=C_MUTED_2))

    # ========================= ARROWS =========================
    # Tier-1 generic → Tier-2 format-specific (down arrows)
    mid_t1  = tier1_top + TIER1_H + 2
    mid_t2  = tier2_top - 2
    cx_left  = C1_X + 14 + (C1_W - 28) // 2
    cx_right = (C1_X + 14 + rx_bw + 20) + vr_bw // 2
    for cx in (cx_left, cx_right):
        parts.append(arrow_line(cx, mid_t1, cx, mid_t2,
                                marker="arrow", color=C_MUTED_2, width=2, dash="5 4"))

    # Tier-2 format-specific → Tier-3 core (down arrows)
    mid_t2b = tier2_top + TIER2_H + 2
    mid_t3  = tier3_top - 2
    for i in range(3):
        fx_c = C1_X + 14 + i * (col_w + 10) + col_w // 2
        parts.append(arrow_line(fx_c, mid_t2b, fx_c, mid_t3,
                                marker="arrow-fuse", color=C_FUSE, width=1.8, dash="4 3"))

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 12,
                      "databrickslabs/geobrix  ·  lightweight tier  ·  file_gbx common functions",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 12, "docs/common-functions",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    import os
    import sys
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "diagrams", "rasterx", "gbx-common-functions.svg")
    out = sys.argv[1] if len(sys.argv) > 1 else default
    with open(out, "w") as f:
        f.write(render())
        f.write("\n")
    print(f"wrote {out}")
