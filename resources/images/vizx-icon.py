#!/usr/bin/env python3
"""Generate the VizX package logo to match RasterX / VectorX / GridX.

A single isometric cube with three visible faces (top rhombus, left and right
parallelograms) drawn with a thick rounded outline in one package color, with
the package wordmark centered below.

VizX = visualization. Motifs:
  - right face : the shared 4-point compass / navigation star (accent color)
  - top  face : a colormap / palette — a row of distinct color swatches skewed
                onto the rhombus (the "viz = color mapping" cue)
  - left face : a small bar chart (4 bars of varying height) skewed onto the
                vertical parallelogram

Color: emerald green (distinct from RasterX teal-blue, VectorX orange,
GridX purple).

Each visible face is parameterized by a face origin plus two edge vectors;
motif coordinates (u, v) in [0,1] are mapped through that basis so grids/bars
sit in the cube's isometric perspective (same trick the references use).

Re-render after editing this script:

    python3 resources/images/vizx-icon.py        # writes resources/images/VizX.svg
    # High-res screenshot, then frame onto the sibling-icon canvas (7990x4098, AR 1.95,
    # artwork at ~0.91 of canvas height, centered) so VizX is a drop-in match for the
    # RasterX / VectorX / GridX PNGs. Do NOT just bbox-trim — that strips the canvas
    # whitespace and yields a portrait crop that renders too narrow in the icon lineup.
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=10 --window-size=860,460 \\
        --screenshot=/tmp/vizx_hi.png resources/images/VizX.svg
    python3 -c "from PIL import Image, ImageChops; \\
      s=Image.open('/tmp/vizx_hi.png').convert('RGB'); \\
      bb=ImageChops.difference(s, Image.new('RGB', s.size, (255,255,255))).getbbox(); \\
      c=s.crop(bb); H=4098; W=round(H*7990/4098); th=round(0.91*H); \\
      c=c.resize((round(c.width*th/c.height), th), Image.LANCZOS); \\
      cv=Image.new('RGB',(W,H),(255,255,255)); cv.paste(c,((W-c.width)//2,(H-c.height)//2)); \\
      cv.save('resources/images/VizX.png')"
"""
import math
import os

# --- Palette (emerald green) --------------------------------------------------

C_OUTLINE = "#1E7A45"   # dark green — cube outline + wordmark
C_FACE_LT = "#BFE6CE"   # light green — primary face fill (top)
C_FACE_MD = "#6FC795"   # mid green — secondary face fill (left/right)
C_ACCENT  = "#2E9E5B"   # accent green — compass star + chart marks

WHITE = "#FFFFFF"

# --- Canvas / cube geometry ---------------------------------------------------
# 2:1 canvas (matches the references' ~8000x4098 → 800x400 display ratio).
CANVAS_W = 800
CANVAS_H = 400

# Cube center (the meeting point of the three faces) and isometric half-extents.
# Tuned so cube width ≈ 0.265 * canvas (matches references) and the cube sits in
# the upper area with the wordmark below.
CX = 400.0          # horizontal center
CY = 168.0          # vertical center of the cube body (the front meeting vertex)
HALF_W = 132.0      # half the rhombus width (apex-to-side horizontal span)
RISE = 66.0         # vertical rise of the top rhombus (isometric: HALF_W/2)
SIDE_H = 132.0      # height of the vertical (left/right) faces

STROKE = 11.0       # outline weight (thick, rounded) — matches references

# --- Cube vertices ------------------------------------------------------------
# Top rhombus (4 corners), then the two lower-front corners.
#
#            T (top apex)
#          /   \
#        L       R          <- top rhombus side corners
#          \   /
#            M  (mid: top-front vertex, where all three faces meet)
#        |   |   |
#       BL   |   BR         <- bottom-left / bottom-right corners
#            B  (bottom-front vertex)
#
T  = (CX,            CY - RISE * 2)        # top apex of rhombus
L  = (CX - HALF_W,   CY - RISE)            # left corner of rhombus
R  = (CX + HALF_W,   CY - RISE)            # right corner of rhombus
M  = (CX,            CY)                   # center / front-top meeting vertex
BL = (CX - HALF_W,   CY - RISE + SIDE_H)   # bottom-left
BR = (CX + HALF_W,   CY - RISE + SIDE_H)   # bottom-right
B  = (CX,            CY + SIDE_H)          # bottom-front vertex


def _poly(pts, fill, *, stroke=C_OUTLINE, sw=STROKE):
    p = " ".join(f"{x:.2f},{y:.2f}" for x, y in pts)
    return (f'<polygon points="{p}" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="{sw}" stroke-linejoin="round"/>')


# --- Face basis: map (u, v) in [0,1] onto a skewed face -----------------------

def _face_map(origin, edge_u, edge_v):
    """Return f(u, v) -> (x, y) mapping unit-square coords onto a face.

    origin is the (u=0, v=0) corner; edge_u and edge_v are the two edge vectors
    (each a (dx, dy)). Lets motifs be drawn in face-local coords then skewed
    into the cube's isometric perspective.
    """
    ox, oy = origin
    (ux, uy), (vx, vy) = edge_u, edge_v

    def f(u, v):
        return (ox + u * ux + v * vx, oy + u * uy + v * vy)

    return f


def _sub(a, b):
    return (b[0] - a[0], b[1] - a[1])


# --- Motif: top face = colormap / palette swatches ---------------------------

def top_palette():
    """A row of distinct color swatches skewed onto the top rhombus.

    Top face spans L (left) -> T (top/back) along one edge and L -> M along the
    other. u runs L->R direction across the rhombus; v runs the depth direction.
    A viridis-ish set of swatches conveys "viz = color mapping".
    """
    # Origin L; edge_u toward R (across), edge_v toward... we need the 4th corner.
    # Rhombus corners in order: T (back), R (right), M (front), L (left).
    # Use L as origin, edge_u = L->M? Build basis from L with edges to T and M.
    # Actually map u along L->T->? Simpler: u from L->M is one diagonal; better to
    # use the two edges L->T and L->M? Those are adjacent edges of the rhombus.
    f = _face_map(L, _sub(L, T), _sub(L, M))
    # Now (u,v)=(0,0)->L, (1,0)->T, (0,1)->M, (1,1)->R. The unit square maps onto
    # the full rhombus.
    # Viridis-ish ramp.
    swatches = ["#2E9E5B", "#54B97B", "#8FD3A0", "#CFEAD2"]
    out = []
    n = len(swatches)
    margin = 0.12
    gap = 0.05
    span = 1 - 2 * margin
    cell = (span - (n - 1) * gap) / n
    for i, col in enumerate(swatches):
        u0 = margin + i * (cell + gap)
        u1 = u0 + cell
        v0, v1 = margin, 1 - margin
        quad = [f(u0, v0), f(u1, v0), f(u1, v1), f(u0, v1)]
        out.append(_poly(quad, col, stroke=C_OUTLINE, sw=2.4))
    return "".join(out)


# --- Motif: left face = bar chart --------------------------------------------

def left_bars():
    """A small bar chart (4 bars of varying height) skewed onto the left face.

    Left face corners: L (top-back), M (top-front), B (bottom-front),
    BL (bottom-back). Use BL as origin so v grows upward (bar height) and u runs
    along the bottom edge BL->B.
    """
    # (u,v)=(0,0)->BL, (1,0)->B (along bottom front edge), (0,1)->L (up back edge).
    f = _face_map(BL, _sub(BL, B), _sub(BL, L))
    heights = [0.40, 0.68, 0.50, 0.82]
    out = []
    n = len(heights)
    margin = 0.14
    gap = 0.06
    span = 1 - 2 * margin
    cell = (span - (n - 1) * gap) / n
    base_v = 0.16
    for i, h in enumerate(heights):
        u0 = margin + i * (cell + gap)
        u1 = u0 + cell
        v0 = base_v
        v1 = base_v + h * (1 - base_v - 0.12)
        quad = [f(u0, v0), f(u1, v0), f(u1, v1), f(u0, v1)]
        out.append(_poly(quad, WHITE, stroke=C_OUTLINE, sw=2.4))
    return "".join(out)


# --- Motif: right face = 4-point compass / navigation star -------------------

def right_compass():
    """The shared 4-pointed navigation star, centered on the right face.

    Right face corners: M (top-back/left), R (top-right), BR (bottom-right),
    B (bottom-front/left). Center the star at the face centroid and build the
    star in face-local (u, v) space so it shares the isometric skew.
    """
    f = _face_map(M, _sub(M, R), _sub(M, B))
    cu, cv = 0.5, 0.5
    # 4-point star: long points to N/S/E/W, short notches at the diagonals.
    longr = 0.48
    shortr = 0.10
    pts_uv = []
    for k in range(8):
        ang = math.radians(90 * (k // 2) + (45 if k % 2 else 0))
        r = longr if k % 2 == 0 else shortr
        pts_uv.append((cu + r * math.cos(ang), cv - r * math.sin(ang)))
    pts = [f(u, v) for u, v in pts_uv]
    out = [_poly(pts, C_ACCENT, stroke=C_OUTLINE, sw=2.6)]
    # center pivot dot
    ccx, ccy = f(cu, cv)
    out.append(f'<circle cx="{ccx:.2f}" cy="{ccy:.2f}" r="5.2" '
               f'fill="{WHITE}" stroke="{C_OUTLINE}" stroke-width="2.2"/>')
    return "".join(out)


# --- Wordmark -----------------------------------------------------------------

def wordmark():
    y = CY + SIDE_H + 96
    return (f'<text x="{CX:.1f}" y="{y:.1f}" text-anchor="middle" '
            f'font-family="Inter, -apple-system, Helvetica, Arial, sans-serif" '
            f'font-size="92" font-weight="800" fill="{C_OUTLINE}" '
            f'letter-spacing="-1">VizX</text>')


# --- Compose ------------------------------------------------------------------

def render():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}">',
        f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="{WHITE}"/>',
    ]

    # Face base fills first (so motif strokes sit on top), drawn with the thick
    # outline. Order: left, right, top (top drawn last to crown the cube).
    left_face  = [L, M, B, BL]
    right_face = [M, R, BR, B]
    top_face   = [T, R, M, L]

    parts.append(_poly(left_face, C_FACE_MD))
    parts.append(_poly(right_face, C_FACE_MD))
    parts.append(_poly(top_face, C_FACE_LT))

    # Motifs.
    parts.append(left_bars())
    parts.append(right_compass())
    parts.append(top_palette())

    # Re-stroke the three outer cube edges + the three inner edges on top so the
    # thick rounded outline reads cleanly above the motif fills.
    outline_paths = [
        # outer silhouette
        [T, R, BR, B, BL, L],
        # inner Y (three edges meeting at M)
        [L, M], [R, M], [B, M],
    ]
    parts.append(f'<polygon points="{" ".join(f"{x:.2f},{y:.2f}" for x,y in outline_paths[0])}" '
                 f'fill="none" stroke="{C_OUTLINE}" stroke-width="{STROKE}" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
    for seg in outline_paths[1:]:
        (x1, y1), (x2, y2) = seg
        parts.append(f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
                     f'stroke="{C_OUTLINE}" stroke-width="{STROKE}" '
                     f'stroke-linecap="round"/>')

    parts.append(wordmark())
    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    out = os.path.join(here, "VizX.svg")
    with open(out, "w") as f:
        f.write(render())
    print(f"wrote {out}")
