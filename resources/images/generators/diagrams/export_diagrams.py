"""Export GeoBrix HTML diagrams (each `.dg` block) to static PNGs and animated GIFs, in the light and dark themes.

Adapted from the Databricks Geospatial Hub diagram exporter. It drives the system Google Chrome
headless through Playwright. Capture is deterministic: SMIL and CSS animations are paused and stepped to
exact timestamps (no screen recording), so a re-export of an unchanged source gives the same picture
and the GIF loops close.

Usage (run through uvx, so nothing is installed globally):
    uvx --with playwright python -I export_diagrams.py SOURCE.html [...] --out-dir DIR
    uvx --with playwright --with imageio-ffmpeg python -I export_diagrams.py SOURCE.html --out-dir DIR --gif --slide

Each SOURCE.html writes DIR/<stem>-light.png and DIR/<stem>-dark.png (use --theme for one).
  --gif    also writes DIR/<stem>-<theme>.gif: 12 fps, 4.8 s loop (58 frames), 1400 px viewport x 1.25
           (1730 px wide), encoded with ffmpeg: one palette, Sierra dithering.
  --slide  also writes the full-bleed 16:9 export, DIR/<stem>-slide-<theme>.png (the first frame), at
           --slide-size (default 2560x1440). With --gif it also writes DIR/<stem>-slide-<theme>.gif
           (15 fps, 72 frames, the same encoding).
The static PNG is the frame at --warmup seconds (3.0), which is also the first frame of every GIF, so the
PNG and the GIF start on the same picture.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

from playwright.sync_api import sync_playwright

SET_TIME_JS = """
([el, t]) => {
  for (const svg of el.querySelectorAll('svg')) {
    if (svg.pauseAnimations) { svg.pauseAnimations(); svg.setCurrentTime(t); }
  }
  for (const a of el.getAnimations({subtree: true})) {
    a.pause();
    const timing = a.effect && a.effect.getComputedTiming();
    if (timing && timing.iterations === Infinity) { a.currentTime = t * 1000; }
    else { a.finish(); }
  }
}
"""

STAGE = {"dark": "#0f1e24", "light": "#f8f6f2"}  # the slide background is the diagram's own stage colour

# A full-bleed slide: square corners, the diagram laid out at LAYOUT px and scaled to fit the frame, centred
# on its own background colour (the Hub exporter's approach).
SLIDE_CSS = """
html, body { margin: 0 !important; padding: 0 !important; width: 100%; height: 100%;
             overflow: hidden; background: __STAGE__ !important; }
.dg { border-radius: 0 !important; margin: 0 !important; position: absolute; left: 0; top: 0;
      width: __LAYOUT__px; transform-origin: 0 0; box-sizing: border-box; }
"""

FIT_JS = """
([el, vw, vh, layout]) => {
  const h = el.offsetHeight;
  const s = Math.min(vw / layout, vh / h);
  const x = (vw - layout * s) / 2, y = (vh - h * s) / 2;
  el.style.transform = `translate(${x}px, ${y}px) scale(${s})`;
  return [h, s];
}
"""


def _one_block(page, html: Path):
    blocks = page.query_selector_all(".dg")
    if len(blocks) != 1:
        raise SystemExit(f"{html}: expected one .dg block, found {len(blocks)}")
    return blocks[0]


def export(files: list[Path], out_dir: Path, themes: list[str], width: int, scale: float, at: float,
           gif: bool = False, fps: int = 12, seconds: float = 4.8) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    n = max(1, round(fps * seconds))
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": width, "height": 1200}, device_scale_factor=scale)
        for html in files:
            for theme in themes:
                page.goto(html.resolve().as_uri())
                page.wait_for_load_state("load")
                page.evaluate("(t) => { document.documentElement.dataset.theme = t; }", theme)
                page.wait_for_timeout(400)
                el = _one_block(page, html)
                el.scroll_into_view_if_needed()
                page.evaluate(SET_TIME_JS, [el, at])
                png = out_dir / f"{html.stem}-{theme}.png"
                el.screenshot(path=str(png), type="png")
                print(f"wrote {png} ({theme})")
                if gif:
                    write_gif(page, el, out_dir / f"{html.stem}-{theme}.gif", n, fps, at)
        browser.close()


GIF_FILTER = ("split[a][b];[a]palettegen=max_colors=256:stats_mode=full:reserve_transparent=0[p];"
              "[b][p]paletteuse=dither=sierra2_4a:diff_mode=rectangle")


def encode_gif(frames_dir: Path, out: Path, fps: int) -> None:
    """ffmpeg (bundled by imageio-ffmpeg): one palette generated from all frames, Sierra error-diffusion
    dithering and changed-rectangle updates, so static areas do not shimmer. Loops forever."""
    import imageio_ffmpeg

    subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(), "-y", "-loglevel", "error", "-framerate", str(fps),
                    "-i", str(frames_dir / "f_%04d.png"), "-vf", GIF_FILTER, "-loop", "0", str(out)], check=True)


def write_gif(page, el, path: Path, n: int, fps: int, start: float) -> None:
    """n frames from `start`, one every 1/fps s, stepped to exact times, then encoded by encode_gif."""
    tmp = Path(tempfile.mkdtemp(prefix="dggif-"))
    try:
        for f in range(n):
            page.evaluate(SET_TIME_JS, [el, start + f / fps])
            el.screenshot(path=str(tmp / f"f_{f:04d}.png"), type="png")
        encode_gif(tmp, path, fps)
        print(f"wrote {path}: {n} frames @ {fps} fps, {path.stat().st_size / 1e6:.1f} MB")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def export_slides(files: list[Path], out_dir: Path, themes: list[str], size: tuple[int, int], at: float,
                  gif: bool = False, fps: int = 15, seconds: float = 4.8, layout: int = 1384) -> None:
    """16:9 (or any WxH) full-bleed exports. The PNG is the first frame. The GIF is encoded with ffmpeg
    (bundled by imageio-ffmpeg): one palette generated from all frames, Sierra error-diffusion dithering
    and changed-rectangle updates, so static areas do not shimmer."""
    out_w, out_h = size
    vw = 1600
    vh = round(vw * out_h / out_w)
    n = max(1, round(fps * seconds)) if gif else 1
    out_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": vw, "height": vh}, device_scale_factor=out_w / vw)
        for html in files:
            for theme in themes:
                page.goto(html.resolve().as_uri())
                page.wait_for_load_state("load")
                page.add_style_tag(content=SLIDE_CSS.replace("__LAYOUT__", str(layout)).replace("__STAGE__", STAGE[theme]))
                page.evaluate("(t) => { document.documentElement.dataset.theme = t; }", theme)
                page.wait_for_timeout(400)
                el = _one_block(page, html)
                page.evaluate(FIT_JS, [el, vw, vh, layout])
                tmp = Path(tempfile.mkdtemp(prefix="dgslide-"))
                try:
                    for f in range(n):
                        page.evaluate(SET_TIME_JS, [el, at + f / fps])
                        page.screenshot(path=str(tmp / f"f_{f:04d}.png"), type="png")
                    png = out_dir / f"{html.stem}-slide-{theme}.png"
                    shutil.copy(tmp / "f_0000.png", png)
                    print(f"wrote {png} ({theme}, {out_w}x{out_h})")
                    if gif:
                        out = out_dir / f"{html.stem}-slide-{theme}.gif"
                        encode_gif(tmp, out, fps)
                        print(f"wrote {out}: {out_w}x{out_h}, {n} frames @ {fps} fps, {out.stat().st_size / 1e6:.1f} MB")
                finally:
                    shutil.rmtree(tmp, ignore_errors=True)
        browser.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="+", type=Path, help="diagram .html files")
    ap.add_argument("--out-dir", type=Path, required=True, help="folder for the PNGs and GIFs")
    ap.add_argument("--theme", choices=["light", "dark", "both"], default="both")
    ap.add_argument("--width", type=int, default=1400, help="viewport width in CSS px (default 1400)")
    ap.add_argument("--scale", type=float, default=1.25, help="device scale factor (default 1.25)")
    ap.add_argument("--at", "--warmup", dest="at", type=float, default=3.0,
                    help="animation time of the PNG and of the first GIF frame, in seconds (default 3.0)")
    ap.add_argument("--gif", action="store_true", help="also export animated GIFs (12 fps, 4.8 s loop)")
    ap.add_argument("--fps", type=int, default=12, help="GIF frame rate (default 12)")
    ap.add_argument("--seconds", type=float, default=4.8, help="GIF loop length in seconds (default 4.8)")
    ap.add_argument("--slide", action="store_true", help="also export the full-bleed 16:9 slide (PNG, and GIF with --gif)")
    ap.add_argument("--slide-size", default="2560x1440", metavar="WxH", help="slide size in px (default 2560x1440)")
    ap.add_argument("--slide-fps", type=int, default=15, help="slide GIF frame rate (default 15)")
    a = ap.parse_args()
    themes = ["light", "dark"] if a.theme == "both" else [a.theme]
    export(a.sources, a.out_dir, themes, a.width, a.scale, a.at, a.gif, a.fps, a.seconds)
    if a.slide:
        w, h = (int(v) for v in a.slide_size.lower().split("x"))
        export_slides(a.sources, a.out_dir, themes, (w, h), a.at, a.gif, a.slide_fps, a.seconds)


if __name__ == "__main__":
    main()
