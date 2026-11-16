"""Export GeoBrix HTML diagrams (each `.dg` block) to static PNGs, in the light and dark themes.

Adapted from the Databricks Geospatial Hub diagram exporter. It drives the system Google Chrome
headless through Playwright. Animations are paused at a fixed time, so a re-export of an
unchanged source gives the same picture.

Usage (run through uvx, so nothing is installed globally):
    uvx --with playwright python -I export_diagrams.py SOURCE.html [...] --out-dir DIR

Each SOURCE.html writes DIR/<stem>-light.png and DIR/<stem>-dark.png (use --theme for one).
With --slide it writes DIR/<stem>-slide-light.png and -slide-dark.png instead: a 1920 x 1080 (16:9) frame
with the diagram scaled to fit and centred (the intro paragraph is left off the slide).
"""
from __future__ import annotations

import argparse
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


SLIDE_W, SLIDE_H = 1920, 1080
# A fixed 16:9 frame: the title block on top, the legend below, the SVG scaled to fit between them.
SLIDE_CSS = f"""
body{{padding:0}}
.dg{{width:{SLIDE_W}px;height:{SLIDE_H}px;box-sizing:border-box;margin:0;border-radius:0;padding:30px 44px 18px;
    display:flex;flex-direction:column}}
.dg h3{{font-size:34px}}
.dg .k{{font-size:12px}}
.dg .sub{{display:none}}
.dg svg>rect{{display:none}}
.dg h3{{margin-bottom:10px}}
.dg svg{{flex:1 1 0;min-height:0;width:100%;height:auto}}
.dg .legend{{font-size:13px;margin-top:8px}}
"""


def export(files: list[Path], out_dir: Path, themes: list[str], width: int, scale: float, at: float, slide: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if slide:
        width, scale = SLIDE_W + 16, 1.0
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": width, "height": SLIDE_H + 60 if slide else 1200}, device_scale_factor=scale)
        for html in files:
            for theme in themes:
                page.goto(html.resolve().as_uri())
                page.wait_for_load_state("load")
                page.evaluate("(t) => { document.documentElement.dataset.theme = t; }", theme)
                if slide:
                    page.add_style_tag(content=SLIDE_CSS)
                page.wait_for_timeout(400)
                blocks = page.query_selector_all(".dg")
                if len(blocks) != 1:
                    raise SystemExit(f"{html}: expected one .dg block, found {len(blocks)}")
                el = blocks[0]
                el.scroll_into_view_if_needed()
                page.evaluate(SET_TIME_JS, [el, at])
                png = out_dir / (f"{html.stem}-slide-{theme}.png" if slide else f"{html.stem}-{theme}.png")
                el.screenshot(path=str(png), type="png")
                print(f"wrote {png} ({theme})")
        browser.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="+", type=Path, help="diagram .html files")
    ap.add_argument("--out-dir", type=Path, required=True, help="folder for the PNGs")
    ap.add_argument("--theme", choices=["light", "dark", "both"], default="both")
    ap.add_argument("--width", type=int, default=1400, help="viewport width in CSS px (default 1400)")
    ap.add_argument("--scale", type=float, default=1.25, help="device scale factor (default 1.25)")
    ap.add_argument("--at", type=float, default=3.0, help="animation time to capture, in seconds (default 3.0)")
    ap.add_argument("--slide", action="store_true", help="export a 1920 x 1080 (16:9) slide PNG instead")
    a = ap.parse_args()
    themes = ["light", "dark"] if a.theme == "both" else [a.theme]
    export(a.sources, a.out_dir, themes, a.width, a.scale, a.at, a.slide)


if __name__ == "__main__":
    main()
