"""Execute the VizX raster rendering doc examples (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import vizx_raster as ex  # noqa: E402


def test_plot_mosaic_example():
    ex.plot_mosaic_example()
