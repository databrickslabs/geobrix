"""Execute the VizX viewers doc examples (Docker)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import vizx_viewers as ex  # noqa: E402


def test_pmtiles_info_example():
    ex.pmtiles_info_example()


# The static-render demo (max_embed_mb=0) intentionally emits benign UserWarnings
# — the "falling back to static render" fallback notice and a contextily
# "inferred zoom level … not valid for the current tile provider" notice — which
# pytest.ini's `-W error::UserWarning` would otherwise promote to failures. Scope
# an ignore to this one demo test; the example still asserts a figure was produced.
@pytest.mark.filterwarnings("ignore::UserWarning")
def test_plot_pmtiles_static_example():
    ex.plot_pmtiles_static_example()


def test_plot_cog_example():
    ex.plot_cog_example()
