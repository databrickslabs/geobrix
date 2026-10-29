"""Tests for vizx._interactive (MapLibre-based plot_interactive).

The old folium-based tests were removed when the folium implementation
was retired. The new implementation is tested in test_interactive_maplibre.py.
This file retains the public-API wiring check that is still valid.
"""


def test_plot_interactive_exported():
    from databricks.labs.gbx import vizx

    assert "plot_interactive" in vizx.__all__
    assert hasattr(vizx, "plot_interactive")
