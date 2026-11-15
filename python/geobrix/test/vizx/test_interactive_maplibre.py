"""Tests for plot_interactive on MapLibre GL; folium retired."""

import builtins

import geopandas as gpd
from shapely.geometry import Point

from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._layers import vector_layer


def _block_display_channels(monkeypatch):
    """Force both notebook-display channels off so plot_interactive returns the HTML string."""
    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    real_import = builtins.__import__

    def _no_ipython_display(name, *args, **kwargs):
        if name == "IPython.display":
            raise ImportError("IPython.display disabled for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ipython_display)


def test_interactive_returns_maplibre_html_for_layers(monkeypatch):
    _block_display_channels(monkeypatch)
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    html = plot_interactive([vector_layer(gdf)])
    assert "maplibregl.Map" in html


def test_notebook_display_html_prefers_ipython_user_ns(monkeypatch):
    """When displayHTML is in the IPython user namespace, use it."""
    import databricks.labs.gbx.vizx._interactive as itx

    sentinel = object()

    class _IP:
        user_ns = {"displayHTML": sentinel}

    monkeypatch.setattr(itx, "get_ipython", lambda: _IP(), raising=False)
    assert itx._notebook_display_html() is sentinel


def test_notebook_display_html_falls_back_to_dbruntime(monkeypatch):
    """On Databricks runtimes where displayHTML is NOT in user_ns (Serverless /
    newer DBR kernels), resolve it via dbruntime.display so we still hit the
    iframe path (no Jupyter output-size cap) instead of IPython.display.HTML."""
    import sys
    import types

    import databricks.labs.gbx.vizx._interactive as itx

    # IPython present but displayHTML absent from user_ns.
    class _IP:
        user_ns: dict = {}

    monkeypatch.setattr(itx, "get_ipython", lambda: _IP(), raising=False)

    # Inject a fake dbruntime.display module exposing displayHTML.
    sentinel = object()
    fake_dbruntime = types.ModuleType("dbruntime")
    fake_display = types.ModuleType("dbruntime.display")
    fake_display.displayHTML = sentinel
    fake_dbruntime.display = fake_display
    monkeypatch.setitem(sys.modules, "dbruntime", fake_dbruntime)
    monkeypatch.setitem(sys.modules, "dbruntime.display", fake_display)

    assert itx._notebook_display_html() is sentinel


def test_notebook_display_html_none_when_unavailable(monkeypatch):
    """Plain Python / no notebook channel -> None (callers degrade)."""
    import sys

    import databricks.labs.gbx.vizx._interactive as itx

    class _IP:
        user_ns: dict = {}

    monkeypatch.setattr(itx, "get_ipython", lambda: _IP(), raising=False)
    monkeypatch.setitem(sys.modules, "dbruntime", None)
    monkeypatch.setitem(sys.modules, "dbruntime.display", None)
    assert itx._notebook_display_html() is None


def test_no_folium_import_in_vizx():
    import importlib
    import os
    import pkgutil
    import subprocess

    import databricks.labs.gbx.vizx as v

    for m in pkgutil.iter_modules(v.__path__):
        src = importlib.import_module(f"databricks.labs.gbx.vizx.{m.name}")
        assert "folium" not in (
            getattr(src, "__file__", "") or ""
        )  # sanity; real check below
    # grep-style: no module imports folium
    root = os.path.dirname(v.__file__)
    out = subprocess.run(
        ["grep", "-rl", "import folium", root], capture_output=True, text=True
    )
    assert out.stdout.strip() == "", f"folium still imported in: {out.stdout}"
