"""Tests for gbx.vizx public API exports (Task 12)."""

import databricks.labs.gbx.vizx as vizx


def test_new_public_symbols_exported():
    """Layer constructors, simplify helpers, and audit are accessible from the package."""
    for name in (
        "vector_layer",
        "raster_layer",
        "grid_layer",
        "pmtiles_layer",
        "simplify_tiles_from_source",
        "audit_layers",
    ):
        assert hasattr(vizx, name), f"missing: {name}"
        assert name in vizx.__all__, f"not in __all__: {name}"


def test_plot_tiles_exported():
    """plot_tiles is importable from databricks.labs.gbx.vizx and in __all__."""
    assert hasattr(vizx, "plot_tiles"), "plot_tiles missing from vizx"
    assert "plot_tiles" in vizx.__all__, "plot_tiles not in __all__"
    assert callable(vizx.plot_tiles), "plot_tiles is not callable"


def test_sri_hashes_are_real():
    """SRI hash constants are real sha384 values — no placeholder remains."""
    from databricks.labs.gbx.vizx import _maplibre as m

    assert m._MAPLIBRE_JS_SRI.startswith(
        "sha384-"
    ), f"_MAPLIBRE_JS_SRI is not a real hash: {m._MAPLIBRE_JS_SRI!r}"
    assert (
        "REPLACE" not in m._MAPLIBRE_JS_SRI
    ), f"_MAPLIBRE_JS_SRI still contains placeholder: {m._MAPLIBRE_JS_SRI!r}"
    assert m._MAPLIBRE_CSS_SRI.startswith(
        "sha384-"
    ), f"_MAPLIBRE_CSS_SRI is not a real hash: {m._MAPLIBRE_CSS_SRI!r}"
    assert (
        "REPLACE" not in m._MAPLIBRE_CSS_SRI
    ), f"_MAPLIBRE_CSS_SRI still contains placeholder: {m._MAPLIBRE_CSS_SRI!r}"
    assert m._PMTILES_JS_SRI.startswith(
        "sha384-"
    ), f"_PMTILES_JS_SRI is not a real hash: {m._PMTILES_JS_SRI!r}"
    assert (
        "REPLACE" not in m._PMTILES_JS_SRI
    ), f"_PMTILES_JS_SRI still contains placeholder: {m._PMTILES_JS_SRI!r}"


def test_build_html_contains_pinned_version_and_sri():
    """build_html output references the pinned maplibre + pmtiles versions with SRI."""
    from databricks.labs.gbx.vizx._maplibre import build_html

    html = build_html([])
    assert "maplibre-gl@4.7.1" in html
    assert "pmtiles@3.2.0" in html
    assert "REPLACE" not in html
    # integrity attributes are present (real SRI hashes) for JS and CSS
    assert 'integrity="sha384-' in html
    # CSS <link> tag has SRI integrity attribute
    assert 'rel="stylesheet" integrity="sha384-' in html


def test_layer_constructors_return_layer_objects():
    """Layer constructors from _layers are importable and return Layer instances."""
    from databricks.labs.gbx.vizx._layers import Layer

    assert isinstance(vizx.vector_layer(None), Layer)
    assert isinstance(vizx.raster_layer(None), Layer)
    assert isinstance(vizx.grid_layer(None, grid_system="h3"), Layer)
    assert isinstance(vizx.pmtiles_layer(b""), Layer)


def test_plot_interactive_dynamic_exported():
    """plot_interactive_dynamic is in __all__ and resolves to a callable via lazy import.

    The lazy import requires anywidget + traitlets (the [vizx] extra); if they are
    absent the __all__ membership is still asserted and the callable check is skipped.
    """
    assert (
        "plot_interactive_dynamic" in vizx.__all__
    ), "plot_interactive_dynamic missing from __all__"
    try:
        fn = vizx.plot_interactive_dynamic
        assert callable(fn), "plot_interactive_dynamic is not callable"
    except ImportError:
        # anywidget / traitlets not installed in this env — lazy guard is working.
        pass


def test_bare_vizx_import_safe():
    """Bare 'import databricks.labs.gbx.vizx' succeeds without anywidget/traitlets present."""
    import importlib

    mod = importlib.import_module("databricks.labs.gbx.vizx")
    # The lazy guard means plot_interactive_dynamic is not imported at module load time;
    # accessing it triggers the lazy branch which requires anywidget/traitlets.
    assert mod is not None


def test_build_pmtiles_html_is_gone():
    """_build_pmtiles_html and duplicate CDN constants were removed from _pmtiles."""
    import databricks.labs.gbx.vizx._pmtiles as p

    assert not hasattr(
        p, "_build_pmtiles_html"
    ), "_build_pmtiles_html was not removed from _pmtiles.py"
    assert not hasattr(
        p, "_MAPLIBRE_JS"
    ), "duplicate _MAPLIBRE_JS constant was not removed from _pmtiles.py"
    assert not hasattr(
        p, "_PMTILES_JS"
    ), "duplicate _PMTILES_JS constant was not removed from _pmtiles.py"
    assert not hasattr(
        p, "_MAPLIBRE_CSS"
    ), "duplicate _MAPLIBRE_CSS constant was not removed from _pmtiles.py"
