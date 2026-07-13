"""Offline tests for plot_pmtiles (interactive HTML + static fallback)."""

import io

import matplotlib
import numpy as np
import pytest
from matplotlib.image import imsave
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

matplotlib.use("Agg")

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


_VL_DEFAULT = object()  # sentinel: distinguish "not given" from None ("omit metadata")


def _build_archive(tiles, tile_type, *, name="demo", vector_layers=_VL_DEFAULT):
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(
        tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])
    ):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    md = {"name": name}
    if vector_layers is _VL_DEFAULT:
        md["vector_layers"] = [{"id": "demo"}]
    elif vector_layers is not None:
        md["vector_layers"] = vector_layers
    # vector_layers=None -> omit entirely (a metadata-less archive)
    w.finalize(header, md)
    return buf.getvalue()


def test_is_raster_type():
    from databricks.labs.gbx.vizx import _pmtiles as p

    assert p._is_raster_type("png") is True
    assert p._is_raster_type("jpeg") is True
    assert p._is_raster_type("webp") is True
    assert p._is_raster_type("avif") is True
    assert p._is_raster_type("mvt") is False
    assert p._is_raster_type("unknown") is False


def test_pmtiles_layer_opacity_and_color_reach_paint():
    """pmtiles_layer(opacity=, color=) sets raster-opacity (raster) / fill paint (vector),
    so a hillshade underlay can be dimmed and buildings styled in a multi-layer overlay.
    """
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _pmtiles

    raster = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    _, rlayers, _ = _pmtiles(pmtiles_layer(raster, opacity=0.55), 0)
    assert rlayers[0]["type"] == "raster"
    assert rlayers[0]["paint"]["raster-opacity"] == 0.55
    # opacity set -> emphasis must not override it (no raster-opacity in the pending list)
    assert "raster-opacity" not in rlayers[0].get("_gbx_emphasis", [])

    vec = _build_archive([(0, 0, 0, _real_mvt_tile(0, 0, 0))], TileType.MVT)
    _, vlayers, _ = _pmtiles(pmtiles_layer(vec, opacity=0.25, color="#3a3a3a"), 1)
    assert vlayers[0]["type"] == "fill"
    assert vlayers[0]["paint"]["fill-opacity"] == 0.25
    assert vlayers[0]["paint"]["fill-color"] == "#3a3a3a"


def test_pmtiles_renders_all_declared_source_layers():
    """A multi-layer vector archive renders EVERY declared source-layer (not just the
    first), each with fill+line+circle sub-layers so any geometry draws, and a distinct
    palette color per layer when no explicit pmtiles_layer color is set.
    """
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _pmtiles

    vec = _build_archive(
        [(0, 0, 0, _real_mvt_tile(0, 0, 0))],
        TileType.MVT,
        vector_layers=[{"id": "hotspots"}, {"id": "plumes"}, {"id": "wells"}],
    )
    _, layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    assert {ly["source-layer"] for ly in layers} == {"hotspots", "plumes", "wells"}
    types_per = {}
    for ly in layers:
        types_per.setdefault(ly["source-layer"], set()).add(ly["type"])
    for sl in ("hotspots", "plumes", "wells"):
        assert types_per[sl] == {"fill", "line", "circle"}
    fill_colors = {
        ly["source-layer"]: ly["paint"]["fill-color"]
        for ly in layers
        if ly["type"] == "fill"
    }
    assert len(set(fill_colors.values())) == 3  # distinct palette color per layer
    # Each sub-layer is filtered to its geometry type so polygon layers don't also draw
    # spurious circles at every vertex (fill=Polygon, circle=Point, line=non-Point).
    for ly in layers:
        if ly["type"] == "circle":
            assert ly["filter"] == [
                "match",
                ["geometry-type"],
                ["Point", "MultiPoint"],
                True,
                False,
            ]
        elif ly["type"] == "fill":
            assert ly["filter"] == [
                "match",
                ["geometry-type"],
                ["Polygon", "MultiPolygon"],
                True,
                False,
            ]
        elif ly["type"] == "line":
            assert ly["filter"] == [
                "match",
                ["geometry-type"],
                ["Point", "MultiPoint"],
                False,
                True,
            ]


def test_pmtiles_emits_categorical_legend_per_source_layer():
    """A multi-layer vector archive emits a categorical legend (one color swatch per
    source-layer) so the rendered map is legible, and build_html renders those swatches.
    """
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import (
        _pmtiles,
        build_html,
        layer_to_sources_layers,
    )

    vec = _build_archive(
        [(0, 0, 0, _real_mvt_tile(0, 0, 0))],
        TileType.MVT,
        vector_layers=[{"id": "hotspots"}, {"id": "plumes"}, {"id": "wells"}],
    )
    src, layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    (sid,) = list(src)
    legend = src[sid]["_gbx_legend"]
    assert [it["label"] for it in legend["items"]] == ["hotspots", "plumes", "wells"]
    colors = {it["color"] for it in legend["items"]}
    assert len(colors) == 3  # distinct palette swatch per layer

    # the legend swatches reach the rendered HTML, and the sidecar is stripped from
    # the MapLibre source (unknown source keys are rejected by MapLibre).
    html = build_html([layer_to_sources_layers(pmtiles_layer(vec), 0)])
    for sl in ("hotspots", "plumes", "wells"):
        assert f">{sl}<" in html
    assert "_gbx_legend" not in html


def test_pmtiles_source_declares_archive_zoom_range():
    """The MapLibre source declares the archive's minzoom/maxzoom so the renderer
    overzooms the top-level tiles instead of requesting non-existent tiles past max_zoom
    (the "jumpy" zoom where features blink out and snap back)."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _pmtiles

    # tiles spanning z10..z12 -> archive min_zoom 10, max_zoom 12
    vec = _build_archive(
        [
            (10, 163, 395, _real_mvt_tile(10, 163, 395)),
            (12, 654, 1583, _real_mvt_tile(12, 654, 1583)),
        ],
        TileType.MVT,
    )
    src, _layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    (sid,) = list(src)
    assert src[sid]["minzoom"] == 10
    assert src[sid]["maxzoom"] == 12


def test_pmtiles_discovers_source_layer_from_tiles_when_no_metadata():
    """No `vector_layers` metadata -> discover the source-layer from the tiles
    (the "demo" layer _real_mvt_tile encodes), never a guessed domain name."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _pmtiles

    vec = _build_archive(
        [(0, 0, 0, _real_mvt_tile(0, 0, 0))], TileType.MVT, vector_layers=None
    )
    _, layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    assert {ly["source-layer"] for ly in layers} == {
        "demo"
    }  # from the tile, not a guess
    assert len(layers) == 3  # fill + line + circle


def test_archive_bytes_passthrough_and_path(tmp_path):
    from databricks.labs.gbx.vizx import _pmtiles as p

    raw = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    assert p._archive_bytes(raw) == raw
    f = tmp_path / "a.pmtiles"
    f.write_bytes(raw)
    assert p._archive_bytes(str(f)) == raw
    assert p._archive_bytes("dbfs:" + str(f)) == raw


def _real_png_tile():
    # A real 8x8 RGB PNG so PIL Image.open can decode it.
    buf = io.BytesIO()
    imsave(buf, (np.random.rand(8, 8, 3)), format="png")
    return buf.getvalue()


def _real_mvt_tile(z, x, y):
    # Encode a polygon in tile-local pixel space for tile (z,x,y) (origin NW),
    # the same convention pyvx writes, so the fallback reprojects it back to 4326.
    import mapbox_vector_tile as mvt
    from shapely.geometry import box

    return mvt.encode(
        {
            "name": "demo",
            "features": [
                {"geometry": box(1000, 1000, 3000, 3000), "properties": {"v": 1}}
            ],
        },
        default_options={"extents": 4096, "y_coord_down": True},
    )


def test_plot_pmtiles_interactive_routes_through_displayhtml(monkeypatch):
    """plot_pmtiles delegates to plot_interactive which uses _notebook_display_html."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    captured = {}
    # plot_pmtiles now delegates to plot_interactive, which calls _notebook_display_html
    # from _interactive module.
    monkeypatch.setattr(
        "databricks.labs.gbx.vizx._interactive._notebook_display_html",
        lambda: (lambda html: captured.update(html=html)),
    )
    out = p.plot_pmtiles(archive)  # small -> interactive
    assert out is None  # displayHTML render returns None
    html = captured["html"]
    # HTML comes from _maplibre.build_html (pinned at maplibre-gl@4.7.1 / pmtiles@3.2.0)
    assert "maplibre-gl@4.7.1" in html and "pmtiles@3.2.0" in html
    assert "pmtiles://" in html


def test_plot_pmtiles_size_guard_uses_static_fallback(monkeypatch):
    """When the archive exceeds the budget, plot_pmtiles falls back to the static path.

    Task 7: plot_pmtiles delegates entirely to plot_interactive → prepare_layers.
    When the budget is exceeded, prepare_layers returns mode='static' and
    plot_interactive calls plot_static. We verify plot_static is called and a
    warning is issued by prepare_layers.
    """
    import warnings

    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)

    # Stub plot_static to avoid actual rendering (raw tile bytes aren't a full GeoTIFF).
    called = {}
    import databricks.labs.gbx.vizx._interactive as itx
    import databricks.labs.gbx.vizx._static_map as sm

    monkeypatch.setattr(sm, "plot_static", lambda *a, **kw: called.update(static=True))
    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        # tiny max_embed_mb forces static path
        p.plot_pmtiles(archive, max_embed_mb=1e-9)
    # A prepare_layers warning must have been issued.
    assert any(
        "fallback" in str(w.message).lower() or "exceed" in str(w.message).lower()
        for w in ws
    )
    # plot_static was called.
    assert called.get("static") is True


def test_plot_pmtiles_size_guard_uses_vector_static_fallback(monkeypatch):
    """Vector pmtiles over budget delegates to prepare_layers static path."""
    import builtins
    import warnings

    from databricks.labs.gbx.vizx import _pmtiles as p

    blob = _real_mvt_tile(10, 163, 395)
    archive = _build_archive([(10, 163, 395, blob)], TileType.MVT)

    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(archive, max_embed_mb=1e-9)
    # A fallback warning must have been issued by prepare_layers.
    assert any(
        "fallback" in str(w.message).lower() or "exceed" in str(w.message).lower()
        for w in ws
    )


def test_plot_pmtiles_oversized_without_fallback_raises():
    """fallback=False on an oversized archive raises ValueError via prepare_layers."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    with pytest.raises(ValueError, match="exceeds budget"):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, fallback=False)


def test_plot_pmtiles_unknown_tile_type_treated_as_vector(monkeypatch):
    """Task 7: unknown tile types are treated as vector (no validation error).

    The old plot_pmtiles explicitly validated tile type and raised ValueError.
    After Task 7, plot_pmtiles delegates to plot_interactive → prepare_layers
    → layer_to_sources_layers, which treats unknown tile types as vector
    (no raise). This documents the new expected behavior.
    """
    import builtins

    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)

    def _fake_pmtiles_info(data):
        return {
            "tile_type": "unknown",
            "tile_compression": "none",
            "min_zoom": 0,
            "max_zoom": 0,
            "bounds": (-122.52, 37.70, -122.35, 37.83),
            "center": (-122.44, 37.76, 0),
            "tile_count": 1,
            "metadata": {},
        }

    monkeypatch.setattr(
        "databricks.labs.gbx.pmtiles.pmtiles_info",
        _fake_pmtiles_info,
    )

    # Block display channels so we get the HTML string back.
    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    # Should NOT raise; unknown tile_type is treated as vector by _maplibre._pmtiles.
    html = p.plot_pmtiles(archive)
    assert html is not None
    assert "maplibregl.Map" in html


def test_public_exports():
    import databricks.labs.gbx.vizx as vizx

    assert hasattr(vizx, "plot_pmtiles")
    assert hasattr(vizx, "plot_cog")
    assert "plot_pmtiles" in vizx.__all__
    assert "plot_cog" in vizx.__all__


# ---------------------------------------------------------------------------
# interactive_fit='downzoom' — auto-fit an oversized archive to stay interactive
# ---------------------------------------------------------------------------


def _multi_zoom_vector_archive():
    import os

    tiles = []
    for z in range(3):
        n = 2**z
        for x in range(n):
            for y in range(n):
                tiles.append((z, x, y, os.urandom(4096)))
    return _build_archive(tiles, TileType.MVT)


def test_plot_pmtiles_interactive_fit_downzoom_stays_interactive(monkeypatch):
    """interactive_fit='downzoom' reduces an over-budget archive so it embeds inline."""
    import builtins

    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _multi_zoom_vector_archive()

    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    # Budget below the full archive but above the coarse levels: without
    # interactive_fit this would fall back to static; with 'downzoom' it down-zooms
    # and stays interactive (MapLibre HTML).
    target_mb = (len(archive) * 0.6) / 1_048_576
    html = p.plot_pmtiles(archive, max_embed_mb=target_mb, interactive_fit="downzoom")
    assert html is not None
    assert (
        "maplibregl.Map" in html
    ), "interactive_fit='downzoom' should stay interactive"


def test_plot_pmtiles_interactive_fit_none_does_not_reduce(monkeypatch):
    """Explicit interactive_fit=None over budget must NOT auto-fit: the autofit
    reducer is never called and the path routes to static fallback, rather than
    down-zooming to stay interactive. (The default is now 'downzoom'; None opts out.)"""
    import databricks.labs.gbx.vizx._pmtiles_autofit as af
    from databricks.labs.gbx.vizx import _pmtiles as p

    called = []
    real = af.autofit_archive

    def _spy(*a, **kw):
        called.append(True)
        return real(*a, **kw)

    monkeypatch.setattr(af, "autofit_archive", _spy)

    archive = _multi_zoom_vector_archive()
    # Over budget with the reducer explicitly OFF -> reducer must NOT run; over-budget
    # routes to static (which raises on these synthetic non-MVT tiles -- that's
    # fine, it proves we did NOT stay interactive and did NOT auto-fit).
    with pytest.raises(Exception):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, fallback=True, interactive_fit=None)
    assert not called, "interactive_fit=None must not invoke the autofit reducer"


def test_plot_pmtiles_interactive_fit_all_not_yet_implemented():
    """interactive_fit='all' is the planned multi-shard halo feature; until built it
    must raise a clear NotImplementedError, not silently degrade."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _multi_zoom_vector_archive()
    with pytest.raises(NotImplementedError, match="interactive_fit='all'"):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, interactive_fit="all")


def test_plot_pmtiles_interactive_fit_rejects_bad_value():
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _multi_zoom_vector_archive()
    with pytest.raises(ValueError, match="interactive_fit"):
        p.plot_pmtiles(archive, interactive_fit="bogus")


# ---------------------------------------------------------------------------
# Regression: _maybe_gunzip — gzip-compressed tile decoding (github issue fix)
# PMTiles archives with tile_compression=gzip yield raw gzip-wrapped bytes from
# all_tiles/MemorySource; rasterio.MemoryFile and mapbox_vector_tile.decode both
# reject gzip bytes, so they must be inflated before decoding.
# ---------------------------------------------------------------------------


def test_maybe_gunzip_idempotent_on_raw():
    """_maybe_gunzip is a no-op when the payload is already raw (not gzip)."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    raw = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
    # Non-gzip passthrough
    assert p._maybe_gunzip(raw) == raw
    # Also idempotent on empty bytes
    assert p._maybe_gunzip(b"") == b""


def test_maybe_gunzip_decompresses_gzip():
    """_maybe_gunzip decompresses a gzip-wrapped payload to the original bytes."""
    import gzip

    from databricks.labs.gbx.vizx import _pmtiles as p

    original = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    compressed = gzip.compress(original)
    assert compressed[:2] == b"\x1f\x8b", "test pre-condition: gzip magic"
    result = p._maybe_gunzip(compressed)
    assert result == original


def test_decode_mvt_to_geoms_strips_gzip(monkeypatch):
    """_decode_mvt_to_geoms de-gzips an MVT payload before passing to mvt.decode.

    Regression for: mapbox_vector_tile.decode rejecting gzip-wrapped MVT bytes
    when tile_compression=gzip.  We gzip-compress a real encoded MVT payload and
    assert that _decode_mvt_to_geoms still returns valid geometries.
    """
    import gzip

    import mapbox_vector_tile as mvt
    from shapely.geometry import box

    from databricks.labs.gbx.vizx import _pmtiles as p

    # Encode a simple polygon tile
    raw_mvt = mvt.encode(
        {
            "name": "demo",
            "features": [
                {"geometry": box(500, 500, 3500, 3500), "properties": {"v": 42}}
            ],
        },
        default_options={"extents": 4096, "y_coord_down": True},
    )
    gzipped_mvt = gzip.compress(raw_mvt)
    assert gzipped_mvt[:2] == b"\x1f\x8b"

    z, x, y = 10, 163, 395
    result = p._decode_mvt_to_geoms(gzipped_mvt, z, x, y)
    assert len(result) >= 1, "no geometries decoded from gzip-compressed MVT"
    geom, props = result[0]
    assert not geom.is_empty
    assert props.get("v") == 42


# ---------------------------------------------------------------------------
# Regression: live static-fallback path (_decode_pmtiles_for_static)
# ---------------------------------------------------------------------------


def test_plot_pmtiles_static_raster_no_rasterioioerror(monkeypatch):
    """plot_pmtiles(archive, max_embed_mb=0) must not raise RasterioIOError for raster archives.

    Regression for NB02 crash: the dead _static_raster_fallback passed raw tile bytes
    to plot_raster/rasterio (not georeferenced), which raised RasterioIOError.
    The live path (_decode_pmtiles_for_static in _maplibre.py) mosaics the finest-zoom
    tiles via PIL and returns raster_layer(ndarray) -> _draw_one_layer -> ax.imshow
    (no rasterio).
    """
    import matplotlib.pyplot as plt
    from rasterio.errors import RasterioIOError

    import databricks.labs.gbx.vizx._interactive as itx
    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)

    # Stub display so plot_interactive doesn't try to call displayHTML.
    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    # Must not raise RasterioIOError.
    try:
        p.plot_pmtiles(archive, max_embed_mb=0)
    except RasterioIOError as e:
        pytest.fail(f"plot_pmtiles raised RasterioIOError: {e}")
    finally:
        plt.close("all")


def test_raster_pmtiles_source_sets_tilesize_from_tile():
    """A raster pmtiles source must declare tileSize = the actual tile pixel size, not
    rely on MapLibre's 512 default. gbx_rst_xyzpyramid emits 256px tiles; a 256 tile
    rendered as 512 lands at 2x scale and the wrong position. A vector source has no
    tileSize."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers

    # _real_png_tile() is an 8x8 PNG -> tileSize must be detected as 8 (not 512).
    raster = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    rsrc = layer_to_sources_layers(pmtiles_layer(raster), 0)[0]["gbx0"]
    assert rsrc["type"] == "raster"
    assert rsrc["tileSize"] == 8, "raster tileSize must come from the tile, not 512"

    # Vector source must not carry tileSize.
    vec = _build_archive([(0, 0, 0, _real_mvt_tile(0, 0, 0))], TileType.MVT)
    vsrc = layer_to_sources_layers(pmtiles_layer(vec), 0)[0]["gbx0"]
    assert vsrc["type"] == "vector"
    assert "tileSize" not in vsrc


def test_build_html_frames_archive_bounds_clamped_to_min_zoom():
    """build_html opens the auto-view on the embedded archive's extent via an EXPLICIT
    center+zoom (constructor + a jumpTo once the container is measured), clamped so it
    never underzooms below the archive min zoom (a z12-16 archive floors at minZoom 12 --
    MapLibre doesn't render below a source's min zoom). It must NOT use the container-
    measured fitBounds: a notebook-iframe container is 0-sized at construction and
    unreliable even on load, so a measured fit would collapse to a globe or a blank map.
    An explicit zoom, derived from the extent container-independently, opens framed."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    # MVT tiles at z12 + z13 -> min_zoom 12; _build_archive bounds = SF AOI.
    archive = _build_archive(
        [(12, 0, 0, _real_mvt_tile(12, 0, 0)), (13, 0, 0, _real_mvt_tile(13, 0, 0))],
        TileType.MVT,
    )
    html = build_html([layer_to_sources_layers(pmtiles_layer(archive), 0)])
    assert "map.jumpTo(" in html, "must open via an explicit center+zoom"
    assert "map.fitBounds(" not in html, "must NOT use container-measured fitBounds"
    # View + canvas sizing are (re)applied once the container has real dimensions.
    assert (
        "ResizeObserver" in html and "map.resize()" in html
    ), "must resize + apply the view once the container has real dimensions"
    assert "minZoom: 12" in html, "must floor at the archive min_zoom (12) -> non-blank"
    assert (
        "-122" in html and "37." in html
    ), "must open centered on the archive (SF) extent"


def test_build_html_falls_back_to_sf_default_without_archive():
    """With no embedded pmtiles (e.g. a vector GeoJSON layer only), build_html keeps the
    SF default view rather than crashing on the auto-view path."""
    from databricks.labs.gbx.vizx._maplibre import build_html

    html = build_html([])  # no layers, no pmtiles
    assert "zoom: 11" in html
    assert "-122.43" in html


def _mvt_tile_n(z, x, y, n):
    """MVT tile with ``n`` distinct polygon features (tile-local pixel space)."""
    import mapbox_vector_tile as mvt
    from shapely.geometry import box

    feats = [
        {
            "geometry": box(50 * i, 50 * i, 50 * i + 40, 50 * i + 40),
            "properties": {"v": i},
        }
        for i in range(n)
    ]
    return mvt.encode(
        {"name": "demo", "features": feats},
        default_options={"extents": 4096, "y_coord_down": True},
    )


def test_plot_pmtiles_static_vector_uses_one_populated_zoom(monkeypatch):
    """plot_pmtiles static path decodes a SINGLE zoom for vector archives -- never sums
    across levels (a normal pyramid repeats features at every zoom, so summing would
    render ~N_levels x the geometries). It uses the COARSEST sufficiently-populated zoom;
    for a normal pyramid the min zoom is already populated, so only min_zoom is decoded.

    _maplibre._decode_pmtiles_for_static uses a local `from _pmtiles import
    _decode_mvt_to_geoms`, so patching _pmtiles._decode_mvt_to_geoms intercepts it.
    """
    import warnings

    import databricks.labs.gbx.vizx._interactive as itx
    import databricks.labs.gbx.vizx._pmtiles as p_mod

    z_min, x_min, y_min = 3, 4, 4
    z_fine, x_fine, y_fine = 5, 16, 16
    # Normal-pyramid shape: the min zoom is fully populated (>= the static threshold),
    # so the decoder stops there and never touches the finer zoom.
    blob_min = _mvt_tile_n(z_min, x_min, y_min, 60)
    blob_fine = _mvt_tile_n(z_fine, x_fine, y_fine, 60)
    archive = _build_archive(
        [(z_min, x_min, y_min, blob_min), (z_fine, x_fine, y_fine, blob_fine)],
        TileType.MVT,
    )

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    decoded_zooms = []
    real_decode = p_mod._decode_mvt_to_geoms

    def _spy_decode(payload, z, x, y):
        decoded_zooms.append(z)
        return real_decode(payload, z, x, y)

    # _maplibre uses `from _pmtiles import _decode_mvt_to_geoms` inside the function
    # body, so re-importing from _pmtiles at call time. Patch the source module.
    monkeypatch.setattr(p_mod, "_decode_mvt_to_geoms", _spy_decode)

    # Suppress contextily basemap errors (no network in tests).
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        p_mod.plot_pmtiles(archive, max_embed_mb=0)

    assert decoded_zooms, "spy never called — _decode_mvt_to_geoms not reached"
    assert all(
        z == z_min for z in decoded_zooms
    ), f"Expected only z={z_min}, got zoom levels {set(decoded_zooms)}"


def _solid_png(size, value):
    """A solid-color RGB PNG of (size x size) with all channels == value."""
    buf = io.BytesIO()
    arr = np.full((size, size, 3), value / 255.0, dtype=np.float64)
    imsave(buf, arr, format="png")
    return buf.getvalue()


def test_plot_pmtiles_static_raster_uses_finest_zoom(monkeypatch):
    """Static raster overview mosaics the FINEST zoom, not the coarsest (min) zoom.

    Regression for the NB02 "washed-out" basemap: raster pyramid levels are
    independently resampled, so the coarsest overview averages source pixels and
    lowers contrast. _decode_pmtiles_for_static must pick the finest zoom that fits
    the tile budget. We tag each zoom's tile with a distinct solid value and assert
    the returned raster_layer ndarray carries the FINEST-zoom value.
    """
    import databricks.labs.gbx.vizx._interactive as itx
    import databricks.labs.gbx.vizx._maplibre as mlib
    from databricks.labs.gbx.vizx._layers import pmtiles_layer

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    # z=0 single coarse tile (value 40); z=2 finer tile (value 210). The mosaic
    # must come from z=2 (finest), so the decoded array's max channel ~= 210.
    coarse = _solid_png(64, 40)
    fine = _solid_png(64, 210)
    archive = _build_archive([(0, 0, 0, coarse), (2, 1, 1, fine)], TileType.PNG)

    layer = pmtiles_layer(archive)
    decoded = mlib._decode_pmtiles_for_static(layer)

    assert decoded.kind == "raster"
    rgb = decoded.data[..., :3]
    # Finest tile is value ~210; coarsest is ~40. Assert we got the finest.
    assert rgb.max() > 150, (
        f"Expected finest-zoom tile (value ~210), got max channel {rgb.max()} "
        "— static raster path likely still uses the coarsest (washed-out) zoom"
    )
