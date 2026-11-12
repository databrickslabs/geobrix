"""Offline tests for plot_pmtiles (interactive HTML + static fallback)."""

import gzip
import io
import os
import warnings

import mapbox_vector_tile as mvt
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pytest
from matplotlib.image import imsave
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from shapely.geometry import box

import databricks.labs.gbx.vizx._interactive as itx
import databricks.labs.gbx.vizx._maplibre as mlib
import databricks.labs.gbx.vizx._pmtiles as p
from databricks.labs.gbx.vizx._layers import pmtiles_layer
from databricks.labs.gbx.vizx._maplibre import (
    _pmtiles,
    build_html,
    layer_to_sources_layers,
)

matplotlib.use("Agg")

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
_VL_DEFAULT = object()  # sentinel: distinguish "not given" from None ("omit metadata")
_MVT_OPTS = {"extents": 4096, "y_coord_down": True}


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
    elif vector_layers is not None:  # None -> a metadata-less archive
        md["vector_layers"] = vector_layers
    w.finalize(header, md)
    return buf.getvalue()


def _real_png_tile():
    buf = io.BytesIO()  # a real 8x8 RGB PNG so PIL can decode it
    imsave(buf, (np.random.rand(8, 8, 3)), format="png")
    return buf.getvalue()


def _solid_png(size, value):
    buf = io.BytesIO()
    imsave(buf, np.full((size, size, 3), value / 255.0), format="png")
    return buf.getvalue()


def _mvt_tile(n=1, bounds=(1000, 1000, 3000, 3000), props=None):
    """MVT tile (tile-local pixel space, origin NW: the convention pyvx writes)."""
    if n == 1:
        feats = [{"geometry": box(*bounds), "properties": props or {"v": 1}}]
    else:
        feats = [
            {
                "geometry": box(50 * i, 50 * i, 50 * i + 40, 50 * i + 40),
                "properties": {"v": i},
            }
            for i in range(n)
        ]
    return mvt.encode({"name": "demo", "features": feats}, default_options=_MVT_OPTS)


def _real_mvt_tile(z=0, x=0, y=0):
    return _mvt_tile()


def _multi_zoom_vector_archive():
    tiles = [
        (z, x, y, os.urandom(4096))
        for z in range(3)
        for x in range(2**z)
        for y in range(2**z)
    ]
    return _build_archive(tiles, TileType.MVT)


def _warned_about_the_budget(ws):
    msgs = [str(w.message).lower() for w in ws]
    return any("fallback" in m or "exceed" in m for m in msgs)


@pytest.fixture
def returns_html(monkeypatch):
    """No displayHTML and no IPython.display: plot_* return the html (or static result)."""
    import builtins

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)
    yield
    plt.close("all")


def test_pmtiles_layer_opacity_and_color_reach_paint():
    raster = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    _, rlayers, _ = _pmtiles(pmtiles_layer(raster, opacity=0.55), 0)
    assert rlayers[0]["type"] == "raster"
    assert rlayers[0]["paint"]["raster-opacity"] == 0.55
    # opacity set -> emphasis must not override it
    assert "raster-opacity" not in rlayers[0].get("_gbx_emphasis", [])

    vec = _build_archive([(0, 0, 0, _real_mvt_tile())], TileType.MVT)
    _, vlayers, _ = _pmtiles(pmtiles_layer(vec, opacity=0.25, color="#3a3a3a"), 1)
    assert vlayers[0]["type"] == "fill"
    assert vlayers[0]["paint"]["fill-opacity"] == 0.25
    assert vlayers[0]["paint"]["fill-color"] == "#3a3a3a"


def test_every_declared_source_layer_renders_with_a_legend_and_geometry_filters():
    names = ["hotspots", "plumes", "wells"]
    vec = _build_archive(
        [(0, 0, 0, _real_mvt_tile())],
        TileType.MVT,
        vector_layers=[{"id": n} for n in names],
    )
    src, layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    assert {ly["source-layer"] for ly in layers} == set(names)
    for sl in names:
        assert {ly["type"] for ly in layers if ly["source-layer"] == sl} == {
            "fill",
            "line",
            "circle",
        }
    fills = {
        ly["source-layer"]: ly["paint"]["fill-color"]
        for ly in layers
        if ly["type"] == "fill"
    }
    assert len(set(fills.values())) == 3  # a distinct palette colour per layer
    # Each sub-layer filters to its geometry type so polygons don't also draw circles.
    point, poly = ["Point", "MultiPoint"], ["Polygon", "MultiPolygon"]
    expected = {
        "circle": ["match", ["geometry-type"], point, True, False],
        "fill": ["match", ["geometry-type"], poly, True, False],
        "line": ["match", ["geometry-type"], point, False, True],
    }
    assert all(ly["filter"] == expected[ly["type"]] for ly in layers)

    (sid,) = list(src)
    legend = src[sid]["_gbx_legend"]
    assert [it["label"] for it in legend["items"]] == names
    assert len({it["color"] for it in legend["items"]}) == 3
    html = build_html([layer_to_sources_layers(pmtiles_layer(vec), 0)])
    assert all(f">{sl}<" in html for sl in names)
    assert "_gbx_legend" not in html  # MapLibre rejects unknown source keys


def test_pmtiles_source_declares_zoom_range_tilesize_and_discovers_the_layer():
    # minzoom/maxzoom stop MapLibre requesting non-existent tiles past max_zoom ("jumpy" zoom).
    vec = _build_archive(
        [(10, 163, 395, _real_mvt_tile()), (12, 654, 1583, _real_mvt_tile())],
        TileType.MVT,
    )
    src, _layers, _ = _pmtiles(pmtiles_layer(vec), 0)
    (sid,) = list(src)
    assert (src[sid]["minzoom"], src[sid]["maxzoom"]) == (10, 12)
    assert src[sid]["type"] == "vector" and "tileSize" not in src[sid]

    # A raster source declares the actual tile size (gbx_rst_xyzpyramid emits 256px tiles),
    # not MapLibre's 512 default.
    raster = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    rsrc = layer_to_sources_layers(pmtiles_layer(raster), 0)[0]["gbx0"]
    assert rsrc["type"] == "raster" and rsrc["tileSize"] == 8

    # No vector_layers metadata: discover the source-layer from the tiles, never guess.
    bare = _build_archive(
        [(0, 0, 0, _real_mvt_tile())], TileType.MVT, vector_layers=None
    )
    _, layers, _ = _pmtiles(pmtiles_layer(bare), 0)
    assert {ly["source-layer"] for ly in layers} == {"demo"} and len(layers) == 3


def test_build_html_frames_the_archive_clamped_to_its_min_zoom():
    # An explicit center+zoom (constructor + jumpTo once the container is measured), never the
    # container-measured fitBounds: a notebook iframe is 0-sized at construction, so a measured
    # fit collapses to a globe or a blank map.
    archive = _build_archive(
        [(12, 0, 0, _real_mvt_tile()), (13, 0, 0, _real_mvt_tile())], TileType.MVT
    )
    html = build_html([layer_to_sources_layers(pmtiles_layer(archive), 0)])
    assert "map.jumpTo(" in html and "map.fitBounds(" not in html
    assert "ResizeObserver" in html and "map.resize()" in html
    assert "minZoom: 12" in html  # never underzooms below the archive min zoom
    assert "-122" in html and "37." in html  # centred on the archive extent
    # No pmtiles layer at all: keep the default view.
    default = build_html([])
    assert "zoom: 11" in default and "-122.43" in default


def test_plot_pmtiles_interactive_takes_bytes_or_a_path_and_embeds_the_pinned_libs(
    monkeypatch, tmp_path
):
    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    f = tmp_path / "a.pmtiles"
    f.write_bytes(archive)
    captured = []
    monkeypatch.setattr(
        itx, "_notebook_display_html", lambda: (lambda html: captured.append(html))
    )
    for source in (archive, str(f), "dbfs:" + str(f)):
        assert p.plot_pmtiles(source) is None  # displayHTML render returns None
    assert len(captured) == 3
    assert all("maplibre-gl@4.7.1" in h and "pmtiles@3.2.0" in h for h in captured)
    assert all("pmtiles://" in h for h in captured)


def test_an_unknown_tile_type_is_treated_as_vector(monkeypatch, returns_html):
    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    info = {
        "tile_type": "unknown",
        "tile_compression": "none",
        "min_zoom": 0,
        "max_zoom": 0,
        "bounds": (-122.52, 37.70, -122.35, 37.83),
        "center": (-122.44, 37.76, 0),
        "tile_count": 1,
        "metadata": {},
    }
    monkeypatch.setattr("databricks.labs.gbx.pmtiles.pmtiles_info", lambda data: info)
    assert "maplibregl.Map" in p.plot_pmtiles(archive)


def test_an_oversized_archive_warns_and_falls_back_to_static_or_raises(
    monkeypatch, returns_html
):
    import databricks.labs.gbx.vizx._static_map as sm

    static_calls = []
    monkeypatch.setattr(sm, "plot_static", lambda *a, **kw: static_calls.append(1))
    png = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(png, max_embed_mb=1e-9)  # a tiny budget forces the static path
    assert _warned_about_the_budget(ws)
    assert static_calls == [1]

    vector = _build_archive([(10, 163, 395, _real_mvt_tile())], TileType.MVT)
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(vector, max_embed_mb=1e-9)
    assert _warned_about_the_budget(ws)

    with pytest.raises(ValueError, match="exceeds budget"):
        p.plot_pmtiles(png, max_embed_mb=1e-9, fallback=False)


def test_interactive_fit_downzoom_keeps_it_interactive_and_other_values_are_handled(
    monkeypatch, returns_html
):
    import databricks.labs.gbx.vizx._pmtiles_autofit as af

    archive = _multi_zoom_vector_archive()
    # A budget below the full archive but above the coarse levels: 'downzoom' stays interactive.
    target_mb = (len(archive) * 0.6) / 1_048_576
    html = p.plot_pmtiles(archive, max_embed_mb=target_mb, interactive_fit="downzoom")
    assert "maplibregl.Map" in html

    # interactive_fit=None opts out: the reducer never runs and the path goes to static
    # (which raises on these synthetic non-MVT tiles: proof it did not stay interactive).
    called = []
    real = af.autofit_archive
    monkeypatch.setattr(
        af, "autofit_archive", lambda *a, **kw: called.append(1) or real(*a, **kw)
    )
    with pytest.raises(Exception):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, fallback=True, interactive_fit=None)
    assert not called

    # 'all' is the planned multi-shard feature: a clear error, not a silent degrade.
    with pytest.raises(NotImplementedError, match="interactive_fit='all'"):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, interactive_fit="all")
    with pytest.raises(ValueError, match="interactive_fit"):
        p.plot_pmtiles(archive, interactive_fit="bogus")


def test_a_gzip_compressed_mvt_tile_is_inflated_before_decoding():
    # mapbox_vector_tile.decode rejects gzip bytes, and PMTiles archives with
    # tile_compression=gzip yield exactly that.
    raw = _mvt_tile(bounds=(500, 500, 3500, 3500), props={"v": 42})
    result = p._decode_mvt_to_geoms(gzip.compress(raw), 10, 163, 395)
    assert len(result) >= 1
    geom, props = result[0]
    assert not geom.is_empty and props.get("v") == 42


def test_static_fallback_for_a_raster_archive_does_not_hit_rasterio(returns_html):
    # The live path mosaics the finest-zoom tiles via PIL and draws with imshow; passing raw
    # tile bytes to rasterio (not georeferenced) used to raise RasterioIOError.
    from rasterio.errors import RasterioIOError

    archive = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    try:
        p.plot_pmtiles(archive, max_embed_mb=0)
    except RasterioIOError as e:
        pytest.fail(f"plot_pmtiles raised RasterioIOError: {e}")


def test_static_fallback_decodes_one_vector_zoom_and_the_finest_raster_zoom(
    monkeypatch, returns_html
):
    # Vector: never sum across zoom levels (a pyramid repeats features at every zoom); the
    # coarsest sufficiently populated zoom is decoded, here the min zoom only.
    archive = _build_archive(
        [(3, 4, 4, _mvt_tile(60)), (5, 16, 16, _mvt_tile(60))], TileType.MVT
    )
    zooms = []
    real_decode = p._decode_mvt_to_geoms

    def _spy(payload, z, x, y):
        zooms.append(z)
        return real_decode(payload, z, x, y)

    monkeypatch.setattr(p, "_decode_mvt_to_geoms", _spy)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        p.plot_pmtiles(archive, max_embed_mb=0)
    assert zooms and set(zooms) == {3}

    # Raster: the FINEST zoom, because each pyramid level is resampled and the coarsest
    # overview washes out contrast. Tag each zoom with a solid value and read it back.
    pyramid = _build_archive(
        [(0, 0, 0, _solid_png(64, 40)), (2, 1, 1, _solid_png(64, 210))], TileType.PNG
    )
    decoded = mlib._decode_pmtiles_for_static(pmtiles_layer(pyramid))
    assert decoded.kind == "raster"
    assert decoded.data[..., :3].max() > 150


def test_an_explicit_zero_embed_budget_is_quiet(returns_html, capsys):
    archive = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(archive, max_embed_mb=0)
    assert not _warned_about_the_budget(ws), [str(w.message)[:80] for w in ws]
    # The one-line audit stays: it states the verdict, it does not scold the choice.
    assert "static fallback" in capsys.readouterr().out


def test_a_default_budget_that_drops_the_map_to_static_still_warns(
    monkeypatch, returns_html
):
    # Nobody asked for static here: the default silently dropped interactivity.
    monkeypatch.setattr(mlib, "MAX_EMBED_MB_CAP_RAISED", 1e-9)
    archive = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(archive)
    assert _warned_about_the_budget(ws)


def test_prepare_layers_zero_budget_is_quiet_but_a_positive_one_warns():
    archive = _build_archive([(0, 0, 0, _real_png_tile())], TileType.PNG)
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        out = mlib.prepare_layers([pmtiles_layer(archive)], max_embed_mb=0)
    assert out["mode"] == "static"
    assert not _warned_about_the_budget(ws)
    assert out["warnings"] == []
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        out = mlib.prepare_layers([pmtiles_layer(archive)], max_embed_mb=1e-9)
    assert out["mode"] == "static" and _warned_about_the_budget(ws)
