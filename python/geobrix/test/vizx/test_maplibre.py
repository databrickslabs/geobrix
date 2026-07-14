"""Tests for vizx._maplibre — per-layer MapLibre GL adapter."""

import base64
import io

import geopandas as gpd
import matplotlib
import numpy as np
import pytest
from shapely.geometry import LineString, Point, Polygon

matplotlib.use("Agg")

from databricks.labs.gbx.vizx._layers import (  # noqa: E402
    pmtiles_layer,
    raster_layer,
    vector_layer,
)
from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers  # noqa: E402

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _tiny_geotiff_bytes(width=8, height=8, crs="EPSG:4326"):
    """Build a tiny in-memory GeoTIFF (1 band, float32) using rasterio."""
    import rasterio
    from rasterio.transform import from_bounds

    buf = io.BytesIO()
    transform = from_bounds(-122.5, 37.7, -122.4, 37.8, width, height)
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(np.random.rand(height, width).astype("float32"), 1)
    return buf.getvalue()


def _tiny_geotiff_path(tmp_path):
    p = tmp_path / "tiny.tif"
    p.write_bytes(_tiny_geotiff_bytes())
    return str(p)


def _build_pmtiles_archive(tile_type_str="mvt", layer_name="buildings"):
    """Build a minimal PMTiles archive with one dummy tile.

    Args:
        tile_type_str: ``"mvt"`` or ``"png"``.
        layer_name:    The vector-layer id embedded in TileJSON metadata
                       (ignored for raster archives but harmless to pass).
    """
    from pmtiles.tile import Compression, TileType, zxy_to_tileid
    from pmtiles.writer import Writer

    _TILE_TYPES = {
        "mvt": TileType.MVT,
        "png": TileType.PNG,
    }
    tile_type = _TILE_TYPES[tile_type_str]
    payload = b"DUMMY"
    buf = io.BytesIO()
    w = Writer(buf)
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": 0,
        "max_zoom": 2,
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": 0,
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    w.write_tile(zxy_to_tileid(0, 0, 0), payload)
    w.finalize(header, {"name": "test", "vector_layers": [{"id": layer_name}]})
    return buf.getvalue()


# ---------------------------------------------------------------------------
# vector layer (the brief's required test)
# ---------------------------------------------------------------------------


def test_vector_layer_becomes_geojson_source_and_fill_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])
        ],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf, column="v"), 0)
    assert "gbx0" in sources and sources["gbx0"]["type"] == "geojson"
    assert any(layer["type"] in ("fill", "line", "circle") for layer in layers)
    assert embed > 0


def test_vector_grid_fill_is_data_driven_by_column_cmap():
    """A column+cmap layer (e.g. an H3 solar-score grid) gets a PER-FEATURE fill color
    from the colormap — fill-color = ['get','_gbx_color'] and the GeoJSON carries varied
    _gbx_color values across differing column values (not one flat fill)."""
    polys = [
        Polygon([(x, 37.7), (x + 0.01, 37.7), (x + 0.01, 37.71), (x, 37.71)])
        for x in (-122.50, -122.49, -122.48)
    ]
    gdf = gpd.GeoDataFrame({"solar": [0.1, 0.5, 0.9]}, geometry=polys, crs="EPSG:4326")
    sources, layers, _ = layer_to_sources_layers(
        vector_layer(gdf, column="solar", cmap="RdYlGn"), 0
    )
    fill = next(ly for ly in layers if ly["type"] == "fill")
    assert fill["paint"]["fill-color"] == ["get", "_gbx_color"], fill["paint"][
        "fill-color"
    ]
    colors = [
        f["properties"]["_gbx_color"] for f in sources["gbx0"]["data"]["features"]
    ]
    assert len(set(colors)) == 3, f"expected 3 distinct ramp colors, got {colors}"


def test_vector_explicit_color_overrides_cmap():
    """An explicit scalar color wins over column/cmap (flat fill, no _gbx_color)."""
    gdf = gpd.GeoDataFrame(
        {"solar": [0.1, 0.9]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)]),
            Polygon([(-122.3, 37.7), (-122.2, 37.7), (-122.2, 37.8), (-122.3, 37.8)]),
        ],
        crs="EPSG:4326",
    )
    sources, layers, _ = layer_to_sources_layers(
        vector_layer(gdf, column="solar", color="#ff0000"), 0
    )
    fill = next(ly for ly in layers if ly["type"] == "fill")
    assert fill["paint"]["fill-color"] == "#ff0000"


def test_cmap_hex_colors_ramp_and_degenerate():
    from databricks.labs.gbx.vizx._maplibre import _cmap_hex_colors

    ramp = _cmap_hex_colors([0.0, 0.5, 1.0], "viridis")
    assert (
        len(ramp) == 3 and len(set(ramp)) == 3 and all(c.startswith("#") for c in ramp)
    )
    same = _cmap_hex_colors([5, 5, 5], "viridis")  # single value -> mid-ramp, all equal
    assert len(set(same)) == 1
    grey = _cmap_hex_colors([None, float("nan")], "viridis")  # non-finite -> grey
    assert grey == ["#cccccc", "#cccccc"]


def test_cmap_hex_colors_quantile_spreads_skewed_distribution():
    """quantile scale maps by percentile rank, so a skewed distribution (a dense low
    range + a long tail) spreads across the full ramp instead of collapsing into one end
    the way linear does -- the fix for an indistinguishable low range (e.g. roof counts
    where most H3 cells hold a small count but a few hold many)."""
    from databricks.labs.gbx.vizx._maplibre import _cmap_hex_colors

    vals = [1, 1, 2, 2, 3, 4, 5, 100]  # dense 1..5, long tail to 100
    lin = _cmap_hex_colors(vals, "YlOrRd", "linear")
    qnt = _cmap_hex_colors(vals, "YlOrRd", "quantile")

    def _lum(h):
        h = h.lstrip("#")
        r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
        return 0.299 * r + 0.587 * g + 0.114 * b

    # Under linear the dense low range crushes into the pale ramp start (tiny luminance
    # spread); quantile spreads it pale->dark across the ramp (large luminance spread).
    lo_lin = [_lum(c) for c in lin[:7]]
    lo_qnt = [_lum(c) for c in qnt[:7]]
    assert (max(lo_qnt) - min(lo_qnt)) > (
        max(lo_lin) - min(lo_lin)
    ), "quantile must spread the dense low range across the ramp"


def test_legend_for_quantile_adds_median_tick():
    """quantile legend carries a median (bar-midpoint) tick so labels stay honest about
    the non-linear ramp; linear has only vmin/vmax."""
    from databricks.labs.gbx.vizx._maplibre import _legend_for

    lg = _legend_for([1, 1, 2, 2, 3, 4, 5, 100], "YlOrRd", "roofs", "quantile")
    assert lg["vmin"] == 1 and lg["vmax"] == 100 and "mid" in lg
    assert "mid" not in _legend_for(
        [1, 1, 2, 2, 3, 4, 5, 100], "YlOrRd", "roofs", "linear"
    )


def test_build_html_renders_cmap_legend_for_data_driven_layer():
    """A column+cmap layer adds a colormap legend (gradient bar + value range) to the
    HTML, and the _gbx_legend sidecar is stripped from the serialized sources."""
    polys = [
        Polygon([(x, 37.7), (x + 0.01, 37.7), (x + 0.01, 37.71), (x, 37.71)])
        for x in (-122.50, -122.49, -122.48)
    ]
    from databricks.labs.gbx.vizx._maplibre import build_html

    gdf = gpd.GeoDataFrame({"solar": [0.1, 0.5, 0.9]}, geometry=polys, crs="EPSG:4326")
    prepared = [
        layer_to_sources_layers(vector_layer(gdf, column="solar", cmap="RdYlGn"), 0)
    ]
    html = build_html(prepared)
    assert "linear-gradient(to right" in html, "no cmap legend gradient in the HTML"
    assert "solar" in html  # legend label defaults to the column name
    assert "_gbx_legend" not in html  # sidecar must not leak into the MapLibre sources


def test_build_html_no_legend_without_data_driven_layer():
    """A flat (explicit-color) layer adds no legend."""
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])
        ],
        crs="EPSG:4326",
    )
    from databricks.labs.gbx.vizx._maplibre import build_html

    prepared = [layer_to_sources_layers(vector_layer(gdf, color="#ff0000"), 0)]
    html = build_html(prepared)
    assert "linear-gradient(to right" not in html


def test_gdf_for_grid_carries_value_column(monkeypatch):
    """A grid layer must pass its value column through cells_as_gdf (extra_cols), or the
    data-driven fill/legend can't read it and the heatmap renders one flat color."""
    from shapely.geometry import Polygon

    from databricks.labs.gbx.vizx import _maplibre, _vector
    from databricks.labs.gbx.vizx._layers import grid_layer

    captured = {}

    def _fake(df, cell_col="cellid", extra_cols=(), **kw):
        captured["cell_col"] = cell_col
        captured["extra_cols"] = list(extra_cols)
        cols = {cell_col: [1]}
        for c in extra_cols:
            cols[c] = [0.5]
        return gpd.GeoDataFrame(
            cols, geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])], crs="EPSG:4326"
        )

    monkeypatch.setattr(_vector, "cells_as_gdf", _fake)
    lyr = grid_layer(object(), grid_system="h3", cellid_col="h3_cell", column="n_roofs")
    gdf = _maplibre._gdf_for(lyr)
    assert captured["cell_col"] == "h3_cell"
    assert captured["extra_cols"] == ["n_roofs"]
    assert "n_roofs" in gdf.columns


def test_vector_layer_polygon_gets_fill_and_line_layers():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])
        ],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 1)
    types = {layer["type"] for layer in layers}
    assert "fill" in types
    assert "line" in types
    # no circle for polygon-only gdf
    assert "circle" not in types
    assert sources["gbx1"]["type"] == "geojson"
    assert embed > 0


def test_vector_layer_point_gets_circle_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(-122.4, 37.7)],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 2)
    types = {layer["type"] for layer in layers}
    assert "circle" in types
    assert embed > 0


def test_vector_layer_line_gets_line_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[LineString([(-122.5, 37.7), (-122.4, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 3)
    types = {layer["type"] for layer in layers}
    assert "line" in types
    assert "fill" not in types
    assert embed > 0


def test_vector_layer_source_data_is_valid_geojson():
    """The 'data' key in the source must be a GeoJSON FeatureCollection dict."""
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])
        ],
        crs="EPSG:4326",
    )
    sources, _, _ = layer_to_sources_layers(vector_layer(gdf), 0)
    gj = sources["gbx0"]["data"]
    assert gj["type"] == "FeatureCollection"
    assert len(gj["features"]) == 1


def test_vector_layer_reprojected_to_4326():
    """A EPSG:27700 (BNG) input must be reprojected to EPSG:4326 in the source."""
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[box(530000, 179000, 531000, 180000)],
        crs="EPSG:27700",
    )
    sources, _, _ = layer_to_sources_layers(vector_layer(gdf), 0)
    gj = sources["gbx0"]["data"]
    coords = gj["features"][0]["geometry"]["coordinates"][0]
    # London area — longitude ~-0.1 to 0.0, latitude ~51.4 to 51.6
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    assert all(-1 < lon < 1 for lon in lons), f"Unexpected longitudes: {lons}"
    assert all(51 < lat < 52 for lat in lats), f"Unexpected latitudes: {lats}"


def test_vector_layer_idx_keys_source_and_layers():
    """idx drives the source key and layer ids."""
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[
            Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])
        ],
        crs="EPSG:4326",
    )
    sources, layers, _ = layer_to_sources_layers(vector_layer(gdf), 7)
    assert "gbx7" in sources
    for layer in layers:
        assert layer["source"] == "gbx7"
        assert layer["id"].startswith("gbx7-")


# ---------------------------------------------------------------------------
# raster layer
# ---------------------------------------------------------------------------


def test_raster_layer_from_path_produces_image_source(tmp_path):
    """raster_layer(path) -> image source with 4-corner coordinates + raster layer."""
    path = _tiny_geotiff_path(tmp_path)
    sources, layers, embed = layer_to_sources_layers(raster_layer(path), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4  # [ul, ur, lr, ll] lon/lat pairs
    for corner in src["coordinates"]:
        assert len(corner) == 2
    assert len(layers) == 1
    assert layers[0]["type"] == "raster"
    assert embed > 0


def test_raster_layer_from_bytes_produces_image_source():
    """raster_layer(bytes) -> image source (accepts in-memory GeoTIFF bytes)."""
    tif_bytes = _tiny_geotiff_bytes()
    sources, layers, embed = layer_to_sources_layers(raster_layer(tif_bytes), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4
    assert embed > 0


def test_raster_layer_from_ndarray_produces_image_source():
    """raster_layer(ndarray) -> image source (no geo metadata, unit-square coords)."""
    arr = np.random.rand(4, 8).astype("float32")
    sources, layers, embed = layer_to_sources_layers(raster_layer(arr), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4
    assert embed > 0


def test_raster_layer_corners_are_lon_lat(tmp_path):
    """Corner coordinates are in lon/lat ([-180..180], [-90..90])."""
    path = _tiny_geotiff_path(tmp_path)
    sources, _, _ = layer_to_sources_layers(raster_layer(path), 0)
    for lon, lat in sources["gbx0"]["coordinates"]:
        assert -180 <= lon <= 180, f"lon out of range: {lon}"
        assert -90 <= lat <= 90, f"lat out of range: {lat}"


def test_raster_layer_b64_is_valid_png(tmp_path):
    """The embedded base64 decodes to a valid PNG magic-bytes sequence."""
    path = _tiny_geotiff_path(tmp_path)
    sources, _, _ = layer_to_sources_layers(raster_layer(path), 0)
    url = sources["gbx0"]["url"]
    b64_part = url.split(",", 1)[1]
    raw = base64.b64decode(b64_part)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n", "Embedded image is not a PNG"


def test_raster_layer_decimation(tmp_path):
    """A large raster should be decimated to <= raster_max_px on its longest side."""
    import rasterio
    from rasterio.transform import from_bounds

    buf = io.BytesIO()
    w, h = 4096, 2048
    transform = from_bounds(-122.5, 37.7, -122.4, 37.8, w, h)
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(np.random.rand(h, w).astype("float32"), 1)
    big_bytes = buf.getvalue()

    from databricks.labs.gbx.vizx._maplibre import _raster_to_image

    png_b64, corners = _raster_to_image(raster_layer(big_bytes), raster_max_px=512)
    raw = base64.b64decode(png_b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    # The decimated PNG should be much smaller than the raw 4096x2048 raster bytes
    assert len(raw) < len(big_bytes) // 10


# ---------------------------------------------------------------------------
# pmtiles layer — url mode (no real archive needed)
# ---------------------------------------------------------------------------


def test_pmtiles_layer_url_mode():
    """An http(s) URL -> url mode sidecar, embed_bytes=0."""
    import pytest

    url = "https://example.com/tiles.pmtiles"
    layer = pmtiles_layer(url)
    # A remote archive's source-layers can't be discovered at build time (no bytes to
    # sample, no metadata), and vizx does not guess a name -> it warns and renders the
    # basemap only (0 overlay layers). The source itself is still wired up correctly so
    # pmtiles.js resolves the archive at runtime.
    with pytest.warns(UserWarning, match="no vector source-layer"):
        sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    # URL mode: the source key must be the remote URL (pmtiles.js keys PMTiles(url) by
    # the URL), not the source id, or MapLibre can't resolve the archive.
    assert src["url"] == f"pmtiles://{url}"
    assert "_gbx_pmtiles" in src
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "url"
    assert info["url"] == url
    assert embed == 0
    assert len(layers) == 0


def test_pmtiles_layer_embed_mode_from_bytes():
    """bytes archive -> embed mode, embed_bytes == len(bytes)."""
    archive = _build_pmtiles_archive("mvt")
    layer = pmtiles_layer(archive)
    sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    assert src["url"] == "pmtiles://gbx0"
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "embed"
    assert "bytes" in info
    assert embed == len(info["bytes"])
    assert embed > 0
    # A vector source-layer renders as fill + line + circle sub-layers (so polygons,
    # lines and points all draw), each filtered to its geometry type.
    assert len(layers) == 3
    assert {ly["type"] for ly in layers} == {"fill", "line", "circle"}


def test_pmtiles_layer_embed_mode_from_path(tmp_path):
    """path archive -> embed mode, bytes read from disk."""
    archive = _build_pmtiles_archive("mvt")
    p = tmp_path / "test.pmtiles"
    p.write_bytes(archive)
    layer = pmtiles_layer(str(p))
    sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "embed"
    assert info["bytes"] == archive
    assert embed == len(archive)


def test_pmtiles_layer_vector_source_type():
    """MVT pmtiles -> source type 'vector'."""
    archive = _build_pmtiles_archive("mvt")
    sources, _, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    assert sources["gbx0"]["type"] == "vector"


def test_pmtiles_layer_raster_source_type():
    """PNG pmtiles -> source type 'raster'."""
    archive = _build_pmtiles_archive("png")
    sources, _, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    assert sources["gbx0"]["type"] == "raster"


def test_pmtiles_layer_idx_drives_key():
    url = "https://example.com/tiles.pmtiles"
    sources, layers, _ = layer_to_sources_layers(pmtiles_layer(url), 5)
    assert "gbx5" in sources
    # URL mode: pmtiles.js keys the registered PMTiles by the REMOTE URL, so the
    # source's "pmtiles://<key>" must use that URL (not the source id) or MapLibre
    # can't resolve the archive.
    assert sources["gbx5"]["url"] == f"pmtiles://{url}"
    for layer in layers:
        assert layer["source"] == "gbx5"


def test_embed_filesource_name_matches_source_url_key():
    """Regression: an embedded archive's FileSource File name is its protocol key, and
    MUST equal the "pmtiles://<key>" in its source URL. The old code named the File
    "gbx0.pmtiles" while the source looked up "gbx0" -> MapLibre never found the archive
    -> every embedded pmtiles map rendered blank (basemap only / white)."""
    import re

    from databricks.labs.gbx.vizx._maplibre import build_html

    archive = _build_pmtiles_archive("mvt")
    entry = layer_to_sources_layers(pmtiles_layer(archive), 0)
    html = build_html([entry])
    # build_html namespaces the embed key per map (uid_sid); the registered File name
    # MUST equal the source URL's "pmtiles://<key>" so the protocol resolves it.
    m = re.search(r'"url":\s*"pmtiles://([^"]+)"', html)
    assert m, "no pmtiles source URL in html"
    key = m.group(1)
    assert key.endswith("_gbx0"), f"embed key should be <uid>_gbx0, got {key!r}"
    assert f"new File([_bgbx0.buffer], '{key}')" in html
    assert "'gbx0.pmtiles'" not in html, "stale File name must be gone"


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


def test_unknown_kind_raises():
    from databricks.labs.gbx.vizx._layers import Layer

    layer = object.__new__(Layer)
    object.__setattr__(layer, "kind", "unknown_kind")
    with pytest.raises(ValueError):
        layer_to_sources_layers(layer, 0)


# ---------------------------------------------------------------------------
# pmtiles — source-layer derived from archive metadata (Fix 1)
# ---------------------------------------------------------------------------


def test_pmtiles_vector_source_layer_from_metadata():
    """source-layer must be derived from the archive's vector_layers metadata,
    not hardcoded.  Build an archive whose layer is named 'roads' and assert
    the MapLibre fill layer carries source-layer='roads'."""
    archive = _build_pmtiles_archive("mvt", layer_name="roads")
    sources, layers, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    fill_layers = [layer for layer in layers if layer.get("type") == "fill"]
    assert fill_layers, "expected at least one fill layer for a vector PMTiles"
    assert fill_layers[0]["source-layer"] == "roads", (
        f"source-layer should be 'roads' (from archive metadata), "
        f"got {fill_layers[0]['source-layer']!r}"
    )


def test_extract_vector_layer_names_unit():
    """Unit test for _extract_vector_layer_names with a TileJSON-shaped dict."""
    from databricks.labs.gbx.vizx._maplibre import _extract_vector_layer_names

    assert _extract_vector_layer_names({}) == []
    assert _extract_vector_layer_names({"vector_layers": []}) == []
    assert _extract_vector_layer_names(
        {"vector_layers": [{"id": "parks"}, {"id": "water"}]}
    ) == ["parks", "water"]
    # Malformed entries (no 'id') are skipped.
    assert _extract_vector_layer_names(
        {"vector_layers": [{"name": "bad"}, {"id": "good"}]}
    ) == ["good"]


# ---------------------------------------------------------------------------
# build_html — Task 5
# ---------------------------------------------------------------------------


def test_build_html_is_self_contained_and_sri_pinned():
    """The brief's required smoke test: vector layer → build_html → assertions."""
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    prepared = [layer_to_sources_layers(vector_layer(gdf), 0)]
    html = build_html(prepared)
    assert "maplibregl.Map" in html
    assert 'integrity="sha384-' in html
    assert 'crossorigin="anonymous"' in html
    assert "carto" in html.lower()  # basemap wired
    assert "gbx0" in html  # source id present


def test_build_html_pmtiles_embed_mode_no_sidecar():
    """build_html reads the _gbx_pmtiles sidecar without mutating it, emits FileSource JS."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    archive = _build_pmtiles_archive("mvt")
    prepared = [layer_to_sources_layers(pmtiles_layer(archive), 0)]
    html = build_html(prepared)
    # Sidecar must be gone from the serialised HTML (MapLibre rejects unknown keys).
    assert "_gbx_pmtiles" not in html
    # FileSource registration must be present.
    assert "pmtiles.FileSource" in html
    # Lock the base64→Uint8Array round-trip JS.
    assert "charCodeAt(0)" in html


def test_build_html_pmtiles_url_mode():
    """build_html url-mode emits new pmtiles.PMTiles(<url>) and no sidecar."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    url = "https://example.com/tiles.pmtiles"
    prepared = [layer_to_sources_layers(pmtiles_layer(url), 0)]
    html = build_html(prepared)
    assert "_gbx_pmtiles" not in html
    assert "new pmtiles.PMTiles(" in html
    assert url in html


def test_build_html_basemap_none():
    """basemap='none' renders an empty style literal, not a CARTO URL."""
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    prepared = [layer_to_sources_layers(vector_layer(gdf), 0)]
    html = build_html(prepared, basemap="none")
    assert "carto" not in html.lower()
    assert "version:8" in html


def test_build_html_custom_center_and_zoom():
    """center and zoom are written into the Map constructor."""
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0.0, 51.5)], crs="EPSG:4326")
    prepared = [layer_to_sources_layers(vector_layer(gdf), 0)]
    html = build_html(prepared, center=[0.0, 51.5], zoom=8)
    assert "[0.0, 51.5]" in html
    assert "zoom: 8" in html


def test_build_html_multi_layer():
    """N prepared tuples → N source ids present in the HTML."""
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    gdf0 = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    gdf1 = gpd.GeoDataFrame({"v": [2]}, geometry=[Point(-122.5, 37.8)], crs="EPSG:4326")
    prepared = [
        layer_to_sources_layers(vector_layer(gdf0), 0),
        layer_to_sources_layers(vector_layer(gdf1), 1),
    ]
    html = build_html(prepared)
    assert "gbx0" in html
    assert "gbx1" in html


# ---------------------------------------------------------------------------
# security + idempotency fixes
# ---------------------------------------------------------------------------


def test_build_html_escapes_script_breakout():
    """Crafted feature-attribute values must not break out of the <script> block."""
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    gdf = gpd.GeoDataFrame(
        {"name": ["PWNED</script><img src=x onerror=alert(1)>"]},
        geometry=[Point(-122.4, 37.7)],
        crs="EPSG:4326",
    )
    html = build_html([layer_to_sources_layers(vector_layer(gdf), 0)])
    # Raw breakout string must NOT appear verbatim.
    assert "PWNED</script>" not in html
    # The data payload must still survive (in Unicode-escaped form).
    assert "PWNED" in html
    # The </script> token must be escaped.
    assert "\\u003c/script" in html or "u003c" in html


def test_build_html_does_not_mutate_prepared():
    """build_html must not mutate the caller's prepared list; calling it twice is idempotent."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import build_html, layer_to_sources_layers

    archive = _build_pmtiles_archive("mvt")
    prepared = [layer_to_sources_layers(pmtiles_layer(archive), 0)]
    sid = next(iter(prepared[0][0]))

    # Sidecar must be present before the first call.
    assert "_gbx_pmtiles" in prepared[0][0][sid]

    h1 = build_html(prepared)

    # Sidecar must NOT have been popped — caller's dict is unchanged.
    assert (
        "_gbx_pmtiles" in prepared[0][0][sid]
    ), "build_html mutated prepared: _gbx_pmtiles was popped from the caller's source dict"

    h2 = build_html(prepared)

    # build_html assigns a fresh per-map uid each call (so multiple maps in one document
    # don't collide), so the two outputs differ only by that fixed-length uid -- NOT
    # byte-identical, but the SAME length, which is what prepare_layers' size measurement
    # relies on.
    assert len(h1) == len(h2), "build_html output length must be stable for measurement"
    # PMTiles registration must be present in the output.
    assert "pmtiles.FileSource" in h2
