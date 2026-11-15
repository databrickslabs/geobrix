"""Tests for vizx._maplibre — per-layer MapLibre GL adapter."""

import base64
import io

import geopandas as gpd
import matplotlib
import numpy as np
import pytest
from shapely.geometry import LineString, Point, Polygon, box

matplotlib.use("Agg")

from databricks.labs.gbx.vizx._layers import (  # noqa: E402
    pmtiles_layer,
    raster_layer,
    vector_layer,
)
from databricks.labs.gbx.vizx._maplibre import (  # noqa: E402
    _cmap_hex_colors,
    _legend_for,
    build_html,
    layer_to_sources_layers,
)

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
# vector layer
# ---------------------------------------------------------------------------

_POLY = Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])


def _gdf(geoms, **cols):
    return gpd.GeoDataFrame(
        cols or {"v": list(range(len(geoms)))}, geometry=geoms, crs="EPSG:4326"
    )


def _ramp_gdf():
    polys = [
        Polygon([(x, 37.7), (x + 0.01, 37.7), (x + 0.01, 37.71), (x, 37.71)])
        for x in (-122.50, -122.49, -122.48)
    ]
    return _gdf(polys, solar=[0.1, 0.5, 0.9])


@pytest.mark.parametrize(
    "geom, expected, absent",
    [
        (_POLY, {"fill", "line"}, {"circle"}),
        (Point(-122.4, 37.7), {"circle"}, set()),
        (LineString([(-122.5, 37.7), (-122.4, 37.8)]), {"line"}, {"fill"}),
    ],
)
def test_vector_layer_geometry_type_picks_the_maplibre_layer_types(
    geom, expected, absent
):
    sources, layers, embed = layer_to_sources_layers(vector_layer(_gdf([geom])), 7)
    types = {layer["type"] for layer in layers}
    assert expected <= types and not (absent & types)
    assert embed > 0
    # idx drives the source key and the layer ids
    assert sources["gbx7"]["type"] == "geojson"
    assert all(
        layer["source"] == "gbx7" and layer["id"].startswith("gbx7-")
        for layer in layers
    )
    gj = sources["gbx7"]["data"]
    assert gj["type"] == "FeatureCollection" and len(gj["features"]) == 1


def test_vector_layer_is_reprojected_to_4326():
    gdf = gpd.GeoDataFrame(
        {"v": [1]}, geometry=[box(530000, 179000, 531000, 180000)], crs="EPSG:27700"
    )
    sources, _, _ = layer_to_sources_layers(vector_layer(gdf), 0)
    coords = sources["gbx0"]["data"]["features"][0]["geometry"]["coordinates"][0]
    assert all(-1 < c[0] < 1 and 51 < c[1] < 52 for c in coords)  # London


def test_column_and_cmap_drive_a_per_feature_fill_unless_a_color_is_given():
    sources, layers, _ = layer_to_sources_layers(
        vector_layer(_ramp_gdf(), column="solar", cmap="RdYlGn"), 0
    )
    fill = next(ly for ly in layers if ly["type"] == "fill")
    assert fill["paint"]["fill-color"] == ["get", "_gbx_color"]
    colors = [
        f["properties"]["_gbx_color"] for f in sources["gbx0"]["data"]["features"]
    ]
    assert len(set(colors)) == 3  # a ramp, not one flat fill

    _, layers, _ = layer_to_sources_layers(
        vector_layer(_ramp_gdf(), column="solar", color="#ff0000"), 0
    )
    assert next(ly for ly in layers if ly["type"] == "fill")["paint"]["fill-color"] == (
        "#ff0000"
    )


def test_cmap_hex_colors_ramp_degenerate_and_quantile_spread():
    ramp = _cmap_hex_colors([0.0, 0.5, 1.0], "viridis")
    assert len(set(ramp)) == 3 and all(c.startswith("#") for c in ramp)
    assert len(set(_cmap_hex_colors([5, 5, 5], "viridis"))) == 1  # single value
    assert _cmap_hex_colors([None, float("nan")], "viridis") == ["#cccccc"] * 2

    # quantile maps by percentile rank: a dense low range plus a long tail spreads across
    # the ramp instead of collapsing into its pale end the way linear does.
    vals = [1, 1, 2, 2, 3, 4, 5, 100]

    def lum(h):
        r, g, b = (int(h.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
        return 0.299 * r + 0.587 * g + 0.114 * b

    def spread(scale):
        low = [lum(c) for c in _cmap_hex_colors(vals, "YlOrRd", scale)[:7]]
        return max(low) - min(low)

    assert spread("quantile") > spread("linear")


def test_the_legend_shows_for_data_driven_layers_only_with_a_median_tick_for_quantile():
    lg = _legend_for([1, 1, 2, 2, 3, 4, 5, 100], "YlOrRd", "roofs", "quantile")
    assert lg["vmin"] == 1 and lg["vmax"] == 100 and "mid" in lg
    assert "mid" not in _legend_for([1, 1, 2, 2, 3, 4, 5, 100], "YlOrRd", "r", "linear")

    html = build_html(
        [
            layer_to_sources_layers(
                vector_layer(_ramp_gdf(), column="solar", cmap="RdYlGn"), 0
            )
        ]
    )
    assert "linear-gradient(to right" in html and "solar" in html
    assert (
        "_gbx_legend" not in html
    )  # the sidecar must not leak into the MapLibre sources
    flat = build_html(
        [layer_to_sources_layers(vector_layer(_gdf([_POLY]), color="#ff0000"), 0)]
    )
    assert "linear-gradient(to right" not in flat


def test_a_grid_layer_passes_its_value_column_to_cells_as_gdf(monkeypatch):
    # Otherwise the data-driven fill and legend can't read it: one flat colour.
    from databricks.labs.gbx.vizx import _maplibre, _vector
    from databricks.labs.gbx.vizx._layers import grid_layer

    captured = {}

    def _fake(df, cell_col="cellid", extra_cols=(), **kw):
        captured.update(cell_col=cell_col, extra_cols=list(extra_cols))
        cols = {cell_col: [1], **{c: [0.5] for c in extra_cols}}
        return gpd.GeoDataFrame(
            cols, geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])], crs="EPSG:4326"
        )

    monkeypatch.setattr(_vector, "cells_as_gdf", _fake)
    lyr = grid_layer(object(), grid_system="h3", cellid_col="h3_cell", column="n_roofs")
    gdf = _maplibre._gdf_for(lyr)
    assert captured == {"cell_col": "h3_cell", "extra_cols": ["n_roofs"]}
    assert "n_roofs" in gdf.columns


# ---------------------------------------------------------------------------
# raster layer
# ---------------------------------------------------------------------------


def test_raster_layer_from_path_bytes_or_ndarray_is_an_image_source_in_lon_lat(
    tmp_path,
):
    path = _tiny_geotiff_path(tmp_path)
    inputs = [path, _tiny_geotiff_bytes(), np.random.rand(4, 8).astype("float32")]
    for data in inputs:
        sources, layers, embed = layer_to_sources_layers(raster_layer(data), 0)
        src = sources["gbx0"]
        assert src["type"] == "image" and len(src["coordinates"]) == 4
        assert all(
            len(c) == 2 and -180 <= c[0] <= 180 and -90 <= c[1] <= 90
            for c in src["coordinates"]
        )
        assert src["url"].startswith("data:image/png;base64,")
        assert base64.b64decode(src["url"].split(",", 1)[1])[:8] == b"\x89PNG\r\n\x1a\n"
        assert [ly["type"] for ly in layers] == ["raster"] and embed > 0


def test_a_large_raster_is_decimated_to_the_max_pixel_budget():
    import rasterio
    from rasterio.transform import from_bounds

    from databricks.labs.gbx.vizx._maplibre import _raster_to_image

    w, h = 4096, 2048
    buf = io.BytesIO()
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_bounds(-122.5, 37.7, -122.4, 37.8, w, h),
    ) as dst:
        dst.write(np.random.rand(h, w).astype("float32"), 1)
    big = buf.getvalue()
    png_b64, _corners = _raster_to_image(raster_layer(big), raster_max_px=512)
    raw = base64.b64decode(png_b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n" and len(raw) < len(big) // 10


# ---------------------------------------------------------------------------
# pmtiles layer
# ---------------------------------------------------------------------------


def test_pmtiles_url_mode_keys_the_source_by_the_remote_url():
    url = "https://example.com/tiles.pmtiles"
    # A remote archive's source-layers can't be discovered at build time and vizx does not
    # guess a name: it warns and renders the basemap only.
    with pytest.warns(UserWarning, match="no vector source-layer"):
        sources, layers, embed = layer_to_sources_layers(pmtiles_layer(url), 5)
    src = sources["gbx5"]
    # pmtiles.js keys PMTiles(url) by the URL, not the source id, or MapLibre can't resolve it.
    assert src["url"] == f"pmtiles://{url}"
    assert src["_gbx_pmtiles"]["mode"] == "url" and src["_gbx_pmtiles"]["url"] == url
    assert embed == 0 and layers == []


def test_pmtiles_embed_mode_from_bytes_or_path_with_source_type_and_layer_from_metadata(
    tmp_path,
):
    archive = _build_pmtiles_archive("mvt", layer_name="roads")
    p = tmp_path / "test.pmtiles"
    p.write_bytes(archive)
    for data in (archive, str(p)):
        sources, layers, embed = layer_to_sources_layers(pmtiles_layer(data), 0)
        src = sources["gbx0"]
        info = src["_gbx_pmtiles"]
        assert src["url"] == "pmtiles://gbx0" and src["type"] == "vector"
        assert info["mode"] == "embed" and info["bytes"] == archive
        assert embed == len(archive)
        # fill + line + circle so polygons, lines and points all draw, per the archive's
        # declared source-layer (not a hardcoded one)
        assert {ly["type"] for ly in layers} == {"fill", "line", "circle"}
        assert {ly["source-layer"] for ly in layers} == {"roads"}
    raster, _, _ = layer_to_sources_layers(
        pmtiles_layer(_build_pmtiles_archive("png")), 0
    )
    assert raster["gbx0"]["type"] == "raster"


def test_an_embedded_archive_is_registered_under_the_key_its_source_url_uses():
    # The File name was "gbx0.pmtiles" while the source looked up "gbx0": MapLibre never
    # found the archive and every embedded pmtiles map rendered blank.
    import re

    html = build_html(
        [layer_to_sources_layers(pmtiles_layer(_build_pmtiles_archive("mvt")), 0)]
    )
    m = re.search(r'"url":\s*"pmtiles://([^"]+)"', html)
    assert m and m.group(1).endswith("_gbx0")  # namespaced per map: <uid>_gbx0
    assert f"new File([_bgbx0.buffer], '{m.group(1)}')" in html
    assert "'gbx0.pmtiles'" not in html


def test_unknown_layer_kind_raises():
    from databricks.labs.gbx.vizx._layers import Layer

    layer = object.__new__(Layer)
    object.__setattr__(layer, "kind", "unknown_kind")
    with pytest.raises(ValueError):
        layer_to_sources_layers(layer, 0)


# ---------------------------------------------------------------------------
# build_html
# ---------------------------------------------------------------------------


def _prepared(*geoms):
    return [
        layer_to_sources_layers(vector_layer(_gdf([g])), i) for i, g in enumerate(geoms)
    ]


def test_build_html_is_self_contained_sri_pinned_and_honours_the_view_options():
    pt = Point(0.0, 51.5)
    html = build_html(_prepared(pt, Point(-122.5, 37.8)))
    assert "maplibregl.Map" in html and "carto" in html.lower()
    assert 'integrity="sha384-' in html and 'crossorigin="anonymous"' in html
    assert "gbx0" in html and "gbx1" in html  # one source id per prepared layer

    html = build_html(_prepared(pt), center=[0.0, 51.5], zoom=8)
    assert "[0.0, 51.5]" in html and "zoom: 8" in html
    none = build_html(_prepared(pt), basemap="none")
    assert "carto" not in none.lower() and "version:8" in none


def test_build_html_wires_pmtiles_embed_and_url_modes_without_the_sidecar():
    archive = _build_pmtiles_archive("mvt")
    html = build_html([layer_to_sources_layers(pmtiles_layer(archive), 0)])
    assert "_gbx_pmtiles" not in html  # MapLibre rejects unknown source keys
    assert "pmtiles.FileSource" in html and "charCodeAt(0)" in html

    url = "https://example.com/tiles.pmtiles"
    with pytest.warns(UserWarning):
        prepared = [layer_to_sources_layers(pmtiles_layer(url), 0)]
    html = build_html(prepared)
    assert "_gbx_pmtiles" not in html
    assert "new pmtiles.PMTiles(" in html and url in html


def test_build_html_escapes_script_breakout_from_attribute_values():
    gdf = gpd.GeoDataFrame(
        {"name": ["PWNED</script><img src=x onerror=alert(1)>"]},
        geometry=[Point(-122.4, 37.7)],
        crs="EPSG:4326",
    )
    html = build_html([layer_to_sources_layers(vector_layer(gdf), 0)])
    assert "PWNED</script>" not in html
    assert "PWNED" in html and "u003c" in html  # the payload survives, escaped


def test_build_html_does_not_mutate_the_prepared_layers():
    prepared = [
        layer_to_sources_layers(pmtiles_layer(_build_pmtiles_archive("mvt")), 0)
    ]
    sid = next(iter(prepared[0][0]))
    h1 = build_html(prepared)
    assert (
        "_gbx_pmtiles" in prepared[0][0][sid]
    )  # the caller's dict still has its sidecar
    h2 = build_html(prepared)
    # A fresh fixed-length per-map uid is used each call, so the outputs differ but keep
    # the same length: what prepare_layers' size measurement relies on.
    assert len(h1) == len(h2) and "pmtiles.FileSource" in h2
