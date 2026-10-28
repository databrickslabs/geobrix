"""Tests for vizx PMTiles auto-fit reduction (interactive_fit='downzoom').

The reducer down-zooms an oversized archive (drops the highest zoom levels)
until the base64-rendered embed size fits the interactive budget, so a single
large archive can still display interactively at reduced detail. Tier-agnostic:
works for both raster and vector tiles by rebuilding a smaller archive from the
tiles already present (no re-tiling, no tippecanoe).
"""

import io

import pytest
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive


def _build_archive(
    tiles, tile_type=TileType.MVT, *, name="demo", tile_compression=Compression.NONE
):
    """Build a PMTiles archive from (z, x, y, payload) tuples."""
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": tile_compression,
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
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


def _multi_zoom_archive(tile_type=TileType.MVT, payload_size=4096):
    """An archive whose size is dominated by the highest zoom level.

    z0: 1 tile, z1: ~4 tiles, z2: ~16 tiles. Each tile gets DISTINCT, effectively
    incompressible bytes (os.urandom) so PMTiles internal compression + tile
    dedup can't collapse them -- dropping z2 then z1 monotonically shrinks the
    archive (the property the reducer relies on).
    """
    import os

    tiles = []
    for z in range(3):
        n = 2**z
        for x in range(n):
            for y in range(n):
                tiles.append((z, x, y, os.urandom(payload_size)))
    return _build_archive(tiles, tile_type)


def _tile_zooms(archive_bytes):
    from pmtiles.reader import MemorySource, all_tiles

    return sorted({z for (z, _, _), _ in all_tiles(MemorySource(archive_bytes))})


def test_autofit_drops_high_zoom_until_under_budget():
    """A multi-zoom archive over budget is reduced by dropping top zoom levels."""
    archive = _multi_zoom_archive()
    full_zooms = _tile_zooms(archive)
    assert full_zooms == [0, 1, 2]

    # Budget small enough to require dropping the densest (z2) level, but big
    # enough to keep z0/z1. Budget is in MB; archive here is tens of KB, so use
    # a fractional MB that lands between the z<=1 and z<=2 sizes.
    full_size = len(archive)
    # Target ~ between the z<=1 subset and the full archive.
    target_mb = (full_size * 0.6) / 1_048_576

    reduced, report = autofit_archive(archive, max_embed_mb=target_mb)

    # Reduced archive fits the (base64-inflated) budget.
    assert len(reduced) * (4.0 / 3.0) <= target_mb * 1_048_576
    # It dropped the top zoom(s) but kept the coarse ones.
    reduced_zooms = _tile_zooms(reduced)
    assert reduced_zooms, "reduced archive must keep at least one zoom level"
    assert max(reduced_zooms) < max(full_zooms), "top zoom should have been dropped"
    assert 0 in reduced_zooms, "coarsest zoom must be preserved"
    # Report is informative.
    assert report["dropped_zooms"]
    assert report["kept_max_zoom"] == max(reduced_zooms)
    assert report["fits"] is True


def test_autofit_reduces_an_uncompressed_directory_archive():
    """The heavy writer stores directories uncompressed (internal_compression
    NONE); the reducer keeps the same tiles from it as from the gzip form."""
    from test.pmtiles_light._archives import uncompressed_internals

    archive = _multi_zoom_archive()
    plain = uncompressed_internals(archive)
    target_mb = (len(archive) * 0.6) / 1_048_576  # forces a downzoom (drops z2)

    reduced, report = autofit_archive(plain, max_embed_mb=target_mb)
    want, want_report = autofit_archive(archive, max_embed_mb=target_mb)

    assert report["dropped_zooms"], "this test needs an actual reduction"
    for key in ("dropped_zooms", "original_max_zoom", "kept_max_zoom", "fits"):
        assert report[key] == want_report[key], key
    assert _tile_zooms(reduced) == _tile_zooms(want)


def test_autofit_preserves_source_data_bounds():
    """Downzoom must keep the SOURCE data bounds, not recompute a huge coarse-tile extent.

    build_header_info would set bounds to the union of the KEPT tiles' grid bboxes — for a
    low-zoom overview that balloons to a coarse-tile chunk (e.g. a z0/z1 world quadrant),
    which then makes the viewer's fitBounds open on the globe. The rebuild must preserve
    the original SF bounds (~-122.52..-122.35, 37.70..37.83)."""
    from pmtiles.reader import MemorySource, Reader

    archive = _multi_zoom_archive()  # z0-2, SF header bounds from _build_archive
    target_mb = (len(archive) * 0.6) / 1_048_576  # forces a downzoom (drops z2)
    reduced, report = autofit_archive(archive, max_embed_mb=target_mb)
    assert report["dropped_zooms"], "this test needs an actual reduction"

    hdr = Reader(MemorySource(reduced)).header()
    assert abs(hdr["min_lon_e7"] / 1e7 - (-122.52)) < 0.01, hdr["min_lon_e7"]
    assert abs(hdr["max_lon_e7"] / 1e7 - (-122.35)) < 0.01, hdr["max_lon_e7"]
    assert abs(hdr["min_lat_e7"] / 1e7 - 37.70) < 0.01, hdr["min_lat_e7"]
    assert abs(hdr["max_lat_e7"] / 1e7 - 37.83) < 0.01, hdr["max_lat_e7"]


def test_autofit_noop_when_already_fits():
    """When the archive already fits, return it unchanged with fits=True."""
    archive = _multi_zoom_archive()
    big_budget_mb = (len(archive) * 10) / 1_048_576
    reduced, report = autofit_archive(archive, max_embed_mb=big_budget_mb)
    assert reduced == archive
    assert report["fits"] is True
    assert report["dropped_zooms"] == []


def test_autofit_preserves_tile_type_vector():
    """Reduced vector archive is still readable and reports MVT tile type."""
    from databricks.labs.gbx.pmtiles import pmtiles_info

    archive = _multi_zoom_archive(TileType.MVT)
    target_mb = (len(archive) * 0.5) / 1_048_576
    reduced, _ = autofit_archive(archive, max_embed_mb=target_mb)
    info = pmtiles_info(reduced)
    assert info["tile_type"] == "mvt"


def test_autofit_keeps_at_least_coarsest_even_if_over_budget():
    """If even the coarsest level exceeds budget, keep it (degenerate) and flag
    fits=False so the caller can route to static rather than emit nothing."""
    archive = _multi_zoom_archive(payload_size=4096)
    # Absurdly tiny budget: even z0 alone won't fit.
    reduced, report = autofit_archive(archive, max_embed_mb=0.0000001)
    reduced_zooms = _tile_zooms(reduced)
    assert reduced_zooms == [0], "must retain only the coarsest level as last resort"
    assert report["fits"] is False


def test_autofit_rejects_bad_budget():
    archive = _multi_zoom_archive()
    with pytest.raises(ValueError):
        autofit_archive(archive, max_embed_mb=0)
    with pytest.raises(ValueError):
        autofit_archive(archive, max_embed_mb=-1)


def test_prepare_layers_static_audit_matches_mode_and_uses_rendered_size():
    """Regression: when an over-budget pmtiles archive routes to the static
    fallback, the audit must report the RENDERED (base64-inflated ~4/3x) size and
    a 'static' verdict -- CONSISTENT with mode='static'.

    Previously the static-path audit summed RAW archive bytes while the mode
    decision used the inflated rendered size, so for a budget between the raw and
    rendered sizes the audit said verdict='embed'/fits=True while mode='static'
    (the audit line lied). Pick exactly such a budget to expose it.
    """
    import warnings

    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _BASE64_INFLATION, prepare_layers

    archive = _multi_zoom_archive(payload_size=8192)
    raw_mb = len(archive) / 1_048_576
    # raw < budget < raw*inflation -> raw "fits" but the rendered HTML does not.
    budget_mb = raw_mb * 1.10
    assert raw_mb < budget_mb < raw_mb * _BASE64_INFLATION

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = prepare_layers([pmtiles_layer(archive)], max_embed_mb=budget_mb)

    a = res["audit"]
    assert res["mode"] == "static"
    assert a["verdict"] == "static", "verdict must agree with mode (was 'embed')"
    assert a["fits"] is False
    # The audit total is the RENDERED (inflated) measure, not the raw archive bytes.
    assert a["total_embed_bytes"] == int(len(archive) * _BASE64_INFLATION)


def test_simplify_layer_archive_uses_binary_free_downzoom(monkeypatch):
    """_simplify_layer on a PMTiles archive must reduce it via the binary-free
    autofit down-zoom -- NOT via tippecanoe/tile-join (which segfault on x86_64
    Linux). Verify the result is a reduced pmtiles layer and that no subprocess
    was ever spawned.
    """
    import subprocess

    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _simplify_layer

    archive = _multi_zoom_archive(payload_size=8192)
    full_zooms = _tile_zooms(archive)
    assert full_zooms == [0, 1, 2]

    # Guard: any shell-out (tippecanoe/tile-join) fails the test loudly.
    def _no_subprocess(*a, **k):  # noqa: ANN001
        raise AssertionError("simplify archive path must not spawn a subprocess")

    monkeypatch.setattr(subprocess, "run", _no_subprocess)

    layer = pmtiles_layer(archive, label="big")
    # Budget that forces dropping the densest (z2) level.
    target_mb = (len(archive) * 0.6) / 1_048_576
    out = _simplify_layer(layer, {"max_z": 1}, max_embed_mb=target_mb)

    assert out.kind == "pmtiles"
    assert out.label == "big"
    reduced_zooms = _tile_zooms(out.data)
    assert max(reduced_zooms) < max(full_zooms), "top zoom should have been dropped"
    assert 0 in reduced_zooms, "coarsest zoom must be preserved"


def test_simplify_layer_url_archive_returned_unchanged():
    """A URL-mode pmtiles layer can't be down-zoomed locally -> returned as-is."""
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _simplify_layer

    layer = pmtiles_layer("https://example.com/tiles.pmtiles", label="remote")
    out = _simplify_layer(layer, {"max_z": 1}, max_embed_mb=1.0)
    assert out is layer, "URL-mode archive must be returned unchanged"


def test_autofit_preserves_gzip_tile_compression():
    """Regression: tiles are copied through verbatim, so a GZIP source archive must stay
    labeled GZIP in the reduced header. Mislabeling gzipped tiles as NONE makes pmtiles.js
    skip the gunzip and hand gzipped bytes to the MVT decoder -> zero features render (a
    silently blank interactive map). tippecanoe and gbx_pmtiles_agg both gzip their tiles.
    """
    import gzip
    import os

    from pmtiles.reader import MemorySource, Reader, all_tiles
    from pmtiles.tile import Compression

    # GZIP archive: gzip each payload AND declare tile_compression=GZIP.
    tiles = []
    for z in range(3):
        n = 2**z
        for x in range(n):
            for y in range(n):
                tiles.append((z, x, y, gzip.compress(os.urandom(4096))))
    archive = _build_archive(tiles, tile_compression=Compression.GZIP)
    assert (
        Reader(MemorySource(archive)).header()["tile_compression"] == Compression.GZIP
    )

    target_mb = (len(archive) * 0.5) / 1_048_576
    reduced, report = autofit_archive(archive, max_embed_mb=target_mb)
    assert report["dropped_zooms"], "test needs a real downzoom to exercise the rebuild"

    hdr = Reader(MemorySource(reduced)).header()
    assert (
        hdr["tile_compression"] == Compression.GZIP
    ), "reduced header must keep the source GZIP label (tiles are copied gzipped)"
    # The kept payloads are still gzip-magic -> consistent with the GZIP header.
    first = next(p for _, p in all_tiles(MemorySource(reduced)))
    assert first[:2] == b"\x1f\x8b"


def test_prepare_layers_downzooms_each_pmtiles_layer_to_fit():
    """A multi-layer overlay of embedded pmtiles must embed interactively: each archive
    is down-zoomed to its share of the budget (budget / n_pmtiles). Without this, the
    full pair busts the budget and falls back to static (the NB-02 overlay symptom)."""
    import warnings

    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import prepare_layers

    a1 = _multi_zoom_archive(payload_size=8192)
    a2 = _multi_zoom_archive(payload_size=8192)
    # Budget ~ ONE archive's rendered size: the full PAIR (~2x) busts it, but each
    # down-zoomed to budget/2 fits, so the overlay should stay interactive.
    budget_mb = len(a1) * (4.0 / 3.0) / 1_048_576

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = prepare_layers(
            [pmtiles_layer(a1), pmtiles_layer(a2)], max_embed_mb=budget_mb
        )
    assert res["mode"] == "interactive", "overlay must embed after per-layer downzoom"
    assert len(res["prepared"]) == 2


def test_decode_for_static_scans_past_empty_low_zoom():
    """Regression: a tippecanoe drop_densest overview can have NO features at its min
    zoom (z0). The static fallback decoder must scan UP to the first zoom that has
    features, not decode only min_zoom -- which raised 'no geometries' -> plot_static([])
    -> 'no layers provided' (the static View step blew up). Here z0 is an empty MVT and
    z1 has a polygon; the decoder must return that polygon, not raise."""
    import mapbox_vector_tile as mvt
    from shapely.geometry import Polygon

    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _decode_pmtiles_for_static

    poly = Polygon([(1000, 1000), (2000, 1000), (2000, 2000), (1000, 2000)])
    real = mvt.encode(
        [{"name": "buildings", "features": [{"geometry": poly, "properties": {}}]}]
    )
    empty = mvt.encode([{"name": "buildings", "features": []}])
    archive = _build_archive(
        [(0, 0, 0, empty), (1, 0, 0, real)], tile_type=TileType.MVT
    )

    out = _decode_pmtiles_for_static(pmtiles_layer(archive))
    assert out.kind == "vector"
    assert len(out.data) >= 1, "must decode the z1 polygon after skipping the empty z0"


def test_plot_pmtiles_defaults_to_downzoom():
    """plot_pmtiles auto-fits by default: interactive_fit defaults to 'downzoom' so an
    over-budget archive is reduced to fit instead of silently falling to static."""
    import inspect

    from databricks.labs.gbx.vizx._pmtiles import plot_pmtiles

    default = inspect.signature(plot_pmtiles).parameters["interactive_fit"].default
    assert (
        default == "downzoom"
    ), f"interactive_fit default should be 'downzoom', got {default!r}"
