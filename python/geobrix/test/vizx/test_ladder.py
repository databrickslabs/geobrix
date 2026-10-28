"""Tests for vizx._maplibre.prepare_layers — the >64 MB budget ladder."""

import shutil

import geopandas as gpd
import pytest
from shapely.geometry import Point

from databricks.labs.gbx.vizx._layers import pmtiles_layer, vector_layer
from databricks.labs.gbx.vizx._maplibre import audit_layers, prepare_layers


def _small_gdf():
    return gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")


def test_under_budget_is_interactive():
    out = prepare_layers([vector_layer(_small_gdf())], max_embed_mb=64)
    assert out["mode"] == "interactive"


def test_oversize_pmtiles_without_url_or_spec_falls_back_to_static():
    big = pmtiles_layer(b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024))
    out = prepare_layers([big], max_embed_mb=1, fallback=True)
    assert out["mode"] == "static"
    assert any("static" in w.lower() for w in out["warnings"])


def test_fallback_false_raises():
    big = pmtiles_layer(b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024))
    with pytest.raises(ValueError, match="budget"):
        prepare_layers([big], max_embed_mb=1, fallback=False)


def test_url_mode_pmtiles_is_interactive_zero_embed_cost():
    """A pmtiles layer with an http URL streams remotely — embed_bytes==0, always interactive."""
    url_layer = pmtiles_layer("https://example.com/tiles.pmtiles")
    # With a generous budget, a URL-mode layer must always be interactive.
    out = prepare_layers([url_layer], max_embed_mb=64)
    assert out["mode"] == "interactive"
    assert not out["warnings"]


def test_warnings_are_list():
    out = prepare_layers([vector_layer(_small_gdf())])
    assert isinstance(out["warnings"], list)


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_spec_produces_interactive():
    """An oversize vector layer + simplify_tiles_spec → mode='interactive' with 'simplified' warning."""
    import warnings as _warnings

    import geopandas as gpd
    from shapely.geometry import box

    # Create a vector layer with some polygons; use a generous budget so it fits
    # after simplification (simplify is applied first, then budget is checked).
    gdf = gpd.GeoDataFrame(
        {"v": range(5)},
        geometry=[box(i * 0.1, 0, i * 0.1 + 0.1, 0.1) for i in range(5)],
        crs="EPSG:4326",
    )
    lyr = vector_layer(gdf)
    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        out = prepare_layers(
            [lyr],
            max_embed_mb=100,
            simplify_tiles_spec={"max_z": 4, "budget_mb": 8},
        )
    assert out["mode"] == "interactive"
    # Should have a warning mentioning "simplified"
    combined = " ".join(out["warnings"]) + " ".join(str(x.message) for x in w)
    assert "simplified" in combined.lower()


# ---------------------------------------------------------------------------
# audit_layers and dry_run tests
# ---------------------------------------------------------------------------


def test_audit_layers_small_is_embed():
    """audit_layers on a small vector layer returns fits=True, verdict='embed'."""
    result = audit_layers([vector_layer(_small_gdf())])
    assert result["fits"] is True
    assert result["verdict"] == "embed"
    assert result["total_embed_bytes"] > 0
    assert len(result["layers"]) == 1
    entry = result["layers"][0]
    assert "label" in entry
    assert "kind" in entry
    assert "embed_bytes" in entry


def test_plot_interactive_dry_run_returns_audit_no_render():
    """plot_interactive with dry_run=True returns an audit dict, not rendered HTML."""
    from databricks.labs.gbx.vizx._interactive import plot_interactive

    result = plot_interactive([vector_layer(_small_gdf())], dry_run=True)
    assert isinstance(result, dict), "dry_run=True must return a dict, not HTML"
    assert "fits" in result
    assert "verdict" in result
    assert "total_embed_bytes" in result


def test_audit_oversize_verdict():
    """An oversize layer (tiny max_embed_mb) returns fits=False, verdict in {simplify,static}."""
    result = audit_layers([vector_layer(_small_gdf())], max_embed_mb=0.0001)
    assert result["fits"] is False
    assert result["verdict"] in {"simplify", "static", "url"}


def _build_pmtiles_archive_for_ladder():
    """Minimal PMTiles archive with one real non-empty tile (no tippecanoe needed)."""
    import io

    from pmtiles.tile import Compression, TileType, zxy_to_tileid
    from pmtiles.writer import Writer

    payload = b"FAKEMVTDATA_LADDER_TEST"
    buf = io.BytesIO()
    w = Writer(buf)
    header = {
        "tile_type": TileType.MVT,
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
    w.finalize(header, {"name": "ladder-test", "vector_layers": [{"id": "test"}]})
    return buf.getvalue()


def test_audit_max_tile_bytes_for_archive():
    """audit_layers reports max_tile_bytes as a positive int for an embedded pmtiles archive.

    This test specifically closes the coverage gap where _max_tile_bytes always
    returned None because it called pmtiles_info (which has no 'tiles' key) instead
    of iterating tiles via all_tiles.
    """
    try:
        archive = _build_pmtiles_archive_for_ladder()
    except Exception as e:
        pytest.skip(f"pmtiles writer not available: {e}")

    layer = pmtiles_layer(archive, label="test-archive")
    result = audit_layers([layer])
    assert len(result["layers"]) == 1
    entry = result["layers"][0]
    assert entry["kind"] == "pmtiles"
    assert (
        entry["max_tile_bytes"] is not None
    ), "_max_tile_bytes returned None — it is not reading real tile data from the archive"
    assert isinstance(entry["max_tile_bytes"], int)
    assert (
        entry["max_tile_bytes"] > 0
    ), f"max_tile_bytes should be positive, got {entry['max_tile_bytes']}"


@pytest.mark.parametrize("plain", [False, True], ids=["gzip", "uncompressed-directory"])
def test_the_archive_helpers_read_the_tiles_of_either_directory_layout(plain):
    """The tile-size audit, the MVT layer discovery and the raster tile size read the same tiles
    whether the archive's directories are gzipped or stored plain (the heavy writer's layout).
    """
    import io
    from test.pmtiles_light._archives import uncompressed_internals

    import mapbox_vector_tile as mvt
    from PIL import Image
    from pmtiles.tile import TileType

    from databricks.labs.gbx.vizx._maplibre import (
        _max_tile_bytes,
        _mvt_layer_names_from_bytes,
        _raster_tile_px,
    )

    from .test_pmtiles_autofit import _build_archive

    vector = mvt.encode(
        [
            {
                "name": "roads",
                "features": [{"geometry": "POINT(10 10)", "properties": {}}],
            }
        ]
    )
    png = io.BytesIO()
    Image.new("RGBA", (32, 32), (1, 2, 3, 255)).save(png, format="PNG")
    png = png.getvalue()
    archives = {
        "vector": _build_archive([(0, 0, 0, vector)], TileType.MVT),
        "raster": _build_archive([(0, 0, 0, png)], TileType.PNG),
    }
    if plain:
        archives = {k: uncompressed_internals(v) for k, v in archives.items()}

    assert _max_tile_bytes(archives["vector"]) == len(vector)
    assert _mvt_layer_names_from_bytes(archives["vector"]) == ["roads"]
    assert _raster_tile_px({"bytes": archives["raster"]}) == 32


# ---------------------------------------------------------------------------
# Bug fix: simplify rung must run BEFORE the over-budget raw-bytes bail.
# ---------------------------------------------------------------------------


def test_oversize_pmtiles_with_simplify_spec_stays_interactive(monkeypatch):
    """Regression: over-budget pmtiles + simplify_tiles_spec must produce mode='interactive'.

    Before the fix, prepare_layers would detect the raw archive exceeds budget,
    append early_oversize_labels, and `continue` BEFORE calling _simplify_layer,
    so simplify_tiles_spec was dead exactly when needed. The reorder ensures
    _simplify_layer is called first; if it reduces the archive under budget the
    layer stays interactive.
    """
    import databricks.labs.gbx.vizx._maplibre as _ml

    # A real valid small PMTiles archive (under the 1 MB budget) used as the
    # "simplified" replacement returned by the monkeypatched _simplify_layer.
    try:
        small_valid = _build_pmtiles_archive_for_ladder()
    except Exception as e:
        pytest.skip(f"pmtiles writer not available: {e}")

    # An archive whose raw bytes exceed a 1 MB budget (padded far beyond small_valid).
    BIG_ARCHIVE = small_valid + b"\x00" * (2 * 1024 * 1024)

    # Track whether _simplify_layer was actually invoked.
    simplify_called = []

    def fake_simplify_layer(layer, spec, max_embed_mb):
        simplify_called.append(True)
        return pmtiles_layer(small_valid, label=getattr(layer, "label", None))

    monkeypatch.setattr(_ml, "_simplify_layer", fake_simplify_layer)

    big_layer = pmtiles_layer(BIG_ARCHIVE, label="big")
    spec = {"max_z": 4, "budget_mb": 0.5}
    out = prepare_layers(
        [big_layer], max_embed_mb=1, simplify_tiles_spec=spec, fallback=True
    )

    assert (
        simplify_called
    ), "_simplify_layer was never called — simplify rung did not run"
    assert (
        out["mode"] == "interactive"
    ), f"Expected 'interactive' after simplify reduced archive under budget, got {out['mode']!r}"


def test_oversize_pmtiles_no_spec_still_static():
    """Over-budget pmtiles WITHOUT a spec must still fall back to static (no regression)."""
    BIG_ARCHIVE = b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024)
    big_layer = pmtiles_layer(BIG_ARCHIVE, label="big-no-spec")
    out = prepare_layers([big_layer], max_embed_mb=1, fallback=True)
    assert out["mode"] == "static"


# ---------------------------------------------------------------------------
# Serverless cell-output cap: default budget + rendered-HTML-aware guard + remedy
# ---------------------------------------------------------------------------


def test_default_max_embed_mb_is_serverless_safe():
    """The default embed budget must fit the Serverless cell-output cap.

    Serverless notebooks cap cell output at 10 MB default, 20 MB max
    (%set_cell_max_output_size_in_mb). max_embed_mb is compared against the
    base64-RENDERED HTML size (~4/3x the archive), so it is itself a rendered-MB
    budget. The un-raised DEFAULT_MAX_EMBED_MB applies when the cap is NOT raised,
    so the rendered HTML it permits must stay under the 10 MB default cap. The old
    default of 64 MB rendered to ~85 MB and was silently truncated.
    """
    from databricks.labs.gbx.vizx._maplibre import DEFAULT_MAX_EMBED_MB

    # The budget is the rendered-size budget; un-raised it must clear the 10 MB
    # default cap with margin (raising the cap to 20 MB shifts the ceiling to
    # MAX_EMBED_MB_CAP_RAISED=18, covered separately).
    assert DEFAULT_MAX_EMBED_MB < 10, (
        f"default max_embed_mb={DEFAULT_MAX_EMBED_MB} does not clear the 10 MB "
        "default Serverless cell-output cap"
    )
    assert DEFAULT_MAX_EMBED_MB > 0


def test_all_entrypoints_share_the_default():
    """No stray legacy budget (the old 64) left behind in any signature.

    The cap-aware entrypoints (plot_pmtiles / plot_interactive) default
    max_embed_mb to None -- the sentinel that defers to _resolve_embed_budget so
    the budget tracks set_cell_max_output (18 when the cap is raised, else 8). The
    lower-level audit_layers / prepare_layers don't take set_cell_max_output, so
    they default to the conservative DEFAULT_MAX_EMBED_MB directly.
    """
    import inspect

    from databricks.labs.gbx.vizx import _interactive, _maplibre, _pmtiles

    default = _maplibre.DEFAULT_MAX_EMBED_MB
    for fn in (_pmtiles.plot_pmtiles, _interactive.plot_interactive):
        got = inspect.signature(fn).parameters["max_embed_mb"].default
        assert (
            got is None
        ), f"{fn.__name__} default max_embed_mb={got} != None (resolve sentinel)"
    for fn in (_maplibre.audit_layers, _maplibre.prepare_layers):
        got = inspect.signature(fn).parameters["max_embed_mb"].default
        assert got == default, (
            f"{fn.__name__} default max_embed_mb={got} != "
            f"DEFAULT_MAX_EMBED_MB={default}"
        )


def test_guard_measures_rendered_html_not_archive_bytes():
    """The over-budget pre-check must compare base64-INFLATED archive size to the
    budget, not the raw archive bytes.

    A raw archive just under the byte budget still renders to ~4/3x its size in
    base64; measuring raw bytes lets a too-large embed slip through and get
    truncated by the cell cap. With an archive whose RAW size is under the budget
    but whose base64 size exceeds it, the verdict must be over-budget (static),
    not embed.
    """
    # 1.0 MB budget. Raw archive = 0.9 MB (under budget by raw bytes) but base64
    # inflates to ~1.2 MB (over budget). Must NOT embed.
    budget_mb = 1.0
    raw_size = int(0.9 * 1024 * 1024)
    archive = b"PMTiles" + b"\x03" + b"\x00" * (raw_size - 8)
    layer = pmtiles_layer(archive, label="base64-inflate")
    result = audit_layers([layer], max_embed_mb=budget_mb)
    assert result["fits"] is False, (
        "guard let a raw-under-budget archive through; it must account for the "
        "~4/3x base64 inflation of the rendered HTML"
    )
    assert result["verdict"] == "static"


def test_remedy_message_mentions_serverless_cell_cap():
    """The over-budget fallback warning must point at the Serverless cell-output
    cap lever (%set_cell_max_output_size_in_mb) and NOT advise 'just increase
    max_embed_mb' (which fails -- the cap is a hard platform ceiling)."""
    BIG_ARCHIVE = b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024)
    big_layer = pmtiles_layer(BIG_ARCHIVE, label="big")
    out = prepare_layers([big_layer], max_embed_mb=1, fallback=True)
    assert out["mode"] == "static"
    combined = " ".join(out["warnings"]).lower()
    assert (
        "set_cell_max_output_size_in_mb" in combined
    ), "remedy must mention the Serverless cell-output cap lever"
    # Must NOT tell the user the misleading 'increase max_embed_mb' remedy alone.
    assert "increase max_embed_mb if your notebook" not in combined
