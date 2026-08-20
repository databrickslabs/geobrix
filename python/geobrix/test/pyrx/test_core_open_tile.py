"""open_tile front-door: raster precedence, lazy windowed read across the 3
layouts (== full-read slice), multi-block window, clip, and a WarpedVRT probe.
"""

import numpy as np
import pytest
import rasterio
import shapely
import shapely.wkb
from rasterio.windows import Window
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _layouts
from .conftest import make_geotiff_bytes

W, H, BS = 512, 512, 256
WINDOW = (128, 64, 200, 300)  # spans >1 block in both axes (crosses 256 boundary)


def _slice_of_full(path):
    with rasterio.open(path) as ds:
        full = ds.read(1)
    c, r, w, h = WINDOW
    return full[r : r + h, c : c + w]


@pytest.fixture
def layouts(tmp_path):
    return {
        "cog": _layouts.write_cog(str(tmp_path / "a.cog.tif"), W, H, BS),
        "tiled": _layouts.write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), W, H, BS),
        "striped": _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), W, H),
    }


def test_raster_present_precedence_ignores_path():
    # raster set + a bogus path: must open the bytes, never touch path.
    b = make_geotiff_bytes(width=4, height=3)
    tile = VirtualTile(
        cellid=1, raster=b, path="/nonexistent/x.tif", window=(0, 0, 4, 3)
    )
    with ot.open_tile(tile) as ds:
        assert ds.width == 4 and ds.height == 3


@pytest.mark.parametrize("layout", ["cog", "tiled", "striped"])
def test_windowed_read_equals_full_slice(layouts, layout):
    tile = VirtualTile(cellid=2, path=layouts[layout], window=WINDOW)
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
    assert got.shape == (WINDOW[3], WINDOW[2])  # multi-block window honored
    assert np.array_equal(got, _slice_of_full(layouts[layout]))


def test_clip_applied_to_virtual_window(layouts):
    # Clip within the window to a sub-box (partial); result narrower than window.
    with rasterio.open(layouts["cog"]) as ds:
        win_transform = ds.window_transform(Window(*WINDOW))
        # geographic bounds of the left third of the window
        minx = win_transform.c
        maxx = minx + (WINDOW[2] // 3) * abs(win_transform.a)
        maxy = win_transform.f
        miny = maxy - WINDOW[3] * abs(win_transform.e)
    poly = box(minx, miny, maxx, maxy)
    tile = VirtualTile(
        cellid=3,
        path=layouts["cog"],
        window=WINDOW,
        clip_polygon=shapely.wkb.dumps(poly),
        clip_crs="EPSG:4326",
    )
    with ot.open_tile(tile) as ds:
        assert ds.width < WINDOW[2]


def test_warpedvrt_probe_reprojects_window(layouts):
    # crs set & != source -> lazy warp; result carries target CRS.
    tile = VirtualTile(cellid=4, path=layouts["tiled"], window=WINDOW, crs="EPSG:3857")
    with ot.open_tile(tile) as ds:
        assert ds.crs.to_epsg() == 3857
        assert ds.width > 0 and ds.height > 0


def test_disjoint_clip_yields_valid_empty_1x1(layouts):
    # Clip polygon far from the tile's geographic footprint -> clip_dataset
    # returns None -> open_tile must yield a VALID, readable 1x1 NoData dataset
    # (not an error), created from a clean minimal profile (no source tiling).
    far = box(1000.0, 1000.0, 1001.0, 1001.0)  # nowhere near origin (10, 50)
    tile = VirtualTile(
        cellid=6,
        path=layouts["cog"],
        window=WINDOW,
        clip_polygon=shapely.wkb.dumps(far),
        clip_crs="EPSG:4326",
    )
    with rasterio.open(layouts["cog"]) as src:
        src_nodata = src.nodata
    with ot.open_tile(tile) as ds:
        assert ds.width == 1 and ds.height == 1
        arr = ds.read()  # must succeed
        assert arr.shape[-2:] == (1, 1)
        assert arr.flat[0] == src_nodata


def test_materialize_returns_array_transform_profile(layouts):
    arr, transform, profile = ot.materialize_array(
        VirtualTile(cellid=5, path=layouts["striped"], window=WINDOW)
    )
    assert arr.shape[-2:] == (WINDOW[3], WINDOW[2])
    assert profile["width"] == WINDOW[2]


def test_open_tile_windowless_virtual_raises_valueerror(tmp_path):
    """A windowless virtual tile (path, no window) must raise ValueError at the
    read site — not a bare TypeError from a None unpack."""
    import rasterio

    # Write a tiny GeoTIFF so the path exists; the guard must fire before open.
    p = str(tmp_path / "src.tif")
    with rasterio.open(
        p, "w", driver="GTiff", height=4, width=4, count=1, dtype="uint8"
    ) as ds:
        import numpy as np

        ds.write(np.zeros((1, 4, 4), dtype="uint8"))

    vt = VirtualTile(cellid=0, path=p)  # no window, path_mode=None
    with pytest.raises(ValueError, match="windowed read"):
        with ot.open_tile(vt):
            pass


def test_tile_to_bytes_windowless_virtual_raises_valueerror(tmp_path):
    """_tile_to_bytes must raise the same descriptive ValueError for a windowless
    virtual tile."""
    import rasterio

    p = str(tmp_path / "src.tif")
    with rasterio.open(
        p, "w", driver="GTiff", height=4, width=4, count=1, dtype="uint8"
    ) as ds:
        import numpy as np

        ds.write(np.zeros((1, 4, 4), dtype="uint8"))

    vt = VirtualTile(cellid=0, path=p)  # no window
    with pytest.raises(ValueError, match="windowed read"):
        ot._tile_to_bytes(vt)


# ---------------------------------------------------------------------------
# Tests for metadata-key helpers (path_file_size / tile_size)
# ---------------------------------------------------------------------------


def test_read_size_key_parses_valid_string_int():
    """_read_size_key parses a valid string-encoded int to int."""
    metadata = {"path_file_size": "1048576"}
    assert ot._read_size_key(metadata, "path_file_size") == 1048576

    metadata = {"tile_byte_size": "2097152"}
    assert ot._read_size_key(metadata, "tile_byte_size") == 2097152


def test_read_size_key_returns_none_for_absent_key():
    """_read_size_key returns None when the key is absent."""
    metadata = {"other_key": "value"}
    assert ot._read_size_key(metadata, "path_file_size") is None
    assert ot._read_size_key({}, "tile_byte_size") is None


def test_read_size_key_returns_none_for_unparseable_value():
    """_read_size_key returns None when the value is not a valid int."""
    metadata = {"path_file_size": "not_a_number"}
    assert ot._read_size_key(metadata, "path_file_size") is None

    metadata = {"tile_byte_size": ""}
    assert ot._read_size_key(metadata, "tile_byte_size") is None


def test_read_size_key_none_metadata():
    """_read_size_key handles None metadata gracefully."""
    assert ot._read_size_key(None, "path_file_size") is None


def test_parse_pending_ignores_size_keys():
    """_parse_pending does NOT treat path_file_size/tile_size as pending edits."""
    metadata = {
        ot.PENDING_BANDS: "1,2",
        ot.PENDING_NODATA: "0",
        "path_file_size": "1048576",
        "tile_byte_size": "2097152",
    }
    bands, nodata, srid, crs_str = ot._parse_pending(metadata)
    # Size keys are informational; _parse_pending ignores them.
    assert bands == [1, 2]
    assert nodata == 0.0
    assert srid is None
    assert crs_str is None


def test_without_pending_preserves_size_keys():
    """_without_pending does NOT strip path_file_size/tile_size (they survive)."""
    metadata = {
        ot.PENDING_BANDS: "1,2",
        ot.PENDING_NODATA: "0",
        "path_file_size": "1048576",
        "tile_byte_size": "2097152",
        "other_key": "value",
    }
    result = ot._without_pending(metadata)
    # Pending keys are removed; size keys persist.
    assert ot.PENDING_BANDS not in result
    assert ot.PENDING_NODATA not in result
    assert result.get("path_file_size") == "1048576"
    assert result.get("tile_byte_size") == "2097152"
    assert result.get("other_key") == "value"
