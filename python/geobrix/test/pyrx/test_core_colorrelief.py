import numpy as np
from rasterio.enums import ColorInterp
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import terrain


def _dem(data):
    data = np.asarray(data, dtype="float32")
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_color_relief_interpolates_rgb(tmp_path):
    table = tmp_path / "colors.txt"
    # elev=0 -> black (R=0 G=0 B=0); elev=100 -> white (R=255 G=255 B=255)
    table.write_text("0 0 0 0\n100 255 255 255\n")
    dem = np.array([[0, 50], [100, 100]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        assert o.count == 3
        assert o.dtypes[0] == "uint8"
        rgb = o.read()  # (3, 2, 2)
        # elev 0 -> black (0,0,0); elev 100 -> white (255,255,255); 50 -> ~128
        assert tuple(int(rgb[c, 0, 0]) for c in range(3)) == (0, 0, 0)
        assert tuple(int(rgb[c, 1, 0]) for c in range(3)) == (255, 255, 255)
        assert all(abs(int(rgb[c, 0, 1]) - 128) <= 2 for c in range(3))


def test_color_relief_rgba_when_alpha_present(tmp_path):
    table = tmp_path / "colors_a.txt"
    table.write_text("0 0 0 0 0\n10 255 0 0 255\n")
    dem = np.array([[0, 10]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        assert o.count == 4
        assert o.colorinterp[3] == ColorInterp.alpha


def test_color_relief_without_alpha_labels_no_band_alpha(tmp_path):
    table = tmp_path / "colors_rgb.txt"
    table.write_text("0 0 0 0\n10 255 0 0\n")
    dem = np.array([[0, 10]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        assert o.count == 3
        assert ColorInterp.alpha not in o.colorinterp


def test_color_relief_nv_color_fills_nodata(tmp_path):
    """NoData pixels receive the nv color, not the interpolated value."""
    table = tmp_path / "colors_nv.txt"
    table.write_text("nv 0 128 255\n0 0 0 255\n100 255 255 255\n")
    dem = np.array([[0.0, -9999.0]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        rgb = o.read()
        # nodata pixel gets nv color (0, 128, 255)
        assert int(rgb[0, 0, 1]) == 0
        assert int(rgb[1, 0, 1]) == 128
        assert int(rgb[2, 0, 1]) == 255


def test_color_relief_percent_stops(tmp_path):
    """% stops resolve against band min/max."""
    table = tmp_path / "colors_pct.txt"
    table.write_text("0% 0 0 0\n100% 100 100 100\n")
    dem = np.array([[0, 100]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        rgb = o.read()
        assert tuple(int(rgb[c, 0, 0]) for c in range(3)) == (0, 0, 0)
        assert tuple(int(rgb[c, 0, 1]) for c in range(3)) == (100, 100, 100)


def test_color_relief_hash_comments_and_blank_lines(tmp_path):
    """Lines starting with # and blank lines are ignored."""
    table = tmp_path / "colors_comments.txt"
    table.write_text("# this is a comment\n\n0 0 0 0\n100 255 255 255\n")
    dem = np.array([[0, 100]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        assert o.count == 3
        rgb = o.read()
        assert tuple(int(rgb[c, 0, 0]) for c in range(3)) == (0, 0, 0)
        assert tuple(int(rgb[c, 0, 1]) for c in range(3)) == (255, 255, 255)


def test_color_relief_comma_separated(tmp_path):
    """Color table entries can use commas as separators."""
    table = tmp_path / "colors_csv.txt"
    table.write_text("0,0,0,255\n100,255,255,255\n")
    dem = np.array([[0, 100]], dtype="float32")
    with _serde.open_tile(_dem(dem)) as ds:
        out = terrain.color_relief(ds, str(table))
    with _serde.open_tile(out) as o:
        assert o.count == 3
