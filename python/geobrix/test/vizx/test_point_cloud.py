"""point_cloud_layer: static LiDAR/point-cloud rendering in plot_static."""

import matplotlib

matplotlib.use("Agg")

import numpy as np  # noqa: E402
import pytest  # noqa: E402

from databricks.labs.gbx.vizx._layers import (  # noqa: E402
    Layer,
    as_layers,
    point_cloud_layer,
)
from databricks.labs.gbx.vizx._static_map import plot_static  # noqa: E402


def _synthetic_xyz(n=500, seed=0):
    """A pandas DataFrame of n points with x/y (EPSG:4326-ish) + z elevation."""
    import pandas as pd

    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "x": rng.uniform(-122.5, -122.4, n),
            "y": rng.uniform(37.7, 37.8, n),
            "z": rng.uniform(0.0, 100.0, n),
        }
    )


def test_point_cloud_layer_builds_kind():
    lyr = point_cloud_layer(_synthetic_xyz())
    assert isinstance(lyr, Layer)
    assert lyr.kind == "point_cloud"


def test_laz_and_las_paths_autodetect_as_point_cloud():
    # as_layers inspects the extension only (no file read), so a bare path routes
    # a point cloud to point_cloud_layer instead of falling through to vector_layer.
    for ext in (".laz", ".las", ".LAZ"):
        layers = as_layers(f"/tmp/whatever{ext}")
        assert len(layers) == 1
        assert layers[0].kind == "point_cloud", ext


def test_plot_static_point_cloud_colors_by_elevation():
    df = _synthetic_xyz(300)
    ax = plot_static([point_cloud_layer(df, crs="EPSG:4326")], basemap=False)
    assert ax.collections, "no scatter drawn"
    arr = ax.collections[-1].get_array()  # the per-point scalar mapped through cmap
    assert arr is not None and len(arr) == 300


def test_point_cloud_decimation_caps_points():
    df = _synthetic_xyz(1000)
    ax = plot_static(
        [point_cloud_layer(df, max_points=200, crs="EPSG:4326")], basemap=False
    )
    assert len(ax.collections[-1].get_offsets()) == 200


def test_point_cloud_category_colors_legend():
    df = _synthetic_xyz(200)
    df["cls"] = ["ground" if i % 2 else "veg" for i in range(len(df))]
    cc = {"veg": "#2ca02c", "ground": "#808080"}
    ax = plot_static(
        [point_cloud_layer(df, column="cls", category_colors=cc, crs="EPSG:4326")],
        basemap=False,
    )
    leg = ax.get_legend()
    assert leg is not None
    assert {t.get_text() for t in leg.get_texts()} == {"veg", "ground"}


def test_plot_static_reads_las_path(tmp_path):
    laspy = pytest.importorskip("laspy")
    df = _synthetic_xyz(150)
    header = laspy.LasHeader(point_format=0)
    las = laspy.LasData(header)
    las.x = df["x"].to_numpy()
    las.y = df["y"].to_numpy()
    las.z = df["z"].to_numpy()
    p = tmp_path / "cloud.las"
    las.write(str(p))

    # crs override (the synthetic .las carries no CRS VLR).
    ax = plot_static([point_cloud_layer(str(p), crs="EPSG:4326")], basemap=False)
    assert ax.collections and len(ax.collections[-1].get_offsets()) == 150


def test_load_point_cloud_refuses_a_laz_with_a_bad_chunk_table(tmp_path, monkeypatch):
    """A compressed file whose chunk table offset lies outside the file is a
    ValueError naming it, raised before laspy decodes (the decoder sizes its
    buffers from that table and aborts the process on a bad one)."""
    import struct

    laspy = pytest.importorskip("laspy")
    from databricks.labs.gbx.vizx._pointcloud import load_point_cloud

    df = _synthetic_xyz(150)
    las = laspy.LasData(laspy.LasHeader(point_format=0))
    las.x, las.y, las.z = (df[c].to_numpy() for c in "xyz")
    p = tmp_path / "bad.laz"
    las.write(str(p), do_compress=True)
    with open(p, "r+b") as fh:
        raw = fh.read(100)
        fh.seek(struct.unpack_from("<I", raw, 96)[0])
        fh.write(struct.pack("<q", 1 << 40))

    def decoder_reached(*a, **k):
        raise AssertionError("laspy decoded a file whose chunk table was refused")

    monkeypatch.setattr(laspy, "read", decoder_reached)
    with pytest.raises(ValueError, match=r"bad\.laz.*chunk table offset"):
        load_point_cloud(str(p))


def test_load_point_cloud_refuses_a_laz_with_a_run_of_ff(tmp_path, monkeypatch):
    """Compressed points holding a run of 0xFF bytes (a failed copy) overflow the
    decoder's stack and end the process: a ValueError naming the file, raised
    before laspy decodes."""
    import io
    import struct

    laspy = pytest.importorskip("laspy")
    lazrs = pytest.importorskip("lazrs")
    from databricks.labs.gbx.vizx._pointcloud import load_point_cloud

    df = _synthetic_xyz(150)
    las = laspy.LasData(laspy.LasHeader(point_format=3))  # a format with GPS time
    las.x, las.y, las.z = (df[c].to_numpy() for c in "xyz")
    p = tmp_path / "ff.laz"
    las.write(str(p), do_compress=True)
    raw = p.read_bytes()
    start = struct.unpack_from("<I", raw, 96)[0]
    vlr = lazrs.LazVlr(
        laspy.LasHeader.read_from(io.BytesIO(raw)).vlrs.get("LasZipVlr")[0].record_data
    )
    at = io.BytesIO(raw)
    at.seek(start)
    points = lazrs.read_chunk_table(at, vlr)[0][0]
    body = raw[start + 8 : start + 8 + vlr.item_size()] + b"\xff" * 4096
    table = io.BytesIO()
    lazrs.write_chunk_table(table, [(points, len(body))], vlr)
    p.write_bytes(
        raw[:start] + struct.pack("<q", start + 8 + len(body)) + body + table.getvalue()
    )

    def decoder_reached(*a, **k):
        raise AssertionError("laspy decoded a file with a run of 0xFF bytes")

    monkeypatch.setattr(laspy, "read", decoder_reached)
    with pytest.raises(ValueError, match=r"ff\.laz.*a run of 0xFF bytes"):
        load_point_cloud(str(p))
