import numpy as np
import pandas as pd
import pytest
from databricks.labs.gbx.vizx._pointcloud import load_point_cloud

def _write_laz(path, n=20, with_rgb=True):
    laspy = pytest.importorskip("laspy")
    hdr = laspy.LasHeader(point_format=(3 if with_rgb else 1))  # fmt 3 has RGB, fmt 1 none
    las = laspy.LasData(hdr)
    las.x = np.arange(n, dtype="float64"); las.y = np.arange(n, dtype="float64"); las.z = np.zeros(n)
    if with_rgb:
        # 16-bit color: store 0..65535; loader must downscale to 0..255
        las.red = np.full(n, 256 * 10, dtype="uint16")   # -> 10
        las.green = np.full(n, 256 * 20, dtype="uint16")  # -> 20
        las.blue = np.full(n, 256 * 30, dtype="uint16")   # -> 30
    las.write(str(path))

def test_load_laz_rgb(tmp_path):
    p = tmp_path / "c.laz"; _write_laz(p, n=20, with_rgb=True)
    x, y, z, values, rgb, _ = load_point_cloud(str(p), max_points=0)
    assert rgb is not None and rgb.shape == (20, 3) and rgb.dtype == np.uint8
    assert (rgb[:, 0] == 10).all() and (rgb[:, 1] == 20).all() and (rgb[:, 2] == 30).all()

def test_load_laz_no_rgb(tmp_path):
    p = tmp_path / "c.laz"; _write_laz(p, n=20, with_rgb=False)
    *_, rgb, _ = load_point_cloud(str(p), max_points=0)
    assert rgb is None

def test_load_df_rgb():
    df = pd.DataFrame({"x":[0,1,2.], "y":[0,1,2.], "z":[0,0,0.],
                       "r":[1,2,3], "g":[4,5,6], "b":[7,8,9]})
    *_, rgb, _ = load_point_cloud(df, max_points=0)
    assert rgb is not None and rgb.shape == (3, 3) and list(rgb[0]) == [1, 4, 7]

def test_rgb_decimation_aligned():
    n = 1000
    df = pd.DataFrame({"x":np.arange(n), "y":np.arange(n), "z":np.zeros(n),
                       "r":np.arange(n) % 256, "g":np.zeros(n), "b":np.zeros(n)})
    x, *_1, rgb, _2 = load_point_cloud(df, max_points=100, seed=0)
    assert len(x) == 100 and rgb.shape == (100, 3)
    assert (rgb[:, 0] == (x.astype(int) % 256)).all()  # rgb row i corresponds to x row i

def test_render_3d_rgb_returns_figure():
    import matplotlib; matplotlib.use("Agg")
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d
    n = 500
    x = np.random.default_rng(0).random(n); y = np.random.default_rng(1).random(n); z = np.random.default_rng(2).random(n)
    rgb = np.random.default_rng(3).integers(0, 256, size=(n, 3), dtype=np.uint8)
    fig = render_point_cloud_3d(x, y, z, rgb=rgb, point_size=1.5, title="t")
    import matplotlib.figure
    assert isinstance(fig, matplotlib.figure.Figure)
    ax = fig.axes[0]
    assert ax.name == "3d"  # Axes3D
    assert len(ax.collections) >= 1  # the scatter

def test_render_3d_cmap_path_and_singleton():
    import matplotlib; matplotlib.use("Agg")
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d
    # cmap-on-z path (no rgb) + a 1-point cloud must not raise
    fig = render_point_cloud_3d(np.array([0.0]), np.array([0.0]), np.array([0.0]),
                                values=np.array([0.0]), cmap="viridis")
    assert fig.axes[0].name == "3d"

def test_build_pointcloud_html():
    from databricks.labs.gbx.vizx._pointcloud_html import build_pointcloud_html
    n = 50
    x = np.linspace(0, 10, n); y = np.linspace(0, 5, n); z = np.linspace(0, 2, n)
    rgb = np.full((n, 3), 128, dtype=np.uint8)
    html = build_pointcloud_html(x, y, z, rgb=rgb, point_size=3.0)
    assert isinstance(html, str)
    assert "unpkg.com/deck.gl@9.0.0" in html            # pinned CDN (matches _maplibre host)
    assert 'integrity="sha384-' in html and 'crossorigin="anonymous"' in html  # SRI (security hook + convention)
    assert "PointCloudLayer" in html and "OrbitView" in html
    assert "atob" in html   # binary buffer decode in JS
    # point count embedded
    assert "50" in html
