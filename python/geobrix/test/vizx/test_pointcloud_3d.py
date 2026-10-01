import math
import re

import numpy as np
import pandas as pd
import pytest
from databricks.labs.gbx.vizx._pointcloud import load_point_cloud


def _write_laz(path, n=20, with_rgb=True):
    laspy = pytest.importorskip("laspy")
    hdr = laspy.LasHeader(point_format=(3 if with_rgb else 1))  # fmt 3 has RGB, fmt 1 none
    las = laspy.LasData(hdr)
    las.x = np.arange(n, dtype="float64")
    las.y = np.arange(n, dtype="float64")
    las.z = np.zeros(n)
    if with_rgb:
        # 16-bit color: store 0..65535; loader must downscale to 0..255
        las.red = np.full(n, 256 * 10, dtype="uint16")   # -> 10
        las.green = np.full(n, 256 * 20, dtype="uint16")  # -> 20
        las.blue = np.full(n, 256 * 30, dtype="uint16")   # -> 30
    las.write(str(path))


def test_load_laz_rgb(tmp_path):
    p = tmp_path / "c.laz"
    _write_laz(p, n=20, with_rgb=True)
    x, y, z, values, rgb, _ = load_point_cloud(str(p), max_points=0)
    assert rgb is not None and rgb.shape == (20, 3) and rgb.dtype == np.uint8
    assert (rgb[:, 0] == 10).all() and (rgb[:, 1] == 20).all() and (rgb[:, 2] == 30).all()


def test_load_laz_no_rgb(tmp_path):
    p = tmp_path / "c.laz"
    _write_laz(p, n=20, with_rgb=False)
    *_, rgb, _ = load_point_cloud(str(p), max_points=0)
    assert rgb is None


def test_load_df_rgb():
    df = pd.DataFrame({"x": [0, 1, 2.], "y": [0, 1, 2.], "z": [0, 0, 0.],
                       "r": [1, 2, 3], "g": [4, 5, 6], "b": [7, 8, 9]})
    *_, rgb, _ = load_point_cloud(df, max_points=0)
    assert rgb is not None and rgb.shape == (3, 3) and list(rgb[0]) == [1, 4, 7]


def test_rgb_decimation_aligned():
    n = 1000
    df = pd.DataFrame({"x": np.arange(n), "y": np.arange(n), "z": np.zeros(n),
                       "r": np.arange(n) % 256, "g": np.zeros(n), "b": np.zeros(n)})
    x, *_1, rgb, _2 = load_point_cloud(df, max_points=100, seed=0)
    assert len(x) == 100 and rgb.shape == (100, 3)
    assert (rgb[:, 0] == (x.astype(int) % 256)).all()  # rgb row i corresponds to x row i


def test_render_3d_rgb_returns_figure():
    import matplotlib
    matplotlib.use("Agg")
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d
    n = 500
    x = np.random.default_rng(0).random(n)
    y = np.random.default_rng(1).random(n)
    z = np.random.default_rng(2).random(n)
    rgb = np.random.default_rng(3).integers(0, 256, size=(n, 3), dtype=np.uint8)
    fig = render_point_cloud_3d(x, y, z, rgb=rgb, point_size=1.5, title="t")
    import matplotlib.figure
    assert isinstance(fig, matplotlib.figure.Figure)
    ax = fig.axes[0]
    assert ax.name == "3d"  # Axes3D
    assert len(ax.collections) >= 1  # the scatter


def test_render_3d_cmap_path_and_singleton():
    import matplotlib
    matplotlib.use("Agg")
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d
    # cmap-on-z path (no rgb) + a 1-point cloud must not raise
    fig = render_point_cloud_3d(np.array([0.0]), np.array([0.0]), np.array([0.0]),
                                values=np.array([0.0]), cmap="viridis")
    assert fig.axes[0].name == "3d"


def test_build_pointcloud_html():
    from databricks.labs.gbx.vizx._pointcloud_html import build_pointcloud_html
    n = 50
    x = np.linspace(0, 10, n)
    y = np.linspace(0, 5, n)
    z = np.linspace(0, 2, n)
    rgb = np.full((n, 3), 128, dtype=np.uint8)
    html = build_pointcloud_html(x, y, z, rgb=rgb, point_size=3.0)
    assert isinstance(html, str)
    assert "unpkg.com/deck.gl@9.0.0" in html            # pinned CDN (matches _maplibre host)
    assert 'integrity="sha384-' in html and 'crossorigin="anonymous"' in html  # SRI (security hook + convention)
    assert "PointCloudLayer" in html and "OrbitView" in html
    assert "atob" in html   # binary buffer decode in JS
    # point count embedded
    assert "50" in html


def test_plot_point_cloud_static_when_embed_zero():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.figure
    from databricks.labs.gbx import vizx as vz
    df = pd.DataFrame({"x": np.arange(100.), "y": np.arange(100.), "z": np.zeros(100),
                       "r": np.full(100, 10, "int"), "g": np.full(100, 20, "int"), "b": np.full(100, 30, "int")})
    out = vz.plot_point_cloud(df, color="rgb", max_embed_mb=0)   # force static
    assert isinstance(out, matplotlib.figure.Figure)


def test_plot_point_cloud_html_outside_notebook():
    from databricks.labs.gbx import vizx as vz
    df = pd.DataFrame({"x": np.arange(100.), "y": np.arange(100.), "z": np.zeros(100),
                       "r": np.full(100, 10, "int"), "g": np.full(100, 20, "int"), "b": np.full(100, 30, "int")})
    out = vz.plot_point_cloud(df, color="rgb")   # default budget; outside a notebook -> HTML string
    assert isinstance(out, str) and "PointCloudLayer" in out


def test_plot_point_cloud_rgb_fallback_warns():
    import matplotlib
    matplotlib.use("Agg")
    from databricks.labs.gbx import vizx as vz
    df = pd.DataFrame({"x": [0, 1.], "y": [0, 1.], "z": [0, 0.]})  # no color
    with pytest.warns(UserWarning):
        out = vz.plot_point_cloud(df, color="rgb", max_embed_mb=0)
    import matplotlib.figure
    assert isinstance(out, matplotlib.figure.Figure)


def test_plot_point_cloud_color_gates_rgb_vs_cmap(monkeypatch):
    """color='z' (or a named column) must not render true-color even when the
    source carries RGB -- load_point_cloud returns rgb unconditionally, so
    plot_point_cloud must gate it on `color` before handing it to the backend.
    """
    from databricks.labs.gbx.vizx import _pointcloud_html as pch

    captured = {}

    def _fake_build_pointcloud_html(x, y, z, *, rgb=None, values=None, **kw):
        captured["rgb"] = rgb
        return "<div>PointCloudLayer</div>"

    monkeypatch.setattr(pch, "build_pointcloud_html", _fake_build_pointcloud_html)

    from databricks.labs.gbx import vizx as vz

    df = pd.DataFrame(
        {
            "x": [0, 1, 2.0],
            "y": [0, 1, 2.0],
            "z": [0, 0, 0.0],
            "r": [1, 2, 3],
            "g": [4, 5, 6],
            "b": [7, 8, 9],
        }
    )
    vz.plot_point_cloud(df, color="rgb")
    assert captured["rgb"] is not None

    vz.plot_point_cloud(df, color="z")
    assert captured["rgb"] is None


def test_plot_point_cloud_static_forwards_max_points_and_seed(monkeypatch):
    """The forced-static fallback must forward max_points/seed to
    render_point_cloud_3d, not silently re-decimate at its own defaults.
    """
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = {}

    def _fake_render_point_cloud_3d(x, y, z, **kw):
        captured.update(kw)
        return object()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render_point_cloud_3d)

    from databricks.labs.gbx import vizx as vz

    df = pd.DataFrame(
        {
            "x": [0, 1, 2.0],
            "y": [0, 1, 2.0],
            "z": [0, 0, 0.0],
            "r": [1, 2, 3],
            "g": [4, 5, 6],
            "b": [7, 8, 9],
        }
    )
    vz.plot_point_cloud(df, max_embed_mb=0, max_points=777, seed=5)
    assert captured["max_points"] == 777
    assert captured["seed"] == 5


def test_plot_point_cloud_embed_raises_output_cap(monkeypatch):
    """plot_point_cloud's embed branch must raise the Serverless cell-output cap
    before handing the HTML to displayHTML -- mirrors plot_interactive. The 6MB
    embed budget plot_point_cloud resolves is only safe when the cell-output cap
    is raised to 20MB; otherwise displayHTML's ~2-3x inflation truncates/blanks it.
    """
    import databricks.labs.gbx.vizx._interactive as itx

    calls = []
    captured = {}
    monkeypatch.setattr(
        itx, "_raise_cell_output_cap", lambda: (calls.append(1) or True)
    )

    def _fake_dh(html):
        captured["html"] = html

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: _fake_dh)

    from databricks.labs.gbx import vizx as vz

    df = pd.DataFrame(
        {
            "x": [0, 1, 2.0],
            "y": [0, 1, 2.0],
            "z": [0, 0, 0.0],
            "r": [1, 2, 3],
            "g": [4, 5, 6],
            "b": [7, 8, 9],
        }
    )
    out = vz.plot_point_cloud(df, color="rgb")
    assert out is None
    assert calls == [1], "cap-raise helper must be called on the embed path"
    assert "html" in captured and "PointCloudLayer" in captured["html"]


def test_pointcloud_html_zoom_fits_extent():
    """The opening zoom must be derived from the cloud's own extent (scale-invariant),
    not a hardcoded constant -- a UTM-meter cloud (hundreds of units) and a small
    cloud (tens of units) must open at different, framed zoom levels.
    """
    from databricks.labs.gbx.vizx._pointcloud_html import build_pointcloud_html

    n = 50
    x = np.linspace(0, 10, n)
    y = np.linspace(0, 5, n)
    z = np.linspace(0, 2, n)
    rgb = np.full((n, 3), 128, dtype=np.uint8)

    html_small = build_pointcloud_html(x, y, z, rgb=rgb)
    html_big = build_pointcloud_html(x * 100, y * 100, z * 100, rgb=rgb)

    def _zoom(html):
        m = re.search(r"zoom:\s*([-\d.]+)", html)
        assert m, "no `zoom:` found in HTML"
        return float(m.group(1))

    zoom_small = _zoom(html_small)
    zoom_big = _zoom(html_big)
    assert zoom_big < zoom_small, "a bigger cloud must open at a smaller zoom"
    assert math.isfinite(zoom_small) and math.isfinite(zoom_big)
    assert -20.0 <= zoom_small <= 24.0
    assert -20.0 <= zoom_big <= 24.0


def test_load_laz_empty_rgb_no_crash(tmp_path):
    """A 0-point LAZ in an RGB point format must not crash in the arr.max() reduction."""
    p = tmp_path / "empty.laz"
    _write_laz(p, n=0, with_rgb=True)
    x, y, z, values, rgb, _ = load_point_cloud(str(p), max_points=0)
    assert x.shape[0] == 0
    assert rgb is None or rgb.shape == (0, 3)


def test_load_df_rgb_16bit_downscaled():
    """DataFrame r/g/b carrying 16-bit color (0..65535) must downscale to 0..255,
    not wrap mod 256 (2560 must become 10, not 0).
    """
    df = pd.DataFrame(
        {
            "x": [0, 1, 2.0],
            "y": [0, 1, 2.0],
            "z": [0, 0, 0.0],
            "r": [2560, 2560, 2560],
            "g": [5120, 5120, 5120],
            "b": [7680, 7680, 7680],
        }
    )
    *_, rgb, _ = load_point_cloud(df, max_points=0)
    assert rgb is not None and rgb.shape == (3, 3)
    assert list(rgb[0]) == [10, 20, 30]


def test_pointcloud_html_unique_container_id():
    """Two embedded clouds in one document must not collide on getElementById."""
    from databricks.labs.gbx.vizx._pointcloud_html import build_pointcloud_html

    n = 10
    x = np.arange(n, dtype="float64")
    y = np.arange(n, dtype="float64")
    z = np.zeros(n)
    rgb = np.full((n, 3), 1, dtype=np.uint8)

    html1 = build_pointcloud_html(x, y, z, rgb=rgb)
    html2 = build_pointcloud_html(x, y, z, rgb=rgb)

    def _container_id(html):
        m = re.search(r'<div id="([^"]+)"', html)
        assert m, "no container <div id=...> found"
        return m.group(1)

    assert _container_id(html1) != _container_id(html2)
