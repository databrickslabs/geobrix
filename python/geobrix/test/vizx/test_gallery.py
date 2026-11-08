"""Tests for gbx.vizx.plot_gallery (small-multiples contact sheet)."""

from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")  # headless: no display needed

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402

from databricks.labs.gbx.vizx import plot_gallery  # noqa: E402
from databricks.labs.gbx.vizx._gallery import _resolve_sources  # noqa: E402


@pytest.fixture(autouse=True)
def _close_figures():
    yield
    plt.close("all")


def _pngs(d, n):
    from PIL import Image

    for i in range(n):
        arr = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        Image.fromarray(arr, "RGB").save(d / f"img_{i:02d}.png")


def _geotiff(path):
    import rasterio
    from rasterio.transform import from_origin

    data = np.arange(64, dtype="uint16").reshape(8, 8) * 50
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="uint16",
        crs="EPSG:32630",
        transform=from_origin(400000.0, 4600000.0, 10.0, 10.0),
    ) as ds:
        ds.write(data, 1)


def test_the_grid_has_rows_times_cols_axes_and_draws_every_image(tmp_path):
    _pngs(tmp_path, 4)
    fig = plot_gallery(tmp_path, mode="all", cols=3, renderer="photo", thumb_px=32)
    assert len(fig.axes) == 6  # 2 rows x 3 cols, two blank trailing
    assert sum(len(ax.get_images()) for ax in fig.axes) >= 4


def test_a_geotiff_goes_through_the_raster_renderer_automatically(tmp_path):
    _geotiff(str(tmp_path / "test.tif"))
    fig = plot_gallery(tmp_path, mode="all", cols=1, renderer="auto", thumb_px=32)
    assert fig is not None and fig.axes[0].get_images()


def test_mode_limit_and_sample_pick_how_many_of_the_images_are_shown(tmp_path):
    _pngs(tmp_path, 5)

    def shown(mode, limit):
        fig = plot_gallery(
            tmp_path, mode=mode, limit=limit, cols=5, renderer="photo", thumb_px=16
        )
        return sum(len(ax.get_images()) for ax in fig.axes), fig

    assert shown("all", 3)[0] == 5
    assert shown("limit", 3)[0] == 3
    n, fig = shown("sample", 3)
    assert n == 3
    text = fig._suptitle.get_text()  # the auto title says "N of M images"
    assert "3" in text and "5" in text
    assert shown("sample", 24)[0] == 5  # a small set is shown whole
    with pytest.raises(ValueError, match="mode must be"):
        plot_gallery(tmp_path, mode="random")


def test_labels_none_leaves_the_titles_empty_and_title_becomes_the_suptitle(tmp_path):
    _pngs(tmp_path, 2)
    fig = plot_gallery(
        tmp_path, mode="all", cols=2, renderer="photo", thumb_px=16, labels=None
    )
    assert all(ax.get_title() == "" for ax in fig.axes)
    fig = plot_gallery(
        tmp_path, mode="all", cols=2, renderer="photo", thumb_px=16, title="My Gallery"
    )
    assert fig._suptitle.get_text() == "My Gallery"


def test_an_empty_directory_returns_none(tmp_path):
    assert plot_gallery(tmp_path, extensions=(".jpg",)) is None


def test_sources_are_filtered_by_extension_case_insensitively_and_scheme_stripped(
    tmp_path,
):
    for name in ("a.jpg", "b.PNG", "c.txt", "d.tif"):
        (tmp_path / name).write_text("")
    names = [
        p.rsplit("/", 1)[-1]
        for p in _resolve_sources(str(tmp_path), (".jpg", ".png", ".tif"))
    ]
    assert names == sorted(names) == ["a.jpg", "b.PNG", "d.tif"]
    assert _resolve_sources(str(tmp_path / "d.tif"), (".tif",)) == [
        str(tmp_path / "d.tif")
    ]

    # explicit lists pass through unfiltered, with dbfs: / file:// stripped
    assert _resolve_sources(["/data/a.jpg", "/data/b.png"], (".jpg",)) == [
        "/data/a.jpg",
        "/data/b.png",
    ]
    for src in ("dbfs:/Volumes/c/s/v/img.jpg", "file:///Volumes/c/s/v/img.jpg"):
        assert _resolve_sources([src], (".jpg",)) == ["/Volumes/c/s/v/img.jpg"]


def test_directory_walks_go_through_list_files(tmp_path):
    # fail-on-revert: the recursive walk is the shared listing helper, not a local os.walk
    (tmp_path / "a.tif").write_bytes(b"x")
    with patch("databricks.labs.gbx.ds._listing.list_files") as mock_lf:
        mock_lf.return_value = []
        _resolve_sources(str(tmp_path), (".tif",))
    mock_lf.assert_called_once()
    assert mock_lf.call_args[1].get("recursive", True) is True
