"""Tests for gbx.vizx.plot_gallery (small-multiples contact sheet).

Run with:
  bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_gallery.py
"""

import os
import tempfile

import matplotlib

matplotlib.use("Agg")  # headless: no display needed

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tiny_png(path, size=(8, 8)):
    """Write a tiny RGB PNG to *path* via PIL."""
    from PIL import Image

    arr = np.random.randint(0, 256, (*size, 3), dtype=np.uint8)
    Image.fromarray(arr, "RGB").save(path)


def _make_tiny_geotiff(path, *, width=8, height=8, epsg=32630):
    """Write a 1-band uint16 GeoTIFF to *path*."""
    import rasterio
    from rasterio.transform import from_origin

    transform = from_origin(400000.0, 4600000.0, 10.0, 10.0)
    data = np.arange(width * height, dtype="uint16").reshape(height, width) * 50
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="uint16",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


# ---------------------------------------------------------------------------
# Unit tests: _select_images
# ---------------------------------------------------------------------------


def test_select_images_all():
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = [str(i) for i in range(100)]
    result = _select_images(paths, "all", 24)
    assert len(result) == 100
    assert result == paths


def test_select_images_limit():
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = [str(i) for i in range(100)]
    result = _select_images(paths, "limit", 24)
    assert len(result) == 24
    assert result == paths[:24]


def test_select_images_sample_count():
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = [str(i) for i in range(100)]
    result = _select_images(paths, "sample", 24)
    # step = max(1, 100//24) = 4 → paths[::4][:24]
    assert len(result) == 24


def test_select_images_sample_spacing():
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = [str(i) for i in range(100)]
    result = _select_images(paths, "sample", 24)
    step = max(1, 100 // 24)
    expected = paths[::step][:24]
    assert result == expected


def test_select_images_sample_small_set():
    """When set is smaller than limit, all items are returned (step=1)."""
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = ["a", "b", "c"]
    result = _select_images(paths, "sample", 24)
    assert result == ["a", "b", "c"]


def test_select_images_limit_fewer_than_limit():
    from databricks.labs.gbx.vizx._gallery import _select_images

    paths = ["a", "b"]
    result = _select_images(paths, "limit", 24)
    assert result == ["a", "b"]


def test_select_images_invalid_mode():
    from databricks.labs.gbx.vizx._gallery import _select_images

    with pytest.raises(ValueError, match="mode must be"):
        _select_images(["a"], "random", 5)


# ---------------------------------------------------------------------------
# Unit tests: _resolve_sources
# ---------------------------------------------------------------------------


def test_resolve_sources_directory():
    """Directory scan returns only image extensions, sorted."""
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    with tempfile.TemporaryDirectory() as d:
        # Create some files
        open(os.path.join(d, "a.jpg"), "w").close()
        open(os.path.join(d, "b.PNG"), "w").close()  # uppercase ext
        open(os.path.join(d, "c.txt"), "w").close()  # non-image
        open(os.path.join(d, "d.tif"), "w").close()

        exts = (".jpg", ".jpeg", ".png", ".tif", ".tiff")
        result = _resolve_sources(d, exts)
        names = [os.path.basename(p) for p in result]
        assert sorted(names) == names  # sorted
        assert "c.txt" not in names
        # Case-insensitive: .PNG should be included
        assert any(n.lower() in ("b.png",) for n in [n.lower() for n in names])
        # .tif included
        assert any(n.lower() == "d.tif" for n in names)


def test_resolve_sources_directory_excludes_non_image():
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    with tempfile.TemporaryDirectory() as d:
        open(os.path.join(d, "photo.jpg"), "w").close()
        open(os.path.join(d, "readme.txt"), "w").close()
        result = _resolve_sources(d, (".jpg",))
        assert len(result) == 1
        assert result[0].endswith("photo.jpg")


def test_resolve_sources_list_passthrough():
    """Explicit list of paths is returned as-is (no filtering)."""
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    paths = ["/data/a.jpg", "/data/b.png"]
    result = _resolve_sources(paths, (".jpg", ".png"))
    assert result == paths


def test_resolve_sources_scheme_strip_dbfs():
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    result = _resolve_sources(["dbfs:/Volumes/cat/sch/vol/img.jpg"], (".jpg",))
    assert result == ["/Volumes/cat/sch/vol/img.jpg"]


def test_resolve_sources_scheme_strip_file():
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    result = _resolve_sources(["file:///Volumes/cat/sch/vol/img.jpg"], (".jpg",))
    assert result == ["/Volumes/cat/sch/vol/img.jpg"]


def test_resolve_sources_single_file():
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    with tempfile.NamedTemporaryFile(suffix=".tif") as f:
        result = _resolve_sources(f.name, (".tif",))
        assert result == [f.name]


def test_resolve_sources_empty_directory():
    from databricks.labs.gbx.vizx._gallery import _resolve_sources

    with tempfile.TemporaryDirectory() as d:
        result = _resolve_sources(d, (".jpg",))
        assert result == []


# ---------------------------------------------------------------------------
# Unit tests: _pick_renderer
# ---------------------------------------------------------------------------


def test_pick_renderer_tif_auto():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.tif", "auto") == "raster"


def test_pick_renderer_tiff_auto():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.tiff", "auto") == "raster"


def test_pick_renderer_vrt_auto():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.vrt", "auto") == "raster"


def test_pick_renderer_jpg_auto():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.jpg", "auto") == "photo"


def test_pick_renderer_png_auto():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.png", "auto") == "photo"


def test_pick_renderer_override_photo():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.tif", "photo") == "photo"


def test_pick_renderer_override_raster():
    from databricks.labs.gbx.vizx._gallery import _pick_renderer

    assert _pick_renderer("/path/to/file.jpg", "raster") == "raster"


# ---------------------------------------------------------------------------
# Integration tests: plot_gallery
# ---------------------------------------------------------------------------


def test_plot_gallery_photos_returns_figure():
    """3 tiny PNGs → Figure with correct axes count and AxesImages drawn."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        for i in range(3):
            _make_tiny_png(os.path.join(d, f"img_{i:02d}.png"))

        fig = plot_gallery(d, mode="all", cols=3, renderer="photo", thumb_px=32)

    try:
        assert fig is not None
        assert hasattr(fig, "axes")
        # cols=3, rows=ceil(3/3)=1 → 3 axes
        assert len(fig.axes) == 3
        # Count AxesImages drawn (at least n=3)
        n_images = sum(len(ax.get_images()) for ax in fig.axes)
        assert n_images >= 3
    finally:
        plt.close("all")


def test_plot_gallery_photos_axes_count_with_padding():
    """4 images in a 3-col grid → rows=2, 6 axes (2 blank trailing)."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        for i in range(4):
            _make_tiny_png(os.path.join(d, f"img_{i:02d}.png"))

        fig = plot_gallery(d, mode="all", cols=3, renderer="photo", thumb_px=32)

    try:
        assert fig is not None
        assert len(fig.axes) == 6  # 2 rows × 3 cols
    finally:
        plt.close("all")


def test_plot_gallery_raster_no_exception():
    """Single-band uint16 GeoTIFF → Figure, no exception via raster renderer."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        tif = os.path.join(d, "test.tif")
        _make_tiny_geotiff(tif)

        fig = plot_gallery(d, mode="all", cols=1, renderer="auto", thumb_px=32)

    try:
        assert fig is not None
    finally:
        plt.close("all")


def test_plot_gallery_empty_returns_none(capsys):
    """Empty directory → None + a friendly skip message."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        result = plot_gallery(d, extensions=(".jpg",))

    assert result is None
    captured = capsys.readouterr()
    assert "skip" in captured.out.lower() or "no images" in captured.out.lower()


def test_plot_gallery_labels_none():
    """labels=None → axes have no title set."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        for i in range(2):
            _make_tiny_png(os.path.join(d, f"img_{i}.png"))

        fig = plot_gallery(
            d, mode="all", cols=2, renderer="photo", thumb_px=16, labels=None
        )

    try:
        assert fig is not None
        for ax in fig.axes:
            assert ax.get_title() == ""
    finally:
        plt.close("all")


def test_plot_gallery_custom_title():
    """Explicit title= appears as suptitle."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        _make_tiny_png(os.path.join(d, "img.png"))

        fig = plot_gallery(
            d,
            mode="all",
            cols=1,
            renderer="photo",
            thumb_px=16,
            title="My Gallery",
        )

    try:
        assert fig is not None
        assert fig._suptitle is not None
        assert fig._suptitle.get_text() == "My Gallery"
    finally:
        plt.close("all")


def test_plot_gallery_auto_title_n_of_m():
    """Auto title shows 'N of M images' when title=None."""
    from databricks.labs.gbx.vizx import plot_gallery

    with tempfile.TemporaryDirectory() as d:
        for i in range(5):
            _make_tiny_png(os.path.join(d, f"img_{i}.png"))

        # limit=3 sample → 3 of 5
        fig = plot_gallery(
            d,
            mode="sample",
            limit=3,
            cols=3,
            renderer="photo",
            thumb_px=16,
            title=None,
        )

    try:
        assert fig is not None
        assert fig._suptitle is not None
        text = fig._suptitle.get_text()
        assert "3" in text
        assert "5" in text
    finally:
        plt.close("all")
