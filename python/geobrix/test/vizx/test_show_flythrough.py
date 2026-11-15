"""Tests for show_flythrough: display helper for fly-through animations.

Headless (no cluster, no notebook). The flythrough renderer is monkeypatched to write
real GIF / MP4 artifacts without rendering point-cloud frames.
"""

import pathlib

import numpy as np
import pytest

import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod
import databricks.labs.gbx.vizx._show_flythrough as sft_mod
from databricks.labs.gbx.vizx._show_flythrough import show_flythrough


def _make_gif(path, n_frames=4):
    """A real multi-frame GIF (distinct solid frames, which PIL does not collapse)."""
    from PIL import Image

    rng = np.random.default_rng(0)
    imgs = [
        Image.fromarray(np.full((30, 30, 3), rng.integers(0, 256, 3), dtype=np.uint8))
        for _ in range(n_frames)
    ]
    imgs[0].save(
        str(path),
        save_all=True,
        append_images=imgs[1:],
        duration=200,
        loop=0,
        optimize=False,
    )


@pytest.fixture
def flythrough(monkeypatch):
    """Install the stub renderer; ``calls`` counts renders, ``mp4_size`` sets the MP4 stub."""

    class _Stub:
        calls = 0
        mp4_size = 0

    stub = _Stub()

    def _fn(source, *, out_path, formats, **kwargs):
        stub.calls += 1
        if "gif" in formats:
            _make_gif(pathlib.Path(str(out_path) + ".gif"), 8)
        if "mp4" in formats:
            pathlib.Path(str(out_path) + ".mp4").write_bytes(b"\x00" * stub.mp4_size)
        return {}

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _fn)
    return stub


def test_only_point_cloud_kind_is_supported():
    with pytest.raises(ValueError, match="point_cloud"):
        show_flythrough(None, kind="raster")


@pytest.mark.parametrize("overwrite, renders", [(False, 0), (True, 1)])
def test_an_existing_gif_is_re_rendered_only_with_overwrite(
    tmp_path, flythrough, overwrite, renders
):
    _make_gif(tmp_path / "fly.gif")
    show_flythrough(None, out_dir=str(tmp_path), name="fly", overwrite=overwrite)
    assert flythrough.calls == renders


def test_the_gif_is_a_real_file_in_a_created_out_dir_with_a_markdown_snippet(
    tmp_path, flythrough
):
    out = str(tmp_path / "a" / "b" / "c")  # nested parents are created
    result = show_flythrough(None, out_dir=out, name="anim")
    gif = pathlib.Path(out) / "anim.gif"
    assert result["gif"] == gif
    assert gif.stat().st_size > 100  # not an 8-byte stub (FUSE seek issue)
    assert result["md_snippet"] == f"![flythrough]({out}/anim.gif)"


def test_persist_false_writes_nothing_and_has_no_snippet_or_mp4(tmp_path, flythrough):
    flythrough.mp4_size = 200
    kept = show_flythrough(None, out_dir=str(tmp_path), name="kept")
    assert (tmp_path / "kept.gif").exists() and "kept.gif" in kept["md_snippet"]

    live = show_flythrough(
        None,
        out_dir=str(tmp_path),
        name="live",
        persist=False,
        mp4=True,
        mp4_volume_dir=None,
    )
    assert not (tmp_path / "live.gif").exists()
    assert live["gif"] is None and live["md_snippet"] is None and live["mp4"] is None


def test_the_mp4_preview_embeds_a_video_only_under_the_inline_cap(tmp_path, flythrough):
    flythrough.mp4_size = 5 * 1024
    with pytest.warns(UserWarning, match="inline limit"):
        over = show_flythrough(
            None, out_dir=str(tmp_path), name="big", preview_mp4=True, cap_mb=0.001
        )
    assert over["_preview_html"] is None

    flythrough.mp4_size = 100
    under = show_flythrough(
        None, out_dir=str(tmp_path), name="small", preview_mp4=True, cap_mb=100
    )
    html = under["_preview_html"]
    assert "data:video/mp4;base64," in html and "<details>" in html


def test_the_name_comes_from_the_source_path_unless_given(tmp_path, flythrough):
    derived = show_flythrough("/data/lidar/goldengate.laz", out_dir=str(tmp_path))
    assert derived["gif"].name == "goldengate.gif"
    named = show_flythrough(
        "/data/lidar/goldengate.laz", out_dir=str(tmp_path), name="custom_stem"
    )
    assert named["gif"].name == "custom_stem.gif"
    with pytest.raises(ValueError, match="cannot derive one from the source"):
        show_flythrough(None)  # not a path and no name=


def test_the_gif_is_displayed_as_an_image_not_as_a_data_uri_in_html(
    tmp_path, flythrough, monkeypatch
):
    # GitHub's notebook renderer sanitizes text/html data-URIs; image/gif renders natively.
    gifs, htmls = [], []
    monkeypatch.setattr(
        sft_mod, "_try_display_gif", lambda path: gifs.append(str(path))
    )
    monkeypatch.setattr(sft_mod, "_try_display_html", lambda html: htmls.append(html))

    show_flythrough(None, out_dir=str(tmp_path), name="shown", cap_mb=100)
    assert len(gifs) == 1
    assert all("data:image/gif;base64," not in h for h in htmls)

    # display=False: the %md cell is the sole render point; still written, snippet returned.
    gifs.clear()
    htmls.clear()
    result = show_flythrough(None, out_dir=str(tmp_path), name="quiet", display=False)
    assert (tmp_path / "quiet.gif").exists() and "quiet.gif" in result["md_snippet"]
    assert not gifs and not htmls
