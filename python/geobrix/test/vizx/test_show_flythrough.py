"""Tests for show_flythrough — display helper for fly-through animations.

Headless (no cluster, no IPython notebook required).  The flythrough renderer
is monkeypatched to produce real GIF / MP4 artifacts without rendering actual
point-cloud frames.

Run with:
  bash scripts/commands/gbx-test-python.sh \\
    --path python/geobrix/test/vizx/test_show_flythrough.py
"""

import pathlib

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gif(path, n_frames=4):
    """Write a minimal real multi-frame GIF (PIL always available in vizx env)."""
    from PIL import Image

    rng = np.random.default_rng(0)
    imgs = [
        Image.fromarray(np.full((20, 20, 3), rng.integers(0, 256, 3), dtype=np.uint8))
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


def _mock_flythrough(gif_frames=4, mp4_size_bytes=0):
    """Return a callable that mimics plot_point_cloud_flythrough.

    Writes a real GIF (so display code can read_bytes it) and/or an
    MP4 stub of the requested byte length, then returns ``{}``.
    """

    def _fn(source, *, out_path, formats, **kwargs):
        if "gif" in formats:
            _make_gif(pathlib.Path(str(out_path) + ".gif"), gif_frames)
        if "mp4" in formats:
            pathlib.Path(str(out_path) + ".mp4").write_bytes(b"\x00" * mp4_size_bytes)
        return {}

    return _fn


# ---------------------------------------------------------------------------
# kind guard
# ---------------------------------------------------------------------------


def test_kind_guard_raises_value_error():
    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    with pytest.raises(ValueError):
        show_flythrough(None, kind="raster")


def test_kind_guard_message_names_unsupported_kind():
    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    with pytest.raises(ValueError, match="'raster' is not supported yet"):
        show_flythrough(None, kind="raster")


def test_kind_guard_message_mentions_point_cloud():
    """Error message reminds the user of the supported kind."""
    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    with pytest.raises(ValueError, match="point_cloud"):
        show_flythrough(None, kind="lidar_scan")


# ---------------------------------------------------------------------------
# overwrite logic
# ---------------------------------------------------------------------------


def test_overwrite_false_skips_render(tmp_path, monkeypatch):
    """Pre-existing GIF → renderer NOT called when overwrite=False (default)."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    call_count = [0]

    def _counting_mock(source, *, out_path, formats, **kwargs):
        call_count[0] += 1
        _make_gif(pathlib.Path(str(out_path) + ".gif"))
        return {}

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _counting_mock)

    # Pre-write the GIF so it already exists in out_dir
    _make_gif(tmp_path / "fly.gif")

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    show_flythrough(None, out_dir=str(tmp_path), name="fly", overwrite=False)

    assert call_count[0] == 0, (
        f"renderer was called {call_count[0]} time(s) even though GIF existed "
        "and overwrite=False"
    )


def test_overwrite_true_rerenders_existing(tmp_path, monkeypatch):
    """overwrite=True forces a re-render even when the GIF already exists."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    call_count = [0]

    def _counting_mock(source, *, out_path, formats, **kwargs):
        call_count[0] += 1
        _make_gif(pathlib.Path(str(out_path) + ".gif"))
        return {}

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _counting_mock)

    # Pre-write the GIF
    _make_gif(tmp_path / "fly.gif")

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    show_flythrough(None, out_dir=str(tmp_path), name="fly", overwrite=True)

    assert call_count[0] == 1, (
        f"renderer was called {call_count[0]} time(s); expected exactly 1 "
        "with overwrite=True"
    )


# ---------------------------------------------------------------------------
# cap_mb / MP4 preview logic
# ---------------------------------------------------------------------------


def test_preview_mp4_over_cap_emits_warning_no_embed(tmp_path, monkeypatch):
    """preview_mp4=True with MP4 > cap_mb → UserWarning issued, no video embed."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    # cap_mb = 1 KB; MP4 stub = 5 KB → over cap
    # GIF (4-frame 20×20) ≈ a few hundred bytes → under cap (no GIF warning)
    monkeypatch.setattr(
        pft_mod,
        "plot_point_cloud_flythrough",
        _mock_flythrough(gif_frames=4, mp4_size_bytes=5 * 1024),
    )

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    with pytest.warns(UserWarning, match="inline limit"):
        result = show_flythrough(
            None,
            out_dir=str(tmp_path),
            name="big",
            preview_mp4=True,
            cap_mb=0.001,  # 1 KB
        )

    assert (
        result["_preview_html"] is None
    ), "_preview_html must be None when MP4 exceeds cap"


def test_preview_mp4_under_cap_embeds_video(tmp_path, monkeypatch):
    """preview_mp4=True with MP4 < cap_mb → base64 <video> returned in _preview_html."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    # cap_mb = 100 MB (huge); MP4 stub = 100 bytes → well under cap
    monkeypatch.setattr(
        pft_mod,
        "plot_point_cloud_flythrough",
        _mock_flythrough(gif_frames=4, mp4_size_bytes=100),
    )

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(
        None,
        out_dir=str(tmp_path),
        name="small",
        preview_mp4=True,
        cap_mb=100,
    )

    assert (
        result["_preview_html"] is not None
    ), "_preview_html should be populated when MP4 is under cap"
    assert "data:video/mp4;base64," in result["_preview_html"]
    assert "<details>" in result["_preview_html"]
    assert "Preview video here" in result["_preview_html"]


# ---------------------------------------------------------------------------
# out_dir creation, path, md_snippet
# ---------------------------------------------------------------------------


def test_out_dir_created_automatically(tmp_path, monkeypatch):
    """out_dir (including nested parents) is created when it does not exist."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    nested = str(tmp_path / "a" / "b" / "c")

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    show_flythrough(None, out_dir=nested, name="test")

    assert pathlib.Path(nested).is_dir(), f"out_dir {nested!r} was not created"


def test_gif_path_returned_correctly(tmp_path, monkeypatch):
    """result['gif'] points to the written GIF file and the file exists."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(None, out_dir=str(tmp_path), name="mytest")

    expected = pathlib.Path(str(tmp_path)) / "mytest.gif"
    assert result["gif"] == expected, f"expected {expected}, got {result['gif']}"
    assert result["gif"].exists(), "result['gif'] path does not exist on disk"


def test_md_snippet_format(tmp_path, monkeypatch):
    """md_snippet is in the form '![flythrough](<out_dir>/<name>.gif)'."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    out = str(tmp_path / "subdir")
    result = show_flythrough(None, out_dir=out, name="anim")

    expected_snippet = f"![flythrough]({out}/anim.gif)"
    assert result["md_snippet"] == expected_snippet, (
        f"md_snippet mismatch:\n  got:      {result['md_snippet']!r}\n"
        f"  expected: {expected_snippet!r}"
    )


# ---------------------------------------------------------------------------
# temp-then-copy: GIF is non-trivial (not a stub)
# ---------------------------------------------------------------------------


def test_gif_written_via_temp_is_non_trivial(tmp_path, monkeypatch):
    """GIF copied from /tmp to out_dir is a real file, not an 8-byte stub.

    The mock writes 8 solid-colour 30×30 frames — a real PIL GIF that is
    well above any stub size.
    """
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    def _real_gif_mock(source, *, out_path, formats, **kwargs):
        if "gif" in formats:
            from PIL import Image

            rng = np.random.default_rng(42)
            imgs = [
                Image.fromarray(
                    np.full((30, 30, 3), rng.integers(0, 256, 3), dtype=np.uint8)
                )
                for _ in range(8)
            ]
            imgs[0].save(
                str(pathlib.Path(str(out_path) + ".gif")),
                save_all=True,
                append_images=imgs[1:],
                duration=100,
                loop=0,
                optimize=False,
            )
        return {}

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _real_gif_mock)

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(None, out_dir=str(tmp_path), name="smoke")

    gif = result["gif"]
    assert gif is not None and gif.exists(), "GIF file was not written"
    size = gif.stat().st_size
    assert (
        size > 100
    ), f"GIF at {gif} is only {size} bytes — looks like a stub (FUSE-seek issue)"


# ---------------------------------------------------------------------------
# persist=True / persist=False
# ---------------------------------------------------------------------------


def test_persist_true_writes_gif_and_snippet(tmp_path, monkeypatch):
    """persist=True (default): GIF written to out_dir, md_snippet is non-None."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(None, out_dir=str(tmp_path), name="kept", persist=True)

    assert (tmp_path / "kept.gif").exists(), "GIF must be written with persist=True"
    assert result["gif"] is not None
    assert result["md_snippet"] is not None
    assert "kept.gif" in result["md_snippet"]


def test_persist_false_leaves_no_file_in_out_dir(tmp_path, monkeypatch):
    """persist=False: no file written to out_dir, md_snippet=None, gif=None."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(
        None, out_dir=str(tmp_path), name="ephemeral", persist=False
    )

    assert not (
        tmp_path / "ephemeral.gif"
    ).exists(), "GIF must NOT be written to out_dir with persist=False"
    assert result["gif"] is None
    assert result["md_snippet"] is None


def test_persist_false_mp4_none_without_volume_dir(tmp_path, monkeypatch):
    """persist=False + mp4=True but no mp4_volume_dir → result['mp4'] is None."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(
        pft_mod,
        "plot_point_cloud_flythrough",
        _mock_flythrough(mp4_size_bytes=200),
    )

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(
        None,
        out_dir=str(tmp_path),
        name="live",
        persist=False,
        mp4=True,
        mp4_volume_dir=None,
    )

    assert (
        result["mp4"] is None
    ), "mp4 key should be None when persist=False and mp4_volume_dir is None"


# ---------------------------------------------------------------------------
# name derivation
# ---------------------------------------------------------------------------


def test_name_derived_from_path_source(tmp_path, monkeypatch):
    """When source is a path string, name is auto-derived from its stem."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    # source is a path-like string; no name= supplied
    result = show_flythrough("/data/lidar/goldengate.laz", out_dir=str(tmp_path))

    assert result["gif"] is not None
    assert (
        result["gif"].name == "goldengate.gif"
    ), f"expected goldengate.gif, got {result['gif'].name}"


def test_name_required_for_non_path_source_raises():
    """Non-path source (DataFrame / None) without name= raises a clear ValueError."""
    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    with pytest.raises(ValueError, match="cannot derive one from the source"):
        show_flythrough(None)  # no name= and source is not a path


def test_explicit_name_overrides_path_source_basename(tmp_path, monkeypatch):
    """An explicit name= takes precedence over the source path's stem."""
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod

    monkeypatch.setattr(pft_mod, "plot_point_cloud_flythrough", _mock_flythrough())

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(
        "/data/lidar/goldengate.laz", out_dir=str(tmp_path), name="custom_stem"
    )

    assert (
        result["gif"].name == "custom_stem.gif"
    ), f"expected custom_stem.gif, got {result['gif'].name}"


# ---------------------------------------------------------------------------
# Public API export
# ---------------------------------------------------------------------------


def test_show_flythrough_exported_from_vizx():
    """show_flythrough is exported from databricks.labs.gbx.vizx and is callable."""
    from databricks.labs.gbx import vizx as vz

    assert hasattr(vz, "show_flythrough"), "show_flythrough missing from vizx"
    assert callable(vz.show_flythrough)


# ---------------------------------------------------------------------------
# GIF display: IPython.display.Image (image/gif) not displayHTML
# ---------------------------------------------------------------------------


def test_display_false_skips_display_but_writes_file_and_returns_snippet(
    tmp_path, monkeypatch
):
    """display=False: GIF is written, md_snippet is returned, nothing is displayed.

    When display=False, the %md cell is the sole render point; the code cell
    must not also display the GIF (which would cause double-display).
    """
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod
    import databricks.labs.gbx.vizx._show_flythrough as sft_mod

    monkeypatch.setattr(
        pft_mod, "plot_point_cloud_flythrough", _mock_flythrough(gif_frames=4)
    )

    display_gif_calls: list = []
    display_html_calls: list = []
    monkeypatch.setattr(
        sft_mod, "_try_display_gif", lambda path: display_gif_calls.append(str(path))
    )
    monkeypatch.setattr(
        sft_mod, "_try_display_html", lambda html: display_html_calls.append(html)
    )

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    result = show_flythrough(
        None, out_dir=str(tmp_path), name="test_no_display", display=False
    )

    # File written
    assert (
        tmp_path / "test_no_display.gif"
    ).exists(), "GIF must be written even when display=False"
    # md_snippet returned
    assert (
        result["md_snippet"] is not None
    ), "md_snippet must be returned even when display=False"
    assert "test_no_display.gif" in result["md_snippet"]
    # Nothing displayed
    assert (
        not display_gif_calls
    ), "_try_display_gif must NOT be called when display=False"
    assert (
        not display_html_calls
    ), "_try_display_html must NOT be called when display=False"


def test_gif_displayed_via_ipy_image_not_html(tmp_path, monkeypatch):
    """GIF display uses _try_display_gif (image/gif), not displayHTML base64 <img>.

    GitHub's notebook renderer sanitizes text/html data-URIs; image/gif renders
    natively.  Verify that _try_display_gif is called, and _try_display_html is
    NOT called with GIF HTML content, when the GIF is under the cap.
    """
    import databricks.labs.gbx.vizx._pointcloud_flythrough as pft_mod
    import databricks.labs.gbx.vizx._show_flythrough as sft_mod

    monkeypatch.setattr(
        pft_mod, "plot_point_cloud_flythrough", _mock_flythrough(gif_frames=4)
    )

    gif_display_calls: list = []
    html_display_calls: list = []

    monkeypatch.setattr(
        sft_mod, "_try_display_gif", lambda path: gif_display_calls.append(str(path))
    )
    monkeypatch.setattr(
        sft_mod, "_try_display_html", lambda html: html_display_calls.append(html)
    )

    from databricks.labs.gbx.vizx._show_flythrough import show_flythrough

    show_flythrough(None, out_dir=str(tmp_path), name="testfly", cap_mb=100)

    assert len(gif_display_calls) == 1, "_try_display_gif must be called once for GIF"
    # No GIF base64 should appear in any HTML display call
    for html in html_display_calls:
        assert (
            "data:image/gif;base64," not in html
        ), "GIF must not be embedded as data-URI HTML (GitHub sanitizes it)"
