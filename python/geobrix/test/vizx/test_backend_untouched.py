"""vizx must never change the session's matplotlib backend.

A notebook runs the inline backend; switching it (e.g. to Agg) makes every later
``pyplot.show()`` a silent no-op, so later figures never display.
"""

import hashlib

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
from PIL import Image

from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
from databricks.labs.gbx.vizx import _pointcloud_static as pcs
from databricks.labs.gbx.vizx import plot_gallery

# Non-Agg, headless backends stand in for a notebook's inline backend.
_BACKENDS = ["svg", "pdf"]


@pytest.fixture(params=_BACKENDS)
def session_backend(request):
    """A non-Agg backend selected the way a notebook does, restored afterwards."""
    prev = matplotlib.get_backend()
    plt.switch_backend(request.param)
    yield matplotlib.get_backend()
    plt.close("all")
    plt.switch_backend(prev)


def _pts(n=300):
    rng = np.random.default_rng(0)
    return pd.DataFrame({"x": rng.random(n), "y": rng.random(n), "z": rng.random(n)})


def test_flythrough_keeps_the_session_backend_and_leaves_no_figures(
    tmp_path, session_backend
):
    pft.plot_point_cloud_flythrough(
        _pts(),
        out_path=str(tmp_path / "f"),
        formats=("gif",),
        frames=2,
        color="z",
        figsize=(1, 1),
        dpi=50,
    )
    assert matplotlib.get_backend() == session_backend
    assert plt.get_fignums() == []


def test_flythrough_without_output_keeps_the_session_backend(session_backend):
    pft.plot_point_cloud_flythrough(
        _pts(), out_path=None, frames=2, color="z", figsize=(1, 1), dpi=50
    )
    assert matplotlib.get_backend() == session_backend


def test_flythrough_frames_are_unchanged_by_the_detached_canvas(tmp_path, monkeypatch):
    """Same frame count, frame size and pixels as the pyplot-figure render."""
    kw = dict(frames=3, color="z", figsize=(1.5, 1), dpi=60)

    def run(detached):
        real = pcs.render_point_cloud_3d
        monkeypatch.setattr(
            pcs,
            "render_point_cloud_3d",
            lambda *a, **k: real(*a, **{**k, "detached": detached}),
        )
        out = tmp_path / ("d" if detached else "p")
        pft.plot_point_cloud_flythrough(
            _pts(), out_path=str(out), formats=("gif",), **kw
        )
        plt.close("all")
        with Image.open(f"{out}.gif") as im:
            frames = []
            for i in range(im.n_frames):
                im.seek(i)
                frames.append(np.asarray(im.convert("RGB")))
        return frames

    plain, detached = run(False), run(True)
    assert len(detached) == len(plain) == 3
    assert [f.shape for f in detached] == [f.shape for f in plain] == [(60, 90, 3)] * 3

    def digest(fs):
        return [hashlib.sha1(f.tobytes()).hexdigest() for f in fs]

    assert digest(detached) == digest(plain)


def test_detached_render_is_not_registered_with_pyplot(session_backend):
    fig = pcs.render_point_cloud_3d(
        [0, 1], [0, 1], [0, 1], max_points=0, figsize=(1, 1), dpi=50, detached=True
    )
    assert plt.get_fignums() == []
    assert fig.canvas.get_width_height() == (50, 50)
    assert matplotlib.get_backend() == session_backend


def test_plot_gallery_keeps_the_session_backend(tmp_path, session_backend):
    for i in range(2):
        arr = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        Image.fromarray(arr, "RGB").save(tmp_path / f"img_{i}.png")
    plot_gallery(tmp_path, mode="all", cols=2, renderer="photo", thumb_px=32)
    assert matplotlib.get_backend() == session_backend
