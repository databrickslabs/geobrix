"""Run from a snapshot: copy the files a launch uses into a temp dir at launch.

A launcher reads its inputs over minutes (one notebook after another, a run that lasts hours). An edit
to one of them while the run is live would change what a later step uploads. ``snapshot_files`` copies
the inputs once, at launch, so the run uses what was there then. The temp dir is removed when the
launcher exits.

Pure Python, no databricks-sdk.
"""

from __future__ import annotations

import atexit
import pathlib
import shutil
import tempfile


def snapshot_files(paths, *, prefix: str = "gbx-snap-") -> dict:
    """Copy each existing file in ``paths`` into one new temp dir; return ``{original: copy}``.

    Each copy keeps the original's file name (a workspace upload names the notebook by it) in a
    subdirectory of its own, so two files of one name do not collide. A path that does not exist is left
    out of the result, for the caller to report as it always has. The dir is deleted at exit.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix=prefix))
    atexit.register(shutil.rmtree, root, ignore_errors=True)
    out = {}
    for i, src in enumerate(pathlib.Path(p) for p in paths):
        if not src.is_file():
            continue
        dst = root / str(i) / src.name
        dst.parent.mkdir()
        shutil.copy2(src, dst)
        out[src] = dst
    return out
