"""``file_mode``: how a manifest refers to the files of a store.

Only ``fuse`` is available so far: the manifest's ``file`` column holds path strings
relative to the store root. ``auto`` resolves to it. The FILE modes (``external`` and
``managed``) need a workspace with the File type preview, so asking for one is an error
that names the working choice.
"""

from __future__ import annotations

from databricks.labs.gbx.zarr.errors import ZarrUnsupported

__all__ = ["FILE_MODE_CHOICES", "resolve"]

FILE_MODE_CHOICES = ("auto", "fuse", "external", "managed")


def resolve(spark, file_mode: str, *, has_filespace: bool) -> str:
    """The file mode to use, never ``"auto"``: ``"fuse"``, ``"external"`` or ``"managed"``.

    ``spark`` and ``has_filespace`` are for the FILE modes, which choose by what the
    runtime supports and whether a filespace was given; they are not read yet.
    """
    if file_mode not in FILE_MODE_CHOICES:
        raise ValueError(
            f"file_mode must be one of {FILE_MODE_CHOICES}, got {file_mode!r}. "
            'Fix: pass file_mode="auto" or "fuse".'
        )
    if file_mode in ("external", "managed"):
        raise ZarrUnsupported(
            f"file_mode={file_mode!r} stores files as FILE values, which need a "
            "workspace with the File type preview enabled and are not available yet.",
            fix='use file_mode="auto" or "fuse" (the default)',
        )
    return "fuse"
