"""``file_mode``: how a store's manifest refers to its files (spec 4.3).

============  ====================================  ================================
``file_mode``  the manifest's ``file`` column         who deletes superseded files
============  ====================================  ================================
``fuse``       STRING, a path relative to the root    GeoBrix vacuum
``external``   FILE EXTERNAL (``try_to_file``)        GeoBrix vacuum
``managed``    FILE MANAGED, the data root is the     the table (Delta VACUUM)
               table's filespace
============  ====================================  ================================

Whatever the mode, the rows GeoBrix reads and writes carry ``file`` as a path string
relative to the root it lies under: the FILE value is only how the column stores it, so
nothing outside the manifest module has to know the mode. ``path_of`` turns either
form into the bare ``/Volumes/...`` path that opens through the FUSE mount.

The FILE modes need a workspace with the File type preview, which this release has not
verified on a cluster (gate G4, spec 16). ``VERIFIED_FILE_MODES`` is that gate: while a
mode is not in it, ``auto`` never picks it and asking for it raises ``ZarrUnsupported``.
A passing G4 adds the mode to the set; nothing else changes.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass

from databricks.labs.gbx.zarr.errors import ZarrUnsupported

__all__ = [
    "FILE_MODE_CHOICES",
    "GEOBRIX_FILE_MODES",
    "VERIFIED_FILE_MODES",
    "FileLayout",
    "file_column_type",
    "file_supported",
    "layout_for",
    "path_of",
    "relative_to",
    "require_geobrix_files",
    "resolve",
    "table_properties",
]

FILE_MODE_CHOICES = ("auto", "fuse", "external", "managed")

# Gate G4: the FILE modes whose behaviour on a FILE-enabled workspace has been verified.
# Empty until the on-cluster pass has run (spec 16, G4).
VERIFIED_FILE_MODES: frozenset[str] = frozenset()

# The modes whose files GeoBrix itself writes, reads and vacuums. A managed store's files
# belong to the table: GeoBrix reads them and runs Delta's VACUUM, and does not yet write
# new ones (a task creates a managed file with ``FileRef.from_local_file``; how that value
# reaches the commit is what G4.2 settles).
GEOBRIX_FILE_MODES = ("fuse", "external")

_COLUMN_TYPES = {
    "fuse": "STRING",
    "external": "FILE EXTERNAL",
    "managed": "FILE MANAGED",
}
_UNVERIFIED = (
    "FILE modes need a workspace with the File type preview enabled and were not "
    "verified here"
)
_FUSE_FIX = 'use file_mode="fuse" (the default)'
_SCHEMES = ("dbfs:/", "file:/")  # the schemes ``to_local_path`` strips


def file_supported(spark) -> bool:
    """Whether this session can use FILE values: ``ds.file_gbx.file_supported``, imported
    on first use because importing ``ds`` registers the data sources."""
    from databricks.labs.gbx.ds.file_gbx import file_supported as probe

    return probe(spark)


def _usable(mode: str, spark) -> bool:
    return mode in VERIFIED_FILE_MODES and file_supported(spark)


def resolve(
    spark, file_mode: str, *, has_filespace: bool, registered: bool = False
) -> str:
    """The file mode to use, never ``"auto"``: ``"fuse"``, ``"external"`` or ``"managed"``.

    ``auto`` is the best verified mode the runtime supports: ``managed`` when a filespace
    root is given (``has_filespace``), else ``external``, else ``fuse``; it never raises.
    An explicit ``external`` or ``managed`` that cannot be used raises ``ZarrUnsupported``.
    A registered store (``registered=True``) is never ``managed``: managed ingest copies
    each file, so the base store would be duplicated.
    """
    if file_mode not in FILE_MODE_CHOICES:
        raise ValueError(
            f"file_mode must be one of {FILE_MODE_CHOICES}, got {file_mode!r}. "
            'Fix: pass file_mode="auto" or "fuse".'
        )
    if registered and file_mode == "managed":
        raise ZarrUnsupported(
            'file_mode="managed" cannot be used for a registered store: managed ingest '
            "copies every file it is given, so the registered store would be duplicated",
            fix='use file_mode="external" (references without a copy) or "fuse"',
        )
    if file_mode == "fuse":
        return "fuse"
    if file_mode == "auto":
        if has_filespace and not registered and _usable("managed", spark):
            return "managed"
        return "external" if _usable("external", spark) else "fuse"
    if file_mode not in VERIFIED_FILE_MODES:
        raise ZarrUnsupported(_UNVERIFIED, fix=_FUSE_FIX)
    if not file_supported(spark):
        raise ZarrUnsupported(
            f"file_mode={file_mode!r} needs a workspace with the File type preview "
            "enabled, and this runtime does not support FILE values",
            fix=_FUSE_FIX,
        )
    return file_mode


def file_column_type(mode: str) -> str:
    """The SQL type of the manifest's ``file`` column in ``mode``."""
    try:
        return _COLUMN_TYPES[mode]
    except KeyError:
        raise ValueError(
            f"file_mode must be one of {tuple(_COLUMN_TYPES)} here, got {mode!r}. "
            "Fix: resolve 'auto' with resolve() first."
        ) from None


def table_properties(mode: str, root: str, overlay_root: str | None = None) -> dict:
    """The table properties ``mode`` needs beyond the store's own: a managed table's
    filespace is the overlay root, else the data root."""
    if mode != "managed":
        return {}
    from databricks.labs.gbx.pyrx.file_props import FILESPACE_KEY

    return {FILESPACE_KEY: overlay_root or root}


def require_geobrix_files(label: str, mode: str, verb: str) -> None:
    """Raise ``ZarrUnsupported`` unless GeoBrix itself ``verb``s the files of ``label``
    (a store, or a call about to create one) in ``mode``."""
    if mode not in GEOBRIX_FILE_MODES:
        raise ZarrUnsupported(
            f"{label} keeps its files in file_mode {mode!r}, which GeoBrix does not "
            f"{verb} yet",
            fix=f"use a store with file_mode {' or '.join(map(repr, GEOBRIX_FILE_MODES))}",
        )


def path_of(file_value, root: str | None = None) -> str:
    """The bare path of a manifest ``file`` value, the form that opens through FUSE.

    A FILE value (a mapping or object with a ``uri``) gives its ``uri`` with the ``dbfs:``
    scheme stripped: ``dbfs:/Volumes/...`` is ``/Volumes/...``. A STRING is a path
    relative to ``root`` and is joined with it; an absolute one, or any when no ``root``
    is given, is returned as it is.
    """
    if isinstance(file_value, Mapping):
        file_value = file_value["uri"]
    elif not isinstance(file_value, str):
        file_value = file_value.uri
    path = file_value
    if path.startswith(_SCHEMES):
        # importing ds loads PySpark and rasterio: only a FILE uri needs it, never a
        # relative or bare path, which is all a task ever reads
        from databricks.labs.gbx.ds._listing import to_local_path

        path = to_local_path(path)
    if root and not path.startswith("/"):
        return posixpath.join(root, path)
    return path


def relative_to(path: str, root: str | None) -> str:
    """``path`` relative to ``root`` when it lies under it, else unchanged: the inverse
    of joining a root onto a manifest path."""
    if root:
        prefix = root.rstrip("/") + "/"
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


@dataclass(frozen=True)
class FileLayout:
    """How one manifest keeps its files: the mode, and the two roots its relative paths
    lie under. ``gbx_root`` holds what GeoBrix wrote (a managed store's data root, a
    registered store's overlay), ``base_root`` a registered store's own files."""

    mode: str = "fuse"
    gbx_root: str | None = None
    base_root: str | None = None

    @classmethod
    def of(cls, props) -> "FileLayout":
        """The layout of the store with ``props`` (a ``StoreProps``)."""
        return layout_for(props.kind, props.root, props.overlay_root, props.file_mode)

    def root_of(self, origin: str | None) -> str | None:
        """The root a row of ``origin`` is relative to."""
        if origin == "external" and self.base_root:
            return self.base_root
        return self.gbx_root or self.base_root

def layout_for(
    kind: str | None, root: str | None, overlay_root: str | None, mode: str
) -> FileLayout:
    """The layout of a store of ``kind`` (``"managed"`` or ``"registered"``): a registered
    store's relative paths lie under its base (``root``) or, for what GeoBrix wrote, its
    overlay; a managed store's under its data root."""
    if kind == "registered":
        return FileLayout(mode, gbx_root=overlay_root, base_root=root)
    return FileLayout(mode, gbx_root=root)
