"""``file_mode``: how a store's manifest refers to its files.

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

The FILE modes need a workspace with the File type preview. ``VERIFIED_FILE_MODES`` names the
modes that have been verified on a cluster: while a mode is not in it, ``auto`` never picks it
and asking for it raises ``ZarrUnsupported``.

- ``external`` passed: on a runtime with FILE, ``auto`` picks it (Serverless, the Databricks
  Container Service image and a workspace without the preview stay on ``fuse``).
- ``managed`` failed and is **not available in this release**: Delta VACUUM, a deletion-vector delete
  and ``REORG ... PURGE`` never reclaim the bytes of a managed file, and a write that fails
  after its files were made leaves them behind, so a managed store only grows. It needs a
  sweep of the filespace in ``gbx_zarr_vacuum`` and a pack write in a scalar or Arrow UDF
  (``FileRef.from_local_file`` fails in ``mapInArrow``) before it can open.
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
    "require_file_compute",
    "require_geobrix_files",
    "resolve",
    "table_properties",
]

FILE_MODE_CHOICES = ("auto", "fuse", "external", "managed")

# The FILE modes whose behaviour on a FILE-enabled workspace has been verified on a
# cluster. ``external`` passed; ``managed`` did not (see the module docstring).
VERIFIED_FILE_MODES: frozenset[str] = frozenset({"external"})

# The modes whose files GeoBrix itself writes, reads and vacuums. A managed store's files
# belong to the table: GeoBrix reads them and runs Delta's VACUUM, and does not yet write
# new ones (a task creates a managed file with ``FileRef.from_local_file``; how that value
# reaches the commit is not settled yet).
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
# Why a mode that failed the gate is refused by name (``resolve``): the reason, and what to use.
_REFUSED = {
    "managed": (
        'file_mode="managed" is not available in this release: Delta VACUUM does not reclaim the '
        "bytes of a managed file (nor does a deletion-vector delete or REORG PURGE), so a "
        "managed store would only grow",
        'use file_mode="auto" (the default), "external" or "fuse"',
    )
}
_SCHEMES = ("dbfs:/", "file:/")  # the schemes ``to_local_path`` strips


def file_supported(spark) -> bool:
    """Whether this session can use FILE values: ``ds.file_gbx.file_supported``, imported
    on first use because importing ``ds`` registers the data sources."""
    from databricks.labs.gbx.ds.file_gbx import file_supported as probe

    return probe(spark)


def _usable(mode: str, spark) -> bool:
    """Whether ``auto`` may pick ``mode``: verified, one GeoBrix writes, and supported here.
    (A mode GeoBrix cannot write must never be chosen for the caller, verified or not.)
    """
    return (
        mode in VERIFIED_FILE_MODES
        and mode in GEOBRIX_FILE_MODES
        and file_supported(spark)
    )


def resolve(
    spark, file_mode: str, *, has_filespace: bool, registered: bool = False
) -> str:
    """The file mode to use, never ``"auto"``: ``"fuse"``, ``"external"`` or ``"managed"``.

    ``auto`` is the best verified mode the runtime supports: ``managed`` when a filespace
    root is given (``has_filespace``), else ``external``, else ``fuse``; it never raises. As
    shipped that is ``external`` on a runtime with FILE and ``fuse`` elsewhere.
    An explicit ``external`` or ``managed`` that cannot be used raises ``ZarrUnsupported``
    (``managed`` always does in this release, see the module docstring).
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
            fix="use the default file mode",
        )
    if file_mode == "fuse":
        return "fuse"
    if file_mode == "auto":
        if has_filespace and not registered and _usable("managed", spark):
            return "managed"
        return "external" if _usable("external", spark) else "fuse"
    if file_mode not in VERIFIED_FILE_MODES:
        reason, fix = _REFUSED.get(file_mode, (_UNVERIFIED, _FUSE_FIX))
        raise ZarrUnsupported(reason, fix=fix)
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


def table_properties(mode: str, root: str) -> dict:
    """The table properties ``mode`` needs beyond the store's own: a managed table's
    filespace is its data root (a registered store is never managed)."""
    if mode != "managed":
        return {}
    from databricks.labs.gbx.pyrx.file_props import FILESPACE_KEY

    return {FILESPACE_KEY: root}


def require_geobrix_files(
    label: str, mode: str, verb: str, *, new: bool = False
) -> None:
    """Raise ``ZarrUnsupported`` unless GeoBrix itself ``verb``s the files of ``label``
    in ``mode``. ``new``: ``label`` is a store a call is about to create, so the message
    names the argument and not a store."""
    if mode in GEOBRIX_FILE_MODES:
        return
    if new:
        raise ZarrUnsupported(
            f"file_mode={mode!r} is not available for {label}: GeoBrix does not {verb} "
            "the files of that mode yet",
            fix='pass file_mode="auto" or "fuse"',
        )
    raise ZarrUnsupported(
        f"{label} keeps its files in file_mode {mode!r}, which GeoBrix does not "
        f"{verb} yet",
        fix=f"use a store with file_mode {' or '.join(map(repr, GEOBRIX_FILE_MODES))}",
    )


def require_file_compute(spark, label: str, mode: str) -> None:
    """Raise ``ZarrUnsupported`` when the store ``label``, which keeps its files as FILE values
    (``external`` or ``managed``), is used from compute that does not support them.

    A FILE store is made on a runtime with the File type preview, and its manifest cannot be
    committed to there: the commit mints each value with ``try_to_file``, a function Serverless
    and the Databricks Container Service image lack. What reading such a store does there has not been
    verified, so it is refused the same way, before anything is written. ``fuse`` stores pass
    without a probe; for the others ``file_supported`` caches its answer per session."""
    if mode == "fuse" or file_supported(spark):
        return
    raise ZarrUnsupported(
        f"{label} keeps its files as FILE values (file_mode={mode!r}), and this compute "
        "does not support FILE values, so it can neither read, write nor maintain the store",
        fix="use a Databricks Runtime 19 dedicated cluster (single-user, with "
        "fileReferenceCreationMode=MANAGED in its cluster config) in a workspace with the "
        "File type preview; to use the data elsewhere, copy it with "
        "gbx_zarr_export(method='copy') on such a cluster and register the copy with "
        "file_mode='fuse'; create stores that other compute also uses with file_mode='fuse'",
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
