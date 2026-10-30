"""
Shared artifact-Volume housekeeping for the stage_*_to_volume.py scripts.

The init-script dir (GBX_ARTIFACT_VOLUME) must hold exactly one geobrix wheel and exactly
one geobrix product JAR. ``prune_stale`` lists that dir and deletes OTHER-version files of
one artifact kind; it never touches anything that does not match ``prefix`` + ``suffix``
(so geobrix-gdal-init.sh, platform tarballs, tests.jar, and subdirectories are safe).

Failures raise: a cleanup that silently does nothing leaves two versions in the init dir.
"""
from __future__ import annotations

from pathlib import Path
from typing import List


def prune_stale(
    w, directory: str, prefix: str, suffix: str, keep_name: str, exclude: str = ""
) -> List[str]:
    """Delete files in ``directory`` named ``<prefix>*<suffix>`` other than ``keep_name``, and other than any whose
    name contains ``exclude`` (when given: the plain wheel's stage passes ``"+env"``, so the env wheels survive a
    plain stage into the same directory).

    ``w`` is a databricks.sdk WorkspaceClient. Uses ``files.list_directory_contents``
    (FilesExt has no ``list``). Returns the paths removed; raises on any list/delete error.
    """
    directory = directory.rstrip("/")
    removed: List[str] = []
    for entry in w.files.list_directory_contents(directory):
        if entry.is_directory:
            continue
        # DirectoryEntry.name is the last path component; path is the absolute path.
        filename = entry.name or Path(entry.path or "").name
        if exclude and exclude in filename:
            continue
        if filename.startswith(prefix) and filename.endswith(suffix) and filename != keep_name:
            target = entry.path or f"{directory}/{filename}"
            print(f"Removing stale artifact: {target}")
            w.files.delete(target)
            removed.append(target)
    return removed
