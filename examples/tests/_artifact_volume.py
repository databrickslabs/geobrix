"""
Shared artifact-Volume housekeeping for the stage_*_to_volume.py scripts.

The artifact Volume dir (GBX_ARTIFACT_VOLUME) must hold exactly one geobrix wheel and exactly
one geobrix product JAR. ``prune_stale`` lists that dir and deletes OTHER-version files of
one artifact kind; it never touches anything that does not match ``prefix`` + ``suffix``
(so tests.jar, other files, and subdirectories are safe).

Failures raise: a cleanup that silently does nothing leaves two versions in the artifact dir.
"""
from __future__ import annotations

import os
import re
import sys
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


def bundle_root() -> str:
    """The bundle volroot (``/Volumes/<catalog>/<schema>/<volume>``) from ``GBX_BUNDLE_VOLUME_*``, as the launchers read it."""
    return "/Volumes/%s/%s/%s" % tuple(
        (os.environ.get(k) or d).strip()
        for k, d in (
            ("GBX_BUNDLE_VOLUME_CATALOG", "main"),
            ("GBX_BUNDLE_VOLUME_SCHEMA", "default"),
            ("GBX_BUNDLE_VOLUME_NAME", "geobrix_samples"),
        )
    )


def versioned_dir(root: str, version: str, leaf: str) -> str:
    """``<root>/geobrix/<version>/<leaf>``: where the per-env wheels (``env5``, ``env6``) and the ``jar`` go."""
    return f"{root.rstrip('/')}/geobrix/{version}/{leaf}"


def wheel_version(name: str) -> str:
    """The public version of a geobrix wheel file name (``geobrix-0.6.0+env6-py3-none-any.whl`` -> ``0.6.0``).
    ValueError when the name is not a geobrix wheel."""
    m = re.fullmatch(r"geobrix-([^-+]+)(?:\+[^-]+)?-py3-none-any\.whl", name)
    if not m:
        raise ValueError(f"not a geobrix wheel name: {name}")
    return m.group(1)


def upload_verified(w, local: Path, remote: str) -> int:
    """files.upload, then the remote size must equal the local one (the upload can return without the bytes).
    0 when the bytes landed, else 1 after one line on stderr."""
    print("Uploading to %s (overwrite if exists)..." % remote)
    with open(local.resolve(), "rb") as fh:
        w.files.upload(remote, fh, overwrite=True)
    try:
        remote_size = w.files.get_metadata(remote).content_length
    except Exception as e:
        print(f"Error: failed to verify upload. Could not read remote metadata for {remote}: {e}", file=sys.stderr)
        return 1
    if remote_size != local.stat().st_size:
        print(
            f"Error: upload verification failed. Local size: {local.stat().st_size}, remote size: {remote_size}.",
            file=sys.stderr,
        )
        return 1
    print("Done: %s" % remote)
    return 0
