"""Isolated, self-GC'ing scratch directories for the two-phase light writers.

The light vector and PMTiles writers stage per-partition fragments in a
shared-filesystem (UC Volume / DBFS) scratch directory, then the driver merges
them into the final output. To stay correct under **concurrent jobs and
multiple users** writing to the same parent directory, scratch must be:

  * **uniquely namespaced per write** -- every write gets its own
    ``<parent>/.gbx_scratch/<uuid>/`` subdir, so one write's fragments and one
    write's commit-time ``rmtree`` can never touch another write's in-flight
    fragments. A shared, fixed scratch name (e.g. ``_scratch``) lets two
    concurrent writes corrupt each other.

  * **hidden from readers** -- the container is the dot-prefixed
    ``.gbx_scratch``. Spark's file enumeration and our own recursive vector
    reader both skip ``.``/``_``-prefixed directories, so an in-flight (or
    orphaned) scratch dir is never mistaken for input data.

  * **self-garbage-collecting** -- a hard-killed job (``cancel_run`` / executor
    SIGKILL) runs neither ``commit`` nor ``abort``, orphaning its scratch
    subdir. ``gc_stale_scratch`` reclaims orphans **by age**: only subdirs whose
    mtime is older than a generous TTL are removed, so a concurrent in-flight
    write (fresh mtime) is never deleted. GC runs on the driver only, where the
    writer is constructed -- never racing across executors.
"""

from __future__ import annotations

import os
import shutil
import time
import uuid

# gc_stale_local_temp relocated to the tier-neutral canonical module; re-exported
# so existing callers (ds.vector) keep working. DEFAULT_STALE_TTL_SECONDS lives there now.
from databricks.labs.gbx.pyrx.core.local_temp import (  # noqa: F401
    DEFAULT_STALE_TTL_SECONDS,
    gc_stale_local_temp,
)

#: Hidden container directory holding all per-write scratch subdirs.
SCRATCH_CONTAINER = ".gbx_scratch"


def new_scratch_dir(parent: str) -> str:
    """Return a unique (not-yet-created) scratch path under the hidden container.

    ``parent`` is the directory beside/under which scratch should live (the
    output file's parent dir, or the output dir itself for directory outputs).
    The returned path is ``<parent>/.gbx_scratch/<uuid>``; the caller creates it
    lazily (``os.makedirs(..., exist_ok=True)``) when it first writes a fragment.
    """
    return os.path.join(parent, SCRATCH_CONTAINER, uuid.uuid4().hex)


def remove_scratch_dir(scratch_dir: str) -> None:
    """Remove a per-write scratch subdir AND prune the now-empty container.

    ``shutil.rmtree(scratch_dir)`` alone leaves the parent ``.gbx_scratch``
    container behind as an empty hidden directory in the output location (a
    singleFile write's output dir ends up with a stray ``.gbx_scratch/``). This
    also drops the container via ``os.rmdir`` — which only succeeds when the
    container is empty, so a concurrent in-flight write's sibling subdir keeps
    the container alive and untouched. Best-effort and never raises.
    """
    if not scratch_dir:
        return
    shutil.rmtree(scratch_dir, ignore_errors=True)
    container = os.path.dirname(scratch_dir)
    if os.path.basename(container) == SCRATCH_CONTAINER:
        try:
            os.rmdir(container)  # only removes it if now empty (no live siblings)
        except OSError:
            pass  # not empty (concurrent write) or already gone -- leave it


def gc_stale_scratch(parent: str, ttl_seconds: int = DEFAULT_STALE_TTL_SECONDS) -> None:
    """Best-effort: remove scratch subdirs under ``<parent>/.gbx_scratch`` whose
    mtime is older than ``ttl_seconds``.

    Age-based and idempotent so it is safe to call from every write even when
    other writes are in flight: a concurrent write's fresh-mtime scratch is left
    alone, and racing deletes are swallowed (``ignore_errors`` / per-entry
    ``try``). Never raises.
    """
    container = os.path.join(parent, SCRATCH_CONTAINER)
    try:
        entries = os.listdir(container)
    except OSError:
        return  # no container yet, or not listable -- nothing to GC
    now = time.time()
    for name in entries:
        sub = os.path.join(container, name)
        try:
            if os.path.isdir(sub) and (now - os.path.getmtime(sub)) > ttl_seconds:
                shutil.rmtree(sub, ignore_errors=True)
        except OSError:
            continue
