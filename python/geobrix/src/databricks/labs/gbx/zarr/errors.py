"""Typed errors for GeoBrix's transactional Zarr stores.

Every error message ends with ``Fix: <how>``: the failure, then the action that
resolves it. Construct them as ``ZarrError(message, fix="...")``. ``str(e)`` is
``f"{message} Fix: {fix}"`` and ``e.fix`` holds the action on its own.

The errors pickle and unpickle with their details, so one raised on a Spark worker
or in a process pool reaches the driver intact.
"""

from __future__ import annotations

__all__ = [
    "ZarrError",
    "ZarrWriteConflict",
    "ZarrStoreDrift",
    "ZarrReadOnly",
    "ZarrGridMismatch",
    "ZarrVersionUnavailable",
    "ZarrAccessUnavailable",
    "ZarrUnsupported",
    "ZarrOwnershipError",
]


def _rebuild(cls, message, fix, extra):
    return cls(message, fix=fix, **extra)


class ZarrError(Exception):
    """Base class for every error raised by ``databricks.labs.gbx.zarr``."""

    def __init__(self, message: str, *, fix: str):
        super().__init__(f"{message} Fix: {fix}")
        self.message = message
        self.fix = fix

    def _extra(self) -> dict:
        """Constructor keywords beyond ``fix``, for pickling subclasses with fields."""
        return {}

    def __reduce__(self):
        return (_rebuild, (type(self), self.message, self.fix, self._extra()))


class ZarrWriteConflict(ZarrError):
    """A concurrent commit changed data chunks this write also touches.

    ``keys`` lists the conflicting store keys; ``winner`` is the ``write_id`` of the
    commit that got there first, when known. ``conflicts`` holds one dict per
    conflicting row (``kind``, ``array``, ``key``, ``expected_write_id`` and the live
    ``write_id``, null when the row is gone), so a caller can tell metadata rows from
    chunk rows.
    """

    def __init__(
        self,
        message: str,
        *,
        fix: str,
        keys: tuple[str, ...] = (),
        winner: str | None = None,
        conflicts: tuple[dict, ...] = (),
    ):
        super().__init__(message, fix=fix)
        self.keys = tuple(keys)
        self.winner = winner
        self.conflicts = tuple(conflicts)

    def _extra(self) -> dict:
        return {"keys": self.keys, "winner": self.winner, "conflicts": self.conflicts}


class ZarrStoreDrift(ZarrError):
    """The store's files no longer match what the manifest recorded."""


class ZarrReadOnly(ZarrError):
    """The store or manifest cannot be written from here."""


class ZarrGridMismatch(ZarrError):
    """A write's array shape, chunking or coordinates do not fit the store's grid."""


class ZarrVersionUnavailable(ZarrError):
    """The requested store version has been vacuumed or never existed."""


class ZarrAccessUnavailable(ZarrError):
    """The credentials or storage access the operation needs are not available."""


class ZarrUnsupported(ZarrError):
    """The store uses a Zarr feature this operation does not support."""


class ZarrOwnershipError(ZarrError):
    """The store belongs to a different manifest, or its files are not owned here."""
