"""Timing spans for the planning of a Zarr read.

A read plans on the driver (opens the store, reads its metadata and manifest rows, cuts the
work into units, builds the DataFrame) before any task runs. ``span("name")`` times one of
those steps; it records only inside ``record()``, so a read nobody is timing pays one
``ContextVar`` lookup. The benchmark wraps a read in ``record()`` to see where its planning
goes. Spans may nest (``units`` holds ``rows``): a span's time is its own wall, nested spans
included, and the same name seen twice adds up.
"""

from __future__ import annotations

import contextvars
import time
from contextlib import contextmanager

__all__ = ["record", "span"]

_SPANS: contextvars.ContextVar[dict[str, float] | None] = contextvars.ContextVar(
    "gbx_zarr_spans", default=None
)


@contextmanager
def record():
    """Collect the spans of the reads made inside the block into the dict it yields
    (seconds by name)."""
    spans: dict[str, float] = {}
    token = _SPANS.set(spans)
    try:
        yield spans
    finally:
        _SPANS.reset(token)


@contextmanager
def span(name: str):
    """Time the block under ``name`` when a ``record()`` is active."""
    spans = _SPANS.get()
    if spans is None:
        yield
        return
    t0 = time.perf_counter()
    try:
        yield
    finally:
        spans[name] = spans.get(name, 0.0) + (time.perf_counter() - t0)
