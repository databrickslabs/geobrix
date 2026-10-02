"""Shared basemap tile-caching and request helpers for gbx.vizx.

Centralises the two cross-cutting concerns every ``cx.add_basemap`` call
site needs:

1. **Tile disk cache** — enabling contextily's tile cache once per session
   prevents repeated provider fetches across multiple ``plot_static`` /
   ``plot_cog`` renders of the same AOI.  Without caching, every render
   re-requests the same tiles from OSM / CartoDB and hits rate-limit or
   access-blocked tile responses after a handful of renders.  contextily's
   ``cx.set_cache_dir`` is session-global state, so calling it once is
   sufficient.

2. **User-Agent** — OSM's tile usage policy requires an identifying
   ``User-Agent`` header.  Blank / generic UAs from cloud datacenter IPs
   are a common block trigger.  Spread ``**_basemap_add_kwargs()`` into every
   ``cx.add_basemap`` call — it supplies ``headers=_TILE_USER_AGENT`` only when
   the installed contextily accepts it, so older versions degrade gracefully.

Usage (in _static_map.py / _cog.py)::

    from databricks.labs.gbx.vizx._basemap import (
        _basemap_add_kwargs,
        _enable_tile_cache,
    )
    _enable_tile_cache()
    cx.add_basemap(ax, source=source, crs=crs, **_basemap_add_kwargs())
"""

from __future__ import annotations

import os
import warnings

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------
_DEFAULT_TILE_CACHE: str = "/tmp/gbx_tile_cache"
"""Default contextily tile cache directory for a cluster/Serverless session."""

_TILE_USER_AGENT: dict[str, str] = {
    "User-Agent": (
        "GeoBrix/0.5.2 (github.com/databrickslabs/geobrix; "
        "tile requests — see OSM tile usage policy)"
    )
}
"""HTTP User-Agent header for tile requests.

OSM's tile usage policy requires an identifying User-Agent.  Sending one
(a) avoids the default 'python-requests/…' UA that providers sometimes
block for bulk datacenter requests, and (b) lets providers enforce quotas
per project rather than per IP.
"""

# ---------------------------------------------------------------------------
# Module-level guard — enable the cache at most once per process lifetime
# ---------------------------------------------------------------------------
_tile_cache_enabled: bool = False


def _enable_tile_cache(cache_dir: str | None = None) -> None:
    """Enable contextily's disk tile cache, idempotent per session.

    Call this immediately before every ``cx.add_basemap`` invocation so that
    tile fetches across multiple renders of the same AOI reuse cached tiles
    rather than re-requesting the provider on every render.

    The cache directory is resolved in this order:
    1. The ``cache_dir`` argument (explicit override).
    2. The ``GBX_TILE_CACHE_DIR`` environment variable.
    3. :data:`_DEFAULT_TILE_CACHE` (``/tmp/gbx_tile_cache``).

    Silently skips when contextily is absent or the target path is not
    writable — the basemap will still render (uncached) and the existing
    try/except in each call site handles provider failures.

    Args:
        cache_dir: Optional path override for the tile cache directory.
    """
    global _tile_cache_enabled
    if _tile_cache_enabled:
        return
    target = cache_dir or os.environ.get("GBX_TILE_CACHE_DIR") or _DEFAULT_TILE_CACHE
    try:
        import contextily as cx

        os.makedirs(target, exist_ok=True)
        cx.set_cache_dir(target)
        _tile_cache_enabled = True
    except (ImportError, OSError, PermissionError):
        # contextily absent or cache directory not writable — silently skip;
        # the basemap will render uncached and the call-site try/except handles
        # provider failures.
        pass
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"_enable_tile_cache: unexpected error setting contextily cache "
            f"({type(exc).__name__}: {exc}) — tile caching disabled for this session.",
            RuntimeWarning,
            stacklevel=2,
        )


def _basemap_add_kwargs() -> dict:
    """Extra kwargs for ``cx.add_basemap``: pass the identifying User-Agent only
    if the installed contextily supports ``headers=`` (added in a later release),
    so older cluster contextily degrades to an unheadered (but still cached) render
    rather than raising ``TypeError: add_basemap() got an unexpected keyword argument``.

    Returns:
        ``{"headers": _TILE_USER_AGENT}`` when the installed contextily accepts
        ``headers=``, otherwise ``{}``..
    """
    try:
        import inspect

        import contextily as cx

        if "headers" in inspect.signature(cx.add_basemap).parameters:
            return {"headers": _TILE_USER_AGENT}
    except Exception:  # noqa: BLE001 — contextily absent or signature inspection failed
        pass
    return {}
