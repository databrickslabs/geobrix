"""Shared basemap tile-caching, request helpers, and provider presets for gbx.vizx.

Centralises the three cross-cutting concerns every ``cx.add_basemap`` call
site needs:

1. **Default provider** — the default is ``Esri.WorldStreetMap``, which renders
   keyless from Databricks cluster/Serverless egress.  OpenStreetMap.Mapnik and
   CartoDB providers are blocked or key-gated from Databricks datacenter IPs;
   Esri providers work without an API key.  Use ``basemap_source="imagery"`` or
   ``basemap_source="topo"`` for named Esri presets, or pass any contextily
   provider object directly.

2. **Tile disk cache** — enabling contextily's tile cache once per session
   prevents repeated provider fetches across multiple ``plot_static`` /
   ``plot_cog`` renders of the same AOI.  Without caching, every render
   re-requests the same tiles from the provider and hits rate-limit or
   access-blocked tile responses after a handful of renders.  contextily's
   ``cx.set_cache_dir`` is session-global state, so calling it once is
   sufficient.

3. **User-Agent** — Spread ``**_basemap_add_kwargs()`` into every
   ``cx.add_basemap`` call — it supplies ``headers=_TILE_USER_AGENT`` only when
   the installed contextily accepts it, so older versions degrade gracefully.

Usage (in _static_map.py / _cog.py)::

    from databricks.labs.gbx.vizx._basemap import (
        _basemap_add_kwargs,
        _enable_tile_cache,
        _resolve_basemap_source,
    )
    _enable_tile_cache()
    cx.add_basemap(ax, source=_resolve_basemap_source(basemap_source), crs=crs,
                   **_basemap_add_kwargs())
"""

from __future__ import annotations

import os
import warnings

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------
_DEFAULT_TILE_CACHE: str = "/tmp/gbx_tile_cache"

# ---------------------------------------------------------------------------
# Basemap provider presets
# ---------------------------------------------------------------------------
_DEFAULT_BASEMAP: str = "streetmap"
"""Default basemap preset name.

``Esri.WorldStreetMap`` renders keyless from Databricks cluster/Serverless
egress.  OSM.Mapnik and CartoDB providers are blocked or require an API key
from Databricks datacenter IPs.
"""

# Populated lazily (contextily import deferred to _resolve_basemap_source).
_BASEMAP_PRESETS: dict | None = None
"""Mapping of preset name → contextily provider, populated on first call to
:func:`_resolve_basemap_source`."""
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


def _resolve_basemap_source(basemap_source=None):
    """Resolve a basemap source for ``cx.add_basemap``.

    Three modes:

    * ``None`` — return the default preset (``Esri.WorldStreetMap``), which
      renders keyless from Databricks datacenter egress.  OSM.Mapnik and
      CartoDB providers are blocked or key-gated there.
    * A **preset string** (``"streetmap"``, ``"imagery"``, ``"topo"``) — return
      the corresponding Esri contextily provider object.  Raises
      :exc:`ValueError` listing valid presets for unknown strings.
    * **Anything else** (a contextily provider object, a URL, …) — returned
      unchanged (passthrough).

    Args:
        basemap_source: ``None``, a preset name string, or a contextily
            provider / URL to pass directly to ``cx.add_basemap``.

    Returns:
        A contextily provider (or passthrough value) suitable for
        ``cx.add_basemap(source=...)``.
    """
    global _BASEMAP_PRESETS
    if _BASEMAP_PRESETS is None:
        import contextily as cx

        _BASEMAP_PRESETS = {
            "streetmap": cx.providers.Esri.WorldStreetMap,
            "imagery": cx.providers.Esri.WorldImagery,
            "topo": cx.providers.Esri.WorldTopoMap,
        }

    if basemap_source is None:
        return _BASEMAP_PRESETS[_DEFAULT_BASEMAP]

    if isinstance(basemap_source, str):
        if basemap_source in _BASEMAP_PRESETS:
            return _BASEMAP_PRESETS[basemap_source]
        valid = ", ".join(f'"{k}"' for k in _BASEMAP_PRESETS)
        raise ValueError(
            f"Unknown basemap_source preset {basemap_source!r}. "
            f"Valid presets: {valid}.  "
            "Pass a contextily provider object directly for other providers."
        )

    # Passthrough: already a contextily provider object, URL, or anything else.
    return basemap_source


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
