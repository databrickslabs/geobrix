"""Back-compat shim.

The CRS resolver moved to the tier-neutral ``databricks.labs.gbx.core.crs`` so the
lightweight packages (pyrx AND pyvx) share a single CRS authority instead of forking
it. This module re-exports everything so existing ``pyrx.core.crs`` importers keep
working unchanged. Prefer importing from ``databricks.labs.gbx.core.crs`` in new code.
"""

from databricks.labs.gbx.core.crs import (  # noqa: F401
    _TRANSFORMER_CACHE_SIZE,
    _as_crs,
    _authority_codes,
    _epsg_codes,
    _esri_codes,
    _is_intlike,
    _transformer_cache,
    authority_srid_of,
    crs_equal,
    crs_to_canonical,
    crs_to_proj4,
    get_transformer,
    resolve_crs,
    resolve_source_crs,
    to_pyproj_crs,
)
