"""gbx.vizx — tier-agnostic visualization helpers (requires the [vizx] extra).

Raster rendering (plot_raster / plot_file), static and interactive map
rendering (plot_static / plot_interactive), Spark DataFrame ->
GeoDataFrame adapters (as_gdf / cells_as_gdf), layer constructors
(vector_layer, raster_layer, grid_layer, pmtiles_layer), tile simplification
(simplify_tiles_from_source), and embed-size auditing (audit_layers). Install
with ``pip install 'geobrix[vizx]'``.
"""

from databricks.labs.gbx.vizx._cog import plot_cog, plot_tile
from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._layers import (
    grid_layer,
    pmtiles_layer,
    raster_layer,
    vector_layer,
)
from databricks.labs.gbx.vizx._maplibre import audit_layers
from databricks.labs.gbx.vizx._raster import plot_file, plot_mask_layers, plot_raster
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._vector import as_gdf, cells_as_gdf, grid_as_gdf


def __getattr__(name):
    """Lazy import for optional modules (requires optional extras)."""
    if name == "plot_pmtiles":
        from databricks.labs.gbx.vizx._pmtiles import plot_pmtiles

        return plot_pmtiles
    if name == "simplify_tiles_from_source":
        from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

        return simplify_tiles_from_source
    if name == "plot_interactive_dynamic":
        from databricks.labs.gbx.vizx._dynamic import plot_interactive_dynamic

        return plot_interactive_dynamic
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "plot_raster",
    "plot_file",
    "plot_mask_layers",
    "plot_static",
    "plot_interactive",
    "plot_interactive_dynamic",
    "plot_pmtiles",
    "plot_cog",
    "plot_tile",
    "as_gdf",
    "cells_as_gdf",
    "grid_as_gdf",
    "vector_layer",
    "raster_layer",
    "grid_layer",
    "pmtiles_layer",
    "simplify_tiles_from_source",
    "audit_layers",
]
