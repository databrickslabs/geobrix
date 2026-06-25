"""gbx.vizx — tier-agnostic visualization helpers (requires the [vizx] extra).

Raster rendering (plot_raster / plot_file) and Spark DataFrame -> GeoDataFrame
adapters (as_gdf / cells_as_gdf) for interactive maps. Install with
``pip install 'geobrix[vizx]'``.
"""

from databricks.labs.gbx.vizx._raster import plot_file, plot_mask_layers, plot_raster
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._vector import as_gdf, cells_as_gdf, grid_as_gdf

__all__ = [
    "plot_raster",
    "plot_file",
    "plot_mask_layers",
    "plot_static",
    "as_gdf",
    "cells_as_gdf",
    "grid_as_gdf",
]
