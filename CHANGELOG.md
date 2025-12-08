## v0.1.0 [DBR 17.3 LTS]

This is the beta release series.

### v2 DEC 02, 2025
Altered python bindings to `databricks.labs.gbx.*` to more closely match scala bindings (was `geobrix.*`).
Changed init script to install numpy 2.x which allows GDAL python array operations to successfully execute.
A handful of functions known to throw exceptions during execution have been addressed by the changes to the init script: `gbx_rst_mapalgebra(tiles, expression)` and `gbx_rst_ndvi(tile, red_band, nir_band)`.
Added more error handling to bubble up exceptions during execution; functions now return null or a catch-all default with error messages captured.

### v1 OCT 29, 2025
Initial Beta Release