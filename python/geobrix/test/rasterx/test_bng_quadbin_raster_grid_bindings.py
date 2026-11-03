"""Binding-presence test for the new BNG/quadbin raster-grid functions (the reducers are on the generated API).

Light test: import-only, no Spark session needed. Confirms each symbol exists
as an attribute on the rasterx functions module.
"""


def test_bng_quadbin_raster_grid_bindings_exist():
    from databricks.labs.gbx.rasterx import functions as rx

    for name in [
        "rst_bng_tessellate",
        "rst_quadbin_tessellate",
        "rst_quadbin_rasterize_agg",
        "rst_bng_rasterize_agg",
    ]:
        assert hasattr(rx, name), f"missing binding {name}"
