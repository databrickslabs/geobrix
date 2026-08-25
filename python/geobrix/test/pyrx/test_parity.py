"""Assert pyrx exposes the Phase 0 (Tier-0 + Tier-1) function names with the
same signatures as the heavyweight rasterx bindings."""

import inspect

from databricks.labs.gbx.pyrx import functions as prx

# Phase 0 scope: accessors + coordinate transforms + constructor.
PHASE0 = {
    "rst_fromcontent": ("content", "driver"),
    "rst_width": ("tile",),
    "rst_height": ("tile",),
    "rst_numbands": ("tile",),
    "rst_srid": ("tile",),
    "rst_pixelwidth": ("tile",),
    "rst_pixelheight": ("tile",),
    "rst_upperleftx": ("tile",),
    "rst_upperlefty": ("tile",),
    "rst_boundingbox": ("tile",),
    "rst_metadata": ("tile",),
    "rst_scalex": ("tile",),
    "rst_scaley": ("tile",),
    "rst_isempty": ("tile",),
    "rst_rastertoworldcoordx": ("tile", "x", "y"),
    "rst_rastertoworldcoordy": ("tile", "x", "y"),
    "rst_worldtorastercoordx": ("tile", "x", "y"),
    "rst_worldtorastercoordy": ("tile", "x", "y"),
}


def test_phase0_functions_present_with_matching_params():
    for name, params in PHASE0.items():
        fn = getattr(prx, name, None)
        assert fn is not None, f"pyrx missing {name}"
        got = tuple(inspect.signature(fn).parameters)
        assert got == params, f"{name} params {got} != {params}"
