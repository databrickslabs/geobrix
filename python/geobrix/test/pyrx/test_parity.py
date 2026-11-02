"""Assert pyrx exposes the accessor, coordinate-transform and constructor function names with the
same signatures as the heavyweight rasterx bindings."""

import inspect

from databricks.labs.gbx.pyrx import functions as prx

# Scope: accessors + coordinate transforms + constructor.
# Only names still served by pyrx itself. The accessors and coordinate transforms moved to the generated
# rasterx.functions API in scale-out Task 10; their signatures are held to the manifest by gbx:manifest:gen --check
# (gate G3), so pinning them here again would duplicate that gate.
PHASE0 = {
    "rst_fromcontent": ("content", "driver"),
}


def test_phase0_functions_present_with_matching_params():
    for name, params in PHASE0.items():
        fn = getattr(prx, name, None)
        assert fn is not None, f"pyrx missing {name}"
        got = tuple(inspect.signature(fn).parameters)
        assert got == params, f"{name} params {got} != {params}"
