"""The GDAL spec engine: GDAL's in-process algorithms (``gdal raster ...``) run from the declarative ``gdal``
block of a manifest row.

A block names the algorithm, its inputs and output, a write profile and an argument template. The light tier
turns it into an argv (``argv.build``, pinned by a golden fixture) and runs it through ``osgeo`` (``run``). There is
no JVM form of the engine: it measured no faster than the light form. Inputs and the output are ``/vsimem`` files
the engine writes and removes; the caller never names a file, a format or a process option (``spec`` holds the rules).

- ``spec``: the checks (the allowlist, the deny list, the values the engine refuses) and the error classes;
- ``argv``: the binders and the argv (pure; host-testable);
- ``run``: one call on ``osgeo``;
- ``l3``: the Spark UDFs (row-wise c0, batch c1) the manager registers.
"""
