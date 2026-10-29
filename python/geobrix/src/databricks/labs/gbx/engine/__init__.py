"""The GDAL spec engine: GDAL's in-process algorithms (``gdal raster ...``) run from the declarative ``gdal``
block of a manifest row.

A block names the algorithm, its inputs and output, a write profile and an argument template. Each tier turns
it into the same argv (``argv.build``, mirrored by the JVM's ``GdalSpecArgv`` and pinned by one golden fixture
both test suites read) and runs it on the same GDAL build: the light tier through ``osgeo`` (``run``), the JVM
through ``org.gdal.gdal.Algorithm``. Inputs and the output are ``/vsimem`` files the engine writes and removes;
the caller never names a file, a format or a process option (``spec`` holds the rules).

- ``spec``: the checks (the allowlist, the deny list, the values the engine refuses) and the error classes;
- ``argv``: the binders and the argv (pure; host-testable);
- ``run``: one call on ``osgeo``;
- ``l3``: the Spark UDFs (row-wise c0, batch c1) the manager registers.
"""
