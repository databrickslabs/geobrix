"""Fixtures for the Formats doc tests: the Delta session and the sample cube.

They live in ``docs/tests/python/zarr_delta.py``, which every Zarr doc test shares (the Formats
page, the Zarr reader and writer pages and the Zarr function reference); see its docstring for
how the Delta jars, the session and the cube are found. Importing them here is what makes them
the fixtures of this directory.
"""

from zarr_delta import copy_of_cube, cube, new_table, spark  # noqa: F401
