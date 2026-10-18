"""Read the files a bronze inventory lists with GeoBrix ``read_paths``.

Each bronze table holds one row per staged file (``path`` plus the fields hoisted
from its name). ``read_inventory`` reads every listed file with a light reader and
joins each output row back to its inventory row on ``_source_path``, the file's
bare ``/Volumes/...`` path, so silver keeps the inventory's ``observation_date``,
``band`` and ``item_id`` as the single source of truth. No side effects at import
time: GeoBrix is imported inside the functions.
"""
import os

from pyspark.sql import functions as F

SOURCE_PATH = "_source_path"


def local_path(col):
    """``col`` (a file path) as ``read_paths`` reports it in ``_source_path``: a
    ``dbfs:`` or ``file:`` scheme in front of an absolute path is stripped, any
    other path is kept as is (``databricks.labs.gbx.ds._listing.to_local_path``)."""
    return F.regexp_replace(col, r"^(?:dbfs|file):(?=/)", "")


def staged(folder, fmt):
    """True once ``folder`` holds a file ``read_paths`` can take the table schema
    from: one whose name matches the reader's default file pattern."""
    from databricks.labs.gbx.ds._listing import first_file
    from databricks.labs.gbx.ds.paths import default_glob, glob_match

    if not os.path.isdir(folder):
        return False
    pattern = default_glob(fmt)
    return first_file(folder, lambda name: glob_match(name, pattern)) is not None


def read_inventory(inventory, fmt, folder, options=None, *, keep=(), num_partitions=None):
    """Every file ``inventory`` lists in its ``path`` column, read with the light
    reader ``fmt`` and ``options``, each row carrying the ``keep`` columns of its
    inventory row (and ``_source_path``). ``None`` until ``folder``, where the files
    are staged, holds one to take the schema from.

    A file that cannot be read is skipped with a warning (``on_error="skip"``);
    ``num_partitions`` spreads the files (or raster windows) across that many tasks.
    """
    from databricks.labs.gbx.ds.paths import read_paths

    if not staged(folder, fmt):
        return None
    rows = read_paths(
        inventory,
        fmt,
        options,
        schema_path=folder,
        mtime_col=None,
        num_partitions=num_partitions,
        on_error="skip",
    ).drop("_source_mtime")
    files = inventory.select(local_path("path").alias(SOURCE_PATH), *keep)
    return rows.join(files, SOURCE_PATH)
