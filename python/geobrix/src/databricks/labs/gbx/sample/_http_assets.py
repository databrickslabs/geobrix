"""Shared mechanics of the sample downloaders that fetch plain HTTP files.

Each asset is fetched on the driver with ``core._http.download`` (the host is checked against the downloader's
allow-list on every redirect) into a scratch folder beside the destination, validated, then published with
``os.replace``: a failed or truncated fetch never leaves a partial file under the final name. A file already present
and valid is kept unless ``force``. The result is the six-column contract of ``EarthdataClient`` and ``StacClient``.
"""

import os
from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence, Tuple

from databricks.labs.gbx.core import _http
from databricks.labs.gbx.ds._listing import to_local_path
from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir

RESULT_DDL = (
    "item_id string, asset_name string, out_file_path string, out_file_sz long, "
    "is_out_file_valid boolean, last_update timestamp"
)
FLOOR_BYTES = 1024
Asset = Tuple[
    str, str, str, str
]  # (item_id, asset_name, href, file name under out_dir)


def _spark(spark):
    if spark is not None:
        return spark
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession()


def asset_row(item_id: str, asset_name: str, path: Optional[str], valid: bool) -> tuple:
    exists = bool(path) and os.path.exists(path)
    return (
        item_id,
        asset_name,
        path if exists else None,
        os.path.getsize(path) if exists else 0,
        bool(valid and exists),
        datetime.now(),
    )


def netcdf_valid(variable: Optional[str] = None) -> Callable[[str], bool]:
    """A validator: the file opens as NetCDF and (when given) holds ``variable``."""

    def check(path: str) -> bool:
        if not os.path.exists(path) or os.path.getsize(path) <= FLOOR_BYTES:
            return False
        try:
            import netCDF4

            with netCDF4.Dataset(path) as ds:
                return variable is None or variable in ds.variables
        except Exception:  # noqa: BLE001 - an unreadable file is invalid
            return False

    return check


def raster_valid(path: str) -> bool:
    """A raster (a GeoTIFF or COG) that opens and decodes a window."""
    if not os.path.exists(path) or os.path.getsize(path) <= FLOOR_BYTES:
        return False
    from databricks.labs.gbx.stac._download import read_validate

    return read_validate(path)


def fetch_asset(
    href: str,
    dest: str,
    validate: Callable[[str], bool],
    allowed_hosts: Sequence[str],
    force: bool = False,
    fetch=None,
) -> bool:
    """Make ``dest`` a valid copy of ``href``; return whether ``dest`` is valid. An error a downloader raises on
    purpose (not an ``OSError`` or ``ValueError``, for example a day missing from an archive) propagates.
    """
    if not force and validate(dest):
        return True
    parent = os.path.dirname(dest)
    os.makedirs(parent, exist_ok=True)
    scratch = new_scratch_dir(parent)
    os.makedirs(scratch, exist_ok=True)
    try:
        tmp = os.path.join(scratch, os.path.basename(dest))
        try:
            (fetch or _http.download)(href, tmp, allowed_hosts=allowed_hosts)
        except (
            OSError,
            ValueError,
        ) as exc:  # an HTTP error, a refused host or a short read
            print(f"... {href}: not fetched ({type(exc).__name__}: {exc})")
            return validate(dest)
        if not validate(tmp):
            print(f"... {href}: fetched file is not valid; kept out of {dest}")
            return validate(dest)
        os.replace(tmp, dest)
        return True
    finally:
        remove_scratch_dir(scratch)


def fetch_all(
    assets: Iterable[Asset],
    out_dir: str,
    validate,
    allowed_hosts,
    force: bool = False,
    fetch=None,
    spark=None,
):
    out_dir = to_local_path(out_dir)
    rows = []
    for item_id, asset_name, href, name in assets:  # driver-side, sequential Volume I/O
        dest = os.path.join(out_dir, name)
        ok = fetch_asset(href, dest, validate, allowed_hosts, force=force, fetch=fetch)
        rows.append(asset_row(item_id, asset_name, dest, ok))
    return _spark(spark).createDataFrame(rows, RESULT_DDL)


def discover_frame(assets: Iterable[Asset], spark=None):
    return _spark(spark).createDataFrame(
        [(i, a, h) for i, a, h, _ in assets],
        "item_id string, asset_name string, href string",
    )


def repair_rows(
    target,
    assets: Iterable[Asset],
    out_dir: str,
    validate,
    allowed_hosts,
    where: str = "is_out_file_valid = false",
    fetch=None,
    spark=None,
):
    """Re-fetch the rows of ``target`` (a table name or a DataFrame) that match ``where``. A table is updated in
    place by ``(item_id, asset_name)``; the repaired rows are returned either way."""
    spark = _spark(spark)
    df = spark.table(target) if isinstance(target, str) else target
    want = {
        (r["item_id"], r["asset_name"])
        for r in df.where(where).select("item_id", "asset_name").collect()
    }
    todo = [a for a in assets if (a[0], a[1]) in want]
    fixed = fetch_all(
        todo, out_dir, validate, allowed_hosts, force=True, fetch=fetch, spark=spark
    )
    if isinstance(target, str) and todo:
        fixed.createOrReplaceTempView("_gbx_repaired_assets")
        spark.sql(
            f"MERGE INTO {target} t USING _gbx_repaired_assets u "
            "ON t.item_id = u.item_id AND t.asset_name = u.asset_name WHEN MATCHED THEN UPDATE SET "
            "out_file_path = u.out_file_path, out_file_sz = u.out_file_sz, "
            "is_out_file_valid = u.is_out_file_valid, last_update = u.last_update"
        )
    return fixed
