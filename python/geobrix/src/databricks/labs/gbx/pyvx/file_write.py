"""Function-layer FILE write for vector output (light tier).

Assembles a single vector output file into a FILE-column Delta table via the
generic gbx_file_write core (MANAGED create_file / EXTERNAL try_to_file). This
is the primary FILE-write path for vector; the DataSource writer
(df.write.format) is FUSE-only. Driver, session-ful — Connect-safe.
"""

from __future__ import annotations

import os
import shutil
import uuid
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from databricks.labs.gbx.ds.file_gbx import gbx_file_write, to_local_path

_TILE_SCHEMA = StructType(
    [
        StructField(
            "tile",
            StructType(
                [StructField("path", StringType()), StructField("raster", BinaryType())]
            ),
        )
    ]
)


def vector_file_write(
    spark: SparkSession,
    local_out: str,
    target: str,
    *,
    driver: str,
    file_mode: str,
    filespace: Optional[str] = None,
    layout: str = "order",
    overwrite: bool = False,
) -> None:
    """Write an assembled single vector file into a FILE-column table at *target*.

    ``file_mode="managed"``: read the assembled bytes, build
    ``{tile:{raster:bytes, path:None}}``, call :func:`gbx_file_write`
    (``create_file`` mints managed FILE storage).

    ``file_mode="external"``: copy the file to ``{filespace}/{uuid}/{basename}``
    on the Volume, build ``{tile:{path:volume_path, raster:None}}``, call
    :func:`gbx_file_write` (``try_to_file`` references it).

    Connect-safe: driver / session-ful only — no ``sparkContext``/``_sc``/
    ``_jvm``/``_jsc``/``df.rdd``; ``spark.createDataFrame`` is driver-only.
    """
    if file_mode not in ("managed", "external"):
        raise ValueError(
            f"file_mode must be 'managed' or 'external', got {file_mode!r}"
        )

    if file_mode == "managed":
        with open(local_out, "rb") as fh:
            file_bytes = fh.read()
        rows = [{"tile": {"path": None, "raster": bytearray(file_bytes)}}]
        df = spark.createDataFrame(rows, _TILE_SCHEMA)
        gbx_file_write(
            df,
            target,
            file_mode="managed",
            filespace=filespace,
            layout=layout,
            overwrite=overwrite,
            spark=spark,
        )
    else:
        # external: copy file to Volume, then register via try_to_file.
        if not filespace:
            raise ValueError(
                "file_mode='external' requires a filespace (/Volumes/…) — the Volume "
                "directory where the assembled file is placed before try_to_file "
                "registers it as a FILE reference."
            )
        volume_base = to_local_path(filespace)
        staging_dir = os.path.join(volume_base, uuid.uuid4().hex)
        os.makedirs(staging_dir, exist_ok=True)
        volume_path = os.path.join(staging_dir, os.path.basename(local_out))
        with open(local_out, "rb") as _src, open(volume_path, "wb") as _dst:
            shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
        rows = [{"tile": {"path": volume_path, "raster": None}}]
        df = spark.createDataFrame(rows, _TILE_SCHEMA)
        gbx_file_write(
            df,
            target,
            file_mode="external",
            filespace=None,
            layout=layout,
            overwrite=overwrite,
            spark=spark,
        )
