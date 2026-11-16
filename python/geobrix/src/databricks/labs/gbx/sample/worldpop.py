"""WorldPopDownloader — WorldPop constrained 2020 population (100 m) for a bounding box.

Source: WorldPop "Global_2000_2020_Constrained" (top-down constrained individual-country
population counts, 2020, ~100 m), the BSGM-based USA file:

    https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/BSGM/USA/usa_ppp_2020_constrained.tif

(HEAD returns 200, ``Accept-Ranges: bytes``, ~494 MB. The ``maxar_v1/USA`` folder holds
only the ``usa_ppp_2020_UNadj_constrained.tif`` variant; the plain constrained file is
under ``BSGM``.) Open, no auth.

Reads only the bbox window through ``stac._download.windowed_download`` (GDAL /vsicurl
range reads, COG-aware), restricted to the ``data.worldpop.org`` host, and writes a small
GeoTIFF on the Volume. read() loads via raster_gbx. Serverless-safe (driver-side window
read; no runtime Spark-config mutation or JVM-bridge access).
"""

from __future__ import annotations

import os
import warnings
from datetime import datetime
from typing import Sequence

USA_2020_CONSTRAINED = (
    "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/"
    "2020/BSGM/USA/usa_ppp_2020_constrained.tif"
)
_ALLOWED_HOSTS = ("data.worldpop.org",)
_ASSET = "ppp"
_SCHEMA = (
    "item_id string, asset_name string, out_file_path string, out_file_sz long, "
    "is_out_file_valid boolean, last_update timestamp"
)


def _default_download(href, outpath, bbox, **kw):
    from databricks.labs.gbx.stac._download import windowed_download

    return windowed_download(href, outpath, bbox, **kw)


class WorldPopDownloader:
    def __init__(self, href: str = USA_2020_CONSTRAINED, _download=None):
        self.href = href
        self._download = _download or _default_download

    @property
    def _item_id(self) -> str:
        return os.path.splitext(os.path.basename(self.href))[0]

    def _dest(self, out_dir: str) -> str:
        return os.path.join(out_dir, self._item_id + ".tif")

    def _row(self, dest, valid):
        sz = os.path.getsize(dest) if os.path.exists(dest) else 0
        return (self._item_id, _ASSET, dest, sz, valid and sz > 0, datetime.now())

    def download(self, bbox: Sequence[float], out_dir: str, spark=None):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        os.makedirs(out_dir, exist_ok=True)
        dest = self._dest(out_dir)
        try:
            self._download(
                self.href,
                dest,
                bbox,
                bbox_crs="EPSG:4326",
                allowed_hosts=_ALLOWED_HOSTS,
            )
        except ValueError as exc:
            _remove(dest)
            if "does not overlap" not in str(exc):  # only the no-overlap case
                raise
            warnings.warn(f"WorldPop window not written: {exc}", stacklevel=2)
            return spark.createDataFrame([self._row(dest, False)], _SCHEMA)
        except BaseException:
            _remove(dest)
            raise
        return spark.createDataFrame([self._row(dest, True)], _SCHEMA)

    def repair(self, bbox: Sequence[float], out_dir: str, spark=None):
        """Re-download only if the GeoTIFF is missing or empty."""
        dest = self._dest(out_dir)
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            from pyspark.sql import SparkSession

            spark = spark or SparkSession.getActiveSession()
            return spark.createDataFrame([self._row(dest, True)], _SCHEMA)
        return self.download(bbox, out_dir, spark)

    def read(self, out_dir: str, spark=None):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("raster_gbx")
            .option("filterRegex", r".*\.tif$")
            .load(out_dir)
        )


def _remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def download_worldpop_aoi(spark, bbox: Sequence[float], out_dir: str):
    """One-shot: default WorldPopDownloader + download the USA 2020 window for an AOI."""
    return WorldPopDownloader().download(bbox, out_dir, spark=spark)
