"""WellsDownloader — TX RRC well surface-hole locations via ArcGIS REST (GeoJSON).

Open, no auth. Pages the FeatureServer (resultOffset / resultRecordCount, honoring
exceededTransferLimit), requests f=geojson (ArcGIS reprojects native EPSG:2277 ->
WGS84 lon/lat automatically), merges pages into one GeoJSON on the Volume, and
validates it. read() loads via geojson_gbx. Serverless-safe (driver-side fetch of
one merged file; no spark.conf.set / _jvm / .rdd / cache / persist).
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Sequence

WELLSHL_URL = (
    "https://services3.arcgis.com/8jYUORGmDUL39WkJ/arcgis/rest/"
    "services/WellSHL/FeatureServer/0/query"
)
_FLOOR = 1024


def _default_get(url, params):
    import requests

    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


class WellsDownloader:
    def __init__(self, service_url: str = WELLSHL_URL, _get=None):
        self.service_url = service_url
        self._get = _get or _default_get

    def _fetch_all(self, bbox, page_size):
        w, s, e, n = bbox
        feats, offset = [], 0
        while True:
            params = {
                "where": "1=1",
                "geometry": f"{w},{s},{e},{n}",
                "geometryType": "esriGeometryEnvelope",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            gj = self._get(self.service_url, params)
            page = gj.get("features", [])
            feats.extend(page)
            if not gj.get("exceededTransferLimit") or not page:
                break
            offset += len(page)
        return feats

    def discover(self, bbox: Sequence[float], page_size: int = 1000, spark=None):
        """Count matching wells (driver-side, no file written)."""
        return len(self._fetch_all(bbox, page_size))

    def download(
        self, bbox: Sequence[float], out_dir: str, page_size: int = 1000, spark=None
    ):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        os.makedirs(out_dir, exist_ok=True)
        feats = self._fetch_all(bbox, page_size)
        dest = os.path.join(out_dir, "wells.geojson")
        with open(dest, "w") as fh:
            json.dump({"type": "FeatureCollection", "features": feats}, fh)
        valid = len(feats) > 0 and os.path.getsize(dest) > _FLOOR
        return spark.createDataFrame(
            [(dest, len(feats), valid, datetime.now())],
            "out_file_path string, feature_count long, "
            "is_out_file_valid boolean, last_update timestamp",
        )

    def repair(
        self, bbox: Sequence[float], out_dir: str, page_size: int = 1000, spark=None
    ):
        """Re-download only if wells.geojson is missing / below the size floor."""
        dest = os.path.join(out_dir, "wells.geojson")
        if os.path.exists(dest) and os.path.getsize(dest) > _FLOOR:
            from pyspark.sql import SparkSession

            spark = spark or SparkSession.getActiveSession()
            return spark.createDataFrame(
                [(dest, None, True, datetime.now())],
                "out_file_path string, feature_count long, "
                "is_out_file_valid boolean, last_update timestamp",
            )
        return self.download(bbox, out_dir, page_size, spark)

    def read(self, out_dir: str, spark=None):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("geojson_gbx")
            .option("filterRegex", r".*wells\.geojson$")
            .load(out_dir)
        )


def download_wells_aoi(spark, bbox: Sequence[float], out_dir: str, **kw):
    """One-shot: default WellsDownloader + download TX RRC wells for an AOI."""
    return WellsDownloader().download(bbox, out_dir, spark=spark, **kw)
