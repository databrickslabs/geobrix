"""WellsDownloader — TX RRC well surface-hole locations via ArcGIS REST (GeoJSON).

Open, no auth. Pages the FeatureServer (resultOffset / resultRecordCount, honoring
exceededTransferLimit) and checks the paged total against ``returnCountOnly`` so a
short download fails loudly. Requests f=geojson (ArcGIS reprojects native EPSG:2277 ->
WGS84 lon/lat automatically), merges pages into one GeoJSON on the Volume, and
validates it. read() loads via geojson_gbx. Serverless-safe (driver-side fetch of
one merged file; no runtime Spark-config mutation or JVM-bridge access).
"""

from __future__ import annotations

import json
import os
import warnings
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


def _more_pages(gj) -> bool:
    """True when the server says more rows follow this page.

    ArcGIS reports ``exceededTransferLimit`` at the top level of an ``f=json`` response
    but under the ``properties`` member of an ``f=geojson`` FeatureCollection, e.g.
    ``{"type": "FeatureCollection", "properties": {"exceededTransferLimit": true},
    "features": [...]}``. Read both; reading only the top level stops after page 1.
    """
    if gj.get("exceededTransferLimit"):
        return True
    props = gj.get("properties")
    return bool(isinstance(props, dict) and props.get("exceededTransferLimit"))


class WellsDownloader:
    def __init__(self, service_url: str = WELLSHL_URL, _get=None):
        self.service_url = service_url
        self._get = _get or _default_get

    def _query_params(self, bbox):
        w, s, e, n = bbox
        return {
            "where": "1=1",
            "geometry": f"{w},{s},{e},{n}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
        }

    def _server_count(self, bbox):
        """Matching rows per the server (``returnCountOnly``), or None when unavailable.

        ``f=json`` answers ``{"count": 1201}``; ``f=geojson`` answers
        ``{"properties": {"count": 1201}, "features": []}``. Read both.
        """
        params = {**self._query_params(bbox), "returnCountOnly": "true", "f": "json"}
        try:
            gj = self._get(self.service_url, params)
        except Exception as exc:
            warnings.warn(
                f"Could not read the well count from {self.service_url} ({exc}); "
                "the paged total is not cross-checked.",
                stacklevel=3,
            )
            return None
        count = gj.get("count")
        if count is None and isinstance(gj.get("properties"), dict):
            count = gj["properties"].get("count")
        return int(count) if isinstance(count, (int, float)) else None

    def _fetch_all(self, bbox, page_size):
        feats, offset = [], 0
        while True:
            params = {
                **self._query_params(bbox),
                "outFields": "*",
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            gj = self._get(self.service_url, params)
            page = gj.get("features", [])
            feats.extend(page)
            if not _more_pages(gj) or not page:
                break
            offset += len(page)
        expected = self._server_count(bbox)
        if expected is not None and expected != len(feats):
            raise RuntimeError(
                f"Paged {len(feats)} wells but the server reports {expected} for this "
                f"bbox {tuple(bbox)}. The download would be incomplete. "
                "Fix: re-run it (the data may have changed while paging); if it "
                "persists, lower page_size to the service's maxRecordCount."
            )
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
