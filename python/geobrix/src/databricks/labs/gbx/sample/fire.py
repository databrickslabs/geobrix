"""FirePerimeterDownloader — NIFC WFIGS interagency fire perimeters (ArcGIS REST, GeoJSON).

Open, no auth. Source: the NIFC "WFIGS Interagency Fire Perimeters" ArcGIS FeatureServer
(layer "Perimeters"), the to-date service that holds historic incidents back through
2024 and earlier (not the current-year-only service):

    https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/
    WFIGS_Interagency_Perimeters/FeatureServer/0/query

Fields used (read from the layer's ``?f=json`` field list): ``poly_IncidentName``
(string) and ``attr_FireDiscoveryDateTime`` (date). The layer has no year column, so a
year selects the discovery-date window ``[year-01-01, year+1-01-01)``. The service
matches names case-insensitively; the filter upper-cases both sides to not rely on it.
Example: ``poly_IncidentName`` "Smokehouse Creek" (TX Panhandle, discovered 2024-02-26,
about 1.05 M acres). The layer is EPSG:4326 and the request asks for ``outSR=4326``.

Pages the query (resultOffset / resultRecordCount, honoring exceededTransferLimit) and
checks the paged total against ``returnCountOnly`` so a short download fails loudly.
Merges pages into one GeoJSON on the Volume. read() loads via geojson_gbx.
Serverless-safe (driver-side fetch of one merged file; no runtime Spark-config mutation
or JVM-bridge access).
"""

from __future__ import annotations

import json
import os
import re
import warnings
from datetime import datetime
from typing import Optional

from databricks.labs.gbx.sample.wells import _default_get, _more_pages

WFIGS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "WFIGS_Interagency_Perimeters/FeatureServer/0/query"
)
_NAME_FIELD = "poly_IncidentName"
_DATE_FIELD = "attr_FireDiscoveryDateTime"
_SCHEMA = (
    "out_file_path string, feature_count long, "
    "is_out_file_valid boolean, last_update timestamp"
)


def _slug(incident_name: str, year: Optional[int]) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", incident_name.lower()).strip("_") or "fire"
    return f"{s}_{year}" if year is not None else s


def _where(incident_name: str, year: Optional[int]) -> str:
    name = incident_name.upper().replace("'", "''")
    clause = f"UPPER({_NAME_FIELD}) = '{name}'"
    if year is not None:
        y = int(year)
        clause += (
            f" AND {_DATE_FIELD} >= DATE '{y}-01-01'"
            f" AND {_DATE_FIELD} < DATE '{y + 1}-01-01'"
        )
    return clause


class FirePerimeterDownloader:
    def __init__(self, service_url: str = WFIGS_URL, _get=None):
        self.service_url = service_url
        self._get = _get or _default_get

    def _server_count(self, where):
        params = {"where": where, "returnCountOnly": "true", "f": "json"}
        try:
            gj = self._get(self.service_url, params)
        except Exception as exc:
            warnings.warn(
                f"Could not read the perimeter count from {self.service_url} ({exc}); "
                "the paged total is not cross-checked.",
                stacklevel=3,
            )
            return None
        count = gj.get("count")
        if count is None and isinstance(gj.get("properties"), dict):
            count = gj["properties"].get("count")
        return int(count) if isinstance(count, (int, float)) else None

    def _fetch_all(self, incident_name, year, page_size=2000):
        where = _where(incident_name, year)
        feats, offset = [], 0
        while True:
            params = {
                "where": where,
                "outFields": "*",
                "outSR": "4326",
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
        expected = self._server_count(where)
        if expected is not None and expected != len(feats):
            raise RuntimeError(
                f"Paged {len(feats)} perimeters but the server reports {expected} for "
                f"{incident_name!r} (year {year}). The download would be incomplete. "
                "Fix: re-run it (the data may have changed while paging); if it "
                "persists, lower page_size to the service's maxRecordCount."
            )
        return feats

    def download(
        self,
        incident_name: str,
        out_dir: str,
        year: Optional[int] = None,
        spark=None,
    ):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        os.makedirs(out_dir, exist_ok=True)
        feats = self._fetch_all(incident_name, year)
        dest = os.path.join(out_dir, _slug(incident_name, year) + ".geojson")
        if not feats:
            warnings.warn(
                f"No perimeter found for incident {incident_name!r} (year {year}) in "
                f"{self.service_url}; nothing written. Check the name against "
                f"{_NAME_FIELD} (case-insensitive) and the year against "
                f"{_DATE_FIELD}.",
                stacklevel=2,
            )
            return spark.createDataFrame([(dest, 0, False, datetime.now())], _SCHEMA)
        with open(dest, "w") as fh:
            json.dump({"type": "FeatureCollection", "features": feats}, fh)
        return spark.createDataFrame(
            [(dest, len(feats), True, datetime.now())], _SCHEMA
        )

    def repair(
        self,
        incident_name: str,
        out_dir: str,
        year: Optional[int] = None,
        spark=None,
    ):
        """Re-download only if the GeoJSON is missing or holds no features."""
        dest = os.path.join(out_dir, _slug(incident_name, year) + ".geojson")
        try:
            with open(dest) as fh:
                n = len(json.load(fh).get("features", []))
        except (OSError, ValueError):
            n = 0
        if n > 0:
            from pyspark.sql import SparkSession

            spark = spark or SparkSession.getActiveSession()
            return spark.createDataFrame([(dest, n, True, datetime.now())], _SCHEMA)
        return self.download(incident_name, out_dir, year, spark)

    def read(
        self,
        out_dir: str,
        incident_name: Optional[str] = None,
        year: Optional[int] = None,
        spark=None,
    ):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        regex = (
            r".*" + re.escape(_slug(incident_name, year)) + r"\.geojson$"
            if incident_name
            else r".*\.geojson$"
        )
        return (
            spark.read.format("geojson_gbx").option("filterRegex", regex).load(out_dir)
        )


def download_fire_perimeter_aoi(
    spark, incident_name: str, out_dir: str, year: Optional[int] = None
):
    """One-shot: default FirePerimeterDownloader + download one WFIGS perimeter."""
    return FirePerimeterDownloader().download(
        incident_name, out_dir, year=year, spark=spark
    )
