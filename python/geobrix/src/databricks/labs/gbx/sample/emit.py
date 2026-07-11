"""EmitDownloader — AOI-driven NASA EMIT CH4 staging (thin wrapper over EarthdataClient).

EMIT is on NASA LP DAAC (not Planetary Computer), so this sits on the generic
``EarthdataClient`` (auth + CMR search + validating download + repair) the same way
NaipDownloader/DemDownloader sit on ``StacClient``. This class supplies only the
EMIT specifics: the CMR short-names, the CH4 asset classifier, the COG/GeoJSON
validate-fn, and ``read_enh``/``read_plumes``.

Products: EMITL2BCH4ENH (60 m enhancement COGs — the CH4ENH band) and
EMITL2BCH4PLM (plume-complex COG + GeoJSON outline + emission-rate estimate).
Requires EARTHDATA_TOKEN for real pulls; unit tests inject a fake earthaccess.
Serverless-safe (driver-side download, no spark.conf.set/_jvm/.rdd/cache/persist).
"""

from __future__ import annotations

import os
from typing import Optional, Sequence

from databricks.labs.gbx.earthdata import EarthdataClient

ENH_SHORT = "EMITL2BCH4ENH"
PLM_SHORT = "EMITL2BCH4PLM"
VERSION = "002"
_FLOOR = 1024


def _asset_of(url: str) -> Optional[str]:
    """Classify an EMIT data link into a selected asset label (or None to skip)."""
    u = url.lower()
    if "ch4enh" in u and u.endswith(".tif"):
        return "ch4enh"
    if "ch4plm" in u and u.endswith(".tif"):
        return "plm_cog"
    if "ch4plm" in u and u.endswith(".json"):
        return "plm_geojson"
    return None


def _valid_geojson(path: str) -> bool:
    import json

    try:
        with open(path) as fh:
            gj = json.load(fh)
        return bool(gj.get("features"))
    except Exception:
        return False


def _emit_validate(path: str, asset: str) -> bool:
    """validate_fn for EarthdataClient: parseable GeoJSON, or a readable COG > floor."""
    if not os.path.exists(path):
        return False
    if asset == "plm_geojson":
        # A valid plume GeoJSON can be small; validity = parseable + has features
        # (no binary size floor, which is a COG-truncation guard).
        return _valid_geojson(path)
    if os.path.getsize(path) <= _FLOOR:
        return False
    from databricks.labs.gbx.stac._download import read_validate

    return read_validate(path)  # rasterio window read for the COGs


class EmitDownloader:
    def __init__(
        self,
        enh_short: str = ENH_SHORT,
        plm_short: str = PLM_SHORT,
        version: str = VERSION,
        _earthaccess=None,
        _client=None,
    ):
        self.short_names = [enh_short, plm_short]
        self.version = version
        self.client = _client or EarthdataClient(_earthaccess=_earthaccess)

    def discover(self, bbox: Sequence[float], temporal=None, spark=None):
        return self.client.search(
            self.short_names, self.version, bbox, temporal, _asset_of, spark=spark
        )

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        temporal=None,
        force: bool = False,
        spark=None,
    ):
        self.client.login()
        rows_df = self.discover(bbox, temporal, spark)
        return self.client.download(
            rows_df, out_dir, _emit_validate, force=force, spark=spark
        )

    def repair(
        self, target, where="is_out_file_valid = false", spark=None, out_dir=None
    ):
        return self.client.repair(
            target, _emit_validate, where=where, spark=spark, out_dir=out_dir
        )

    def read_enh(self, out_dir: str, spark=None):
        """Load EMIT CH4 enhancement COGs (raster_gbx tiles)."""
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("raster_gbx")
            .option("filterRegex", r".*CH4ENH.*\.tif$")
            .load(out_dir)
            .repartition(64, F.col("source"))
            .select("source", "tile")
        )

    def read_plumes(self, out_dir: str, spark=None):
        """Load EMIT plume-complex GeoJSON (geojson_gbx vector)."""
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("geojson_gbx")
            .option("filterRegex", r".*CH4PLM.*\.json$")
            .load(out_dir)
        )


def download_emit_aoi(spark, bbox: Sequence[float], out_dir: str, **kw):
    """One-shot: default EmitDownloader + download EMIT CH4 for an AOI."""
    return EmitDownloader().download(bbox, out_dir, spark=spark, **kw)
