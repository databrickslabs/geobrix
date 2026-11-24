"""NASA HLS v2.0 (Landsat L30 and Sentinel-2 S30) red, NIR and Fmask COGs for an AOI and a date window.

Red is B04 for both sensors; NIR is B05 for L30 and B8A for S30 (S30 B05 is a red-edge band). ``read_scenes`` lists
the staged files and returns one row per scene that has all three assets. Needs an Earthdata token
(``EARTHDATA_TOKEN``) and ``earthaccess``.
"""

import os
import re
from datetime import date, timedelta
from typing import Optional, Sequence

from databricks.labs.gbx.earthdata import EarthdataClient
from databricks.labs.gbx.sample._http_assets import raster_valid

L30, S30, VERSION = "HLSL30", "HLSS30", "2.0"
_NAME = re.compile(
    r"(HLS\.(L30|S30)\.(T\w{5})\.(\d{4})(\d{3})T\d{6}\.v2\.0)\.(B04|B05|B8A|Fmask)\.tif$"
)


def asset_of(url: str) -> Optional[str]:
    m = _NAME.search(url.split("/")[-1])
    if m is None:
        return None
    sensor, band = m.group(2), m.group(6)
    if band == "B04":
        return "red"
    if band == "Fmask":
        return "fmask"
    if (sensor, band) in (("L30", "B05"), ("S30", "B8A")):
        return "nir"
    return None


def parse_name(path: str) -> Optional[dict]:
    m = _NAME.search(os.path.basename(path))
    if m is None or asset_of(path) is None:
        return None
    return {
        "scene_id": m.group(1),
        "sensor": m.group(2),
        "mgrs_tile": m.group(3),
        "collect_date": date(int(m.group(4)), 1, 1)
        + timedelta(days=int(m.group(5)) - 1),
        "asset_name": asset_of(path),
    }


def _validate(path: str, asset: str) -> bool:
    """EarthdataClient's validate_fn: a COG above the size floor that decodes a window (all three assets)."""
    return raster_valid(path)


class HlsDownloader:
    def __init__(self, _earthaccess=None, _client=None):
        self.short_names = [L30, S30]
        self.version = VERSION
        self.client = _client or EarthdataClient(_earthaccess=_earthaccess)

    def discover(self, bbox: Sequence[float], temporal=None, spark=None):
        return self.client.search(
            self.short_names, self.version, bbox, temporal, asset_of, spark=spark
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
        return self.client.download(
            self.discover(bbox, temporal, spark),
            out_dir,
            _validate,
            force=force,
            spark=spark,
        )

    def repair(
        self, target, where="is_out_file_valid = false", spark=None, out_dir=None
    ):
        return self.client.repair(
            target, _validate, where=where, spark=spark, out_dir=out_dir
        )

    def read_scenes(self, spark, out_dir: str):
        """One row per scene with red, NIR and Fmask files in ``out_dir``. Files are listed by name only and
        not opened: gate on ``is_out_file_valid`` from the download result when a file may be truncated.
        """
        from databricks.labs.gbx.ds._listing import list_files, to_local_path

        files = list_files(
            to_local_path(out_dir),
            r".*/HLS\.[LS]30\..*\.(B04|B05|B8A|Fmask)\.tif$",
            recursive=False,
            raise_on_empty=False,
        )
        scenes: dict = {}
        for p in files:
            info = parse_name(p)
            if info is not None:
                s = scenes.setdefault(
                    info["scene_id"],
                    {
                        k: info[k]
                        for k in ("scene_id", "sensor", "mgrs_tile", "collect_date")
                    },
                )
                s[info["asset_name"] + "_path"] = p
        rows = [
            (
                s["scene_id"],
                s["sensor"],
                s["mgrs_tile"],
                s["collect_date"],
                s["red_path"],
                s["nir_path"],
                s["fmask_path"],
            )
            for _, s in sorted(scenes.items())
            if all(k in s for k in ("red_path", "nir_path", "fmask_path"))
        ]
        return spark.createDataFrame(
            rows,
            "scene_id string, sensor string, mgrs_tile string, collect_date date, "
            "red_path string, nir_path string, fmask_path string",
        )


def download_hls_aoi(spark, bbox, out_dir, **kw):
    return HlsDownloader().download(bbox, out_dir, spark=spark, **kw)
