"""NASA GRACE-DA (GRACEDADM_CLSM0125US_7D v4.0) weekly groundwater and soil-moisture percentiles, 0.125 degree.

One NetCDF4 file per week; the week's date is the ``A<YYYYMMDD>`` stamp in the file name. Needs an Earthdata token.
"""

import os
import re
from datetime import date
from typing import Optional, Sequence

from databricks.labs.gbx.earthdata import EarthdataClient
from databricks.labs.gbx.sample._http_assets import netcdf_valid

SHORT, VERSION = "GRACEDADM_CLSM0125US_7D", "4.0"
VARIABLES = ("gws_inst", "sfsm_inst")
_DATE = re.compile(r"\.A(\d{8})\.")


def asset_of(url: str) -> Optional[str]:
    return "grace" if url.endswith(".nc4") else None


def week_date_of(path: str) -> date:
    m = _DATE.search(os.path.basename(path))
    if m is None:
        raise ValueError(f"not a GRACE-DA weekly file name: {path!r}")
    s = m.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def _validate(path: str, asset: str) -> bool:
    return all(netcdf_valid(v)(path) for v in VARIABLES)


class GraceDaDownloader:
    def __init__(self, _earthaccess=None, _client=None):
        self.client = _client or EarthdataClient(_earthaccess=_earthaccess)

    def discover(self, bbox: Sequence[float], temporal=None, spark=None):
        return self.client.search(
            [SHORT], VERSION, bbox, temporal, asset_of, spark=spark
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

    def read(self, spark, out_dir: str, variable: str, bbox: Sequence[float]):
        return (
            spark.read.format("netcdf_gbx")
            .option("variable", variable)
            .option("bbox", ",".join(str(x) for x in bbox))
            .option("filterRegex", r".*/GRACEDADM_CLSM0125US_7D\.A\d{8}\..*\.nc4$")
            .load(out_dir)
        )


def download_grace_aoi(spark, bbox, out_dir, **kw):
    return GraceDaDownloader().download(bbox, out_dir, spark=spark, **kw)
