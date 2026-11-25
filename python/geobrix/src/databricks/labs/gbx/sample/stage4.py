"""NCEP Stage IV daily precipitation on the HRAP grid, for chosen seasons and months, from the NWS daily CONUS NetCDF.

The file dated D holds the 24 h total that ends at 12Z (06:00 CST or 07:00 CDT) on D: 17 or 18 of its 24 hours fall on D - 1, so it
counts for D - 1 (``collect_date_of``). The NetCDF holds the daily total (``NWS_VARIABLE``, in ``NWS_UNITS``) and
other variables; ``read`` takes the total through GDAL, which reads the CF polar stereographic grid mapping. Days
the NWS archive lacks are in NCAR GDEX dataset d507005 (monthly tars of daily GRIB files).
"""

import re
from datetime import date, timedelta
from typing import Sequence

from databricks.labs.gbx.sample import _http_assets as H

NWS_TEMPLATE = (
    "https://water.noaa.gov/resources/downloads/precip/stageIV/{y:04d}/{m:02d}/{d:02d}/"
    "nws_precip_1day_{y:04d}{m:02d}{d:02d}_conus.nc"
)
# The daily total and its units attribute (as the file spells it); checked on a real file by the integration test.
NWS_VARIABLE = "observation"
NWS_UNITS = "Inches"
UNITS_TO_MM = {
    "Inches": 25.4,
    "in": 25.4,
    "inch": 25.4,
    "inches": 25.4,
    "mm": 1.0,
    "millimeter": 1.0,
    "kg m-2": 1.0,
    "kg/m2": 1.0,
}
TO_MM = UNITS_TO_MM[NWS_UNITS]  # multiply a value by this to get millimetres
# HRAP: polar stereographic true at 60N, central meridian 105W, on a sphere of radius 6,371,200 m; 4762.5 m cells.
HRAP_CRS = "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=-105 +x_0=0 +y_0=0 +R=6371200 +units=m +no_defs"
_ALLOWED_HOSTS = ("water.noaa.gov",)
_NAME = re.compile(r"nws_precip_1day_(\d{8})_conus\.nc$")
_GLOB = "nws_precip_1day_*_conus.nc"


class StageIvDayMissing(LookupError):
    """A day the NWS archive does not hold (HTTP 404). Not an OSError: the fetch loop must not turn it into a row."""


def collect_date_of(end_date: date) -> date:
    """The day a 24 h total ending at 12Z on ``end_date`` counts for: the day before."""
    return end_date - timedelta(days=1)


def file_end_date(name: str) -> date:
    m = _NAME.search(name)
    if m is None:
        raise ValueError(f"not a Stage IV daily file name: {name!r}")
    s = m.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


class StageIvDownloader:
    def __init__(self, template: str = NWS_TEMPLATE, _fetch=None):
        self.template = template
        self._fetch = _fetch
        self._validate = H.netcdf_valid(NWS_VARIABLE)

    def _fetch_day(self, url, outpath, allowed_hosts=None, **kw):
        """The HTTP fetch, with a 404 (a day the archive lacks) raised as ``StageIvDayMissing`` with its Fix."""
        import requests

        from databricks.labs.gbx.core import _http

        try:
            return (self._fetch or _http.download)(
                url, outpath, allowed_hosts=allowed_hosts, **kw
            )
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
            day = file_end_date(outpath)
            raise StageIvDayMissing(
                f"{url}: the NWS archive has no Stage IV file for the 24 h ending 12Z on {day.isoformat()}. "
                "Fix: take that day from NCAR GDEX dataset d507005 (https://gdex.ucar.edu/datasets/d507005/), "
                "or leave it out of the seasons and months."
            ) from exc

    def _asset(self, end: date):
        y, m, d = end.year, end.month, end.day
        return (
            f"ST4.{y:04d}{m:02d}{d:02d}",
            "apcp_24h",
            self.template.format(y=y, m=m, d=d),
            f"nws_precip_1day_{y:04d}{m:02d}{d:02d}_conus.nc",
        )

    def assets(self, seasons: Sequence[int], months: Sequence[int]) -> list:
        out = []
        for y in seasons:
            for m in months:
                first = date(y, m, 1)
                nxt = date(y + (m == 12), m % 12 + 1, 1)
                end = first + timedelta(days=1)
                while end <= nxt:
                    out.append(self._asset(end))
                    end += timedelta(days=1)
        return out

    def discover(self, seasons, months, spark=None):
        return H.discover_frame(self.assets(seasons, months), spark)

    def download(self, seasons, months, out_dir: str, force: bool = False, spark=None):
        return H.fetch_all(
            self.assets(seasons, months),
            out_dir,
            self._validate,
            _ALLOWED_HOSTS,
            force=force,
            fetch=self._fetch_day,
            spark=spark,
        )

    def repair(
        self,
        target,
        seasons,
        months,
        out_dir: str,
        where: str = "is_out_file_valid = false",
        spark=None,
    ):
        return H.repair_rows(
            target,
            self.assets(seasons, months),
            out_dir,
            self._validate,
            _ALLOWED_HOSTS,
            where=where,
            fetch=self._fetch_day,
            spark=spark,
        )

    def read(self, spark, out_dir: str):
        """One HRAP tile per day: the files' bytes (``binaryFile``), opened by GDAL's netCDF driver, then the daily
        total's subdataset. ``source`` is the file path, so the end date is in it."""
        return (
            spark.read.format("binaryFile")
            .option("pathGlobFilter", _GLOB)
            .option("recursiveFileLookup", "true")
            .load(out_dir)
            .selectExpr(
                "path AS source",
                f"gbx_rst_getsubdataset(gbx_rst_fromcontent(content, 'netCDF'), '{NWS_VARIABLE}') AS tile",
            )
        )


def download_stage4(spark, seasons, months, out_dir, **kw):
    return StageIvDownloader().download(seasons, months, out_dir, spark=spark, **kw)
