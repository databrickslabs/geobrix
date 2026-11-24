"""gridMET daily climate grids (University of Idaho, CC0) for whole seasons: one ``<var>_<YYYY>.nc`` per season.

The files cover CONUS at 1/24 degree in EPSG:4326 with one slice per day (dimension ``day``). ``read`` cuts the AOI
on read (``netcdf_gbx`` ``bbox``) and stacks the days as bands (``bandDim="day"``), so band ``i`` of a season's tile
is day ``i`` (1 = January 1). The values come back scaled: ``wind_speed`` is stored as uint16 counts with
``scale_factor`` 0.1, and the reader returns m/s.
"""

from typing import Optional, Sequence

from databricks.labs.gbx.sample import _http_assets as H

BASE = "https://www.northwestknowledge.net/metdata/data"
_ALLOWED_HOSTS = (".northwestknowledge.net",)
VARIABLE_NAMES = {"vs": "wind_speed"}  # file prefix -> NetCDF variable
TIME_DIM = "day"  # the files' time dimension: one slice per day of the season


class GridmetDownloader:
    def __init__(self, variable: str = "vs", base: str = BASE, _fetch=None):
        self.variable = variable
        self.nc_variable = VARIABLE_NAMES.get(variable, variable)
        self.base = base.rstrip("/")
        self._fetch = _fetch

    def assets(self, seasons: Sequence[int]) -> list:
        v = self.variable
        return [
            (f"{v}_{y}", v, f"{self.base}/{v}_{y}.nc", f"{v}_{y}.nc") for y in seasons
        ]

    def discover(self, seasons, spark=None):
        return H.discover_frame(self.assets(seasons), spark)

    def download(self, seasons, out_dir: str, force: bool = False, spark=None):
        return H.fetch_all(
            self.assets(seasons),
            out_dir,
            H.netcdf_valid(self.nc_variable),
            _ALLOWED_HOSTS,
            force=force,
            fetch=self._fetch,
            spark=spark,
        )

    def repair(
        self,
        target,
        seasons,
        out_dir: str,
        where: str = "is_out_file_valid = false",
        spark=None,
    ):
        return H.repair_rows(
            target,
            self.assets(seasons),
            out_dir,
            H.netcdf_valid(self.nc_variable),
            _ALLOWED_HOSTS,
            where=where,
            fetch=self._fetch,
            spark=spark,
        )

    def read(
        self, spark, out_dir: str, bbox: Sequence[float], season: Optional[int] = None
    ):
        year = str(season) if season is not None else r"\d{4}"
        return (
            spark.read.format("netcdf_gbx")
            .option("variable", self.nc_variable)
            .option("bbox", ",".join(str(x) for x in bbox))
            .option("bandDim", TIME_DIM)
            .option("filterRegex", rf".*/{self.variable}_{year}\.nc$")
            .load(out_dir)
        )


def download_gridmet(spark, seasons, out_dir, **kw):
    return GridmetDownloader().download(seasons, out_dir, spark=spark, **kw)
