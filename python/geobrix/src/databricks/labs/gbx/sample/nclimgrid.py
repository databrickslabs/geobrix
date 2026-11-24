"""NOAA nClimGrid monthly precipitation (public S3) and the NCEI 2006-2020 monthly precipitation normals.

Both are CONUS grids at 1/24 degree in EPSG:4326, in millimetres. ``read_normals`` gives one tile with 12 bands
(January to December; the file's ``time`` is the calendar month); ``read_monthly`` one tile with one band per month
from January 1895. Both cut the AOI on read (``netcdf_gbx`` ``bbox``), so the long monthly stack stays small.
"""

from typing import Sequence

from databricks.labs.gbx.sample import _http_assets as H

NORMALS_URL = (  # NCEI archive copy of the 2006-2020 monthly precipitation normals
    "https://www.ncei.noaa.gov/data/oceans/archive/arc0196/0248761/1.1/data/0-data/"
    "prcp-2006_2020-monthly-normals-v1.0.nc"
)
MONTHLY_URL = "https://noaa-nclimgrid-monthly-pds.s3.amazonaws.com/nclimgrid_prcp.nc"
NORMALS_VARIABLE = "mlyprcp_norm"  # monthly normal precipitation
MONTHLY_VARIABLE = "prcp"
TIME_DIM = "time"  # both files' time dimension: one slice per month
_ALLOWED_HOSTS = ("www.ncei.noaa.gov", "noaa-nclimgrid-monthly-pds.s3.amazonaws.com")
_NORMALS = (
    "prcp_normals_2006_2020",
    "normals",
    NORMALS_URL,
    "prcp-2006_2020-monthly-normals-v1.0.nc",
)
_MONTHLY = ("nclimgrid_prcp", "monthly", MONTHLY_URL, "nclimgrid_prcp.nc")


class NclimgridDownloader:
    def __init__(
        self,
        normals_url: str = NORMALS_URL,
        monthly_url: str = MONTHLY_URL,
        _fetch=None,
    ):
        self._normals = (_NORMALS[0], _NORMALS[1], normals_url, _NORMALS[3])
        self._monthly = (_MONTHLY[0], _MONTHLY[1], monthly_url, _MONTHLY[3])
        self._fetch = _fetch

    def normals(self, out_dir: str, force: bool = False, spark=None):
        return H.fetch_all(
            [self._normals],
            out_dir,
            H.netcdf_valid(NORMALS_VARIABLE),
            _ALLOWED_HOSTS,
            force=force,
            fetch=self._fetch,
            spark=spark,
        )

    def monthly(self, out_dir: str, force: bool = False, spark=None):
        return H.fetch_all(
            [self._monthly],
            out_dir,
            H.netcdf_valid(MONTHLY_VARIABLE),
            _ALLOWED_HOSTS,
            force=force,
            fetch=self._fetch,
            spark=spark,
        )

    def repair(
        self, target, out_dir: str, where: str = "is_out_file_valid = false", spark=None
    ):
        fixed = H.repair_rows(
            target,
            [self._normals],
            out_dir,
            H.netcdf_valid(NORMALS_VARIABLE),
            _ALLOWED_HOSTS,
            where=where,
            fetch=self._fetch,
            spark=spark,
        )
        return fixed.unionByName(
            H.repair_rows(
                target,
                [self._monthly],
                out_dir,
                H.netcdf_valid(MONTHLY_VARIABLE),
                _ALLOWED_HOSTS,
                where=where,
                fetch=self._fetch,
                spark=spark,
            )
        )

    def _read(self, spark, out_dir, name, variable, bbox):
        return (
            spark.read.format("netcdf_gbx")
            .option("variable", variable)
            .option("bbox", ",".join(str(x) for x in bbox))
            .option("bandDim", TIME_DIM)
            .option("filterRegex", rf".*/{name}$")
            .load(out_dir)
        )

    def read_normals(self, spark, out_dir: str, bbox: Sequence[float]):
        return self._read(
            spark, out_dir, self._normals[3].replace(".", r"\."), NORMALS_VARIABLE, bbox
        )

    def read_monthly(self, spark, out_dir: str, bbox: Sequence[float]):
        return self._read(
            spark, out_dir, self._monthly[3].replace(".", r"\."), MONTHLY_VARIABLE, bbox
        )


def download_precip_normals(spark, out_dir, **kw):
    return NclimgridDownloader().normals(out_dir, spark=spark, **kw)
