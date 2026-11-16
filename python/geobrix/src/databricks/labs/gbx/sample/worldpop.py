"""WorldPopDownloader — WorldPop constrained 2020 population (100 m) for a bounding box.

Source: WorldPop "Global_2000_2020_Constrained" (top-down constrained individual-country
population counts, 2020, ~100 m), the BSGM-based USA file:

    https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/BSGM/USA/usa_ppp_2020_constrained.tif

(HEAD returns 200, ``Accept-Ranges: bytes``, ~494 MB. The ``maxar_v1/USA`` folder holds
only the ``usa_ppp_2020_UNadj_constrained.tif`` variant; the plain constrained file is
under ``BSGM``.) Open, no auth.

Reads only the bbox window through ``stac._download.windowed_download`` (GDAL /vsicurl
range reads, COG-aware), restricted to the ``data.worldpop.org`` host, and writes a small
GeoTIFF on the Volume. read() loads via raster_gbx.

NO RANGE SUPPORT (probed 2026-11-15): data.worldpop.org answers ``Range`` requests with a full
200 response, not 206, although it advertises ``Accept-Ranges: bytes``; GDAL /vsicurl then
fails ("Range downloading not supported by this server!"). So the first call probes the href
(one checked byte-range GET, through ``core._http`` under the ``data.worldpop.org`` allow-list)
and, finding no range support, DOWNLOADS THE NATIONAL FILE ONCE (about 0.5 GB for the USA) to
``cache_dir`` (default ``<out_dir>/_source``; any path, e.g. a Volume folder). The copy is
streamed in 1 MiB chunks to a scratch file, checked against the server's Content-Length (and
ETag), then published atomically; a cut-short transfer leaves no file. Every later call, for
any bbox, finds the copy by its size and windows it locally with no download. A source that
does serve ranges keeps the direct windowed read and no copy is made.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import shutil
import warnings
from datetime import datetime
from typing import Optional, Sequence

from databricks.labs.gbx.core import _http
from databricks.labs.gbx.ds._listing import to_local_path
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

USA_2020_CONSTRAINED = (
    "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/"
    "2020/BSGM/USA/usa_ppp_2020_constrained.tif"
)
_ALLOWED_HOSTS = ("data.worldpop.org",)
_ASSET = "ppp"
_SOURCE_DIR = "_source"
# WorldPop 100 m grids are 3 arcsec = 1/1200 degree, one float32 band. Used only to ESTIMATE
# the decoded size of a window before reading it (the real window is clipped to the file, so
# it can only be smaller).
_NATIVE_DEG = 1.0 / 1200.0
_BYTES_PER_PIXEL = 4
_SCHEMA = (
    "item_id string, asset_name string, out_file_path string, out_file_sz long, "
    "is_out_file_valid boolean, last_update timestamp"
)


def _default_download(href, outpath, bbox, **kw):
    from databricks.labs.gbx.stac._download import windowed_download

    return windowed_download(href, outpath, bbox, **kw)


def _check_bbox(bbox) -> tuple:
    """(minx, miny, maxx, maxy) as floats, or ValueError with a Fix."""
    try:
        vals = tuple(float(v) for v in bbox)
    except (TypeError, ValueError):
        vals = ()
    if len(vals) != 4 or not all(math.isfinite(v) for v in vals):
        raise ValueError(
            f"bbox must be 4 finite numbers (minx, miny, maxx, maxy); got {bbox!r}. "
            "Fix: pass e.g. [-101.5, 35.0, -100.0, 36.0] in EPSG:4326 lon/lat."
        )
    minx, miny, maxx, maxy = vals
    if not (minx < maxx and miny < maxy):
        raise ValueError(
            f"bbox must have minx < maxx and miny < maxy; got {tuple(bbox)!r}. "
            "Fix: order it (minx, miny, maxx, maxy)."
        )
    return vals


def _estimate_bytes(bbox, native_deg: float, max_mpp: Optional[float]) -> int:
    """Decoded bytes of the bbox window (one float32 band), after any ``max_mpp`` decimation."""
    from databricks.labs.gbx.pyrx.core.budget import grid_bytes

    minx, miny, maxx, maxy = bbox
    w = math.ceil((maxx - minx) / native_deg)
    h = math.ceil((maxy - miny) / native_deg)
    if max_mpp is not None and max_mpp > native_deg:
        factor = max(1, int(max_mpp / native_deg))  # as windowed_download decimates
        w, h = math.ceil(w / factor), math.ceil(h / factor)
    return grid_bytes(w, h, ["float32"])


class WorldPopDownloader:
    """Download a bbox window of a WorldPop GeoTIFF.

    The file name carries a short hash of the bbox (and ``max_mpp``), so two bboxes in one
    ``out_dir`` do not overwrite each other and ``repair`` checks the file of THIS bbox.
    A window is written on local disk (a Volume cannot take GDAL's seek-back header write), copied to a
    scratch directory under ``out_dir`` and published with an atomic ``os.replace``, so a failed call never damages an earlier file.

    ``max_mpp`` (source-CRS units: DEGREES for these EPSG:4326 grids, 1/1200 is native) is
    passed to ``windowed_download`` to decimate the read, as StacClient and DemDownloader do.
    Before reading, the decoded size of the window is estimated and refused when it is over
    the process decoded budget, ``budget.decoded_budget_bytes("auto")`` (64 MiB on
    Serverless, 1536 MiB on classic), the same cap GeoBrix applies to a tile read. The
    estimate assumes a 3 arcsec float32 grid (``native_deg``); pass ``native_deg`` for
    another source.

    ``cache_dir`` holds the source copy when the server has no range support (see the module
    docstring); a ``/Volumes/...`` path is used as a bare path (``file:`` / ``dbfs:`` schemes are
    stripped). ``max_source_bytes`` refuses a source copy over that size (default 2 GiB).
    """

    def __init__(
        self,
        href: str = USA_2020_CONSTRAINED,
        _download=None,
        native_deg: float = _NATIVE_DEG,
        cache_dir: Optional[str] = None,
        max_source_bytes: Optional[int] = None,
        _probe=None,
        _fetch=None,
    ):
        self.href = href
        self._download = _download or _default_download
        self.native_deg = native_deg
        self.cache_dir = cache_dir
        self.max_source_bytes = max_source_bytes
        self._probe = _probe
        self._fetch = _fetch
        self._info = None  # the range probe, made once per downloader

    def _range_info(self):
        if self._info is None:
            probe = self._probe or _http.probe_range
            self._info = probe(self.href, allowed_hosts=_ALLOWED_HOSTS)
        return self._info

    def _cache_for(self, out_dir: str, cache_dir: Optional[str]) -> str:
        """The bare local path of the source-copy folder (``<out_dir>/_source`` by default)."""
        chosen = cache_dir or self.cache_dir or os.path.join(out_dir, _SOURCE_DIR)
        return to_local_path(chosen)

    def _source(self, out_dir: str, cache_dir: Optional[str]) -> str:
        """The href to window: itself when the server serves ranges, else the local copy."""
        info = self._range_info()
        if info.supports_range:
            return self.href
        from databricks.labs.gbx.stac._download import (
            MAX_SOURCE_BYTES,
            local_source_copy,
        )

        return local_source_copy(
            self.href,
            self._cache_for(out_dir, cache_dir),
            info,
            allowed_hosts=_ALLOWED_HOSTS,
            max_bytes=self.max_source_bytes or MAX_SOURCE_BYTES,
            fetch=self._fetch,
        )

    @property
    def _item_id(self) -> str:
        return os.path.splitext(os.path.basename(self.href))[0]

    def _name(self, bbox, max_mpp) -> str:
        key = ",".join(f"{v:.6f}" for v in bbox) + f"|{max_mpp}"
        return f"{self._item_id}_{hashlib.sha1(key.encode()).hexdigest()[:8]}.tif"

    def _dest(self, out_dir: str, bbox, max_mpp=None) -> str:
        return os.path.join(out_dir, self._name(bbox, max_mpp))

    def _row(self, dest, valid):
        sz = os.path.getsize(dest) if os.path.exists(dest) else 0
        return (self._item_id, _ASSET, dest, sz, valid and sz > 0, datetime.now())

    def _guard_size(self, bbox, max_mpp):
        from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes, size_text

        est, cap = _estimate_bytes(
            bbox, self.native_deg, max_mpp
        ), decoded_budget_bytes("auto")
        if est > cap:
            raise ValueError(
                f"the bbox window is about {size_text(est)} decoded, over this compute's "
                f"decoded budget ({size_text(cap)}). Fix: use a smaller bbox, or pass "
                "max_mpp (in degrees, e.g. 0.005) to decimate the read."
            )

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        spark=None,
        max_mpp: Optional[float] = None,
        cache_dir: Optional[str] = None,
    ):
        from pyspark.sql import SparkSession

        from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir

        bbox = _check_bbox(bbox)
        self._guard_size(bbox, max_mpp)
        spark = spark or SparkSession.getActiveSession()
        out_dir = to_local_path(out_dir)
        os.makedirs(out_dir, exist_ok=True)
        dest = self._dest(out_dir, bbox, max_mpp)
        scratch = new_scratch_dir(out_dir)
        os.makedirs(scratch, exist_ok=True)
        tmp = os.path.join(scratch, os.path.basename(dest))
        # GDAL writes a GeoTIFF's header last, by seeking back, which a Volume does not allow (the file keeps a
        # zero IFD offset and no reader opens it). So the window is written on local disk and copied over in one
        # sequential pass, then published with an atomic rename.
        local_dir = new_local_temp_dir("gbx_worldpop_")
        local = os.path.join(local_dir, os.path.basename(dest))
        kw = {"bbox_crs": "EPSG:4326", "allowed_hosts": _ALLOWED_HOSTS}
        if max_mpp is not None:
            kw["max_mpp"] = max_mpp
        try:
            source = self._source(out_dir, cache_dir)  # the href, or the local copy
            self._download(source, local, bbox, **kw)
            shutil.copyfile(local, tmp)
            os.replace(
                tmp, dest
            )  # publish atomically; an earlier file is untouched on failure
        except ValueError as exc:
            if "does not overlap" not in str(exc):  # only the no-overlap case
                raise
            warnings.warn(f"WorldPop window not written: {exc}", stacklevel=2)
            return spark.createDataFrame([self._row(dest, False)], _SCHEMA)
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
            remove_scratch_dir(scratch)
        return spark.createDataFrame([self._row(dest, True)], _SCHEMA)

    def repair(
        self,
        bbox: Sequence[float],
        out_dir: str,
        spark=None,
        max_mpp: Optional[float] = None,
        cache_dir: Optional[str] = None,
    ):
        """Re-download only if THIS bbox's GeoTIFF is missing or empty."""
        out_dir = to_local_path(out_dir)
        dest = self._dest(out_dir, _check_bbox(bbox), max_mpp)
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            from pyspark.sql import SparkSession

            spark = spark or SparkSession.getActiveSession()
            return spark.createDataFrame([self._row(dest, True)], _SCHEMA)
        return self.download(bbox, out_dir, spark, max_mpp=max_mpp, cache_dir=cache_dir)

    def read(
        self,
        out_dir: str,
        spark=None,
        bbox: Optional[Sequence[float]] = None,
        max_mpp: Optional[float] = None,
    ):
        """Load the GeoTIFFs in ``out_dir``; with ``bbox``, only that window's file."""
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        out_dir = to_local_path(out_dir)
        if bbox is not None:
            name = os.path.basename(self._dest(out_dir, _check_bbox(bbox), max_mpp))
            regex = r".*/" + name.replace(".", r"\.") + "$"
        else:
            # never the 0.5 GB source copy or a scratch folder under out_dir
            regex = r"(?!.*/(?:" + re.escape(_SOURCE_DIR) + r"|\.gbx_scratch)/).*\.tif$"
        return (
            spark.read.format("raster_gbx").option("filterRegex", regex).load(out_dir)
        )


def download_worldpop_aoi(
    spark,
    bbox: Sequence[float],
    out_dir: str,
    max_mpp: Optional[float] = None,
    cache_dir: Optional[str] = None,
):
    """One-shot: default WorldPopDownloader + download the USA 2020 window for an AOI."""
    return WorldPopDownloader().download(
        bbox, out_dir, spark=spark, max_mpp=max_mpp, cache_dir=cache_dir
    )
