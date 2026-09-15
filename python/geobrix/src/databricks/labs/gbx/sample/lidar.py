"""LidarDownloader — AOI-driven USGS 3DEP point-cloud downloader via AWS Open Data EPT.

Pure-Python octree walk over Entwine Point Tiles (EPT), no PDAL / boto3 required.
Reads compressed LAZ nodes from public HTTPS (usgs-lidar-public.s3.amazonaws.com).

**CRS:** EPT coordinates are EPSG:3857 (Web Mercator, metres); z in metres (NAVD88).
Output columns: ``x_3857``, ``y_3857``, ``z_m`` plus convenience ``lon``, ``lat`` (EPSG:4326).

**Serverless-safe:** closure variables only (no custom spark.conf keys), no
``.rdd``/``_jvm``/``_jsc``/``sparkContext``. Parallelism via ``mapInPandas`` when spark
is given; falls back to a driver-side loop otherwise.

**Injection seam (offline tests):** pass ``_get`` (url -> bytes) to bypass network access
in unit tests (see ``test_lidar_downloader.py``).

NOT imported by ``lidar_gbx`` — this module is consumed by the Track G download notebook.
"""

from __future__ import annotations

import io
import json
import math
import os
import time
import urllib.request
from collections import deque
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_MERC_R = 20037508.342789244  # Web Mercator half-circumference (metres)

BUCKET = "https://usgs-lidar-public.s3.amazonaws.com"

SF_PROJECTS: Dict[str, str] = {
    "ARRA-CA_GoldenGate_2010": f"{BUCKET}/ARRA-CA_GoldenGate_2010",
    "ARRA-CA_SanFranCoast_2010": f"{BUCKET}/ARRA-CA_SanFranCoast_2010",
}

SF_AOI_LONLAT = (-122.55, 37.70, -122.35, 37.85)


def lonlat_to_3857(lon: float, lat: float) -> Tuple[float, float]:
    """Convert lon/lat (EPSG:4326 degrees) to Web Mercator (EPSG:3857) metres."""
    x = lon * _MERC_R / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) * _MERC_R / math.pi
    return x, y


def aoi_lonlat_to_3857(
    west: float, south: float, east: float, north: float
) -> Tuple[float, float, float, float]:
    """Convert an AOI bounding box in lon/lat to EPSG:3857 (xmin, ymin, xmax, ymax)."""
    xmin, ymin = lonlat_to_3857(west, south)
    xmax, ymax = lonlat_to_3857(east, north)
    return (xmin, ymin, xmax, ymax)


def _urllib_fetch(url: str, timeout: int = 120, retries: int = 4) -> bytes:
    """Retry-backed urllib fetch; no self-dependency (safe for Spark worker closures).

    Exponential backoff: sleeps 1 s, 2 s, 4 s, … up to 8 s between retries.
    """
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as exc:
            last = exc
            time.sleep(min(2**i, 8))
    raise last  # type: ignore[misc]


def _default_get(url: str) -> bytes:
    """Default injectable ``_get`` for ``Ept`` and ``LidarDownloader``; delegates to ``_urllib_fetch``."""
    return _urllib_fetch(url)


class Ept:
    """Minimal EPT client: load ``ept.json`` and walk the paged octree hierarchy for an AOI.

    Parameters
    ----------
    base_url :
        EPT store root URL, e.g.
        ``https://usgs-lidar-public.s3.amazonaws.com/ARRA-CA_GoldenGate_2010``.
    _get :
        Injectable fetch function ``(url) -> bytes``.  Default: ``_default_get``
        (urllib with exponential-backoff retry).  Override in unit tests to avoid
        live network access.
    """

    def __init__(self, base_url: str, _get: Optional[Callable] = None) -> None:
        self._get = _get or _default_get
        self.base = base_url.rstrip("/") + "/"
        meta = json.loads(self._get(self.base + "ept.json"))
        self.bounds: List[float] = meta["bounds"]  # [xmin,ymin,zmin,xmax,ymax,zmax]
        self.span: int = meta["span"]
        self.points: int = meta["points"]
        self.cube_min: List[float] = self.bounds[:3]
        # EPT bounds are always a cube; cube_size == xmax - xmin == ymax - ymin
        self.cube_size: float = self.bounds[3] - self.bounds[0]

    def node_bounds(self, key: str) -> Tuple[float, float, float, float]:
        """Return the 2-D bounding box ``(xmin, ymin, xmax, ymax)`` for EPT key ``d-x-y-z``."""
        d, xi, yi, _ = (int(v) for v in key.split("-"))
        step = self.cube_size / (2**d)
        xmin = self.cube_min[0] + xi * step
        ymin = self.cube_min[1] + yi * step
        return (xmin, ymin, xmin + step, ymin + step)

    def nodes_in_aoi(
        self,
        aoi_3857: Tuple[float, float, float, float],
        max_depth: int = 18,
    ) -> List[Tuple[str, int]]:
        """Walk the paged octree; return ``[(key, count), ...]`` intersecting the AOI.

        Hierarchy is paged: a node whose count is ``-1`` means its children live in a
        separate page file ``ept-hierarchy/<key>.json``.  We load that page and
        re-examine the node so its children are enqueued correctly.
        """
        axmin, aymin, axmax, aymax = aoi_3857
        hierarchy: Dict[str, int] = {}
        loaded_pages: set = set()

        def load_page(pk: str) -> None:
            hierarchy.update(
                json.loads(self._get(self.base + f"ept-hierarchy/{pk}.json"))
            )
            loaded_pages.add(pk)

        def intersects(key: str) -> bool:
            b = self.node_bounds(key)
            return not (b[2] < axmin or b[0] > axmax or b[3] < aymin or b[1] > aymax)

        load_page("0-0-0-0")
        selected: List[Tuple[str, int]] = []
        stack: deque = deque(["0-0-0-0"])
        seen: set = set()

        while stack:
            key = stack.pop()
            if key in seen or key not in hierarchy or not intersects(key):
                continue
            cnt = hierarchy[key]
            if cnt == -1:  # subtree lives in a sub-page file
                if key in loaded_pages:
                    continue  # page already fetched but node still -1 → no data
                load_page(key)
                stack.append(key)  # re-examine with the real count + children
                continue
            seen.add(key)
            if cnt > 0:
                selected.append((key, cnt))
            d, xi, yi, zi = (int(v) for v in key.split("-"))
            if d < max_depth:
                for dx in (0, 1):
                    for dy in (0, 1):
                        for dz in (0, 1):
                            stack.append(
                                f"{d + 1}-{2 * xi + dx}-{2 * yi + dy}-{2 * zi + dz}"
                            )
        return selected


def _make_fetch_nodes(
    bases: Dict[str, str],
    aoi: Tuple[float, float, float, float],
    out_dir: Optional[str],
    write_laz: bool,
    clip: bool,
    _fetch: Optional[Callable] = None,
) -> Callable:
    """Build the ``mapInPandas`` worker generator, exposed for offline unit tests.

    In production ``_fetch`` is ``None`` → ``_urllib_fetch`` is used (the single copy of
    the retry logic).  In unit tests pass ``_fetch=<mock>`` to avoid live network access
    without needing a Spark cluster.

    Parameters
    ----------
    bases     : ``{project: base_url_with_trailing_slash}``
    aoi       : ``(xmin, ymin, xmax, ymax)`` in EPSG:3857.
    out_dir   : Output directory for ``.laz`` files (``None`` skips writes).
    write_laz : Whether to write raw ``.laz`` nodes to disk.
    clip      : Whether to clip points to the AOI before yielding.
    _fetch    : Fetch function ``(url) -> bytes``; defaults to ``_urllib_fetch``.
    """
    _f = _fetch if _fetch is not None else _urllib_fetch
    _bases = bases
    _aoi = aoi
    _write_laz = write_laz
    _out_dir = out_dir
    _clip = clip
    _merc_r = _MERC_R

    def fetch_nodes(iterator):
        import io as _io
        import math as _math
        import os as _os

        import laspy
        import numpy as np
        import pandas as pd

        for pdf in iterator:
            for proj, key in zip(pdf["project"], pdf["node_key"]):
                try:
                    raw = _f(_bases[proj] + f"ept-data/{key}.laz")
                except Exception:
                    continue  # skip nodes that never come back

                if _write_laz and _out_dir is not None:
                    laz_dir = _os.path.join(_out_dir, proj)
                    _os.makedirs(laz_dir, exist_ok=True)
                    safe_key = key.replace("/", "_")
                    laz_path = _os.path.join(laz_dir, f"{safe_key}.laz")
                    if not _os.path.exists(laz_path):
                        with open(laz_path, "wb") as fh:
                            fh.write(raw)

                las = laspy.read(_io.BytesIO(raw))
                x = np.asarray(las.x)
                y = np.asarray(las.y)
                z = np.asarray(las.z)

                if _clip:
                    m = (
                        (x >= _aoi[0])
                        & (x <= _aoi[2])
                        & (y >= _aoi[1])
                        & (y <= _aoi[3])
                    )
                    if not m.any():
                        continue
                    x, y, z = x[m], y[m], z[m]
                    intensity = np.asarray(las.intensity)[m].astype("int32")
                    classification = np.asarray(las.classification)[m].astype("int32")
                    return_number = np.asarray(las.return_number)[m].astype("int32")
                    number_of_returns = np.asarray(las.number_of_returns)[m].astype(
                        "int32"
                    )
                else:
                    intensity = np.asarray(las.intensity).astype("int32")
                    classification = np.asarray(las.classification).astype("int32")
                    return_number = np.asarray(las.return_number).astype("int32")
                    number_of_returns = np.asarray(las.number_of_returns).astype(
                        "int32"
                    )

                lon = x / _merc_r * 180.0
                lat = np.degrees(
                    2.0 * np.arctan(np.exp(y / _merc_r * _math.pi)) - _math.pi / 2.0
                )
                result = pd.DataFrame(
                    {
                        "project": proj,
                        "node_key": key,
                        "x_3857": x,
                        "y_3857": y,
                        "z_m": z,
                        "lon": lon,
                        "lat": lat,
                        "intensity": intensity,
                        "classification": classification,
                        "return_number": return_number,
                        "number_of_returns": number_of_returns,
                    }
                )
                for col in result.columns:
                    if pd.api.types.is_extension_array_dtype(result[col]):
                        result[col] = result[col].to_numpy(na_value=None)
                yield result

    return fetch_nodes


class LidarDownloader:
    """Distributed, AOI-driven USGS 3DEP LiDAR downloader via AWS Open Data EPT.

    Discovery (``discover``) is driver-side and hierarchy-only (cheap: no LAZ fetches).
    Download (``download``) fans out via ``mapInPandas`` when spark is given; falls back
    to a driver-side loop otherwise.  Raw ``.laz`` nodes are written to
    ``out_dir/<project>/<safe_key>.laz`` (idempotent).

    Parameters
    ----------
    projects  :
        Mapping of project name → EPT base URL (default: ``SF_PROJECTS``).
    bucket    :
        Base HTTPS bucket URL (informational; actual URLs come from ``projects``).
    _get      :
        Injectable fetch function ``(url) -> bytes`` (default: ``_default_get``).
        Override in unit tests to avoid live network access.
    """

    def __init__(
        self,
        projects: Optional[Dict[str, str]] = None,
        bucket: str = BUCKET,
        _get: Optional[Callable] = None,
    ) -> None:
        self.projects = projects if projects is not None else SF_PROJECTS
        self.bucket = bucket
        self._get = _get or _default_get

    def discover(
        self,
        aoi_lonlat: Tuple[float, float, float, float],
        max_depth: int = 18,
    ) -> List[Tuple[str, str, int]]:
        """Driver-side hierarchy walk; returns ``[(project, key, count), ...]``.

        Cheap: only reads ``ept.json`` and ``ept-hierarchy/*.json`` — no LAZ data fetched.
        """
        aoi_3857 = aoi_lonlat_to_3857(*aoi_lonlat)
        rows: List[Tuple[str, str, int]] = []
        for project, base_url in self.projects.items():
            ept = Ept(base_url, _get=self._get)
            for key, cnt in ept.nodes_in_aoi(aoi_3857, max_depth=max_depth):
                rows.append((project, key, cnt))
        return rows

    def download(
        self,
        aoi_lonlat: Optional[Tuple[float, float, float, float]] = None,
        out_dir: Optional[str] = None,
        spark=None,
        max_depth: int = 18,
        num_partitions: int = 256,
        write_laz: bool = True,
        clip: bool = True,
        aoi_3857: Optional[Tuple[float, float, float, float]] = None,
    ) -> "Union[List[dict], DataFrame]":
        """Fetch EPT nodes, write LAZ files, and return clipped point rows.

        Either ``aoi_lonlat`` or ``aoi_3857`` must be supplied.  When both are given,
        ``aoi_3857`` takes precedence (useful in unit tests to skip the projection step).

        When ``spark`` is given the download is distributed via ``mapInPandas`` and returns
        a ``pyspark.sql.DataFrame``; when ``spark`` is ``None`` the driver-side loop runs
        and returns a ``list[dict]`` (suitable for small AOIs and unit tests).

        Parameters
        ----------
        aoi_lonlat :
            ``(west, south, east, north)`` in EPSG:4326 degrees.
        out_dir :
            Directory for ``<project>/<safe_key>.laz`` output files.
        spark :
            Active SparkSession (or ``None`` for driver-only execution).
        max_depth :
            Maximum EPT octree depth to traverse (default 18 = full resolution).
        num_partitions :
            Spark repartition count for the distributed path.
        write_laz :
            When ``True`` write each node's raw ``.laz`` to
            ``out_dir/<project>/<safe_key>.laz`` (idempotent).
        clip :
            Clip returned points to the AOI (default ``True``).
        aoi_3857 :
            Pre-projected AOI in EPSG:3857; bypasses ``aoi_lonlat`` conversion.

        Returns
        -------
        list[dict] (driver path) or pyspark.sql.DataFrame (spark path) with columns:
        ``project, node_key, x_3857, y_3857, z_m, lon, lat, intensity,
        classification, return_number, number_of_returns``.
        """
        if aoi_3857 is None:
            if aoi_lonlat is None:
                raise ValueError("Either aoi_lonlat or aoi_3857 must be provided")
            aoi_3857 = aoi_lonlat_to_3857(*aoi_lonlat)

        # Hierarchy walk is always driver-side (cheap)
        node_rows: List[Tuple[str, str]] = []
        for project, base_url in self.projects.items():
            ept = Ept(base_url, _get=self._get)
            for key, _ in ept.nodes_in_aoi(aoi_3857, max_depth=max_depth):
                node_rows.append((project, key))

        if spark is not None:
            return self._download_spark(
                node_rows, aoi_3857, out_dir, spark, num_partitions, write_laz, clip
            )
        return self._download_driver(node_rows, aoi_3857, out_dir, write_laz, clip)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _download_driver(
        self,
        node_rows: List[Tuple[str, str]],
        aoi_3857: Tuple[float, float, float, float],
        out_dir: Optional[str],
        write_laz: bool,
        clip: bool,
    ) -> List[dict]:
        """Driver-side loop; returns a list of point dicts."""
        import laspy
        import numpy as np

        bases = {p: b.rstrip("/") + "/" for p, b in self.projects.items()}
        all_points: List[dict] = []

        for project, key in node_rows:
            try:
                raw = self._get(bases[project] + f"ept-data/{key}.laz")
            except Exception:
                continue  # skip nodes that fail to download

            if write_laz and out_dir is not None:
                laz_dir = os.path.join(out_dir, project)
                os.makedirs(laz_dir, exist_ok=True)
                safe_key = key.replace("/", "_")
                laz_path = os.path.join(laz_dir, f"{safe_key}.laz")
                if not os.path.exists(laz_path):
                    with open(laz_path, "wb") as fh:
                        fh.write(raw)

            las = laspy.read(io.BytesIO(raw))
            x = np.asarray(las.x)
            y = np.asarray(las.y)
            z = np.asarray(las.z)

            if clip:
                m = (
                    (x >= aoi_3857[0])
                    & (x <= aoi_3857[2])
                    & (y >= aoi_3857[1])
                    & (y <= aoi_3857[3])
                )
                if not m.any():
                    continue
                x, y, z = x[m], y[m], z[m]
                intensity = np.asarray(las.intensity)[m].astype("int32")
                classification = np.asarray(las.classification)[m].astype("int32")
                return_number = np.asarray(las.return_number)[m].astype("int32")
                number_of_returns = np.asarray(las.number_of_returns)[m].astype("int32")
            else:
                intensity = np.asarray(las.intensity).astype("int32")
                classification = np.asarray(las.classification).astype("int32")
                return_number = np.asarray(las.return_number).astype("int32")
                number_of_returns = np.asarray(las.number_of_returns).astype("int32")

            lon = x / _MERC_R * 180.0
            lat = np.degrees(
                2.0 * np.arctan(np.exp(y / _MERC_R * math.pi)) - math.pi / 2.0
            )

            for i in range(len(x)):
                all_points.append(
                    {
                        "project": project,
                        "node_key": key,
                        "x_3857": float(x[i]),
                        "y_3857": float(y[i]),
                        "z_m": float(z[i]),
                        "lon": float(lon[i]),
                        "lat": float(lat[i]),
                        "intensity": int(intensity[i]),
                        "classification": int(classification[i]),
                        "return_number": int(return_number[i]),
                        "number_of_returns": int(number_of_returns[i]),
                    }
                )
        return all_points

    def _download_spark(
        self,
        node_rows: List[Tuple[str, str]],
        aoi_3857: Tuple[float, float, float, float],
        out_dir: Optional[str],
        spark,
        num_partitions: int,
        write_laz: bool,
        clip: bool,
    ) -> "DataFrame":
        """Spark-distributed download via ``mapInPandas``.

        Worker logic lives in ``_make_fetch_nodes`` (module-level, testable without Spark).
        The retry fetch is ``_urllib_fetch`` — single source of truth, no copy here.
        """
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        _OUT_SCHEMA = StructType(
            [
                StructField("project", StringType()),
                StructField("node_key", StringType()),
                StructField("x_3857", DoubleType()),
                StructField("y_3857", DoubleType()),
                StructField("z_m", DoubleType()),
                StructField("lon", DoubleType()),
                StructField("lat", DoubleType()),
                StructField("intensity", IntegerType()),
                StructField("classification", IntegerType()),
                StructField("return_number", IntegerType()),
                StructField("number_of_returns", IntegerType()),
            ]
        )

        # Worker generator: closure variables captured here, no custom spark.conf keys
        # (Serverless-safe). _fetch=None → _make_fetch_nodes uses _urllib_fetch.
        fetch_nodes = _make_fetch_nodes(
            bases={p: b.rstrip("/") + "/" for p, b in self.projects.items()},
            aoi=aoi_3857,
            out_dir=out_dir,
            write_laz=write_laz,
            clip=clip,
        )
        nodes_df = spark.createDataFrame(
            node_rows, "project string, node_key string"
        ).repartition(num_partitions)
        return nodes_df.mapInPandas(fetch_nodes, schema=_OUT_SCHEMA)


def download_lidar_aoi(
    spark,
    aoi_lonlat: Tuple[float, float, float, float],
    out_dir: str,
    **kw,
) -> "DataFrame":
    """One-shot convenience: construct a default LidarDownloader and download a point cloud.

    Mirrors ``download_dem_aoi``/``download_naip_aoi``.  Forwards ``**kw`` (e.g.
    ``max_depth``, ``num_partitions``, ``write_laz``, ``clip``) to
    ``LidarDownloader.download``.
    """
    return LidarDownloader().download(
        aoi_lonlat=aoi_lonlat, out_dir=out_dir, spark=spark, **kw
    )
