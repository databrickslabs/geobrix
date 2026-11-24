"""USDA NASS Crop Sequence Boundaries (CSB) 2018-2025: field polys with a CDL crop code per year, clipped to a state.

The national file is one zip of a FileGDB (layer ``national1825``, Albers = EPSG:5070). Its main table is deflated,
so range reads do not help. The zip is copied once to ``<out_dir>/_source`` (kept while its size and the server's
Last-Modified match: the server sends no ETag). Each clip copies it to the driver's local disk, extracts it there,
clips one state in a single streaming pass with an OGR attribute filter, publishes ``<out_dir>/csb_<state>.gpkg``
and removes the local tree. It runs on the driver only: a Spark worker has about 1 GB of local disk.
"""

import os
import shutil
import zipfile

from databricks.labs.gbx.core import _http
from databricks.labs.gbx.ds._listing import to_local_path
from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir
from databricks.labs.gbx.sample._http_assets import RESULT_DDL, asset_row

CSB_ZIP_URL = (  # the national 2018-2025 release
    "https://www.nass.usda.gov/sites/default/files/Research_and_Science/Crop-Sequence-Boundaries/datasets/"
    "NationalCSB_2018-2025_rev23.zip"
)
CSB_ZIP_BYTES = 3_758_023_527  # its size; the server sends no ETag, so size + Last-Modified identify a copy
MIN_LOCAL_FREE_BYTES = (
    12 * 1024**3
)  # the local zip copy (3.76 GB) + its extract (about 5.4 GB), with room to spare
LAYER = "national1825"
GEOM_COLUMN = (
    "geom"  # the geometry column of csb_<state>.gpkg, whatever the source calls it
)
FIELDS = (
    "CSBID",
    "CSBYEARS",
    "CSBACRES",
    *(f"CDL{y}" for y in range(2018, 2026)),
    "STATEFIPS",
    "CNTYFIPS",
    "INSIDE_X",
    "INSIDE_Y",
)
_ALLOWED_HOSTS = ("www.nass.usda.gov",)
MAX_ZIP_BYTES = (
    6 * 1024**3
)  # the national zip is about 3.76 GB; a source copy above this is refused
_SOURCE = "_source"


def clip_gdb_to_state(
    src: str, layer: str, state_fips: str, dest_gpkg: str, batch_size: int = 100000
) -> int:
    """Copy the features of ``state_fips`` from ``src`` (any OGR dataset with the CSB fields) to a local GeoPackage
    in one streaming pass; return the feature count."""
    import pyogrio
    import pyogrio.raw

    info = pyogrio.read_info(src, layer=layer)
    cols = [c for c in FIELDS if c in list(info["fields"])]
    n = 0
    # use_pyarrow=True: the default reader (pyogrio 0.13) is an _ArrowStream, which is not iterable
    with pyogrio.raw.open_arrow(
        src,
        layer=layer,
        where=f"STATEFIPS = '{state_fips}'",
        columns=cols,
        batch_size=batch_size,
        use_pyarrow=True,
    ) as (meta, reader):
        src_geom = (
            meta.get("geometry_name") or "wkb_geometry"
        )  # the source's name (a FileGDB says SHAPE)
        for batch in reader:  # vectorscan: ok (streamed OGR batches)
            if batch.num_rows == 0:
                continue
            first = (
                {} if n else {"layer_options": {"GEOMETRY_NAME": GEOM_COLUMN}}
            )  # the output column is always geom
            pyogrio.raw.write_arrow(
                batch,
                dest_gpkg,
                layer="csb",
                driver="GPKG",
                geometry_name=src_geom,
                geometry_type="MultiPolygon",
                crs="EPSG:5070",
                append=n > 0,
                **first,
            )
            n += batch.num_rows
    return n


class CsbDownloader:
    def __init__(
        self, href: str = CSB_ZIP_URL, layer: str = LAYER, _fetch=None, _probe=None
    ):
        self.href, self.layer, self._fetch, self._probe = href, layer, _fetch, _probe

    def _zip(self, out_dir: str) -> str:
        """The Volume copy of the zip, current by size and the server's Last-Modified (kept in ``<zip>.last_modified``)."""
        from databricks.labs.gbx.stac._download import local_source_copy

        probe = (self._probe or _http.probe_range)(
            self.href, allowed_hosts=_ALLOWED_HOSTS
        )
        cache = os.path.join(out_dir, _SOURCE)
        stamp = os.path.join(cache, os.path.basename(self.href) + ".last_modified")
        seen = open(stamp).read() if os.path.exists(stamp) else None
        if probe.last_modified is not None and seen != probe.last_modified:
            _forget(
                os.path.join(cache, os.path.basename(self.href))
            )  # another file on the server: fetch again
        path = local_source_copy(
            self.href,
            cache,
            probe,
            allowed_hosts=_ALLOWED_HOSTS,
            max_bytes=MAX_ZIP_BYTES,
            fetch=self._fetch,
        )
        if probe.last_modified is not None and seen != probe.last_modified:
            with open(
                stamp, "w"
            ) as fh:  # written after the copy is published: a cut-short fetch leaves no stamp
                fh.write(probe.last_modified)
        return path

    @staticmethod
    def _extract_local(zip_path: str, local: str) -> str:
        """Copy the zip to the driver's local folder ``local``, extract it there; return the dataset path."""
        if shutil.disk_usage(local).free < MIN_LOCAL_FREE_BYTES:
            raise OSError(
                f"CSB: {shutil.disk_usage(local).free / 1e9:.1f} GB free on the driver's local disk ({local}); the "
                f"zip and its extract need {MIN_LOCAL_FREE_BYTES / 1e9:.1f} GB. Fix: run on a driver with more local "
                "disk, or point GBX_LOCAL_TEMP_DIR at a larger local folder."
            )
        local_zip = os.path.join(local, os.path.basename(zip_path))
        shutil.copyfile(
            zip_path, local_zip
        )  # one sequential Volume read; the extract seeks the local copy
        root = os.path.join(local, "extract")
        with zipfile.ZipFile(local_zip) as zf:
            for (
                m
            ) in zf.infolist():  # vectorscan: ok (members in order; sequential writes)
                zf.extract(m, root)
        os.remove(local_zip)
        for dirpath, dirnames, filenames in os.walk(
            root
        ):  # our own local extract, not a Volume listing
            for d in dirnames:
                if d.lower().endswith(".gdb"):
                    return os.path.join(dirpath, d)
            for f in filenames:
                if f.lower().endswith(".gpkg"):
                    return os.path.join(dirpath, f)
        raise FileNotFoundError(
            f"{zip_path}: no .gdb folder or .gpkg file inside. Fix: check CSB_ZIP_URL."
        )

    def download(
        self, out_dir: str, state_fips: str = "19", force: bool = False, spark=None
    ):
        _refuse_on_worker()
        if not (
            isinstance(state_fips, str)
            and len(state_fips) == 2
            and state_fips.isdigit()
        ):
            raise ValueError(
                f"state_fips must be a two-digit FIPS code such as '19'; got {state_fips!r}"
            )
        from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

        out_dir = to_local_path(out_dir)
        os.makedirs(out_dir, exist_ok=True)
        dest = os.path.join(out_dir, f"csb_{state_fips}.gpkg")
        if force or not _gpkg_valid(dest):
            zip_path = self._zip(out_dir)
            local = new_local_temp_dir("gbx_csb_")
            scratch = new_scratch_dir(out_dir)
            os.makedirs(scratch, exist_ok=True)
            try:
                src = self._extract_local(zip_path, local)
                tmp_local = os.path.join(local, os.path.basename(dest))
                n = clip_gdb_to_state(src, self.layer, state_fips, tmp_local)
                if n == 0:
                    raise ValueError(
                        f"CSB: no field polys with STATEFIPS = {state_fips!r} in layer {self.layer!r}. "
                        "Fix: pass a two-digit state FIPS code that the national file holds, such as '19'."
                    )
                print(f"... CSB: {n:,} field polys in state {state_fips}")
                tmp = os.path.join(scratch, os.path.basename(dest))
                shutil.copyfile(tmp_local, tmp)
                os.replace(tmp, dest)
            finally:
                shutil.rmtree(local, ignore_errors=True)
                remove_scratch_dir(scratch)
        from databricks.labs.gbx.sample._http_assets import _spark

        return _spark(spark).createDataFrame(
            [asset_row(f"csb_{state_fips}", "gpkg", dest, _gpkg_valid(dest))],
            RESULT_DDL,
        )

    def repair(
        self,
        target,
        out_dir: str,
        state_fips: str = "19",
        where: str = "is_out_file_valid = false",
        spark=None,
    ):
        from databricks.labs.gbx.sample._http_assets import _spark

        df = _spark(spark).table(target) if isinstance(target, str) else target
        if df.where(where).where(f"item_id = 'csb_{state_fips}'").count() == 0:
            return df.limit(0)
        return self.download(out_dir, state_fips, force=True, spark=spark)

    def read(self, spark, out_dir: str, state_fips: str = "19"):
        return spark.read.format("gpkg_gbx").load(
            os.path.join(to_local_path(out_dir), f"csb_{state_fips}.gpkg")
        )


def _refuse_on_worker() -> None:
    """The national zip and its extract need about 9 GB of local disk: only the driver has it."""
    from pyspark import TaskContext

    if TaskContext.get() is not None:
        raise RuntimeError(
            "CsbDownloader.download runs on the driver only: a Spark worker has about 1 GB of local disk, and the "
            "CSB zip and its extract need about 9 GB. Fix: call it from the notebook, not from a UDF or a task."
        )


def _forget(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _gpkg_valid(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) <= 1024:
        return False
    try:
        import pyogrio

        return pyogrio.read_info(path, layer="csb")["features"] > 0
    except Exception:  # noqa: BLE001 - an unreadable file is invalid
        return False


def download_csb_state(spark, out_dir, **kw):
    return CsbDownloader().download(out_dir, spark=spark, **kw)
