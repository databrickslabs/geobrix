"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

import atexit
import contextlib
import os
import shutil
import tempfile
import threading
import uuid
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.gbx.ds import _scratch
from databricks.labs.gbx.ds.file_gbx import (
    file_access_tier,
    open_for_write,
    resolve_access,
)

# ---------------------------------------------------------------------------
# Worker-local staging cache (module-level, process-global)
# ---------------------------------------------------------------------------
# Mirrors raster._STAGED_FILES: key = original source path, value = locally-staged path.
# Guarded by _VEC_STAGE_LOCK for thread safety (multiple Spark tasks in one worker).
# Staged copies are held for the process lifetime and removed at exit via atexit.
# This amortizes the staging cost: N partitions of the same source file share one copy.
_VEC_STAGED_FILES: Dict[str, str] = {}
_VEC_STAGE_LOCK = threading.Lock()
_VEC_STAGE_DIR: Optional[str] = None


def _ensure_vec_stage_dir() -> str:
    """Return (creating if needed) the process-wide stage directory. Must be called while holding `_VEC_STAGE_LOCK`."""
    global _VEC_STAGE_DIR
    if _VEC_STAGE_DIR is None:
        _VEC_STAGE_DIR = tempfile.mkdtemp(prefix="gbx_vecstage_")
        atexit.register(_cleanup_vec_stage_dir)
    return _VEC_STAGE_DIR


def _cleanup_vec_stage_dir() -> None:
    global _VEC_STAGE_DIR
    if _VEC_STAGE_DIR and os.path.isdir(_VEC_STAGE_DIR):
        shutil.rmtree(_VEC_STAGE_DIR, ignore_errors=True)
        _VEC_STAGE_DIR = None


def _get_or_stage_vec(source_path: str) -> str:
    """Return a worker-local path for *source_path*, staging it if not yet cached.

    For directories (e.g. ``.gdb``), copies the entire directory tree.
    For regular files, copies the single file.

    The result is cached by source path: multiple partitions targeting the same
    source file on the same worker process share one staged copy, paying the
    sequential-copy cost only once.

    Args:
        source_path: FUSE path to the source file or directory.

    Returns:
        Absolute local path to the staged copy (file or directory).
    """
    with _VEC_STAGE_LOCK:
        if source_path in _VEC_STAGED_FILES:
            return _VEC_STAGED_FILES[source_path]

        stage_dir = _ensure_vec_stage_dir()
        # Use uuid4 to guarantee a collision-free on-disk name even when two
        # different source paths share the same basename.  The cache key remains
        # source_path, so a repeat call for the same path returns the cached
        # entry above and never reaches this branch.
        basename = os.path.basename(source_path.rstrip("/")) or "vector_src"
        safe_name = f"{uuid.uuid4().hex}_{basename}"
        local = os.path.join(stage_dir, safe_name)

        if os.path.isdir(source_path):
            shutil.copytree(source_path, local)
        else:
            shutil.copy(source_path, local)  # sequential read — FUSE-safe

        _VEC_STAGED_FILES[source_path] = local
        return local


# OGR field type (+ subtype) -> Spark type, matching heavy OGR_SchemaInference.getType.
_OGR_TO_SPARK = {
    "OFTInteger": IntegerType,
    "OFTInteger64": LongType,
    "OFTReal": DoubleType,
    "OFTString": StringType,
    "OFTWideString": StringType,
    "OFTDate": DateType,
    "OFTTime": TimestampType,
    "OFTDateTime": TimestampType,
    "OFTBinary": BinaryType,
}
_OGR_LIST_TO_SPARK = {
    "OFTIntegerList": IntegerType,
    "OFTRealList": DoubleType,
    "OFTStringList": StringType,
    "OFTWideStringList": StringType,
}


def _arrow_to_spark(at):
    """Map a pyarrow attribute-column type to the Spark type the reader declares.
    Mirrors _ogr_to_spark's targets so the per-partition output schema (derived from
    the pyogrio Arrow table) matches schema()/_vector_schema for the same source."""
    import pyarrow as pa

    if pa.types.is_boolean(at):
        return BooleanType()
    if pa.types.is_int32(at) or (pa.types.is_integer(at) and at.bit_width <= 32):
        return IntegerType()
    if pa.types.is_integer(at):  # int64 / uint
        return LongType()
    if pa.types.is_floating(at):
        return DoubleType()
    if pa.types.is_date(at):
        return DateType()
    if pa.types.is_timestamp(at):
        return TimestampType()
    if pa.types.is_binary(at) or pa.types.is_large_binary(at):
        return BinaryType()
    if pa.types.is_list(at) or pa.types.is_large_list(at):
        return ArrayType(_arrow_to_spark(at.value_type))
    return StringType()  # string / large_string / anything else


def _ogr_to_spark(ogr_type: str, subtype: str):
    if subtype == "OFSTBoolean":
        return BooleanType()
    if ogr_type in _OGR_LIST_TO_SPARK:
        return ArrayType(_OGR_LIST_TO_SPARK[ogr_type]())
    return _OGR_TO_SPARK.get(ogr_type, StringType)()


def _geom_name(info: Dict) -> str:
    # Heavy uses the OGR geom field name if present, else geom_0 (single-geom v1).
    return info.get("geometry_name") or "geom_0"


def _vector_schema(info: Dict, as_wkb: bool) -> StructType:
    fields: List[StructField] = []
    names = list(info.get("fields", []))
    ogr_types = list(info.get("ogr_types", []))
    subtypes = list(info.get("ogr_subtypes", []))
    for j, name in enumerate(names):
        col = name if name else f"field_{j}"
        ot = ogr_types[j] if j < len(ogr_types) else "OFTString"
        st = subtypes[j] if j < len(subtypes) else "OFSTNone"
        fields.append(StructField(col, _ogr_to_spark(ot, st), True))
    gname = _geom_name(info)
    geom_type = BinaryType() if as_wkb else StringType()
    fields.append(StructField(gname, geom_type, True))
    fields.append(StructField(gname + "_srid", StringType(), True))
    fields.append(StructField(gname + "_srid_proj", StringType(), True))
    return StructType(fields)


def _crs_to_srid_proj(crs) -> Tuple[str, str]:
    """(authority code string e.g. '4326' or '0', PROJ4 string or '')."""
    if not crs:
        return "0", ""
    try:
        from pyproj import CRS

        c = CRS.from_user_input(crs)
        auth = c.to_authority()
        srid = auth[1] if auth else "0"
        try:
            proj4 = c.to_proj4() or ""
        except Exception:
            proj4 = ""
        return srid, proj4
    except Exception:
        return "0", ""


def _zip_vsi(path: str) -> str:
    """Map a zipped vector source to a GDAL /vsizip/ path."""
    if path.lower().endswith(".zip"):
        return "/vsizip/" + path
    return path


_CANONICAL_EXT = {
    "GPKG": ".gpkg",
    "GeoJSON": ".geojson",
    "ESRI Shapefile": ".shp.zip",  # single-file form is zip; non-zip is a dir bundle (out of scope)
    "OpenFileGDB": ".gdb",
}
# Recognized geo extensions, longest-first, so multi-part suffixes match before their parts.
_RECOGNIZED_EXTS = (
    ".shp.zip",
    ".gdb.zip",
    ".pmtiles",
    ".gpkg",
    ".geojson",
    ".gdb",
    ".shp",
)


def _canonical_ext(driver: str, zip_enabled: bool) -> str:
    """Return the canonical file extension for a given OGR driver + zip flag.

    For OpenFileGDB, zip=True produces '.gdb.zip' (a zipped directory archive);
    zip=False produces '.gdb' (the directory itself). All other drivers return
    their single-file extension from _CANONICAL_EXT (zip flag is ignored because
    Shapefile zip is baked into its canonical form; GPKG/GeoJSON are never zipped).
    """
    if driver == "OpenFileGDB":
        return ".gdb.zip" if zip_enabled else ".gdb"
    return _CANONICAL_EXT[driver]


def _complete_ext(name: str, ext: str) -> str:
    """Ensure *name* ends with *ext*, handling partial multi-part extensions.

    Rules:
    - If *ext* is ``""`` (directory unit), return *name* unchanged — no extension
      to append or validate.
    - If *name* already ends with *ext* (case-insensitive), return it unchanged.
    - If *name* ends with a recognized prefix of *ext* (e.g. 'roads.shp' when ext
      is '.shp.zip'), complete it by appending the missing suffix.
    - If *name* ends with a DIFFERENT recognized geo extension, raise ValueError
      (prevents silently double-appending when the caller passes the wrong name).
    - Otherwise append *ext* directly.
    """
    if ext == "":
        return name  # directory unit: no extension to append or validate
    low = name.lower()
    if low.endswith(ext):
        return name
    # Incremental completion for multi-part ext (e.g. "roads.shp" -> "roads.shp.zip").
    for k in range(1, ext.count(".") + 1):
        suffix = "." + ".".join(ext.strip(".").split(".")[:k])
        if low.endswith(suffix) and ext.startswith(suffix):
            return name + ext[len(suffix) :]
    # Reject a DIFFERENT recognized geo extension rather than double-append.
    for other in _RECOGNIZED_EXTS:
        if other != ext and low.endswith(other):
            raise ValueError(
                f"output name '{name}' ends with '{other}' but this writer "
                f"expected {ext} (got a different geo extension)."
            )
    return name + ext


def _resolve_single_file_output(path: str, file_name, ext: str) -> str:
    """Resolve the output path for a single-file/single-unit writer.

    Implements the 3-case naming contract:

    Case 1 — *file_name* given: treat *path* as the parent directory, create it,
        and return ``<path>/<_complete_ext(file_name, ext)>``.
    Case 2 — *file_name* is None/empty and *path* is an EXISTING directory:
        name the output after the directory itself (placed under it), e.g.
        ``/Volumes/.../roads_dir`` -> ``/Volumes/.../roads_dir/roads_dir.gpkg``.
    Case 3 — *file_name* is None/empty and *path* is NOT an existing directory
        (file-like target): complete the extension on the stem, create the parent
        directory, and return the completed path.

    When *ext* is ``""`` the output is a **directory unit** (e.g. a PMTiles shard
    tree): the same 3-case name resolution applies but no extension is appended and
    no wrong-extension rejection occurs.  The parent of the resolved path is created
    (``case 1`` and ``case 2`` mkdir the path itself; ``case 3`` mkdirs the parent).

    Creates parent directories as needed. Pure path logic + one mkdirs side effect.
    """
    path = path.rstrip("/")
    if file_name:  # case 1: path is the parent dir
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, _complete_ext(file_name, ext))
    if os.path.isdir(path):  # case 2: existing dir -> name after it, under it
        return os.path.join(path, _complete_ext(os.path.basename(path), ext))
    # case 3: file-like target -> complete ext, create parent
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    return _complete_ext(path, ext)


def _zip_shapefile_bundle(shp_path: str, zip_path: str) -> None:
    """Zip the shapefile sidecar files (written by pyogrio alongside *shp_path*)
    into a single archive at *zip_path*, flat at the archive root.

    pyogrio writes ``roads.shp``, ``roads.shx``, ``roads.dbf``, ``roads.prj``,
    and ``roads.cpg`` alongside ``shp_path`` (in the same directory); this
    function collects every file in that directory that shares the stem (e.g.
    ``roads``) and packs them into a ZIP archive at ``zip_path``, placing each
    file at the archive root (no subdirectory). The archive is then the only
    artifact that the commit loop copies to the Volume target.
    """
    import zipfile

    parent = os.path.dirname(shp_path)
    stem = os.path.splitext(os.path.basename(shp_path))[0]  # e.g. "roads"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in sorted(os.listdir(parent)):
            # Collect all files with the same stem (roads.shp, roads.shx, …)
            # but not the zip archive itself.
            if name.startswith(stem + ".") and not name.endswith(".zip"):
                zf.write(os.path.join(parent, name), arcname=name)


def _zip_gdb_bundle(gdb_dir: str, zip_path: str) -> None:
    """Zip a FileGDB ``.gdb`` DIRECTORY into a single ``.gdb.zip`` archive,
    preserving the ``.gdb`` directory as the top-level entry so GDAL's OpenFileGDB
    driver opens it via ``/vsizip/<name>.gdb.zip`` (the standard zipped-FileGDB
    layout: one ``<name>.gdb/`` folder at the archive root). The archive is then
    the only artifact the commit loop copies to the Volume target.
    """
    import zipfile

    base = os.path.basename(gdb_dir.rstrip("/"))  # e.g. "roads.gdb"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(gdb_dir):
            for fn in sorted(files):
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, gdb_dir)
                zf.write(full, arcname=os.path.join(base, rel))  # "<name>.gdb/<rel>"


def _geometry_type_of(wkb: bytes) -> str:
    """OGR geometry-type name (e.g. 'Point', 'MultiPolygon') from a WKB blob."""
    from shapely import from_wkb

    return from_wkb(bytes(wkb)).geom_type


def _srid_to_crs(srid: str, proj4: str):
    """Inverse of the reader's CRS encoding: classify a SRID into its canonical
    authority string ('EPSG:<n>' or 'ESRI:<n>'), else the PROJ4 string, else None
    (CRS-less).

    The SRID is classified through the shared resolver (`resolve_crs`), so an ESRI
    code (e.g. 54008) is labeled ``ESRI:54008`` rather than blindly prefixed
    ``EPSG:``. This is a reader/writer edge, not an apply site, so it stays lenient:
    a SRID in neither authority falls back to the PROJ4 string (or None), never
    raising."""
    if srid and srid != "0":
        from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

        try:
            return crs_to_canonical(resolve_crs(srid))
        except Exception:
            pass  # unresolvable SRID -> fall through to the PROJ4 string / None
    if proj4:
        return proj4
    return None


# Output geometry field name per driver. GeoJSON/GeoJSONSeq/Shapefile geometry
# is structural (no named field), so the value is inert there; GPKG/FileGDB name
# the geometry column, so use the format default rather than the input column
# name (which may be arbitrary once geomCol is in play).
# OGR transaction batch size for FileGDB writes.  Large enough to amortise the
# commit overhead; small enough to bound memory and not starve other threads.
# Module-level so tests can monkeypatch it to exercise the boundary code path.
_GDB_TX_BATCH = 100_000

_OUTPUT_GEOM_NAME = {"GPKG": "geom", "OpenFileGDB": "SHAPE"}


def _output_geom_name(driver, geom_col):
    return _OUTPUT_GEOM_NAME.get(driver, geom_col)


def _should_stream(driver):
    """Whether the commit assembles the single output file by STREAMING the
    partition fragments' record batches into one ``write_arrow`` (bounded driver
    memory + single pass), vs. reading all fragments at once.

    Applies to every pyogrio single-file writer: GeoJSON, ESRI Shapefile, GPKG.
    Streaming is the union of what the alternatives lack: one pass (no
    per-fragment file re-opens, and no GeoJSON quadratic re-encode) AND bounded
    memory (never the whole dataset in driver memory, which OOMs the single-node
    driver on large inputs). GDAL sizes the shapefile `.dbf` fields safely across
    the stream (a wider value in a later batch is NOT truncated -- verified).
    GeoJSON/Shapefile geometry is structural (no rename); GPKG's geometry column
    is renamed per batch to its format default (see ``_write_streaming``).

    OpenFileGDB is excluded: its write goes through the native ``osgeo`` path
    (pyogrio's bundled GDAL is read-only for it), so it can't use
    ``write_arrow``; bounding its memory is a separate follow-up.
    """
    return driver in ("GeoJSON", "ESRI Shapefile", "GPKG")


def _stream_record_batches(frag_paths, drop_cols, rename=None):
    """Yield record batches from a sequence of Arrow-IPC fragment files, dropping
    ``drop_cols`` and optionally renaming one column (``rename=(old, new)``, used
    to give GPKG its format-default geometry column name). Streams one batch at a
    time -- never holds all fragments in memory."""
    import pyarrow as pa
    import pyarrow.ipc as ipc

    for fp in frag_paths:
        with pa.memory_map(fp, "r") as src:
            reader = ipc.open_file(src)
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                keep = [c for c in batch.schema.names if c not in drop_cols]
                arrays = [batch.column(c) for c in keep]
                if rename:
                    old, new = rename
                    names = [new if c == old else c for c in keep]
                else:
                    names = keep
                yield pa.record_batch(arrays, names=names)


def _writer_col_roles(schema, geom_col=None, srid_col=None, proj_col=None):
    """(geom_col, srid_col, proj_col, attr_cols) for the vector writers.

    By default the column ``X`` paired with ``X_srid`` is the geometry,
    ``X_srid_proj`` is its PROJ4 fallback, and everything else is an attribute.
    The geomCol / sridCol / projCol options override these by name so the frame
    need not use the convention: each option, when given, must name an existing
    column; when omitted it falls back to its convention name. geom and srid are
    required (clear error if unresolvable); proj is optional.
    """
    names = [f.name for f in schema.fields]

    # geometry (required)
    if geom_col is not None:
        if geom_col not in names:
            raise ValueError(
                f"vector writer geomCol={geom_col!r} is not a column; got {names}"
            )
        geom = geom_col
    else:
        srid_named = [n for n in names if n.endswith("_srid")]
        if not srid_named:
            raise ValueError(
                "vector writer input needs a geometry/'*_srid' column pair (from a "
                f"*_gbx reader) or an explicit geomCol option; got columns {names}"
            )
        geom = srid_named[0][: -len("_srid")]
        if geom not in names:
            raise ValueError(f"no geometry column {geom!r} for srid {srid_named[0]!r}")

    # srid (required: option, else <geom>_srid)
    if srid_col is not None:
        if srid_col not in names:
            raise ValueError(
                f"vector writer sridCol={srid_col!r} is not a column; got {names}"
            )
        srid = srid_col
    else:
        srid = geom + "_srid"
        if srid not in names:
            raise ValueError(
                f"vector writer needs a SRID column: pass sridCol, or add a {srid!r} "
                f"column (authority code, '0' if unknown). Columns: {names}"
            )

    # proj (optional: an explicit projCol must exist; the default may be absent)
    if proj_col is not None:
        if proj_col not in names:
            raise ValueError(
                f"vector writer projCol={proj_col!r} is not a column; got {names}"
            )
        proj = proj_col
    else:
        proj = geom + "_srid_proj"  # optional; may be absent

    attr_cols = [n for n in names if n not in (geom, srid, proj)]
    return geom, srid, proj, attr_cols


def _writer_arrow_table(cols, schema, geom_col):
    """Build an Arrow table from per-column value lists using the DECLARED Spark
    schema for column types.

    Without an explicit schema, ``pa.table`` infers each column's type from its
    Python values -- so a column that is entirely null in a partition infers
    Arrow ``null`` type (format code 'n'), which pyogrio/GDAL cannot create an
    OGR field from ("Type 'n' for field ... is not supported"), and which also
    makes two partitions disagree on a column's type (all-null vs typed),
    breaking the cross-partition ``concat_tables`` merge. Typing every column
    from the reader schema keeps an all-null ``StringType`` column a proper
    (all-null) Arrow ``string``. The geometry column is always emitted as WKB
    ``bytes`` here, so it is typed ``binary`` regardless of its declared type.
    """
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type

    fields = [
        pa.field(
            f.name, pa.binary() if f.name == geom_col else to_arrow_type(f.dataType)
        )
        for f in schema.fields
    ]
    return pa.table(
        {f.name: cols[f.name] for f in schema.fields}, schema=pa.schema(fields)
    )


def _default_gpkg_layer(path):
    """A GeoPackage layer name derived from the output filename, made valid.

    With no explicit ``layerName``, GDAL names the layer after the output file
    stem -- but GeoPackage forbids a layer name that starts with the reserved
    ``gpkg`` prefix (and forbids an empty name), so e.g. saving to ``.../gpkg``
    errors with "layer name may not begin with 'gpkg'". Fall back to ``layer``
    in those cases; override with the ``layerName`` option for anything else.
    """
    stem = os.path.splitext(os.path.basename(path.rstrip("/")))[0]
    if not stem or stem.lower().startswith("gpkg"):
        return "layer"
    return stem


def _copy_file_to_fuse(src, dst):
    """Copy file BYTES only -- never copy mode/stat. shutil.copy/copy2 run
    copymode (chmod) on the destination, which a UC Volume (FUSE) rejects with
    'Operation not permitted'; shutil.copyfile copies content only and is the
    Volume-safe primitive. (The chmod can succeed intermittently, so a plain
    shutil.copy fails non-deterministically partway through a multi-shard write.)
    """
    shutil.copyfile(src, dst)


def _copy_tree_to_fuse(src, dst):
    """Recursively copy a directory (e.g. a FileGDB .gdb) to a FUSE/Volume target
    with byte-only file copies and plain makedirs -- no copystat/chmod, which
    shutil.copytree applies to every file and to the directories themselves and
    which fails on a UC Volume."""
    os.makedirs(dst, exist_ok=True)
    for entry in os.listdir(src):
        s = os.path.join(src, entry)
        d = os.path.join(dst, entry)
        if os.path.isdir(s):
            _copy_tree_to_fuse(s, d)
        else:
            shutil.copyfile(s, d)


class _ChunkPartition(InputPartition):
    """One contiguous feature slice of one layer (picklable)."""

    def __init__(self, path, driver, layer, as_wkb, skip, count, bbox=None, where=None):
        self.path = path
        self.driver = driver
        self.layer = layer
        self.as_wkb = as_wkb
        self.skip = skip
        self.count = count
        self.bbox = bbox
        self.where = where


class VectorGbxReader(DataSourceReader):
    _DRIVER = ""  # named subclasses override

    # Extensions (lower-case) recognised per OGR driver name.  A .gdb directory
    # is always treated as a single FileGDB dataset regardless of the driver.
    # OpenFileGDB includes .gdb.zip and .zip so a directory of copy_*.gdb.zip files
    # is enumerated by _members(); _zip_vsi() prefixes /vsizip/ for .zip paths so
    # each archive is opened correctly by pyogrio / GDAL.
    _EXT_FOR_DRIVER: Dict[str, Tuple[str, ...]] = {
        "GeoJSON": (".geojson", ".json"),
        "GeoJSONSeq": (".geojsonl", ".geojsons"),
        "ESRI Shapefile": (".shp", ".shz", ".zip"),
        "GPKG": (".gpkg",),
        "OpenFileGDB": (".gdb", ".gdb.zip", ".zip"),
    }

    def __init__(self, options: Dict[str, str]):
        raw_path = options.get("path")
        if not raw_path:
            raise ValueError("vector_gbx requires a 'path' (e.g. .load(path)).")
        # FILE access mode: "auto" (default, gracefully downgrades to FUSE if FILE
        # unavailable), "managed" (FILE MANAGED), or "external" (FILE EXTERNAL).
        # The NO-GATING rule is enforced here (__init__ = driver-side), NOT in read()
        # (which runs on executors). On Spark Connect workers (Serverless DBR 19+),
        # SparkSession.getActiveSession() returns None, so file_access_tier() called
        # from read() always resolves to "fuse" — causing a false ValueError for
        # explicit managed/external on FILE-capable runtimes.
        access = options.get("access", "auto")
        if access not in ("auto", "managed", "external"):
            raise ValueError(
                f"vector_gbx 'access' option must be 'auto', 'managed', or 'external'; "
                f"got {access!r}"
            )
        self.access = access

        # Driver-side capability probe: get the active driver SparkSession explicitly
        # to avoid the getOrCreate() path on workers.
        from pyspark.sql import SparkSession as _SpSess

        from databricks.labs.gbx.ds.file_gbx import resolve_local_path

        _driver_spark = _SpSess.getActiveSession()
        # Resolve through resolve_local_path (not inline to_local_path) so vector
        # reader's local-path resolution goes through the base API and is not
        # decorative. This validates access mode and returns the local FUSE path.
        self.path = resolve_local_path(
            raw_path, access=self.access, spark=_driver_spark
        )
        self.driver = options.get("driverName", "") or self._DRIVER
        # `multi=true` reads a DIRECTORY of newline-delimited GeoJSONL shards (the
        # output of geojsonl_gbx): switch a GeoJSON reader to the GeoJSONSeq driver so
        # _members() enumerates .geojsonl/.geojsons and each shard is parsed as a
        # one-Feature-per-line sequence rather than a FeatureCollection.
        if options.get("multi", "false").lower() == "true" and self.driver == "GeoJSON":
            self.driver = "GeoJSONSeq"
        self.as_wkb = options.get("asWKB", "true").lower() != "false"
        self.chunk_size = max(1, int(options.get("chunkSize", "10000")))
        self.layer_number = int(options.get("layerNumber", "0"))
        self.layer_name = options.get("layerName", "")
        # Optional pushdown filters (memory + speed): `bbox` ("xmin,ymin,xmax,ymax"
        # in the layer CRS) and an OGR SQL `where` clause. Passed to pyogrio so only
        # matching features are parsed/materialized -- important on Serverless, where
        # reading a whole large shapefile in one UDF can exceed the 1 GB per-function
        # memory cap. Default None = read all features (prior behavior).
        bbox_opt = options.get("bbox")
        if bbox_opt:
            parts = [float(v) for v in str(bbox_opt).split(",")]
            if len(parts) != 4:
                raise ValueError(
                    "vector bbox option must be 'xmin,ymin,xmax,ymax'; got "
                    f"'{bbox_opt}'"
                )
            self.bbox = tuple(parts)
        else:
            self.bbox = None
        self.where = options.get("where") or None

        # Driver-side capability probe via resolve_access: validates access mode against
        # detected tier. Raises ValueError here (at plan time) for explicit FILE on a
        # fuse-only runtime — once, on the driver, not per-partition on executor workers.
        self._resolved_tier = file_access_tier(spark=_driver_spark)
        resolve_access(self.access, tier=self._resolved_tier)

    def _layer(self):
        return self.layer_name if self.layer_name else self.layer_number

    @staticmethod
    def _gdal_readonly_safe() -> None:
        # Reading a GeoPackage (SQLite) from read-only object storage (a Volume) must
        # not attempt a journal/checkpoint write. DELETE journal mode avoids that.
        import pyogrio

        pyogrio.set_gdal_config_options({"OGR_SQLITE_JOURNAL": "DELETE"})

    def _members(self) -> List[str]:
        """Member paths to read. For a plain directory, enumerate matching vector
        files (by driver extension) RECURSIVELY into sub-directories. A .gdb
        directory is a single FileGDB dataset and is returned as-is. A regular
        file path returns [self.path]."""
        if not os.path.isdir(self.path) or self.path.lower().rstrip("/").endswith(
            ".gdb"
        ):
            return [self.path]
        exts: Tuple[str, ...] = self._EXT_FOR_DRIVER.get(self.driver) or ()
        members = []
        for root, dirs, files in os.walk(self.path):
            # Skip hidden/marker dirs (Spark convention): a writer's in-flight or
            # orphaned .gbx_scratch container, _SUCCESS-style markers, etc. are
            # never input data. Pruning `dirs` in place stops os.walk descending.
            dirs[:] = [d for d in dirs if not d.startswith((".", "_"))]
            for n in sorted(files):
                low = n.lower()
                if (exts and low.endswith(exts)) or low.rstrip("/").endswith(".gdb"):
                    members.append(os.path.join(root, n))
        return sorted(members) or [self.path]

    def _needs_stage(self) -> bool:
        """Random-access formats (GeoPackage = SQLite; FileGDB = seeked multi-file).
        Reading these directly from object storage (a UC Volume) does poorly -- FUSE
        does not serve random/seeked I/O well -- or triggers a read-only write attempt.
        Stage the source to worker-local temp first and read it there."""
        return self.driver in ("GPKG", "OpenFileGDB")

    @contextlib.contextmanager
    def _staged(self, path: str):
        """Yield a locally-readable path.

        For random-access drivers (GPKG, FileGDB), the source is copied to worker-local
        temp via a sequential read (FUSE-safe) so GDAL can do seeked I/O on local disk.
        The copy is **cached per (process, source path)** via the module-level
        ``_VEC_STAGED_FILES`` dict: N partitions of the same file share one staged copy,
        paying the copy cost only once (open amortization).

        For sequential drivers (GeoJSON/Shapefile/GeoJSONSeq), the source is yielded
        in-place — no copy is made or cached.
        """
        if not self._needs_stage():
            yield path
            return
        local = _get_or_stage_vec(path)
        yield local

    def _info_for(self, path: str):
        """Read pyogrio metadata for the given path. Random-access formats are staged
        to local temp first (see _staged) so the open does not seek over a Volume."""
        self._gdal_readonly_safe()
        import pyogrio

        kw: Dict = {"layer": self._layer()}
        if self.driver:
            kw["driver"] = self.driver
        with self._staged(path) as _p:
            return pyogrio.read_info(_zip_vsi(_p), **kw)

    def _info(self):
        return self._info_for(self._members()[0])

    @staticmethod
    def _schema_key(info: Dict) -> tuple:
        """Comparable fingerprint of a member's attribute schema (fields + types).

        Only the attribute columns (name + OGR type) participate in the check —
        geometry/CRS metadata differs per file and is irrelevant for schema
        compatibility. Two members are schema-compatible iff their keys match."""
        fields = list(info.get("fields", []))
        ogr_types = list(info.get("ogr_types", []))
        return tuple(zip(fields, ogr_types))

    def schema(self) -> StructType:
        members = self._members()
        first_info = self._info_for(members[0])
        if len(members) > 1:
            first_key = self._schema_key(first_info)
            for other in members[1:]:
                other_info = self._info_for(other)
                if self._schema_key(other_info) != first_key:
                    stem_a = os.path.splitext(os.path.basename(members[0]))[0]
                    stem_b = os.path.splitext(os.path.basename(other))[0]
                    raise ValueError(
                        f"shapefile reader: shapefiles under {self.path} have "
                        f"differing schemas; load them separately or use a "
                        f"single-stem directory. Stems: {stem_a}, {stem_b}."
                    )
        return _vector_schema(first_info, self.as_wkb)

    def partitions(self) -> Sequence[InputPartition]:
        # ONE partition per member file, read whole (count=0 = all features). Splitting a
        # single file into feature-offset chunks is counterproductive for these formats:
        # OGR's GeoJSON driver re-parses the ENTIRE FeatureCollection into memory on every
        # open, so N chunks = N full parses (~O(features * chunks) -- this was the dominant
        # cost). Parallelism comes from reading many files concurrently (one task per file);
        # within a single read, chunk_size only bounds the Arrow batch size on the yield, not
        # the parse. Random-access formats (GPKG/FileGDB) are staged to local temp + read whole.
        #
        # Layout convention (T8): _members() returns paths sorted alphabetically so
        # partitions are emitted in source-path order. When a worker pulls consecutive
        # tasks from the same source file the per-worker staged-file cache (_VEC_STAGED_FILES)
        # is reused, paying the copy cost only once per source.
        # To control task parallelism or re-align after a join, use
        #   df.repartition(n, 'path').sortWithinPartitions('path')
        # on the read result; n is a parallelism signal (classic ≈ 3–5× cores),
        # never n = file_count.
        return [
            _ChunkPartition(
                member,
                self.driver,
                self._layer(),
                self.as_wkb,
                0,
                0,
                bbox=self.bbox,
                where=self.where,
            )
            for member in self._members()
        ]

    def read(self, partition: "_ChunkPartition"):
        """Arrow-native read: transform the pyogrio Arrow table in Arrow (rename the
        geometry column, vectorized WKB/WKT, constant srid/proj columns, cast to the
        declared StructType) and yield pyarrow.RecordBatch objects. No per-row Python
        tuple construction -- that per-row loop made large reads ~10x slower than the
        JVM reader.

        FILE access mode (``self.access``):
        - ``"auto"``: silently uses FUSE when FILE is unavailable; no error.
        - ``"managed"`` / ``"external"``: explicit FILE. The NO-GATING rule is enforced
          in ``__init__`` (driver-side) — not here. On Spark Connect workers (Serverless
          DBR 19+), ``SparkSession.getActiveSession()`` returns None so probing the tier
          here would always resolve to "fuse", giving a false ValueError for valid
          FILE-capable runtimes.
        """
        # NO-GATING validation was moved to __init__ (driver-side). read() runs on
        # executors and must not re-probe the capability tier.

        import numpy as np
        import pyarrow as pa
        import shapely
        from pyspark.sql.pandas.types import to_arrow_schema

        self._gdal_readonly_safe()
        import pyogrio

        kw: Dict = {
            "layer": partition.layer,
            "skip_features": partition.skip,
            "read_geometry": True,
            "datetime_as_string": False,
        }
        # One partition per file reads the whole member (count==0); max_features is left
        # unset so OGR parses the file once. (A non-zero count would cap features, but the
        # planner no longer splits a file into offset chunks -- see partitions().)
        if partition.count:
            kw["max_features"] = partition.count
        if partition.driver:
            kw["driver"] = partition.driver
        # Pushdown filters -> only matching features are parsed (Serverless memory).
        if partition.bbox:
            kw["bbox"] = partition.bbox
        if partition.where:
            kw["where"] = partition.where
        with self._staged(partition.path) as _p:
            meta, tbl = pyogrio.read_arrow(_zip_vsi(_p), **kw)

        # Arrow table uses 'wkb_geometry' when geometry_name is empty; the declared
        # schema names the geometry column geometry_name or 'geom_0' (see _geom_name).
        gcol = meta.get("geometry_name") or "wkb_geometry"
        out_gname = meta.get("geometry_name") or "geom_0"
        srid, proj4 = _crs_to_srid_proj(meta.get("crs"))
        n = tbl.num_rows

        # Build the declared output Spark schema from THIS table's field types (so we do
        # not re-open the file just to compute it): attrs preserved, geom typed by asWKB,
        # srid/proj string columns -- column NAMES/ORDER match _vector_schema/schema().
        attr_cols = [c for c in tbl.column_names if c != gcol]
        fields: List[StructField] = [
            StructField(c, _arrow_to_spark(tbl.schema.field(c).type), True)
            for c in attr_cols
        ]
        geom_spark = BinaryType() if partition.as_wkb else StringType()
        fields.append(StructField(out_gname, geom_spark, True))
        fields.append(StructField(out_gname + "_srid", StringType(), True))
        fields.append(StructField(out_gname + "_srid_proj", StringType(), True))
        target = to_arrow_schema(StructType(fields))

        if n == 0:
            for batch in target.empty_table().to_batches():
                yield batch
            return

        # Geometry column: keep WKB binary, or vectorized WKB->WKT (shapely 2.x).
        wkb_arr = tbl.column(gcol).combine_chunks()
        if partition.as_wkb:
            geom_out = wkb_arr.cast(pa.binary())
        else:
            geoms = shapely.from_wkb(wkb_arr.to_numpy(zero_copy_only=False))
            wkt = shapely.to_wkt(geoms)  # None -> None preserved
            geom_out = pa.array(np.asarray(wkt, dtype=object), type=pa.string())

        srid_out = pa.array([srid] * n, type=pa.string())
        proj_out = pa.array([proj4] * n, type=pa.string())

        out_cols = [tbl.column(c) for c in attr_cols] + [geom_out, srid_out, proj_out]
        out_names = attr_cols + [
            out_gname,
            out_gname + "_srid",
            out_gname + "_srid_proj",
        ]
        out_table = pa.Table.from_arrays(
            [
                c.combine_chunks() if isinstance(c, pa.ChunkedArray) else c
                for c in out_cols
            ],
            names=out_names,
        )
        # Cast to the declared schema's Arrow types for guaranteed PySpark alignment.
        out = out_table.cast(target, safe=False)
        # Yield in chunk_size-bounded batches: the file is parsed once (above); chunk_size
        # only bounds the per-batch row count handed to Spark (memory), not the parse.
        for batch in out.to_batches(max_chunksize=self.chunk_size):
            yield batch


class VectorGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "vector_gbx"

    _READER = VectorGbxReader

    def schema(self) -> StructType:
        return self._READER(self.options).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return self._READER(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("vector_gbx writer requires an output path (.save(path)).")
        return VectorGbxWriter(
            path, schema, self._READER._DRIVER, dict(self.options), overwrite
        )


class _ShapefileReader(VectorGbxReader):
    _DRIVER = "ESRI Shapefile"


class _GeoJSONReader(VectorGbxReader):
    _DRIVER = "GeoJSON"


class _GpkgReader(VectorGbxReader):
    _DRIVER = "GPKG"


class _FileGdbReader(VectorGbxReader):
    _DRIVER = "OpenFileGDB"


class ShapefileGbxDataSource(VectorGbxDataSource):
    _READER = _ShapefileReader

    @classmethod
    def name(cls) -> str:
        return "shapefile_gbx"


class GeoJSONGbxDataSource(VectorGbxDataSource):
    _READER = _GeoJSONReader

    @classmethod
    def name(cls) -> str:
        return "geojson_gbx"


class GpkgGbxDataSource(VectorGbxDataSource):
    _READER = _GpkgReader

    @classmethod
    def name(cls) -> str:
        return "gpkg_gbx"


class FileGdbGbxDataSource(VectorGbxDataSource):
    _READER = _FileGdbReader

    @classmethod
    def name(cls) -> str:
        return "file_gdb_gbx"


@dataclass
class _VectorCommitMessage(WriterCommitMessage):
    frag_path: str


class VectorGbxWriter(DataSourceWriter):
    """Two-phase vector writer: each partition -> one Arrow-IPC fragment in a
    shared-FS scratch dir; the driver merges fragments into one output file via
    pyogrio.write_arrow (first plain, rest append=True). Mirrors the PMTiles
    writer's executor-scratch / driver-merge shape."""

    def __init__(self, path, schema, driver, options, overwrite):
        from databricks.labs.gbx.ds._listing import to_local_path

        opts = {k.lower(): v for k, v in options.items()}

        # --- FILE-write options (parse before path normalization) ---
        # file_mode: "fuse" (default) | "managed" | "external"
        #   "fuse"     — write assembled file to a Volume FUSE path (default, byte-identical
        #                to existing behavior; self.path is a filesystem path).
        #   "managed"  — route through open_for_write(file_mode="managed"): assemble locally,
        #                read bytes, create a 1-row DataFrame {tile: {raster: <bytes>}}, call
        #                open_for_write; self.path is the target Delta TABLE name.
        #   "external" — route through open_for_write(file_mode="external"): assemble locally,
        #                copy to {filespace}/{uuid}/{basename} on the Volume, create a 1-row
        #                DataFrame {tile: {path: <volume_path>}}, call open_for_write;
        #                self.path is the target Delta TABLE name.
        # filespace: required for managed mode (/Volumes/…).
        #   For external mode, also used as the Volume directory where the assembled file is
        #   placed before try_to_file registers it; both modes share the same option so the
        #   caller only needs to specify one "where my files live" Volume.
        # layout: passed through to open_for_write ("order" | "cluster" | "plain").
        self._file_mode = opts.get("filemode", "fuse").lower()
        self._filespace = opts.get("filespace") or None
        self._layout = opts.get("layout", "order").lower()

        # Validate managed-without-filespace early (before any side effects).
        if self._file_mode == "managed" and not self._filespace:
            raise ValueError(
                "file_mode='managed' requires a 'filespace' option (/Volumes/…). "
                "Pass .option('filespace', '/Volumes/<catalog>/<schema>/<volume>') "
                "or use file_mode='external' with a filespace for the staging Volume."
            )

        if self._file_mode in ("managed", "external"):
            # FILE mode: path is a Delta TABLE name (e.g. "catalog.schema.table").
            # Do NOT apply to_local_path (it is a no-op here but _resolve_single_file_output
            # would wrongly append a geo extension like ".gpkg" to the table name).
            self.path = path
        else:
            # FUSE mode (default): normalize path (strip dbfs:/file: scheme).
            self.path = to_local_path(path)

        self.driver = opts.get("drivername", "") or driver
        if not self.driver:
            raise ValueError(
                "vector_gbx writer requires a 'driverName' option (e.g. 'GeoJSON')."
            )
        # zip=true produces a single zipped archive: <stem>.shp.zip for Shapefile,
        # <stem>.gdb.zip for FileGDB. Honored only for those directory/sidecar formats.
        self.zip = opts.get("zip", "false").lower() == "true" and self.driver in (
            "ESRI Shapefile",
            "OpenFileGDB",
        )
        self._file_name = opts.get(
            "filename"
        )  # .option("fileName", ...) (opts are lower-cased)

        if self._file_mode == "fuse":
            # FUSE mode: apply single-file output path resolution (extension appending).
            # Non-zip shapefile remains a directory bundle (existing behavior).
            if (
                self.driver in ("GPKG", "GeoJSON")
                or self.zip
                or self.driver == "OpenFileGDB"
            ):
                ext = _canonical_ext(self.driver, self.zip)
                self.path = _resolve_single_file_output(self.path, self._file_name, ext)
        else:
            # FILE mode: validate that the driver produces a single-file output.
            # Multi-file Shapefile (zip=false) is not supported — the single-file
            # constraint is required for try_to_file / create_file to receive one URI.
            _is_single = (
                self.driver in ("GPKG", "GeoJSON")
                or self.zip  # Shapefile+zip=true → .shp.zip; FileGDB+zip=true → .gdb.zip
            )
            if not _is_single:
                raise ValueError(
                    f"file_mode='{self._file_mode}' requires a single-file output. "
                    "Use a single-file driver (GPKG, GeoJSON) or set zip=true for "
                    "ESRI Shapefile (produces .shp.zip) or OpenFileGDB (produces .gdb.zip)."
                )

        self.overwrite = overwrite
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema,
            geom_col=opts.get("geomcol"),
            srid_col=opts.get("sridcol"),
            proj_col=opts.get("projcol"),
        )
        self._schema = schema
        self._col_order = [f.name for f in schema.fields]
        self._geom_is_wkb = any(
            f.name == self.geom_col and isinstance(f.dataType, BinaryType)
            for f in schema.fields
        )
        # Scratch dir for executor-written fragments. For FILE mode the "path" is a
        # table name, not a filesystem path; use "." so scratch lands in the working dir.
        parent = os.path.dirname(self.path) or "." if self._file_mode == "fuse" else "."
        # Per-write unique scratch dir under a hidden, self-GC'ing container: the
        # writer instance is created once on the driver and serialized to the
        # executors, so every task of THIS write shares one uuid while a
        # concurrent write to the same parent gets its own. A shared scratch name
        # would let one write's commit cleanup (rmtree) delete another's in-flight
        # fragments. The .gbx_scratch container is hidden from the recursive
        # reader; gc_stale_scratch reclaims (by age) scratch orphaned by a
        # hard-killed job whose commit/abort never ran. See ds/_scratch.py.
        self.scratch_dir = _scratch.new_scratch_dir(parent)
        _scratch.gc_stale_scratch(parent)
        _scratch.gc_stale_local_temp("gbx_vecout_")
        # Append-rejection: only applicable to FUSE mode (FILE mode delegates to open_for_write).
        if self._file_mode == "fuse" and not self.overwrite and self._target_exists():
            raise ValueError(
                "vector_gbx does not support append; use .mode('overwrite')."
            )

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    # ---- executor: partition rows -> one Arrow-IPC fragment ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import pyarrow.feather as feather
        from shapely import from_wkt, to_wkb

        idx = {n: i for i, n in enumerate(self._col_order)}
        cols: Dict[str, list] = {n: [] for n in self._col_order}
        for row in iterator:
            for n in self._col_order:
                v = row[idx[n]]
                if n == self.geom_col and v is not None and not self._geom_is_wkb:
                    v = to_wkb(from_wkt(v))  # WKT input -> WKB
                elif n == self.geom_col and v is not None:
                    v = bytes(v)
                cols[n].append(v)
        if not cols[self.geom_col]:
            return _VectorCommitMessage(frag_path="")  # empty partition
        os.makedirs(self.scratch_dir, exist_ok=True)
        tbl = _writer_arrow_table(cols, self._schema, self.geom_col)
        frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
        feather.write_feather(tbl, frag)
        return _VectorCommitMessage(frag_path=frag)

    # ---- driver: merge fragments into one output file ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        import pyarrow.feather as feather

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, _VectorCommitMessage) and m.frag_path
        ]
        local_dir = None
        try:
            if not frags:
                return
            # Write to driver-local disk first (supports random I/O for SQLite/
            # FileGDB/Shapefile sidecars), then copy to the Volume target with
            # sequential byte copies (FUSE-safe). Mirrors the PMTiles writer.
            local_dir = tempfile.mkdtemp(prefix="gbx_vecout_")
            if self._file_mode == "fuse":
                # FUSE mode: base local_out on the resolved FUSE path (existing behavior).
                local_out = os.path.join(
                    local_dir, os.path.basename(self.path.rstrip("/"))
                )
            else:
                # FILE mode: path is a table name; use a format-appropriate temp filename.
                local_out = os.path.join(
                    local_dir, "output" + _canonical_ext(self.driver, self.zip)
                )
            if self.zip:
                # zip=true: write the normal output to <stem>.shp / <stem>.gdb,
                # then pack it into <stem>.shp.zip / <stem>.gdb.zip; only the
                # archive is copied to the target. local_out ends in ".zip"; strip
                # it to get the inner path the writer produces alongside.
                inner = local_out[: -len(".zip")]  # .../roads.shp or .../roads.gdb
                if self.driver == "OpenFileGDB":
                    # FileGDB: stream fragments one at a time with OGR transaction
                    # batching. Infer geom/CRS from the first fragment only.
                    first_tbl = feather.read_table(frags[0])
                    geom_type, crs = self._infer_geom_crs([first_tbl])
                    del first_tbl
                    self._write_local_osgeo_gdb(frags, inner, geom_type, crs)
                elif _should_stream(self.driver):
                    first_tbl = feather.read_table(frags[0])
                    geom_type, crs = self._infer_geom_crs([first_tbl])
                    del first_tbl
                    self._write_streaming(frags, inner, geom_type, crs)
                else:
                    tables = [feather.read_table(f) for f in frags]
                    geom_type, crs = self._infer_geom_crs(tables)
                    self._write_local(tables, inner, geom_type, crs)
                if self.driver == "OpenFileGDB":
                    # .gdb is a directory: pack the whole tree, then drop it so only
                    # the .gdb.zip remains for the copy-to-Volume loop.
                    _zip_gdb_bundle(inner, local_out)
                    shutil.rmtree(inner, ignore_errors=True)
                else:
                    # Shapefile sidecars are flat files sharing the stem.
                    _zip_shapefile_bundle(inner, local_out)
                    stem_base = os.path.basename(os.path.splitext(inner)[0])  # "roads"
                    for name in list(os.listdir(local_dir)):
                        if name.startswith(stem_base + ".") and not name.endswith(
                            ".zip"
                        ):
                            os.remove(os.path.join(local_dir, name))
            elif self.driver == "OpenFileGDB":
                # FileGDB: stream fragments one at a time with OGR transaction
                # batching. Infer geom/CRS from the first fragment only (bounded;
                # the fragment is released before the write loop begins).
                first_tbl = feather.read_table(frags[0])
                geom_type, crs = self._infer_geom_crs([first_tbl])
                del first_tbl
                self._write_local_osgeo_gdb(frags, local_out, geom_type, crs)
            elif _should_stream(self.driver):
                # Infer geom type + CRS from the first fragment only (one
                # partition, bounded), then stream every fragment into one write.
                first_tbl = feather.read_table(frags[0])
                geom_type, crs = self._infer_geom_crs([first_tbl])
                del first_tbl
                self._write_streaming(frags, local_out, geom_type, crs)
            else:
                # Infer geom/CRS from first fragment only (bounded memory), then
                # stream remaining fragments via generator (lazy, not list).
                first_tbl = feather.read_table(frags[0])
                geom_type, crs = self._infer_geom_crs([first_tbl])
                del first_tbl
                tables_gen = (feather.read_table(f) for f in frags)
                self._write_local(tables_gen, local_out, geom_type, crs)
            if self._file_mode == "fuse":
                # FUSE mode: clear any Spark-created stub at self.path, then copy
                # everything pyogrio produced in local_dir (file, sidecar set, or
                # .gdb dir) to the Volume target. Byte-only copies (FUSE-safe).
                self._prepare_target()
                parent = os.path.dirname(self.path) or "."
                os.makedirs(parent, exist_ok=True)
                for name in os.listdir(local_dir):
                    if name.endswith(("-wal", "-shm", "-journal")):
                        continue  # transient SQLite sidecars -- never publish
                    src = os.path.join(local_dir, name)
                    dst = os.path.join(parent, name)
                    if os.path.isdir(src):
                        _copy_tree_to_fuse(src, dst)
                    else:
                        _copy_file_to_fuse(src, dst)  # byte-only -> Volume-safe
            else:
                # FILE mode: route the assembled single-file output through open_for_write
                # (MANAGED create_file / EXTERNAL try_to_file) rather than a plain FUSE copy.
                # self.path is the target Delta TABLE name.
                self._commit_as_file(spark=None, local_out=local_out)
        finally:
            if local_dir is not None:
                shutil.rmtree(local_dir, ignore_errors=True)
            _scratch.remove_scratch_dir(self.scratch_dir)

    def _commit_as_file(self, spark, local_out: str) -> None:
        """Route the assembled local_out through open_for_write (MANAGED or EXTERNAL).

        Called from commit() when self._file_mode is 'managed' or 'external'.
        self.path is the target Delta TABLE name.

        managed path: reads the assembled file's bytes from local_out, builds a
        1-row DataFrame {tile: {raster: <bytes>, path: None}}, calls
        open_for_write(file_mode="managed", filespace=self._filespace).
        The create_file SQL expression mints the bytes into managed FILE storage.

        external path: copies local_out to {filespace}/{uuid4}/{basename} on the
        Volume (FUSE-safe sequential copy), then builds a 1-row DataFrame
        {tile: {raster: None, path: <volume_path>}}, calls
        open_for_write(file_mode="external").
        The try_to_file SQL expression registers the Volume path as a FILE reference.

        No-gating: if FILE is unavailable on this runtime (fuse tier), open_for_write
        delegates to resolve_access which raises a clear, actionable error. This method
        does NOT hand-roll a second error message.

        Connect-safety: no sparkContext / _sc / _jvm / _jsc; no spark.conf.set/get.
        spark.createDataFrame is driver-only and Connect-safe.
        """
        from pyspark.sql import SparkSession

        if spark is None:
            spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        # 1-row DataFrame schema: tile struct with path (STRING) and raster (BINARY).
        # write_file_table derives plain_cols from all tile fields except raster/path_mode,
        # so the table will have a `path` column plus tile_file FILE MANAGED/EXTERNAL.
        _tile_struct_schema = StructType(
            [
                StructField(
                    "tile",
                    StructType(
                        [
                            StructField("path", StringType()),
                            StructField("raster", BinaryType()),
                        ]
                    ),
                )
            ]
        )

        if self._file_mode == "managed":
            # Read assembled file bytes (single file at local_out).
            with open(local_out, "rb") as fh:
                file_bytes = fh.read()
            rows = [{"tile": {"path": None, "raster": bytearray(file_bytes)}}]
            df = spark.createDataFrame(rows, _tile_struct_schema)
            open_for_write(
                spark,
                df,
                self.path,
                file_mode="managed",
                filespace=self._filespace,
                layout=self._layout,
                overwrite=self.overwrite,
            )
        else:
            # external: copy file to Volume, then register via try_to_file.
            from databricks.labs.gbx.ds._listing import to_local_path

            if not self._filespace:
                raise ValueError(
                    "file_mode='external' requires a 'filespace' option (/Volumes/…) "
                    "specifying the Volume directory where the assembled file is placed "
                    "before registering it as a FILE reference via try_to_file. "
                    "Pass .option('filespace', '/Volumes/<catalog>/<schema>/<volume>')."
                )
            volume_base = to_local_path(self._filespace)
            staging_dir = os.path.join(volume_base, uuid.uuid4().hex)
            os.makedirs(staging_dir, exist_ok=True)
            volume_path = os.path.join(staging_dir, os.path.basename(local_out))
            _copy_file_to_fuse(local_out, volume_path)
            rows = [{"tile": {"path": volume_path, "raster": None}}]
            df = spark.createDataFrame(rows, _tile_struct_schema)
            open_for_write(
                spark,
                df,
                self.path,
                file_mode="external",
                filespace=None,
                layout=self._layout,
                overwrite=self.overwrite,
            )

    def _drop_meta_cols(self, tbl):
        return tbl.drop_columns(
            [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
        )

    def _write_streaming(self, frags, local_out, geom_type, crs) -> None:
        """Assemble the output by streaming the fragment batches into ONE
        write_arrow (bounded driver memory, single pass) for GeoJSON / Shapefile /
        GPKG. The geometry column is written under the format default
        (``_output_geom_name``); for GPKG that differs from the input name, so the
        column is renamed per batch (GeoJSON/Shapefile geometry is structural, no
        rename). Falls back to a concat write only if the GDAL build cannot consume
        a RecordBatchReader."""
        import pyarrow as pa
        import pyarrow.feather as feather
        import pyarrow.ipc as ipc
        import pyogrio

        drop = {c for c in (self.srid_col, self.proj_col) if c}
        out_geom = _output_geom_name(self.driver, self.geom_col)
        rename = None if out_geom == self.geom_col else (self.geom_col, out_geom)
        with pa.memory_map(frags[0], "r") as src0:
            full_schema = ipc.open_file(src0).schema
        fields = [
            (f.with_name(out_geom) if rename and f.name == self.geom_col else f)
            for f in full_schema
            if f.name not in drop
        ]
        stream_schema = pa.schema(fields)
        # GPKG: DELETE journal (no WAL/-shm sidecars) so the output reads back from
        # read-only object storage; harmless for non-SQLite drivers.
        pyogrio.set_gdal_config_options({"OGR_SQLITE_JOURNAL": "DELETE"})
        kw = dict(
            driver=self.driver,
            geometry_name=out_geom,
            geometry_type=geom_type,
            crs=crs,
        )
        if self.layer_name:
            kw["layer"] = self.layer_name
        elif self.driver == "GPKG":
            kw["layer"] = _default_gpkg_layer(self.path)
        try:
            reader = pa.RecordBatchReader.from_batches(
                stream_schema, _stream_record_batches(frags, drop, rename)
            )
            pyogrio.write_arrow(reader, local_out, **kw)
        except Exception as e:  # noqa: BLE001
            # Only fall back if the GDAL build can't consume a streaming reader;
            # re-raise genuine write errors.
            if "does not support" not in str(e) and not isinstance(e, TypeError):
                raise
            # Fall back to _write_local with lazy generator (bounded memory).
            tables_gen = (feather.read_table(f) for f in frags)
            self._write_local(tables_gen, local_out, geom_type, crs)

    def _write_local(self, tables, local_out, geom_type, crs) -> None:
        """Write the merged tables to a local path. Use the fast Arrow path; if the
        driver lacks Arrow-write support, fall back to the classic feature-based path.
        OpenFileGDB is NOT routed here; the commit() method calls _write_local_osgeo_gdb
        directly with fragment paths (streaming + transaction batching).

        Accepts tables as an iterable (list or generator). For generators, the first
        table is read explicitly to detect Arrow-write support, then remaining tables
        are streamed to avoid whole-dataset materialization (bounded memory)."""
        import pyogrio

        # Convert to iterator so we can pull the first table explicitly
        it = iter(tables)
        try:
            first_tbl = next(it)
        except StopIteration:
            return  # no tables, nothing to write

        # Write SQLite-backed formats (GPKG) with a DELETE journal -- no WAL/-shm
        # sidecars -- so the output reads back from read-only object storage (a
        # Volume) without the reader attempting a checkpoint (write). Harmless for
        # non-SQLite drivers. (GDAL config is process-global; set per write.)
        pyogrio.set_gdal_config_options({"OGR_SQLITE_JOURNAL": "DELETE"})
        kw = dict(
            driver=self.driver,
            geometry_name=_output_geom_name(self.driver, self.geom_col),
            geometry_type=geom_type,
            crs=crs,
        )
        if self.layer_name:
            kw["layer"] = self.layer_name
        elif self.driver == "GPKG":
            # GDAL would name the GPKG layer after the file stem, which is invalid
            # when it starts with the reserved 'gpkg' prefix; use a safe default.
            kw["layer"] = _default_gpkg_layer(self.path)
        out_geom = _output_geom_name(self.driver, self.geom_col)

        try:
            # Write the first table to test Arrow-write support; if this raises
            # "does not support write functionality", catch it and fall back to classic.
            self._write_one_arrow(first_tbl, local_out, out_geom, kw, append=False)
            # Arrow write succeeded; stream remaining tables
            for n, tbl in enumerate(it, start=1):
                self._write_one_arrow(tbl, local_out, out_geom, kw, append=True)
        except Exception as e:  # noqa: BLE001
            if "does not support write functionality" not in str(e):
                raise
            # Arrow-write path unsupported for this driver -> classic path. Start
            # from a clean local_out so a partial Arrow attempt doesn't corrupt it.
            if os.path.isdir(local_out):
                shutil.rmtree(local_out, ignore_errors=True)
            elif os.path.isfile(local_out):
                os.remove(local_out)
            # Chain first table + remaining iterator LAZILY. 'it' is pristine here:
            # "does not support" only fires on the first write (before the loop below
            # consumes it), so the classic fallback stays bounded (one fragment at a
            # time) instead of materializing every fragment via list(it).
            from itertools import chain as _chain

            all_tables = _chain((first_tbl,), it)
            self._write_local_classic(all_tables, local_out, geom_type, crs)

    def _write_one_arrow(self, tbl, local_out, out_geom, kw, append):
        """Write one table via the Arrow path, handling geom column rename."""
        import pyogrio

        t = self._drop_meta_cols(tbl)
        # Rename the input geom column to the format-canonical output name
        # (e.g. GPKG: geom_0 -> geom) so pyogrio can find the geometry by
        # the name passed as geometry_name=out_geom.
        if out_geom != self.geom_col and self.geom_col in t.column_names:
            t = t.rename_columns(
                [out_geom if c == self.geom_col else c for c in t.column_names]
            )
        pyogrio.write_arrow(t, local_out, append=append, **kw)

    def _write_local_osgeo_gdb(  # noqa: C901
        self, frags, local_out, geom_type, crs
    ) -> None:
        """Hybrid FileGDB path. pyogrio's bundled GDAL has a read-only OpenFileGDB
        driver; the native GDAL from the heavyweight GDAL init script has write. Use
        osgeo.ogr (native) to encode the .gdb. Requires those natives -- raises a clear
        error otherwise (FileGDB write is unavailable in a lightweight-only env).

        Fragments are consumed ONE AT A TIME (bounded driver memory — never the
        whole dataset in RAM) and features are written inside OGR bulk transactions
        (committed every _GDB_TX_BATCH rows), which eliminates the per-feature
        auto-commit overhead that otherwise makes large writes O(n) slow."""
        try:
            from osgeo import ogr, osr
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "file_gdb_gbx writing requires the native GDAL Python bindings (osgeo) "
                "from the heavyweight GDAL init script; pyogrio's bundled GDAL ships a "
                "read-only OpenFileGDB driver. Install the GeoBrix GDAL natives, or write "
                "gpkg_gbx / geojson_gbx instead."
            ) from e
        import pyarrow as pa
        import pyarrow.feather as feather

        drv = ogr.GetDriverByName("OpenFileGDB")
        if drv is None or not drv.TestCapability(ogr.ODrCCreateDataSource):
            raise RuntimeError(
                "native GDAL OpenFileGDB driver lacks create capability; FileGDB write "
                "needs GDAL >= 3.6 with OpenFileGDB write (the heavyweight GDAL natives)."
            )
        _WKB = {
            "Point": ogr.wkbPoint,
            "LineString": ogr.wkbLineString,
            "Polygon": ogr.wkbPolygon,
            "MultiPoint": ogr.wkbMultiPoint,
            "MultiLineString": ogr.wkbMultiLineString,
            "MultiPolygon": ogr.wkbMultiPolygon,
            "GeometryCollection": ogr.wkbGeometryCollection,
        }
        srs = None
        if crs:
            srs = osr.SpatialReference()
            if str(crs).upper().startswith("EPSG:"):
                srs.ImportFromEPSG(int(str(crs).split(":")[1]))
            else:
                srs.ImportFromProj4(str(crs))

        def _ogr_type(t):
            if pa.types.is_floating(t):
                return ogr.OFTReal
            if pa.types.is_boolean(t):
                return ogr.OFTInteger
            if pa.types.is_integer(t):
                return ogr.OFTInteger64
            if pa.types.is_binary(t) or pa.types.is_large_binary(t):
                return ogr.OFTBinary
            return ogr.OFTString  # strings + anything else

        # Read the first fragment to derive the schema (attr cols + field types).
        # The fragment is released from memory before the main write loop so that
        # only one fragment at a time lives in driver memory.
        first_tbl = feather.read_table(frags[0])
        meta = {self.geom_col, self.srid_col, self.proj_col}
        attr_cols = [c for c in first_tbl.column_names if c not in meta]
        types = {f.name: f.type for f in first_tbl.schema}
        del first_tbl  # release — schema is all we need

        ds = drv.CreateDataSource(local_out)
        if ds is None:
            raise RuntimeError(
                f"OpenFileGDB CreateDataSource returned None for {local_out!r}; "
                "the FileGDB output path must end in '.gdb'."
            )
        use_tx = lyr = None
        try:
            lyr = ds.CreateLayer(
                self.layer_name or "layer",
                srs,
                _WKB.get(geom_type, ogr.wkbUnknown),
                options=[
                    f"GEOMETRY_NAME={_output_geom_name(self.driver, self.geom_col)}"
                ],
            )
            for c in attr_cols:
                lyr.CreateField(ogr.FieldDefn(c, _ogr_type(types[c])))
            defn = lyr.GetLayerDefn()

            # Check once whether the driver supports OGR transactions so we can
            # fall back gracefully (per-feature auto-commit) on drivers that don't.
            use_tx = lyr.TestCapability(ogr.OLCTransactions)
            if use_tx:
                lyr.StartTransaction()
            pending = 0  # features written since last commit

            # Stream fragments one at a time — each is loaded, written, then
            # released before the next is read (bounded driver memory).
            for frag_path in frags:
                tbl = feather.read_table(frag_path)
                cols = {c: tbl.column(c).to_pylist() for c in tbl.column_names}
                geom = cols.get(self.geom_col, [None] * tbl.num_rows)
                for i in range(tbl.num_rows):
                    feat = ogr.Feature(defn)
                    for c in attr_cols:
                        v = cols[c][i]
                        if v is not None:
                            feat.SetField(c, v)
                    g = geom[i]
                    if g is not None:
                        feat.SetGeometry(ogr.CreateGeometryFromWkb(bytes(g)))
                    lyr.CreateFeature(feat)
                    feat = None
                    pending += 1
                    if use_tx and pending >= _GDB_TX_BATCH:
                        lyr.CommitTransaction()
                        lyr.StartTransaction()
                        pending = 0
                del tbl, cols, geom  # release fragment memory before next read

            if use_tx and pending > 0:
                lyr.CommitTransaction()
        except Exception:
            if use_tx and lyr is not None:
                try:
                    lyr.RollbackTransaction()
                except Exception:  # noqa: BLE001
                    pass
            raise
        finally:
            ds = None  # flush + close

    def _write_local_classic(self, tables, local_out, geom_type, crs) -> None:
        """Write tables via the classic (non-Arrow) OGR path. Accepts an iterable
        (list or generator) and streams one table at a time (bounded memory)."""
        import numpy as np
        import pyogrio.raw

        kw = dict(driver=self.driver, geometry_type=geom_type, crs=crs)
        if self.layer_name:
            kw["layer"] = self.layer_name
        meta = {self.geom_col, self.srid_col, self.proj_col}
        for n, tbl in enumerate(tables):
            attr_cols = [c for c in tbl.column_names if c not in meta]
            geometry = np.array(tbl.column(self.geom_col).to_pylist(), dtype=object)
            field_data = [np.array(tbl.column(c).to_pylist()) for c in attr_cols]
            pyogrio.raw.write(
                local_out,
                geometry=geometry,
                field_data=field_data,
                fields=np.array(attr_cols, dtype=object),
                append=(n > 0),
                **kw,
            )

    def _infer_geom_crs(self, tables) -> Tuple[str, Optional[str]]:
        geom_type, crs = self.geometry_type_override, None
        for tbl in tables:
            g = tbl.column(self.geom_col).to_pylist()
            s = (
                tbl.column(self.srid_col).to_pylist()
                if self.srid_col in tbl.column_names
                else []
            )
            p = (
                tbl.column(self.proj_col).to_pylist()
                if self.proj_col in tbl.column_names
                else []
            )
            for i, gv in enumerate(g):
                if gv is None:
                    continue
                if geom_type is None:
                    geom_type = _geometry_type_of(gv)
                if crs is None:
                    crs = _srid_to_crs(
                        s[i] if i < len(s) else "", p[i] if i < len(p) else ""
                    )
                break
            if geom_type is not None and crs is not None:
                break
        return geom_type or "Unknown", crs

    def _prepare_target(self) -> None:
        # PySpark may pre-create self.path as a directory; vector output is a
        # single file (or driver-managed dir). Clear it and write directly --
        # no os.rename (FUSE-unsafe on DBFS/Volumes); write_arrow writes
        # sequentially so a direct write to a FUSE path is safe.
        parent = os.path.dirname(self.path) or "."
        os.makedirs(parent, exist_ok=True)
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        elif os.path.isfile(self.path):
            os.remove(self.path)

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        _scratch.remove_scratch_dir(self.scratch_dir)
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=True)


@dataclass
class _GeoJSONLCommitMessage(WriterCommitMessage):
    shard_paths: Tuple[str, ...]


class GeoJSONLGbxWriter(DataSourceWriter):
    """Multi-file GeoJSONL writer: each partition writes one (or, with
    maxRecordsPerFile, several) newline-delimited GeoJSONL shard(s) directly into
    the output DIRECTORY -- NO driver merge. The natural shape for JSONL: shards
    are splittable + concatenable, so write throughput scales with partitions
    instead of bottlenecking on a single-node assembly. Contrast geojson_gbx,
    which merges all partitions into one FeatureCollection file on the driver.

    Each shard is encoded to worker-local temp via pyogrio (driver GeoJSONSeq),
    then sequentially copied to OUTDIR/part-<uuid>.geojsonl (FUSE-safe on Volumes).
    overwrite clears the target directory once before the executors write; append
    is rejected (matching the other vector writers for v0.4.0)."""

    _DRIVER = "GeoJSONSeq"
    _EXT = ".geojsonl"

    def __init__(self, path, schema, options, overwrite):
        from databricks.labs.gbx.ds._listing import to_local_path

        opts = {k.lower(): v for k, v in options.items()}
        # Strip a dbfs:/file: scheme so all os.* writes hit the bare FUSE path.
        self.path = to_local_path(path)
        self.overwrite = overwrite
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        mrpf = opts.get("maxrecordsperfile")
        self.max_records_per_file = int(mrpf) if mrpf else 0
        if self.max_records_per_file < 0:
            raise ValueError("maxRecordsPerFile must be a non-negative integer.")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema,
            geom_col=opts.get("geomcol"),
            srid_col=opts.get("sridcol"),
            proj_col=opts.get("projcol"),
        )
        self._schema = schema
        self._col_order = [f.name for f in schema.fields]
        self._geom_is_wkb = any(
            f.name == self.geom_col and isinstance(f.dataType, BinaryType)
            for f in schema.fields
        )
        if not self.overwrite and self._target_exists():
            raise ValueError(
                "geojsonl_gbx does not support append; use .mode('overwrite')."
            )
        # Clear the target directory ONCE, on the driver, before executors write
        # (the writer is constructed once on the driver). Sequential rmtree -- no
        # rename -- so it is FUSE-safe on DBFS/Volumes.
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, exist_ok=True)

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    def _drop_meta_cols(self, tbl):
        return tbl.drop_columns(
            [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
        )

    def _infer_geom_crs(self, tbl) -> Tuple[str, Optional[str]]:
        geom_type, crs = self.geometry_type_override, None
        g = tbl.column(self.geom_col).to_pylist()
        s = (
            tbl.column(self.srid_col).to_pylist()
            if self.srid_col in tbl.column_names
            else []
        )
        p = (
            tbl.column(self.proj_col).to_pylist()
            if self.proj_col in tbl.column_names
            else []
        )
        for i, gv in enumerate(g):
            if gv is None:
                continue
            if geom_type is None:
                geom_type = _geometry_type_of(gv)
            if crs is None:
                crs = _srid_to_crs(
                    s[i] if i < len(s) else "", p[i] if i < len(p) else ""
                )
            break
        return geom_type or "Unknown", crs

    # ---- executor: partition rows -> one or more GeoJSONL shards in OUTDIR ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import pyogrio
        from shapely import from_wkt, to_wkb

        idx = {n: i for i, n in enumerate(self._col_order)}
        cols: Dict[str, list] = {n: [] for n in self._col_order}
        for row in iterator:
            for n in self._col_order:
                v = row[idx[n]]
                if n == self.geom_col and v is not None and not self._geom_is_wkb:
                    v = to_wkb(from_wkt(v))  # WKT input -> WKB
                elif n == self.geom_col and v is not None:
                    v = bytes(v)
                cols[n].append(v)
        nrows = len(cols[self.geom_col])
        if nrows == 0:
            return _GeoJSONLCommitMessage(shard_paths=())  # empty partition -> nothing

        tbl = _writer_arrow_table(cols, self._schema, self.geom_col)
        geom_type, crs = self._infer_geom_crs(tbl)
        chunk = self.max_records_per_file or nrows  # split into N-row shards if set
        bounds = list(range(0, nrows, chunk))

        os.makedirs(self.path, exist_ok=True)
        local_dir = tempfile.mkdtemp(prefix="gbx_geojsonl_")
        written: List[str] = []
        try:
            for start in bounds:
                slice_tbl = self._drop_meta_cols(tbl.slice(start, chunk))
                # Per-shard uuid -> unique across tasks AND across chunks of one task.
                name = f"part-{uuid.uuid4().hex}{self._EXT}"
                local_path = os.path.join(local_dir, name)
                kw = dict(
                    driver=self._DRIVER,
                    geometry_name=self.geom_col,
                    geometry_type=geom_type,
                    crs=crs,
                )
                if self.layer_name:
                    kw["layer"] = self.layer_name
                pyogrio.write_arrow(slice_tbl, local_path, **kw)
                dst = os.path.join(self.path, name)
                _copy_file_to_fuse(local_path, dst)  # byte-only -> Volume-safe
                written.append(dst)
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
        return _GeoJSONLCommitMessage(shard_paths=tuple(written))

    # ---- driver: NO merge; just finalize (optional _SUCCESS marker) ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        os.makedirs(self.path, exist_ok=True)
        try:
            with open(os.path.join(self.path, "_SUCCESS"), "w") as fh:
                fh.write("")
        except OSError:
            pass  # marker is advisory; never fail the commit on it

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        # Best-effort cleanup of any shards that did land.
        for m in messages:
            if isinstance(m, _GeoJSONLCommitMessage):
                for shard in m.shard_paths:
                    with contextlib.suppress(OSError):
                        if os.path.isfile(shard):
                            os.remove(shard)


class GeoJSONLGbxDataSource(DataSource):
    """`geojsonl_gbx` -- multi-file GeoJSONL writer (directory of shards). It also
    reads back via the geojson_gbx reader with option('multi','true')."""

    @classmethod
    def name(cls) -> str:
        return "geojsonl_gbx"

    def schema(self) -> StructType:
        # Reading is done via geojson_gbx(multi=true); expose the GeoJSONSeq reader
        # for symmetry so .load() on a shard directory works through this name too.
        opts = dict(self.options)
        opts["driverName"] = opts.get("driverName", "") or "GeoJSONSeq"
        return VectorGbxReader(opts).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        opts = dict(self.options)
        opts["driverName"] = opts.get("driverName", "") or "GeoJSONSeq"
        return VectorGbxReader(opts)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError(
                "geojsonl_gbx writer requires an output path (.save(path))."
            )
        return GeoJSONLGbxWriter(path, schema, dict(self.options), overwrite)
