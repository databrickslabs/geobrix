"""The one native-payload oracle: the same inputs and answers at every level (probes P8, P9; B3).

- ``stack()``: which build each binding runs (GDAL's own ``osgeo`` and ``osgeo_utils`` too),
  whether it came from the payload, and the libgdal files the process maps (one: the payload's).
- ``light()``: raster, vector, CRS, geometry and netCDF checks in this process, no Spark.
- ``light_sql(spark)`` / ``heavy_sql(spark, scratch_dir)``: the same GeoTIFF through the light and
  the heavy SQL functions; heavy also reads a Shapefile and checks the driver JVM's GDAL.
- ``install_paths(spark)``: where the driver and each executor import geobrix from.
- ``payload_size_mb()``, ``installed_closure_mb()``: sizes on disk, in MB (10^6 bytes).

Every function imports what it needs when it runs, so importing this module costs nothing (the UC
sandbox imports it in a function body). The registration calls live only in ``_light_register``
and ``_heavy_register``.
"""

import json
import math
import os
import platform
import re
import sys
import tempfile

WIDTH, HEIGHT = 388, 385  # uint16, values (r * WIDTH + c) % 65535
EPSG = 32630
ORIGIN = (699960.0, 5700000.0)
RES = 10.0
PIXEL_SUM = 4462389585
POINTS_27700 = [(530000.0, 180000.0), (400000.0, 300000.0), (325000.0, 675000.0)]
BINDINGS = ("rasterio", "pyogrio", "pyproj", "shapely", "netCDF4")
GDAL_STACK_LIB = re.compile(
    r"/lib(gdal|proj|geos_c|netcdf|hdf5)(-[0-9a-f]{8})?\.so[.\d]*$"
)
LIBGDAL = re.compile(r"/libgdal(-[0-9a-f]{8})?\.so[.\d]*$")


def _light_register(spark):
    from databricks.labs.gbx.pyrx import functions

    functions.register(spark)


def _heavy_register(spark):
    from databricks.labs.gbx.rasterx import functions

    functions.register(spark)


def _under(path, root):
    return bool(path and root) and os.path.realpath(path).startswith(
        os.path.realpath(root) + os.sep
    )


def _mapped_gdal_stack():
    """The GDAL/PROJ/GEOS/netCDF/HDF5 libraries this process has mapped (Linux), else None."""
    try:
        with open("/proc/self/maps") as fh:
            paths = {ln.split(None, 5)[-1].strip() for ln in fh if "/" in ln}
    except OSError:
        return None
    return sorted(p for p in paths if GDAL_STACK_LIB.search(p))


def stack():
    import netCDF4
    import pyogrio
    import pyproj
    import rasterio
    import shapely

    from databricks.labs.gbx import _native, _osgeo

    osgeo = _osgeo.bindings("_selfcheck.stack")
    gdal_calc = _osgeo.utils("gdal_calc", "_selfcheck.stack")
    a = _native.arch()
    root = _native.payload_root()
    vendor = os.path.join(root, "_vendor", a) if a else None
    mods = dict(zip(BINDINGS, (rasterio, pyogrio, pyproj, shapely, netCDF4)))
    mods["osgeo"] = osgeo.gdal
    mapped = _mapped_gdal_stack()
    native = _native.native_dir()
    return {
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "payload_root": root,
        "native_dir": native,
        "activated_by": _native.activated_by(),
        "rasterio_gdal": rasterio.__gdal_version__,
        "osgeo_gdal": osgeo.gdal.VersionInfo("RELEASE_NAME"),
        "pyogrio_gdal": pyogrio.__gdal_version_string__,
        "pyproj_proj": pyproj.proj_version_str,
        "shapely_geos": shapely.geos_version_string,
        "netcdf": netCDF4.__netcdf4libversion__,
        "hdf5": netCDF4.__hdf5libversion__,
        "from_payload": {
            **{n: _under(m.__file__, vendor) for n, m in mods.items()},
            "osgeo_utils": _under(
                gdal_calc.__file__, os.path.join(root, "_vendor", "common")
            ),
        },
        # every libgdal mapped after rasterio and osgeo imported (None off Linux): one is one GDAL
        "libgdal": None if mapped is None else [p for p in mapped if LIBGDAL.search(p)],
        # mapped GDAL-stack libraries that are NOT the payload's (None off Linux): [] is one stack
        "foreign_libs": (
            None if mapped is None else [p for p in mapped if not _under(p, native)]
        ),
    }


def geotiff_bytes():
    """The oracle raster as GeoTIFF bytes."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    r = np.arange(HEIGHT, dtype=np.int64)[:, None]
    c = np.arange(WIDTH, dtype=np.int64)[None, :]
    data = ((r * WIDTH + c) % 65535).astype("uint16")
    profile = dict(
        driver="GTiff",
        width=WIDTH,
        height=HEIGHT,
        count=1,
        dtype="uint16",
        crs=f"EPSG:{EPSG}",
    )
    with MemoryFile() as mf:
        with mf.open(transform=from_origin(*ORIGIN, RES, RES), **profile) as ds:
            ds.write(data, 1)
        return bytes(mf.getbuffer())


def _write_points(path, driver):
    import numpy as np
    import pyogrio.raw
    import shapely

    wkb = np.array(shapely.to_wkb(shapely.points(POINTS_27700)), dtype=object)
    ids = np.arange(1, len(POINTS_27700) + 1, dtype="int32")
    pyogrio.raw.write(
        path, wkb, [ids], ["id"], driver=driver, geometry_type="Point", crs="EPSG:27700"
    )


def _raster():
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import authority_srid_of

    with MemoryFile(geotiff_bytes()) as mf, mf.open() as ds:
        got = {
            "width": ds.width,
            "height": ds.height,
            "srid": authority_srid_of(ds.crs),
            "sum": int(ds.read(1).astype(np.int64).sum()),
        }
    return (
        got == {"width": WIDTH, "height": HEIGHT, "srid": EPSG, "sum": PIXEL_SUM},
        got,
    )


def _vector():
    import pyogrio.raw

    with tempfile.TemporaryDirectory(prefix="gbx-selfcheck-") as d:
        path = os.path.join(d, "points.gpkg")
        _write_points(path, "GPKG")
        meta, _fids, geometry, _fields = pyogrio.raw.read(path)
    got = {"features": len(geometry), "crs": meta.get("crs")}
    return got == {"features": 3, "crs": "EPSG:27700"}, got


def _transform():
    from databricks.labs.gbx.core.crs import get_transformer

    x, y = POINTS_27700[0]
    lon, lat = get_transformer(27700, 4326).transform(x, y)
    x2, y2 = get_transformer(4326, 27700).transform(lon, lat)
    err = math.hypot(x2 - x, y2 - y)
    # 1 cm, not 1 mm: PROJ inverts the OSGB36 -> WGS84 Helmert approximately (its scale as 1 - s,
    # s = -20.49 ppm), which leaves 0.8-1.0 mm on a round trip of these points with no grid.
    return err < 1e-2, {
        "lon": round(lon, 7),
        "lat": round(lat, 7),
        "round_trip_m": round(err, 6),
    }


def _geometry():
    import shapely

    area = shapely.Point(0, 0).buffer(1, quad_segs=64).area
    return abs(area - math.pi) < 1e-3, {"area": area}


def _netcdf():
    import netCDF4
    import numpy as np

    nc = netCDF4.Dataset("gbx-selfcheck.nc", "w", memory=4096, format="NETCDF4")
    nc.createDimension("x", 4)
    nc.createVariable("v", "f8", ("x",))[:] = np.arange(4.0)
    buf = nc.close()
    with netCDF4.Dataset("gbx-selfcheck.nc", "r", memory=bytes(buf)) as back:
        values = back["v"][:].tolist()
    return values == [0.0, 1.0, 2.0, 3.0], {"values": values}


def _run(fn):
    try:
        ok, got = fn()
        return {"ok": bool(ok), **got}
    except Exception as exc:  # noqa: BLE001 - the oracle reports, it does not raise
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def light():
    checks = {
        "raster": _run(_raster),
        "vector": _run(_vector),
        "transform": _run(_transform),
        "geometry": _run(_geometry),
        "netcdf": _run(_netcdf),
    }
    return {"ok": all(c["ok"] for c in checks.values()), "checks": checks}


_SIZE_SQL = (
    "SELECT gbx_rst_width(gbx_rst_fromcontent(content, 'GTiff')) AS w, "
    "gbx_rst_height(gbx_rst_fromcontent(content, 'GTiff')) AS h FROM {view}"
)


def _size_sql(spark, df, view):
    df.select("content").createOrReplaceTempView(view)
    row = spark.sql(_SIZE_SQL.format(view=view)).first()
    return {"ok": (row.w, row.h) == (WIDTH, HEIGHT), "width": row.w, "height": row.h}


def light_sql(spark):
    """The light functions over the oracle GeoTIFF's bytes: (388, 385)."""
    _light_register(spark)
    return _size_sql(
        spark,
        spark.createDataFrame([(geotiff_bytes(),)], "content binary"),
        "gbx_selfcheck_light",
    )


def _pinned_gdal():
    try:
        from databricks.labs.gbx import _build_info

        return _build_info.GDAL_VERSION
    except ImportError:
        return None


def _jvm_gdal(spark):
    """The driver JVM's GDAL against the pin (dedicated compute only). Asked only when the JVM has
    already mapped the JNI: touching org.gdal first would make it load one from java.library.path.
    """
    want = _pinned_gdal()
    try:
        jvm = spark._jvm
        pid = jvm.java.lang.ProcessHandle.current().pid()
        with open(f"/proc/{pid}/maps") as fh:
            jni = sorted(
                {
                    ln.split(None, 5)[-1].strip()
                    for ln in fh
                    if ln.rstrip().endswith("/libgdalalljni.so")
                }
            )
        if not jni:
            return {
                "ok": True,
                "checked": False,
                "reason": "the driver JVM has not loaded GDAL",
            }
        got = jvm.org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME")
    except (
        Exception
    ) as exc:  # noqa: BLE001 - Spark Connect, another host's JVM, no /proc
        return {"ok": True, "checked": False, "reason": f"{type(exc).__name__}: {exc}"}
    return {
        "ok": want is None or got == want,
        "checked": True,
        "gdal": got,
        "pin": want,
        "jni": jni,
    }


def heavy_sql(spark, scratch_dir):
    """The heavy functions over the oracle GeoTIFF (binaryFile) and Shapefile (shapefile_ogr)."""
    os.makedirs(scratch_dir, exist_ok=True)
    tif = os.path.join(scratch_dir, "gbx-selfcheck.tif")
    with open(tif, "wb") as fh:
        fh.write(geotiff_bytes())
    shp_dir = os.path.join(scratch_dir, "gbx-selfcheck-points")
    os.makedirs(shp_dir, exist_ok=True)
    shp = os.path.join(shp_dir, "points.shp")
    if not os.path.exists(shp):
        _write_points(shp, "ESRI Shapefile")
    _heavy_register(spark)
    size = _size_sql(
        spark, spark.read.format("binaryFile").load(tif), "gbx_selfcheck_heavy"
    )
    count = spark.read.format("shapefile_ogr").load(shp_dir).count()
    jvm = _jvm_gdal(spark)
    calc = '{"calc": "A + 1", "A_index": 0, "A_band": 1}'
    try:  # recorded, not gating: heavy band math shells out to gdal_calc.py
        q = f"SELECT gbx_rst_mapalgebra(array(gbx_rst_fromcontent(content, 'GTiff')), '{calc}') AS t FROM gbx_selfcheck_heavy"
        gdal_calc = {"ok": spark.sql(q).first().t is not None, "error": None}
    except Exception as exc:  # noqa: BLE001
        gdal_calc = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:500]}"}
    return {
        "ok": size["ok"] and count == len(POINTS_27700) and jvm["ok"],
        "raster": size,
        "shapefile_count": count,
        "jvm_gdal": jvm,
        "gdal_calc": gdal_calc,
    }


def _where(_=None):
    import socket

    out = {"host": socket.gethostname(), "python": sys.executable}
    try:
        import databricks.labs.gbx as gbx
        from databricks.labs.gbx import _native

        out.update(
            geobrix=os.path.dirname(os.path.abspath(gbx.__file__)),
            native_dir=_native.native_dir(),
            activated_by=_native.activated_by(),
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return json.dumps(out, sort_keys=True)


def install_paths(spark):
    """Where the driver and the executors import geobrix from (a Python UDF; no ``.rdd``)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    where = F.udf(_where, StringType())
    rows = (
        spark.range(4)
        .repartition(4)
        .select(where("id").alias("w"))
        .distinct()
        .collect()
    )
    return {
        "driver": json.loads(_where()),
        "executors": [json.loads(w) for w in sorted({r.w for r in rows})],
    }


def _tree_bytes(path):
    total = 0
    for dp, _dirs, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(dp, f)) for f in files)
    return total


def payload_size_mb():
    """The payload on disk (``_native``, ``_vendor``, ``_jar``, both architectures), in MB."""
    from databricks.labs.gbx import _native

    root = _native.payload_root()
    return round(
        sum(_tree_bytes(os.path.join(root, d)) for d in ("_native", "_vendor", "_jar"))
        / 1e6,
        1,
    )


def installed_closure_mb():
    """geobrix and every installed distribution its unconditional requirements reach, in MB."""
    from importlib.metadata import PackageNotFoundError, distribution

    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    seen, todo, total = set(), ["geobrix"], 0
    while todo:
        name = canonicalize_name(todo.pop())
        if name in seen:
            continue
        seen.add(name)
        try:
            dist = distribution(name)
        except PackageNotFoundError:
            continue
        total += sum(f.size or 0 for f in dist.files or [])
        for line in dist.requires or []:
            req = Requirement(line)
            if req.marker is None or req.marker.evaluate({"extra": ""}):
                todo.append(req.name)
    return round(total / 1e6, 1)
