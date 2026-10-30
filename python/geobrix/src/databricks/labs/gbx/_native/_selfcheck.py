"""The one native-payload oracle: the same inputs and answers at every level.

- ``stack()``: which build each binding runs (GDAL's own ``osgeo`` and ``osgeo_utils`` too),
  whether it came from the payload, and the libgdal files the process maps (one: the payload's).
  An ``osgeo`` on another GDAL than rasterio's is reported (both versions and files), not raised.
- ``light()``: raster, vector, CRS, geometry and netCDF checks in this process, no Spark.
- ``natives()``: the payload's build options, no Spark: netCDF-4 zstd/blosc/bzip2 through netCDF4 and
  GDAL (the HDF5 filter plugins), the PROJ grids (OSTN15 against Ordnance Survey's test points, OSGM15
  and EGM96 heights, no network), GTiff JXL, REGEXP (PCRE2) and muparser, libjpeg-turbo SIMD and curl
  HTTP/2 (with ``GDAL_HTTP_VERSION`` left unset), and projsync.
- ``light_sql(spark)`` / ``heavy_sql(spark, scratch_dir)``: the same GeoTIFF through the light and
  the heavy SQL functions; heavy also reads a Shapefile (written locally, then copied to the scratch
  dir, which may be a Volume) and checks the driver JVM's GDAL.
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

from databricks.labs.gbx._gdal_policy import open_data

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

    osgeo = _osgeo.libgdal_report()  # imports osgeo where it can; never refuses
    try:
        gdal_calc, osgeo_error = (
            _osgeo.utils("gdal_calc", "_selfcheck.stack").__file__,
            None,
        )
    except _osgeo.OsgeoUnavailable as exc:  # absent, or another GDAL than rasterio's
        gdal_calc, osgeo_error = None, str(exc)
    a = _native.arch()
    root = _native.payload_root()
    vendor = os.path.join(root, "_vendor", a) if a else None
    mods = dict(zip(BINDINGS, (rasterio, pyogrio, pyproj, shapely, netCDF4)))
    mapped = _mapped_gdal_stack()
    native = _native.native_dir()
    return {
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "payload_root": root,
        "native_dir": native,
        "activated_by": _native.activated_by(),
        "rasterio_gdal": rasterio.__gdal_version__,
        "osgeo_gdal": osgeo["osgeo_gdal"],
        "gdal_array": osgeo["gdal_array"],
        # osgeo is rasterio's GDAL: the same version, and each opens a /vsimem file the other wrote
        "shared_libgdal": osgeo["shared"],
        # why that /vsimem round trip failed (None when it passed or the versions differ)
        "vsimem_error": osgeo["vsimem_error"],
        "osgeo_error": osgeo_error,
        "pyogrio_gdal": pyogrio.__gdal_version_string__,
        "pyproj_proj": pyproj.proj_version_str,
        "shapely_geos": shapely.geos_version_string,
        "netcdf": netCDF4.__netcdf4libversion__,
        "hdf5": netCDF4.__hdf5libversion__,
        "from_payload": {
            **{n: _under(m.__file__, vendor) for n, m in mods.items()},
            "osgeo": _under(osgeo["osgeo"], vendor),
            "osgeo_utils": _under(gdal_calc, os.path.join(root, "_vendor", "common")),
        },
        # every libgdal mapped after rasterio and osgeo imported (None off Linux): one is one GDAL
        "libgdal": osgeo["libgdal"],
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


def _publish_points(shp_dir):
    """The oracle Shapefile in ``shp_dir``, whole. A Shapefile closes by rewriting its ``.shx`` and ``.dbf``
    headers, which a ``/Volumes`` FUSE write cannot do (the files would hold no rows): write it on local
    disk, then copy the files over one after another."""
    import shutil

    from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

    os.makedirs(shp_dir, exist_ok=True)
    local = new_local_temp_dir("gbx-selfcheck-shp-")
    try:
        _write_points(os.path.join(local, "points.shp"), "ESRI Shapefile")
        for name in sorted(os.listdir(local)):
            shutil.copyfile(os.path.join(local, name), os.path.join(shp_dir, name))
    finally:
        shutil.rmtree(local, ignore_errors=True)


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


# ---- natives(): the build options of the payload (one row each), in this process, no Spark; the same call on Databricks.

# (E, N) OSGB36 / British National Grid -> (lat, lon) ETRS89: Ordnance Survey's OSTN15 test data
# (OSTN15_TestInput_OSGBtoETRS.txt / OSTN15_TestOutput_OSGBtoETRS.txt in OS's OSTN15-NTv2.zip), points TP01, TP09, TP16,
# TP26 and TP40. With the OSTN15 grid PROJ matches them to a few millimetres; its Helmert fallback misses by metres.
OSTN15_TEST_POINTS = [
    ((91492.146, 11318.804), (49.92226393730, -6.29977752014)),
    ((530624.974, 178388.464), (51.48936564950, -0.11992557180)),
    ((357455.843, 383290.436), (53.34480280190, -2.64049320810)),
    ((256340.925, 664697.269), (55.85399952950, -4.29649016251)),
    ((395999.668, 1138728.951), (60.13308091660, -2.07382822798)),
]
GEOIDS = (  # grid, ellipsoidal CRS, compound CRS with the orthometric height
    ("uk_os_OSGM15_GB.tif", "EPSG:4937", "EPSG:4258+5701"),
    ("us_nga_egm96_15.tif", "EPSG:4979", "EPSG:4326+5773"),
)


def _payload_lib(prefix):
    from databricks.labs.gbx import _native

    d = _native.native_dir()
    names = json.load(open(os.path.join(d, "MANIFEST.json")))["sonames"]
    return os.path.join(
        d, "lib", next(v for k, v in names.items() if k.startswith(prefix))
    )


def _netcdf_filters():
    """zstd, blosc and bzip2 netCDF-4 variables written by netCDF4 read back the same through netCDF4 and GDAL,
    with the payload's filter plugins registered with its own netCDF-C/HDF5 and nothing put in the
    environment (``HDF5_PLUGIN_PATH`` is unchanged here and in a child process)."""
    import subprocess

    import netCDF4
    import numpy as np
    import rasterio

    from databricks.labs.gbx import _native

    env_before = os.environ.get("HDF5_PLUGIN_PATH")

    a = (np.arange(64 * 48, dtype=np.float32).reshape(64, 48) % 251) * 0.5
    out, d = {}, tempfile.mkdtemp(prefix="gbx-nc-filters-")
    for codec in ("zstd", "blosc_lz4", "bzip2"):
        path = os.path.join(d, f"{codec}.nc")
        with netCDF4.Dataset(path, "w", format="NETCDF4") as nc:
            nc.createDimension("y", 64)
            nc.createDimension("x", 48)
            v = nc.createVariable("v", "f4", ("y", "x"), compression=codec, complevel=4)
            v[:] = a
            used = v.filters()
        with netCDF4.Dataset(path) as nc:
            back = np.asarray(nc["v"][:])
        with rasterio.Env(GDAL_NETCDF_BOTTOMUP="NO"):
            with open_data(f'NETCDF:"{path}":v') as ds:
                gdal_back = ds.read(1)
        key = codec.split("_")[0]
        if (
            not used.get(key)
            or not np.array_equal(back, a)
            or not np.array_equal(gdal_back, a)
        ):
            return False, {
                "codec": codec,
                "filters": used,
                "registered": _native.hdf5_plugin_path(),
            }
        out[codec] = os.path.getsize(path)
    registered = _native.hdf5_plugin_path() or []
    child = subprocess.run(
        [sys.executable, "-c", "import os; print(os.environ.get('HDF5_PLUGIN_PATH'))"],
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout.strip()
    env_after = os.environ.get("HDF5_PLUGIN_PATH")
    return (
        _native.hdf5_plugin_dir() in registered
        and env_after == env_before
        and child == str(env_before)
    ), {
        "bytes": out,
        "registered": registered,
        "hdf5_plugin_path_env": env_after,
        "child_env": child,
    }


def _grids():
    """OSTN15 against Ordnance Survey's test points; OSGM15 and EGM96 heights both ways at a node; no network."""
    import numpy as np
    import pyproj

    t = pyproj.Transformer.from_crs("EPSG:27700", "EPSG:4258", always_xy=True)
    errs = []
    for (e, n), (lat, lon) in OSTN15_TEST_POINTS:
        x, y = t.transform(e, n)
        errs.append(
            math.hypot(
                (y - lat) * 111320.0, (x - lon) * 111320.0 * math.cos(math.radians(lat))
            )
        )
    share = pyproj.datadir.get_data_dir().split(os.pathsep)[0]
    heights = {}
    for grid, src, dst in GEOIDS:
        with open_data(os.path.join(share, grid)) as ds:
            r, c = ds.height // 2, ds.width // 2
            v = ds.read(1, window=((r, r + 1), (c, c + 50)))[0]
            c += int(np.argmax(v != ds.nodata)) if ds.nodata is not None else 0
            n_ = float(ds.read(1, window=((r, r + 1), (c, c + 1)))[0, 0])
            lon, lat = ds.xy(r, c)
        fwd = pyproj.Transformer.from_crs(src, dst, always_xy=True)
        h_ = fwd.transform(lon, lat, 100.0)[2]
        back = fwd.transform(lon, lat, h_, direction="INVERSE")[2]
        heights[grid] = {"N": round(n_, 4), "H": round(h_, 4), "back": round(back, 4)}
        if abs(h_ - (100.0 - n_)) > 1e-3 or abs(back - 100.0) > 1e-3 or abs(n_) < 1:
            return False, {"grid": grid, **heights[grid]}
    network = pyproj.network.is_network_enabled()
    return max(errs) < 0.01 and not network, {
        "ostn15_max_error_mm": round(max(errs) * 1000, 2),
        "heights": heights,
        "network": network,
    }


def _jxl():
    """GTiff COMPRESS=JXL, lossless, UInt16: written and read back exactly."""
    import numpy as np

    a = (np.arange(64 * 64, dtype=np.uint16).reshape(64, 64) * 13) % 4096
    path = os.path.join(tempfile.mkdtemp(prefix="gbx-jxl-"), "t.tif")
    jxl = dict(compress="JXL", JXL_LOSSLESS="YES")
    with open_data(
        path, "w", driver="GTiff", width=64, height=64, count=1, dtype="uint16", **jxl
    ) as ds:
        ds.write(a, 1)
    with open_data(path) as ds:
        back, comp = ds.read(1), ds.compression
    return np.array_equal(back, a) and str(comp).upper().endswith("JXL"), {
        "compression": str(comp)
    }


def _regexp_and_calc():
    """REGEXP (PCRE2) on a GPKG through GDAL's SQLite dialect (INDIRECT_SQLITE: GDAL builds PCRE2 into its SQLite
    driver only, so a GPKG's own SQL has REGEXP only once that driver has run in the process), and
    `gdal raster calc --dialect=muparser` (osgeo's gdal.Run)."""
    import numpy as np

    from databricks.labs.gbx._osgeo import bindings

    b = bindings("natives")
    gdal, ogr = b.gdal, b.ogr
    d = tempfile.mkdtemp(prefix="gbx-regexp-")
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(os.path.join(d, "r.gpkg"))
    lyr = ds.CreateLayer("t", None, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for v in ("cafe", "car", "bar"):
        f = ogr.Feature(lyr.GetLayerDefn())
        f.SetField("name", v)
        lyr.CreateFeature(f)
    ds = None
    ds = ogr.Open(os.path.join(d, "r.gpkg"))
    rs = ds.ExecuteSQL(
        "SELECT name FROM t WHERE name REGEXP '^ca' ORDER BY name",
        dialect="INDIRECT_SQLITE",
    )
    names = [r.GetField(0) for r in rs]
    ds.ReleaseResultSet(rs)
    ds = None
    a = np.full((8, 8), 30.0, np.float32)
    b = np.full((8, 8), 10.0, np.float32)
    src = gdal.GetDriverByName("GTiff").Create(
        os.path.join(d, "ab.tif"), 8, 8, 2, gdal.GDT_Float32
    )
    src.GetRasterBand(1).WriteArray(a)
    src.GetRasterBand(2).WriteArray(b)
    src = None
    ab, c = os.path.join(d, "ab.tif"), os.path.join(d, "c.tif")
    calc = dict(dialect="muparser", output_data_type="Float32", output=c)
    with gdal.Run(
        "raster", "calc", input=f"A={ab}", calc="(A[1]-A[2])/(A[1]+A[2])", **calc
    ):
        pass
    out = gdal.Open(os.path.join(d, "c.tif"))  # keep the dataset while its band is read
    got = out.GetRasterBand(1).ReadAsArray()
    return names == ["cafe", "car"] and np.allclose(got, 0.5), {
        "regexp": names,
        "calc": float(got[0, 0]),
    }


def _libraries():
    """libjpeg-turbo has its SIMD dispatcher; libcurl speaks HTTP/2; GDAL_HTTP_VERSION is left to GDAL."""
    import ctypes

    jpeg = open(
        _payload_lib("libjpeg.so."), "rb"
    ).read()  # the dispatcher is compiled in (verify-native times it)
    curl = ctypes.CDLL(_payload_lib("libcurl.so."))
    curl.curl_version.restype = ctypes.c_char_p
    version = curl.curl_version().decode()
    simd = b"JSIMD_FORCENONE" in jpeg
    return simd and "nghttp2/" in version and "GDAL_HTTP_VERSION" not in os.environ, {
        "jpeg_simd_compiled": simd,
        "curl": version,
        "gdal_http_version": os.environ.get("GDAL_HTTP_VERSION"),
    }


def _projsync():
    """projsync runs; it lists the CDN's uk_os files when there is network (else skipped, with the reason)."""
    import subprocess

    from databricks.labs.gbx import _native

    tool = _native.projsync_path()
    if tool is None:
        return False, {"error": "no projsync in the payload"}
    usage = subprocess.run([tool, "--help"], capture_output=True, text=True, timeout=60)
    ok = (
        "--target-dir" in usage.stdout + usage.stderr
    )  # it prints its usage and exits 1
    try:
        r = subprocess.run(
            [tool, "--list-files", "--source-id", "uk_os"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        listed = r.returncode == 0 and "uk_os_OSTN15_NTv2_OSGBtoETRS.tif" in r.stdout
        listing = (
            {"list_files": listed}
            if listed
            else {"list_files": "skipped", "reason": (r.stderr or r.stdout)[-200:]}
        )
    except subprocess.TimeoutExpired:
        listing = {
            "list_files": "skipped",
            "reason": "timed out after 60 s (no network)",
        }
    return ok, {"tool": tool, **listing}


def natives():
    checks = {
        "netcdf_filters": _run(_netcdf_filters),
        "grids": _run(_grids),
        "jxl": _run(_jxl),
        "regexp_and_calc": _run(_regexp_and_calc),
        "libraries": _run(_libraries),
        "projsync": _run(_projsync),
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
    # always: an earlier run on a Volume may have left a header-only Shapefile
    _publish_points(shp_dir)
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
