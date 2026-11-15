#!/databricks/python3/bin/python
"""What `gbx:dcs:build` checks inside the finished DCS image. Runs in the image, with no network.

    verify_image.py          (the build mounts this directory at /mnt/dcs, the lock at /mnt/dcs.lock and the
                              base image's package list at /mnt/dcs-base.txt, all read-only)

Prints `VERIFY FAILED: <what>` for every miss and exits 1; on success prints the STACK line and `VERIFY OK`.
Environment: GBX_RASTERIO is the rasterio distribution version the image must carry.

What it holds, in the order it checks:
- the payload's stack (`_selfcheck.stack()`): every binding, osgeo and osgeo_utils load from the payload, GDAL,
  PROJ and GEOS are the versions `_build_info` records, and the process maps one libgdal;
- the light tier runs (`_selfcheck.light()`);
- the JVM's legacy path: /usr/lib/libgdalalljni.so is the payload's JNI and loads, and the HDF5/netCDF-C default
  plugin directory is the payload's filter plugins;
- osgeo: GDAL's own bindings, on the payload's GDAL, write and read a FileGDB;
- the lock: every locked package is installed at its version, the base image's others are unchanged (click
  included), the rasterio distribution is GeoBrix's build, and no package the build installed was built from a
  source distribution here (a `linux_<arch>` wheel tag; the base image's own packages are not the build's);
- OpenSSL 1.1 is gone: no libssl/libcrypto 1.x under site-packages; the installed rasterio distribution, imported
  alone, runs on the payload's GDAL, which links the payload's libssl; and a process that imports the whole
  visualization stack maps no OpenSSL 1.x library;
- the [vizx] stack imports and its tippecanoe executables exist; the STAC extra is absent.
"""

import importlib
import importlib.metadata as md
import os
import platform
import re
import subprocess
import sys
import sysconfig

sys.path.insert(0, "/mnt/dcs")  # dcs_lock.py: the lock and package-list readers the build itself uses

OPENSSL1 = re.compile(r"lib(ssl|crypto)[^/]*\.so\.1(\.|$)")
LOCK, BASE = "/mnt/dcs.lock", "/mnt/dcs-base.txt"
JNI_LINK = "/usr/lib/libgdalalljni.so"
PLUGIN_DIR = "/usr/local/hdf5/lib/plugin"


def openssl1_files(roots):
    """Files named like OpenSSL 1.x libraries under the given directories."""
    return sorted(
        os.path.join(r, f) for root in roots for r, _, files in os.walk(root) for f in files if OPENSSL1.match(f)
    )


def maps_openssl1(maps_text):
    """The OpenSSL 1.x libraries a /proc/<pid>/maps text maps."""
    return sorted({ln.split()[-1] for ln in maps_text.splitlines() if "/" in ln and OPENSSL1.match(os.path.basename(ln.split()[-1]))})


def linux_tagged(tags_by_dist, built, arch):
    """The built (non-base) distributions whose WHEEL tags include linux_<arch>, rasterio apart."""
    tag = re.compile(rf"linux_{arch}$")
    return sorted(n for n in built if n != "rasterio" and any(tag.search(t) for t in tags_by_dist.get(n, [])))


def wheel_tags(dist):
    return [ln.split(":", 1)[1].strip() for ln in (dist.read_text("WHEEL") or "").splitlines() if ln.startswith("Tag:")]


def payload_libssl(sonames):
    """The hashed names of the payload's libssl of OpenSSL 3 or later, from the MANIFEST `sonames` map."""
    return sorted(
        h for o, h in sonames.items() if re.fullmatch(r"libssl\.so\.(\d+)", o) and int(o.rsplit(".", 1)[1]) >= 3
    )


def ldd(path):
    res = subprocess.run(["ldd", path], capture_output=True, text=True)
    return res.stdout + res.stderr


FILEGDB = """
import os, tempfile
from osgeo import gdal, ogr, osr
gdal.UseExceptions(); ogr.UseExceptions(); osr.UseExceptions()
d = os.path.join(tempfile.mkdtemp(), 't.gdb'); drv = ogr.GetDriverByName('OpenFileGDB')
ds = drv.CreateDataSource(d); sr = osr.SpatialReference(); sr.ImportFromEPSG(4326)
lyr = ds.CreateLayer('p', sr, ogr.wkbPoint); f = ogr.Feature(lyr.GetLayerDefn())
f.SetGeometry(ogr.CreateGeometryFromWkt('POINT (1 2)')); lyr.CreateFeature(f); f = None; ds = None
back = ogr.Open(d)
assert back.GetLayer(0).GetFeatureCount() == 1
print(gdal.__version__)
"""

INSTALLED_RASTERIO = """
import glob, os, rasterio
from databricks.labs.gbx import _build_info as b
assert rasterio.__gdal_version__ == b.GDAL_VERSION, rasterio.__gdal_version__
print(rasterio.__file__)
print(glob.glob(os.path.join(os.path.dirname(rasterio.__file__), '_base*.so'))[0])
"""

VIZ_MAPS = """
import geopandas, contextily, rasterio
import databricks.labs.gbx
print(open('/proc/self/maps').read())
"""


def run_python(code, env=None):
    full = dict(os.environ)
    full.update(env or {})
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=full)


def main():
    failures = []

    def fail(what):
        failures.append(what)
        print(f"VERIFY FAILED: {what}")

    from databricks.labs.gbx import _build_info as build
    from databricks.labs.gbx import _native
    from databricks.labs.gbx._native import _selfcheck
    import dcs_lock

    arch = _native.arch()
    native = _native.native_dir()
    if not native or arch != platform.machine():
        fail(f"no payload for {platform.machine()} (arch {arch}, native dir {native})")
        return 1

    # --- the payload's stack
    stack = _selfcheck.stack()
    off = sorted(k for k, v in stack["from_payload"].items() if not v)
    if off:
        fail(f"loaded from outside the payload: {', '.join(off)}")
    for key, want in (
        ("rasterio_gdal", build.GDAL_VERSION),
        ("osgeo_gdal", build.GDAL_VERSION),
        ("pyogrio_gdal", build.GDAL_VERSION),
        ("pyproj_proj", build.PROJ_VERSION),
        ("shapely_geos", build.GEOS_VERSION),
    ):
        if str(stack[key]) != want:
            fail(f"{key} is {stack[key]}, _build_info has {want}")
    if stack["foreign_libs"]:
        fail(f"GDAL-stack libraries from outside the payload are mapped: {stack['foreign_libs']}")
    if len(stack["libgdal"] or []) != 1:
        fail(f"expected one mapped libgdal, found {stack['libgdal']}")
    if not stack["shared_libgdal"]:
        fail(f"osgeo and rasterio do not share one libgdal: {stack['vsimem_error']}")
    light = _selfcheck.light()
    if not light["ok"]:
        fail("light() failed: " + ", ".join(n for n, c in light["checks"].items() if not c["ok"]))

    # --- the JVM's legacy path and the plugin directory
    lib = os.path.join(native, "lib")
    if os.path.realpath(JNI_LINK) != os.path.realpath(os.path.join(lib, "libgdalalljni.so")):
        fail(f"{JNI_LINK} is {os.path.realpath(JNI_LINK)}, not the payload's JNI")
    out = ldd(JNI_LINK)
    if "not found" in out:
        fail("libgdalalljni.so has unresolved libraries: " + "; ".join(ln.strip() for ln in out.splitlines() if "not found" in ln))
    gdal_line = re.search(r"(libgdal-\S+\.so\.\d+) => (\S+)", out)
    if not gdal_line or not os.path.realpath(gdal_line.group(2)).startswith(os.path.realpath(lib) + os.sep):
        fail(f"libgdalalljni.so resolves libgdal to {gdal_line.group(2) if gdal_line else None}, not into {lib}")
    plugins = _native.hdf5_plugin_dir()
    if not plugins or os.path.realpath(PLUGIN_DIR) != os.path.realpath(plugins) or not os.listdir(PLUGIN_DIR):
        fail(f"{PLUGIN_DIR} is not the payload's HDF5 filter plugins ({plugins})")

    # --- osgeo, FileGDB (the lightweight FileGDB writer needs it)
    res = run_python(FILEGDB)
    if res.returncode != 0 or res.stdout.strip() != build.GDAL_VERSION:
        fail(f"osgeo / FileGDB write and read: {(res.stdout + res.stderr).strip()[-300:]}")

    # --- the lock
    lock = dcs_lock.parse_lock(open(LOCK).read())
    base = dcs_lock.parse_base(open(BASE).read())
    built = dcs_lock.needed(lock, base)
    for name, (version, _) in lock.items():
        try:
            have = md.version(name)
        except md.PackageNotFoundError:
            fail(f"{name}=={version} is in the lock but not installed")
            continue
        if have != version:
            fail(f"{name} is {have}, the lock has {version}")
    for name, version in base.items():
        if name in lock:
            continue
        try:
            have = md.version(name)
        except md.PackageNotFoundError:
            fail(f"{name} (base image {version}) is gone")
            continue
        if have != version:
            fail(f"{name} moved from the base image's {version} to {have}")
    want_rasterio = os.environ.get("GBX_RASTERIO", "")
    if md.version("rasterio") != want_rasterio or lock.get("rasterio", ("",))[0] != want_rasterio:
        fail(f"rasterio is {md.version('rasterio')} (lock {lock.get('rasterio', ('',))[0]}), expected GeoBrix's {want_rasterio}")
    tags = {dcs_lock.canon(d.metadata["Name"]): wheel_tags(d) for d in md.distributions()}
    sdist_built = linux_tagged(tags, built, arch)
    if sdist_built:
        fail(f"built in the image (a linux_{arch} wheel tag): {', '.join(sdist_built)}")

    # --- OpenSSL 1.1 is gone
    sites = sorted({sysconfig.get_paths()["purelib"], sysconfig.get_paths()["platlib"]})
    old = openssl1_files(sites)
    if old:
        fail(f"OpenSSL 1.x libraries under site-packages: {old[:3]}")
    res = run_python(INSTALLED_RASTERIO, {"GBX_NATIVE_DISABLE": "1"})
    if res.returncode != 0:
        fail(f"the installed rasterio, imported alone: {(res.stdout + res.stderr).strip()[-400:]}")
    else:
        rio_file, base_so = res.stdout.split("\n")[:2]
        if os.sep + "_vendor" + os.sep in rio_file:
            fail(f"GBX_NATIVE_DISABLE=1 still imported the payload's rasterio ({rio_file})")
        deps = ldd(base_so)
        m = re.search(r"(libgdal-\S+\.so\.\d+) => (\S+)", deps)
        if not m or not os.path.realpath(m.group(2)).startswith(os.path.realpath(lib) + os.sep):
            fail(f"the installed rasterio resolves libgdal to {m.group(2) if m else None}, not into {lib}")
        else:
            sonames = _native._manifest(native).get("sonames", {})
            ssl = payload_libssl(sonames)
            if len(ssl) != 1:
                fail(f"expected exactly one payload libssl (OpenSSL 3 or later) in the MANIFEST, found {ssl}")
            elif ssl[0] not in ldd(m.group(2)):
                fail(f"the payload's libgdal does not link the payload's libssl ({ssl[0]})")
    res = run_python(VIZ_MAPS)
    if res.returncode != 0:
        fail(f"importing geopandas, contextily, rasterio and geobrix: {res.stderr.strip()[-400:]}")
    elif maps_openssl1(res.stdout):
        fail(f"a process using the visualization stack maps OpenSSL 1.x: {maps_openssl1(res.stdout)}")

    # --- [vizx]
    for mod in ("geopandas", "mapclassify", "contextily", "imageio_ffmpeg", "anywidget"):
        try:
            importlib.import_module(mod)
        except Exception as exc:  # noqa: BLE001
            fail(f"cannot import {mod}: {type(exc).__name__}")
    try:
        import tippecanoe

        exe = os.path.join(os.path.dirname(tippecanoe.__file__), "bin", "tippecanoe")
        if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
            fail("tippecanoe bundled executable missing or not executable")
        if not os.access("/databricks/python3/bin/tippecanoe", os.X_OK):
            fail("tippecanoe console script missing from /databricks/python3/bin")
    except ImportError:
        fail("cannot import tippecanoe")
    try:
        importlib.import_module("pystac_client")
    except ImportError:
        pass
    else:
        fail("pystac_client is installed; the image carries [vizx], not [stac]")

    if failures:
        return 1
    print(
        "STACK machine={m} python={py} gdal={g} proj={p} geos={s} netcdf={n} hdf5={h} activated_by={a} "
        "libgdal={lg} foreign_libs={f} rasterio={r} click={c}".format(
            m=stack["machine"], py=stack["python"], g=stack["rasterio_gdal"], p=stack["pyproj_proj"],
            s=stack["shapely_geos"], n=stack["netcdf"], h=stack["hdf5"], a=stack["activated_by"],
            lg=os.path.basename(stack["libgdal"][0]), f=len(stack["foreign_libs"]), r=md.version("rasterio"),
            c=md.version("click"),
        )
    )
    print("VERIFY OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
