#!/bin/bash
# gbx-native-probe.sh: what the native stack of a geobrix-dev image looks like, as key=value lines.
# build_smart.sh pipes it into `bash -s` in a THROWAWAY container of the finished image (never the
# running geobrix-dev, and with no network) and checks the values; it reads and changes nothing in the image.
#
#   gbx-native-probe.sh                   the base image, which has no GDAL of its own: system PROJ and GEOS
#   gbx-native-probe.sh --payload <root>  the native image: the env wheel's payload at <root> (GBX_NATIVE_ROOT)
#
# Base prints
#   gdal                what `gdalinfo --version` reports (empty: there is no system GDAL)
#   libgdal             every libgdal* in the system library directories (/usr/local/lib, /usr/lib, /usr/lib/<triplet>,
#                       /lib/<triplet>) and in the loader cache, comma separated (empty: none)
#   jni                 every libgdalalljni.so in those directories and /usr/lib/jni, /usr/local/lib/jni (empty: none)
#   osgeo               where GDAL's Python binding would import from, or "absent"
#   proj, geos          the system PROJ and GEOS (noble apt)
#   rasterio_*          its GDAL, wheel tags (manylinux = a PyPI wheel with its own GDAL, linux_<arch> = built from
#                       source), whether the wheel's vendored rasterio.libs directory exists
#   pyogrio, pyproj, shapely   installed versions
# Payload prints
#   gdal, proj, geos    the versions the payload's MANIFEST.json records for its own build (and native_revision,
#                       the build's revision number)
#   jni_soname          the libgdal the JNI library (/usr/lib/libgdalalljni.so, the file the JVM loads) needs,
#                       read from the file, so a check compares what the JNI asks for with where it resolves
#                       and never hard-codes a soname
#   jni_libgdal         where ldd resolves that soname for the JNI
#   jni_unresolved      how many of its libraries ldd cannot find
# and (each measured by a fresh interpreter, so the startup activation is what is tested)
#   jni_link            where the JNI link points
#   hdf5_plugin_dir     where HDF5's and netCDF-C's default plugin directory (/usr/local/hdf5/lib/plugin) points
#   <module>_gdal / _proj / _geos   the library version each binding reports (osgeo, rasterio, pyogrio, pyproj, shapely)
#   <module>_from_payload           true when the module was imported from the payload
#   rasterio_dist       the installed rasterio distribution's version
#   pypi_libs           the *.libs directories of PyPI wheels mapped into that process (none expected)
# stderr carries only what a failing tool says; build_smart.sh prints its tail when the probe fails.

mode=base
root=""
case "${1:-}" in
    --payload)
        mode=payload
        root="${2:-}"
        [ -n "$root" ] || { echo "gbx-native-probe.sh: --payload needs the payload root" >&2; exit 2; }
        root="${root%/}"
        ;;
    "") ;;
    *) echo "gbx-native-probe.sh: unknown argument '$1' (use --payload <root>)" >&2; exit 2 ;;
esac

arch="$(uname -m)"
jni_link=/usr/lib/libgdalalljni.so
if [ "$mode" = payload ]; then
    jni_file="$root/_native/$arch/lib/libgdalalljni.so"
    GBX_MANIFEST="$root/_native/$arch/MANIFEST.json" python3 - <<'PY'
import json
import os

try:
    manifest = json.load(open(os.environ["GBX_MANIFEST"]))
except Exception as e:  # the keys are then empty, and the check names them
    print("manifest_error=" + type(e).__name__)
    manifest = {}
versions = manifest.get("versions", {})
for key in ("gdal", "proj", "geos"):
    print(key + "=" + str(versions.get(key, "")))
print("native_revision=" + str(manifest.get("native_revision", "")))
PY
    echo "jni_link=$(readlink -f "$jni_link" 2>/dev/null)"
    echo "hdf5_plugin_dir=$(readlink -f /usr/local/hdf5/lib/plugin 2>/dev/null)"
    # The soname is read from the JNI file; ldd then says where the loader resolves it for the link the JVM loads.
    soname="$(readelf -d "$jni_file" 2>/dev/null | sed -n 's/.*(NEEDED).*\[\(libgdal[^]]*\)\].*/\1/p' | head -1)"
    ldd_out="$(ldd "$jni_link" 2>&1)"
    echo "jni_soname=$soname"
    echo "jni_libgdal=$(printf '%s\n' "$ldd_out" | awk -v s="$soname" '$1 == s && $2 == "=>" { print $3; exit }')"
    echo "jni_unresolved=$(printf '%s\n' "$ldd_out" | grep -c 'not found')"
else
    # No GDAL of its own: every one of these is empty. Site-packages (a PyPI wheel's *.libs) is not a system
    # library directory, so the wheels' own copies are not counted.
    echo "gdal=$(gdalinfo --version 2>/dev/null | sed -n 's/^GDAL \([0-9][0-9.]*\).*/\1/p')"
    # GBX_PROBE_SYSROOT is for the probe's own tests (scripts/ci/test_dev_dockerfile.py); unset in an image.
    sys="${GBX_PROBE_SYSROOT:-}"
    dirs="$sys/usr/local/lib $sys/usr/lib $sys/usr/lib/$arch-linux-gnu $sys/lib/$arch-linux-gnu"
    found="$(for d in $dirs; do ls -d "$d"/libgdal* 2>/dev/null; done; ldconfig -p 2>/dev/null | awk '$1 ~ /^libgdal/ { print $NF }')"
    echo "libgdal=$(printf '%s\n' "$found" | grep -v 'libgdalalljni' | sed '/^$/d' | sort -u | paste -sd, -)"
    found="$(for d in $dirs "$sys/usr/lib/jni" "$sys/usr/local/lib/jni"; do ls -d "$d"/libgdalalljni* 2>/dev/null; done)"
    echo "jni=$(printf '%s\n' "$found" | sed '/^$/d' | sort -u | paste -sd, -)"
    echo "proj=$(pkg-config --modversion proj 2>/dev/null)"
    echo "geos=$(geos-config --version 2>/dev/null)"
fi

GBX_PROBE_MODE="$mode" GBX_PROBE_ROOT="$root" python3 - <<'PY'
import importlib
import importlib.metadata as md
import os

mode, root = os.environ["GBX_PROBE_MODE"], os.environ["GBX_PROBE_ROOT"]
if mode == "base":
    try:
        import importlib.util

        spec = importlib.util.find_spec("osgeo")
        print("osgeo=" + ("absent" if spec is None else str(spec.origin or spec.submodule_search_locations)))
    except Exception as e:
        print("osgeo=error:" + type(e).__name__)
    try:
        import rasterio

        wheel = md.distribution("rasterio").read_text("WHEEL") or ""
        tags = [l.split(": ", 1)[1].strip() for l in wheel.splitlines() if l.startswith("Tag: ")]
        site = os.path.dirname(os.path.dirname(rasterio.__file__))
        print("rasterio_gdal=" + rasterio.__gdal_version__)
        print("rasterio_tags=" + ",".join(tags))
        print("rasterio_libs=" + str(os.path.isdir(os.path.join(site, "rasterio.libs"))).lower())
    except Exception as e:
        print("rasterio_error=" + type(e).__name__)
    for pkg in ("pyogrio", "pyproj", "shapely"):
        try:
            print(pkg + "=" + md.version(pkg))
        except Exception:
            print(pkg + "=")
else:
    # (module to import, attribute holding the library version, which library it reports)
    reported = (
        ("osgeo.gdal", "__version__", "gdal"),
        ("rasterio", "__gdal_version__", "gdal"),
        ("pyogrio", "__gdal_version_string__", "gdal"),
        ("pyproj", "proj_version_str", "proj"),
        ("shapely", "geos_version_string", "geos"),
    )
    for name, attr, kind in reported:
        key = name.split(".")[0]
        try:
            mod = importlib.import_module(name)
            print(f"{key}_{kind}=" + str(getattr(mod, attr, "")))
            top = importlib.import_module(key)
            print(f"{key}_from_payload=" + str(os.path.abspath(top.__file__).startswith(root + os.sep)).lower())
        except Exception as e:
            print(f"{key}_error=" + type(e).__name__)
    try:
        print("rasterio_dist=" + md.version("rasterio"))
    except Exception:
        print("rasterio_dist=")
    libs = set()
    try:
        for line in open("/proc/self/maps"):
            libs.update(p for p in line.split("/") if p in ("rasterio.libs", "pyogrio.libs", "pyproj.libs", "shapely.libs"))
        print("pypi_libs=" + ",".join(sorted(libs)))
    except OSError:  # not Linux: build_smart.sh fails closed on this value
        print("pypi_libs=unreadable")
PY
