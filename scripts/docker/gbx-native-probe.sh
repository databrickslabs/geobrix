#!/bin/bash
# gbx-native-probe.sh: what the native stack of the geobrix-dev image looks like, as key=value lines.
# build_smart.sh pipes it into `bash -s` in a THROWAWAY container of the finished image (never the
# running geobrix-dev) and checks the values; it reads and changes nothing in the image.
#
#   gdal, proj, geos    the system libraries the heavyweight tier and the osgeo test fixtures use
#   jni_libgdal         where libgdalalljni.so (the file GDALManager loads) resolves libgdal.so.37
#   jni_unresolved      how many of its libraries ldd cannot find
#   rasterio_*          the lightweight tier's rasterio: its GDAL, wheel tags (manylinux = a PyPI
#                       wheel with its own GDAL; linux_<arch> = built from source against the system
#                       GDAL) and whether the wheel's vendored rasterio.libs directory exists
#   pyogrio, pyproj, shapely   installed versions
echo "gdal=$(gdalinfo --version 2>/dev/null | sed -n 's/^GDAL \([0-9][0-9.]*\).*/\1/p')"
echo "proj=$(pkg-config --modversion proj 2>/dev/null)"
echo "geos=$(geos-config --version 2>/dev/null)"
ldd_out="$(ldd /usr/lib/libgdalalljni.so 2>&1)"
echo "jni_libgdal=$(printf '%s\n' "$ldd_out" | sed -n 's/^[[:space:]]*libgdal\.so\.37 => \([^ ]*\).*/\1/p')"
echo "jni_unresolved=$(printf '%s\n' "$ldd_out" | grep -c 'not found')"
python3 - <<'PY'
import importlib.metadata as md
import os

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
PY
