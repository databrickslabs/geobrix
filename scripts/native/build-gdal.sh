#!/bin/bash
# build-gdal.sh: build GDAL (library, tools, JNI, Java classes, osgeo) from the pinned source against the libraries
# build-deps.sh put in /opt/gbx-native. External libraries are opt-in: GDAL_USE_EXTERNAL_LIBS=OFF, then every library
# we ship is switched ON by name and every library with a licence or a closed SDK we do not ship is switched OFF.
# An option cmake ignores fails the build (check_unused); an ON name GDAL does not know is therefore an error.
set -euo pipefail
source /recipes/build-lib.sh

SRC=/src
WORK=/tmp/gbx-build
DEP=gdal
T0="$(date +%s)"
rm -rf "${WORK:?}/gdal"
mkdir -p "$WORK/gdal"
tar -xf "$SRC/$GDAL_FILE" -C "$WORK/gdal"
top=("$WORK/gdal"/*)
cd "${top[0]}"
echo "== gdal ($GDAL_FILE)"
mkdir -p "$P/share/licenses/gdal"
cp LICENSE.TXT "$P/share/licenses/gdal/"

# the packages GDAL's cmake knows (gdal_check_package), upper-cased: an OFF switch is set only for those that exist
known="$(grep -rhoiE 'gdal_check_package\( *[A-Za-z0-9_]+' --include=CMakeLists.txt --include='*.cmake' . | sed -E 's/.*\( *//' | tr 'a-z' 'A-Z' | sort -u)"

opts=(-DGDAL_USE_EXTERNAL_LIBS=OFF)
# ICONV is glibc's iconv (no library is linked): Shapefile/DBF code pages need it. LIBAEC adds CCSDS to GRIB (libaec is already built for HDF5)
# JXL (+ JXL_THREADS): the JPEGXL driver and the GTiff JXL codec. PCRE2: REGEXP in the SQLite dialect and GPKG. MUPARSER:
# VRT expressions and `gdal raster calc --dialect=muparser`.
for x in ZLIB DEFLATE ZSTD LIBLZMA LZ4 BLOSC PNG JPEG WEBP LERC SQLITE3 CURL OPENSSL EXPAT OPENJPEG HDF5 NETCDF GEOS ICONV LIBAEC \
         JXL JXL_THREADS PCRE2 MUPARSER; do
    opts+=("-DGDAL_USE_$x=ON")
done
# GDAL's own copies of libtiff (the same 4.7.2) and libgeotiff: GDAL builds the GTiff JXL codec only with them
# ("Cannot build JXL as a TIFF codec as it requires building with -DGDAL_USE_TIFF_INTERNAL=ON", frmts/gtiff). Their
# symbols are renamed inside libgdal, so the external libtiff PROJ links (grid files) loads beside them without a clash.
opts+=(-DGDAL_USE_TIFF_INTERNAL=ON -DGDAL_USE_GEOTIFF_INTERNAL=ON)
for x in POPPLER PODOFO PDFIUM SPATIALITE MYSQL ODBC FILEGDB ECW MRSID KDU LURATECH OCI HEIF SFCGAL; do
    if printf '%s\n' "$known" | grep -qx "$x"; then opts+=("-DGDAL_USE_$x=OFF"); else echo "note: GDAL has no package $x; no switch set"; fi
done
opts+=(-DBUILD_JAVA_BINDINGS=ON -DSWIG_EXECUTABLE=/opt/swig/bin/swig
       -DBUILD_PYTHON_BINDINGS=ON -DPython_EXECUTABLE=/opt/python/cp312-cp312/bin/python -DGDAL_PYTHON_INSTALL_PREFIX="$P"
       -DGDAL_JAVA_GENERATE_JAVADOC=OFF -DBUILD_TESTING=OFF)

# expat is static (build-deps.sh): keep every symbol of it local to libgdal, so none can be interposed or exported
export LDFLAGS="$LDFLAGS -Wl,--exclude-libs,libexpat.a"
cmake -G Ninja -S . -B _build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$P" -DCMAKE_INSTALL_LIBDIR=lib \
    "${opts[@]}" 2>&1 | tee _cmake.log
check_unused _cmake.log "${opts[@]}"
if grep -q "Cannot build JXL as a TIFF codec" _cmake.log; then echo "BUILD FAILED: gdal: no GTiff JXL codec ($(grep -m1 'Cannot build JXL' _cmake.log))"; exit 1; fi
cmake --build _build --parallel "$JOBS"
cmake --install _build

lg="$(ls "$P"/lib/libgdal.so.*.*.* | head -n 1)"
if readelf -d "$lg" | grep -q 'NEEDED.*libexpat'; then echo "BUILD FAILED: libgdal NEEDs a shared libexpat"; exit 1; fi
n_xml="$(readelf --dyn-syms -W "$lg" | awk '$8 ~ /^XML_/ {n++} END {print n + 0}')"
[ "$n_xml" = 0 ] || { echo "BUILD FAILED: libgdal exports or imports $n_xml expat XML_* symbols"; exit 1; }
echo "gdal $(($(date +%s) - T0))" >> "$P/share/gbx/build-seconds.txt"
cd /
rm -rf "${WORK:?}/gdal"
echo "gdal ok: $("$P/bin/gdalinfo" --version)"
