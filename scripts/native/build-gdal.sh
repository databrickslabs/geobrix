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
for x in ZLIB DEFLATE ZSTD LIBLZMA LZ4 BLOSC PNG JPEG WEBP LERC TIFF GEOTIFF SQLITE3 CURL OPENSSL EXPAT OPENJPEG HDF5 NETCDF GEOS ICONV LIBAEC; do
    opts+=("-DGDAL_USE_$x=ON")
done
for x in POPPLER PODOFO PDFIUM SPATIALITE MYSQL ODBC FILEGDB ECW MRSID KDU LURATECH OCI HEIF SFCGAL; do
    if printf '%s\n' "$known" | grep -qx "$x"; then opts+=("-DGDAL_USE_$x=OFF"); else echo "note: GDAL has no package $x; no switch set"; fi
done
opts+=(-DBUILD_JAVA_BINDINGS=ON -DSWIG_EXECUTABLE=/opt/swig/bin/swig
       -DBUILD_PYTHON_BINDINGS=ON -DPython_EXECUTABLE=/opt/python/cp312-cp312/bin/python -DGDAL_PYTHON_INSTALL_PREFIX="$P"
       -DGDAL_JAVA_GENERATE_JAVADOC=OFF -DBUILD_TESTING=OFF)

cmake -G Ninja -S . -B _build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$P" -DCMAKE_INSTALL_LIBDIR=lib \
    "${opts[@]}" 2>&1 | tee _cmake.log
check_unused _cmake.log "${opts[@]}"
cmake --build _build --parallel "$JOBS"
cmake --install _build

echo "gdal $(($(date +%s) - T0))" >> "$P/share/gbx/build-seconds.txt"
cd /
rm -rf "${WORK:?}/gdal"
echo "gdal ok: $("$P/bin/gdalinfo" --version)"
