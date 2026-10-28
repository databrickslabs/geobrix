#!/bin/bash
# build-deps.sh [group|dep ...]: build the libraries GDAL is linked against, from the pinned sources in /src, into /opt/gbx-native
# (the container's prefix). No argument builds everything, in dependency order. Groups: compress raster net science geo.
# Per dependency it copies the licence files to $P/share/licenses/<dep>/ and appends "<dep> <seconds>" to
# $P/share/gbx/build-seconds.txt. Any ignored CMake option or unrecognised configure option fails the build (check_unused).
set -euo pipefail
source /recipes/build-lib.sh

SRC=/src
WORK=/tmp/gbx-build
SECS="$P/share/gbx/build-seconds.txt"

# enter <dep> <file>: unpack into a fresh directory and cd to the one top-level directory in it.
enter() {
    local top
    DEP="$1"
    T0="$(date +%s)"
    echo "== $DEP ($2)"
    rm -rf "${WORK:?}/$DEP"
    mkdir -p "$WORK/$DEP"
    tar -xf "$SRC/$2" -C "$WORK/$DEP"
    top=("$WORK/$DEP"/*)
    if [ "${#top[@]}" -ne 1 ] || [ ! -d "${top[0]}" ]; then echo "BUILD FAILED: $2 does not unpack to one directory"; exit 1; fi
    cd "${top[0]}"
}

# lic: copy the licence files of the unpacked source (the current directory) next to the build.
lic() {
    local d="$P/share/licenses/$DEP" f n=0
    mkdir -p "$d"
    while IFS= read -r f; do cp -R "$f" "$d/"; n=$((n + 1)); done < <(find . -maxdepth 1 \( -iname 'LICEN[CS]E*' -o -iname 'COPYING*' -o -iname 'COPYRIGHT*' \))
    if [ "$n" -eq 0 ]; then echo "BUILD FAILED: $DEP has no licence file in its source"; exit 1; fi
}

finish() {
    echo "$DEP $(($(date +%s) - T0))" >> "$SECS"
    cd /
    rm -rf "${WORK:?}/$DEP"
}

# cm <srcdir> <opts>: configure with Ninja, fail on an ignored option, build and install.
cm() {
    local src="$1"; shift
    cmake -G Ninja -S "$src" -B _build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$P" -DCMAKE_INSTALL_LIBDIR=lib \
        -DBUILD_SHARED_LIBS=ON -DCMAKE_POLICY_VERSION_MINIMUM=3.5 "$@" 2>&1 | tee _cmake.log
    check_unused _cmake.log "$@"
    cmake --build _build --parallel "$JOBS"
    cmake --install _build
}

# ac <configure options>: configure/make/install an autotools project; an option configure does not know is an error.
ac() {
    ./configure --prefix="$P" --libdir="$P/lib" --disable-static "$@" 2>&1 | tee _configure.log
    if grep -q "unrecognized options" _configure.log; then echo "BUILD FAILED: $DEP ignored a configure option: $(grep 'unrecognized options' _configure.log)"; exit 1; fi
    make -j"$JOBS"
    make install
}

b_zlib() { enter zlib "$ZLIB_FILE"; lic; ./configure --prefix="$P" --libdir="$P/lib" --shared; make -j"$JOBS"; make install; rm -f "$P/lib/libz.a"; finish; }
b_libdeflate() { enter libdeflate "$LIBDEFLATE_FILE"; lic; cm . -DLIBDEFLATE_BUILD_STATIC_LIB=OFF -DLIBDEFLATE_BUILD_GZIP=OFF; finish; }
b_zstd() { enter zstd "$ZSTD_FILE"; lic; cm build/cmake -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_STATIC=OFF -DZSTD_BUILD_TESTS=OFF; finish; }
b_xz() {
    enter xz "$XZ_FILE"; lic
    ac --disable-xz --disable-xzdec --disable-lzmadec --disable-lzmainfo --disable-scripts --disable-doc
    finish
}
b_lz4() { enter lz4 "$LZ4_FILE"; lic; make -C lib -j"$JOBS" PREFIX="$P" BUILD_STATIC=no install; finish; }
b_c-blosc() {
    enter c-blosc "$CBLOSC_FILE"; lic
    cm . -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF -DBUILD_FUZZERS=OFF -DBUILD_STATIC=OFF -DPREFER_EXTERNAL_LZ4=ON \
        -DPREFER_EXTERNAL_ZLIB=ON -DPREFER_EXTERNAL_ZSTD=ON -DDEACTIVATE_SNAPPY=ON
    finish
}
b_libpng() { enter libpng "$LIBPNG_FILE"; lic; cm . -DPNG_STATIC=OFF -DPNG_TESTS=OFF -DPNG_TOOLS=OFF; finish; }
b_libjpeg-turbo() {
    enter libjpeg-turbo "$LIBJPEG_TURBO_FILE"; lic
    cm . -DENABLE_STATIC=OFF -DWITH_TURBOJPEG=OFF -DWITH_SIMD=OFF
    finish
}
b_libwebp() {
    enter libwebp "$LIBWEBP_FILE"; lic
    cm . -DWEBP_BUILD_ANIM_UTILS=OFF -DWEBP_BUILD_CWEBP=OFF -DWEBP_BUILD_DWEBP=OFF -DWEBP_BUILD_GIF2WEBP=OFF \
        -DWEBP_BUILD_IMG2WEBP=OFF -DWEBP_BUILD_VWEBP=OFF -DWEBP_BUILD_WEBPINFO=OFF -DWEBP_BUILD_WEBPMUX=OFF -DWEBP_BUILD_EXTRAS=OFF
    finish
}
b_lerc() { enter lerc "$LERC_FILE"; lic; cm .; finish; }
b_libtiff() {
    enter libtiff "$LIBTIFF_FILE"; lic
    cm . -Dtiff-tools=OFF -Dtiff-tests=OFF -Dtiff-docs=OFF -Dtiff-contrib=OFF -Djbig=OFF -Dwebp=ON -Dzstd=ON -Dlzma=ON \
        -Dlerc=ON -Djpeg=ON -Dlibdeflate=ON -Dzlib=ON
    finish
}
b_sqlite() {
    enter sqlite "$SQLITE_FILE"
    # the amalgamation ships no licence file: its notice is the blessing at the top of sqlite3.h
    { echo "SQLite is in the public domain. Its source files carry this notice:"; echo; sed -n '/^\*\* The author disclaims copyright/,/^\*\*    May you share freely/p' sqlite3.h; } > LICENSE.txt
    grep -q "May you share freely" LICENSE.txt || { echo "BUILD FAILED: sqlite3.h has no blessing"; exit 1; }
    lic
    ac --disable-readline --rtree --fts5 --column-metadata
    # its shared library carries no SONAME, so whatever links it would record an absolute path in NEEDED; give it the
    # distribution-standard name (libsqlite3.so.0) before PROJ is linked against it
    patchelf --set-soname libsqlite3.so.0 "$P/lib/libsqlite3.so.3.54.1"
    readelf -d "$P/lib/libsqlite3.so.3.54.1" | grep -q 'SONAME.*\[libsqlite3.so.0\]' || { echo "BUILD FAILED: sqlite SONAME not set"; exit 1; }
    finish
}
b_openssl() {
    enter openssl "$OPENSSL_FILE"; lic
    ./Configure --prefix="$P" --libdir=lib shared no-tests no-docs
    make -j"$JOBS"
    make install_sw
    rm -f "$P"/lib/libssl.a "$P"/lib/libcrypto.a
    finish
}
b_curl() {
    enter curl "$CURL_FILE"; lic
    cm . -DCURL_USE_OPENSSL=ON -DBUILD_CURL_EXE=OFF -DBUILD_TESTING=OFF -DCURL_USE_LIBPSL=OFF -DCURL_USE_LIBSSH2=OFF \
        -DUSE_NGHTTP2=OFF -DCURL_ZSTD=ON -DCURL_BROTLI=OFF -DUSE_LIBIDN2=OFF -DCURL_CA_BUNDLE=none -DCURL_CA_PATH=none \
        -DCURL_DISABLE_LDAP=ON -DCURL_DISABLE_LDAPS=ON -DBUILD_STATIC_LIBS=OFF -DENABLE_CURL_MANUAL=OFF \
        -DBUILD_LIBCURL_DOCS=OFF -DBUILD_MISC_DOCS=OFF
    finish
}
b_expat() {
    enter expat "$EXPAT_FILE"; lic
    cm . -DEXPAT_BUILD_TOOLS=OFF -DEXPAT_BUILD_EXAMPLES=OFF -DEXPAT_BUILD_TESTS=OFF -DEXPAT_BUILD_DOCS=OFF
    finish
}
b_openjpeg() { enter openjpeg "$OPENJPEG_FILE"; lic; cm . -DBUILD_CODEC=OFF -DBUILD_STATIC_LIBS=OFF; finish; }
b_libaec() { enter libaec "$LIBAEC_FILE"; lic; cm . -DBUILD_STATIC_LIBS=OFF; finish; }
b_hdf5() {
    enter hdf5 "$HDF5_FILE"; lic
    cm . -DHDF5_BUILD_TOOLS=OFF -DHDF5_BUILD_EXAMPLES=OFF -DBUILD_TESTING=OFF -DHDF5_BUILD_CPP_LIB=OFF -DHDF5_BUILD_FORTRAN=OFF \
        -DHDF5_BUILD_HL_LIB=ON -DHDF5_ENABLE_ZLIB_SUPPORT=ON -DHDF5_ENABLE_SZIP_SUPPORT=ON
    finish
}
b_netcdf-c() {
    enter netcdf-c "$NETCDF_C_FILE"; lic
    cm . -DNETCDF_ENABLE_TESTS=OFF -DNETCDF_BUILD_UTILITIES=OFF -DNETCDF_ENABLE_HDF5=ON -DNETCDF_ENABLE_DAP=ON \
        -DNETCDF_ENABLE_PLUGINS=OFF
    finish
}
b_proj() {
    enter proj "$PROJ_FILE"; lic
    cm . -DBUILD_TESTING=OFF -DBUILD_APPS=ON -DENABLE_TIFF=ON -DENABLE_CURL=ON -DBUILD_PROJSYNC=OFF
    finish
}
b_libgeotiff() {
    enter libgeotiff "$LIBGEOTIFF_FILE"; lic
    cm . -DWITH_UTILITIES=OFF -DBUILD_MAN=OFF -DBUILD_DOC=OFF -DWITH_TIFF=ON
    finish
}
b_geos() { enter geos "$GEOS_FILE"; lic; cm . -DBUILD_TESTING=OFF -DBUILD_DOCUMENTATION=OFF; finish; }

GROUP_compress="zlib libdeflate zstd xz lz4 c-blosc"
GROUP_raster="libpng libjpeg-turbo libwebp lerc libtiff"
GROUP_net="sqlite openssl curl expat"
GROUP_science="openjpeg libaec hdf5 netcdf-c"
GROUP_geo="proj libgeotiff geos"

want=("$@")
if [ "${#want[@]}" -eq 0 ]; then want=(compress raster net science geo); fi
for w in "${want[@]}"; do
    g="GROUP_${w}"
    if [ -n "${!g:-}" ]; then deps=(${!g}); else deps=("$w"); fi
    for d in "${deps[@]}"; do
        if ! declare -F "b_$d" > /dev/null; then echo "BUILD FAILED: no recipe for $d"; exit 1; fi
        "b_$d"
    done
done
if [ -d "$P/lib64" ]; then echo "BUILD FAILED: something installed into $P/lib64 (expected lib)"; ls "$P/lib64"; exit 1; fi
echo "deps ok: $(cut -d' ' -f1 "$SECS" | tr '\n' ' ')"
