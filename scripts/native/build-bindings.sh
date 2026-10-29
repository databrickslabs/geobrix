#!/bin/bash
# build-bindings.sh: build rasterio, pyogrio, pyproj, shapely and netCDF4 from their sdists against the staged
# gdal-native tree (/stage/gdal-native), in the `bindings` stage of build.Dockerfile. Writes /out:
#   wheels/            the five wheels as built (linux_<arch>)
#   vendor/            the content of _vendor/<arch>/: the five packages, no *.dist-info, RUNPATH finds
#                      _native/<arch>/lib relative to each file (the layout is _vendor/<arch> beside _native/<arch>/lib)
#   dist/              rasterio-<V>+gbx<REV>-cp312-cp312-linux_<arch>.whl: our rasterio as an installable distribution
#                      (dist-info kept, click requirement relaxed, RUNPATH site-packages-relative to
#                      databricks/labs/gbx/_native/<arch>/lib, so it needs the geobrix payload installed)
#   build-seconds.txt  `<binding> <seconds>` per binding
#   debuginfo/         .build-id/<xx>/<rest>.debug: the debug symbols (-g1) of every extension module in vendor/ and dist/,
#                      split off before they are stripped (the command packs it into a tarball; no wheel holds it)
#   build-ids.txt      `<path> <build-id>` of every extension module in vendor/
# The bindings link through gdal-native's lib-dev/ (and gdal-config, geos-config, nc-config, pkg-config): each records
# the hashed SONAME of every library it needs, which are the file names in _native/<arch>/lib. CMake's find_package is
# not used (it is broken for PROJ, TIFF and HDF5 in that tree). Every failure prints `BUILD FAILED: <what>`.
set -euo pipefail
source /recipes/pins.conf

G=/stage/gdal-native
PY=/opt/python/cp312-cp312/bin/python
GBX_ARCH="$(uname -m)"
fail() { echo "BUILD FAILED: $*"; exit 1; }

# split_debug DIR: every ELF file under DIR to its build-id's debug file in /out/debuginfo (once per build-id: the dist
# wheel's rasterio modules are the vendored ones), then stripped; `<path under DIR> <build-id>` to stdout
split_debug() {
    local f id
    while IFS= read -r f; do
        head -c 4 "$f" | grep -q 'ELF' || continue
        id="$(readelf -n "$f" 2> /dev/null | awk '/Build ID:/ && !id {id = $3} END {print id}')"
        [ -n "$id" ] || fail "${f#"$1"/} has no GNU build-id"
        mkdir -p "/out/debuginfo/.build-id/${id:0:2}"
        [ -e "/out/debuginfo/.build-id/${id:0:2}/${id:2}.debug" ] || objcopy --only-keep-debug "$f" "/out/debuginfo/.build-id/${id:0:2}/${id:2}.debug"
        strip --strip-unneeded "$f"
        echo "${f#"$1"/} $id"
    done < <(find "$1" -type f -name '*.so*')
}

export TMPDIR=/tmp/gbx-build
export PATH="/opt/python/cp312-cp312/bin:$G/bin:$PATH"
export PKG_CONFIG_PATH="$G/lib/pkgconfig"
export GDAL_CONFIG="$G/bin/gdal-config" GEOS_CONFIG="$G/bin/geos-config"
export PROJ_DIR="$G" PROJ_INCDIR="$G/include" PROJ_LIBDIR="$G/lib-dev" PROJ_VERSION
export NETCDF4_DIR="$G" USE_NCCONFIG=1
export CPPFLAGS="-I$G/include" LDFLAGS="-L$G/lib-dev -Wl,-rpath-link,$G/lib -Wl,--build-id=sha1" LIBRARY_PATH="$G/lib-dev"
# Python's own compile flags (-O3 ...) with -g1 (function names and line tables; split off below). setuptools takes a
# CFLAGS or CXXFLAGS from the environment INSTEAD of sysconfig's, so exporting only a -g flag builds every extension at
# -O0 (and netCDF4 then fails to load: pfnc_inq_vardimid, a C99 `inline` it relies on the optimizer to inline). Meson
# (shapely) adds them to its release flags.
CFLAGS="$("$PY" -c 'import sysconfig; print(sysconfig.get_config_var("CFLAGS").replace(" -g ", " -g1 "))')"
case "$CFLAGS" in *" -O3 "*) ;; *) fail "Python's CFLAGS has no -O3: $CFLAGS" ;; esac
export CFLAGS CXXFLAGS="$CFLAGS"

[ -x "$G/bin/gdal-config" ] || fail "no $G/bin/gdal-config (stage-native not built)"
[ "$("$GDAL_CONFIG" --version)" = "$GDAL_VERSION" ] || fail "gdal-config reports $("$GDAL_CONFIG" --version), pins.conf says $GDAL_VERSION"

mkdir -p /out/wheels /out/vendor /out/dist "$TMPDIR/src"
: > /out/build-seconds.txt
for dep in SHAPELY PYPROJ PYOGRIO RASTERIO NETCDF4; do
    name="${dep,,}"; [ "$dep" = NETCDF4 ] && name=netCDF4
    file_var="${dep}_SDIST_FILE"; file="${!file_var}"
    sd="/sdists/$file"
    [ -f "$sd" ] || fail "no sdist $sd for $name"
    t0=$SECONDS
    if [ "$dep" = RASTERIO ]; then  # admit the env6 base's click 8.2.1 (measured by test-bindings.sh), stamp the revision
        rm -rf "$TMPDIR/src/rasterio" && mkdir -p "$TMPDIR/src/rasterio" && tar -xzf "$sd" -C "$TMPDIR/src/rasterio" --strip-components=1
        "$PY" /recipes/relax-click.py "$TMPDIR/src/rasterio" --revision "$GBX_NATIVE_REVISION"
        sd="$TMPDIR/src/rasterio"
    fi
    echo "== build $name from $file"
    "$PY" -m pip wheel --no-index --no-build-isolation --no-deps --no-cache-dir -w /out/wheels "$sd"
    echo "$name $((SECONDS - t0))" >> /out/build-seconds.txt
done
ls /out/wheels

# rasterio keeps its click requirement relaxed in the metadata, and its version carries the revision
rw="$(ls /out/wheels/rasterio-*.whl)"
[ "$(ls /out/wheels/rasterio-*.whl | wc -l)" = 1 ] || fail "more than one rasterio wheel"
case "$rw" in */rasterio-"$RASTERIO_VERSION"+gbx"$GBX_NATIVE_REVISION"-*) ;; *) fail "rasterio wheel $(basename "$rw") is not $RASTERIO_VERSION+gbx$GBX_NATIVE_REVISION" ;; esac
unzip -p "$rw" '*.dist-info/METADATA' | grep -E '^Requires-Dist: click' | tee /out/rasterio-click-requirement.txt
grep -qx 'Requires-Dist: click>=4.0' /out/rasterio-click-requirement.txt || fail "rasterio's click requirement is not 'click>=4.0'"

# rasterio as a distribution (dist-info kept) for DCS and the dev image
rm -rf "$TMPDIR/rd" && "$PY" -m wheel unpack "$rw" -d "$TMPDIR/rd"
split_debug "$(ls -d "$TMPDIR"/rd/rasterio-*)" > /dev/null
"$PY" /recipes/set-rpath.py --tree "$TMPDIR"/rd/rasterio-*/rasterio --tree-at rasterio --lib-at "databricks/labs/gbx/_native/$GBX_ARCH/lib"
"$PY" -m wheel pack "$TMPDIR"/rd/rasterio-* -d /out/dist
ls /out/dist

# every binding as the payload's vendored tree (no dist-info: pip must never see or uninstall them)
for w in /out/wheels/*.whl; do unzip -q -o "$w" -d /out/vendor; done
rm -rf /out/vendor/*.dist-info
split_debug /out/vendor > /out/build-ids.txt
echo "debuginfo: $(wc -l < /out/build-ids.txt | tr -d ' ') extension modules split and stripped"
extra="$(find /out/vendor -maxdepth 1 -mindepth 1 -not -name __pycache__ | xargs -n1 basename | sort | tr '\n' ' ')"
echo "vendor top level: $extra"
"$PY" /recipes/set-rpath.py --tree /out/vendor --tree-at "_vendor/$GBX_ARCH" --lib-at "_native/$GBX_ARCH/lib"
find /out/vendor -name '*.dist-info' | grep -q . && fail "a dist-info is left in vendor/"
cat /out/build-seconds.txt
