#!/bin/bash
# verify-native.sh [root]: the oracle for `gbx:build:native --target stage-native`. Runs in the toolchain image with the
# staged tree mounted (default /gdal-native) and this directory mounted at /verify, under `--network none`.
# Prints `ok: <check>` per check and stops at the first `VERIFY FAILED: <what>`; the last line of a good run is `VERIFY OK`.
#   1 versions, tools      2 licence guard      3 required drivers      4 ELF closure and auditwheel
#   5 JNI smoke            6 osgeo against the one GDAL      7 HTTPS through GDAL_CURL_CA_BUNDLE (no compiled-in CA path)
#   8 the dev kit: headers, pkg-config, gdal-config, cmake and a C program built against include/ + lib-dev/
set -uo pipefail

R="$(cd "${1:-/gdal-native}" && pwd)"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/pins.conf"
ARCH="$(uname -m)"
PY=/opt/python/cp312-cp312/bin/python
WORK="$(mktemp -d)"
trap 'kill "${SERVER_PID:-0}" 2> /dev/null; rm -rf "$WORK"' EXIT
fail() { echo "VERIFY FAILED: $*"; exit 1; }
ok() { echo "ok: $*"; }
# (SSL_CERT_FILE is set by the manylinux image and GDAL honours it as a CA bundle, so it goes too)
unset LD_LIBRARY_PATH GDAL_DATA PROJ_DATA PROJ_LIB PYTHONPATH CURL_CA_BUNDLE GDAL_CURL_CA_BUNDLE SSL_CERT_FILE SSL_CERT_DIR LIBRARY_PATH PKG_CONFIG_PATH
export PYTHONDONTWRITEBYTECODE=1

for d in lib lib-dev bin share/gdal share/proj java python/osgeo python/osgeo_utils licenses include lib/pkgconfig lib/cmake; do
    [ -d "$R/$d" ] || fail "the tree has no $d/"
done
[ -f "$R/MANIFEST.json" ] || fail "no MANIFEST.json"
[ -f "$R/lib/libgdalalljni.so" ] || fail "no lib/libgdalalljni.so"
[ -f "$R/java/gdal-${GDAL_VERSION}.jar" ] || fail "no java/gdal-${GDAL_VERSION}.jar"
if [ -n "$(find "$R/lib" -maxdepth 1 -type l)" ]; then fail "lib/ holds symlinks: $(find "$R/lib" -maxdepth 1 -type l -printf '%f ' | head -c 200)"; fi

# --- 1 versions and tools (no GDAL_DATA, PROJ_DATA or LD_LIBRARY_PATH set: the tree finds its own data and libraries)
v="$("$R/bin/gdalinfo" --version 2>&1)" || fail "gdalinfo --version: $v"
# GDAL 3.13 prints `GDAL 3.13.3 "Iowa City", released 2026/08/13`: the version, then the release name
case "$v" in "GDAL ${GDAL_VERSION},"*|"GDAL ${GDAL_VERSION} "*) ;; *) fail "gdalinfo reports '$v', wanted 'GDAL ${GDAL_VERSION}' first" ;; esac
"$R/bin/projinfo" EPSG:27700 > "$WORK/projinfo.txt" 2>&1 || fail "projinfo EPSG:27700: $(head -c 300 "$WORK/projinfo.txt")"
grep -q "tmerc" "$WORK/projinfo.txt" || fail "projinfo EPSG:27700 printed no transverse Mercator definition"
"$R/bin/ogrinfo" --version | grep -q "^GDAL ${GDAL_VERSION}[ ,]" || fail "ogrinfo version"
ok "gdalinfo '${v%%,*}', projinfo EPSG:27700 (data found relative to the tree)"

# --- 2 licence guard: libraries, NEEDED, driver lists, licence files
BANNED='poppler|podofo|pdfium|rttopo|spatialite|mysql|mariadb|FileGDBAPI|ecw|ltidsdk|kdu|lura|grok|x265|odbc|clntsh|sfcgal|CGAL'
"$R/bin/gdalinfo" --formats > "$WORK/raster-formats.txt" 2>&1 || fail "gdalinfo --formats"
"$R/bin/ogrinfo" --formats > "$WORK/vector-formats.txt" 2>&1 || fail "ogrinfo --formats"
cat "$WORK/raster-formats.txt" "$WORK/vector-formats.txt" > "$WORK/formats.txt"
# driver short names: the text before ' -<kinds>- (' on each line
sed -nE 's/^ +(.+) -[^()]*- \(.*$/\1/p' "$WORK/formats.txt" | sort -u > "$WORK/drivers.txt"
nr="$(sed -nE 's/^ +(.+) -[^()]*- \(.*$/\1/p' "$WORK/raster-formats.txt" | sort -u | wc -l | tr -d ' ')"
nv="$(sed -nE 's/^ +(.+) -[^()]*- \(.*$/\1/p' "$WORK/vector-formats.txt" | sort -u | wc -l | tr -d ' ')"
for d in ECW JP2ECW MrSID JP2MrSID JP2KAK JP2Lura JP2Grok FileGDB MySQL ODBC MSSQLSpatial PGeo OCI; do
    grep -qxF "$d" "$WORK/drivers.txt" && fail "the driver list contains $d (not licence-clean)"
done
n_lic=0
for dep in gdal proj geos libgeotiff libtiff sqlite openssl curl zlib libdeflate zstd xz lz4 c-blosc libpng libjpeg-turbo libwebp lerc expat openjpeg libaec hdf5 netcdf-c; do
    [ -n "$(find "$R/licenses/$dep" -type f -size +0 2> /dev/null | head -n 1)" ] || fail "licenses/$dep is missing or empty"
    n_lic=$((n_lic + 1))
done
ok "licence guard part 1: $n_lic licence directories non-empty; $nr raster and $nv vector drivers, none of ECW/MrSID/KAK/Lura/Grok/FileGDB-SDK/MySQL/ODBC/MSSQL/PGeo/OCI"

# --- 3 required drivers
for d in GTiff COG VRT MEM netCDF HDF5 GRIB JP2OpenJPEG Zarr PNG JPEG WEBP MBTiles GPKG OpenFileGDB "ESRI Shapefile" GeoJSON GeoJSONSeq FlatGeobuf MVT PMTiles CSV KML GML; do
    grep -qxF "$d" "$WORK/drivers.txt" || fail "required driver $d is missing"
done
ok "the 24 required drivers are registered"

# --- 4 ELF closure: RUNPATH, NEEDED, banned libraries, SONAME uniqueness; then auditwheel on a throwaway wheel of the tree
$PY - "$R" "$BANNED" "$ARCH" <<'PYEOF' || exit 1
import os, posixpath, re, subprocess, sys

root, banned, arch = sys.argv[1], re.compile(sys.argv[2], re.I), sys.argv[3]
policy = re.compile(r"^(libc|libm|libdl|librt|libpthread|libstdc\+\+|libgcc_s|libutil|libresolv)\.so\.\d+$|^ld-linux-[\w-]+\.so\.\d+$")
libs = set(os.listdir(os.path.join(root, "lib")))

def dyn(path):
    out = subprocess.run(["readelf", "-d", path], capture_output=True, text=True).stdout
    get = lambda tag: [l.split("[", 1)[1].rsplit("]", 1)[0] for l in out.splitlines() if f"({tag})" in l]
    return get("NEEDED"), get("SONAME"), get("RPATH"), get("RUNPATH")

def fail(msg):
    print("VERIFY FAILED: " + msg)
    sys.exit(1)

sonames, n = {}, 0
for dp, _d, files in os.walk(root):
    for name in files:
        path = os.path.join(dp, name)
        rel = os.path.relpath(path, root)
        if os.path.islink(path) or open(path, "rb").read(4) != b"\x7fELF":
            continue
        n += 1
        needed, so, rpath, runpath = dyn(path)
        want = posixpath.relpath("lib", posixpath.dirname(rel))
        want = "$ORIGIN" if want == "." else "$ORIGIN/" + want
        if rpath:
            fail(f"{rel} has an RPATH {rpath}")
        if runpath != [want]:
            fail(f"{rel} has RUNPATH {runpath}, wanted ['{want}']")
        if banned.search(name):
            fail(f"{rel} matches the licence guard")
        for dep in needed:
            if banned.search(dep):
                fail(f"{rel} needs {dep}, which matches the licence guard")
            if dep not in libs and not policy.match(dep):
                fail(f"{rel} needs {dep}: not in lib/ and not in the manylinux policy set")
        if rel.startswith("lib/") and so:
            if so[0] in sonames:
                fail(f"{rel} and {sonames[so[0]]} share SONAME {so[0]}")
            sonames[so[0]] = rel
        if rel.startswith("lib/") and not so and name != "libgdalalljni.so":
            fail(f"{rel} has no SONAME")
        if rel.startswith("lib/") and so and so[0] != name:
            fail(f"{rel} is not named after its SONAME {so[0]}")
print(f"ok: ELF closure: {n} ELF files, each RUNPATH $ORIGIN-relative to lib/, each NEEDED in lib/ or the manylinux set, "
      f"{len(sonames)} SONAMEs unique and equal to the file names")
PYEOF

W="$WORK/gbxprobe-0"
mkdir -p "$W/gbxprobe/gdal-native" "$W/gbxprobe-0.dist-info"
cp -a "$R/lib" "$R/bin" "$R/python" "$W/gbxprobe/gdal-native/"
rm -rf "$W/gbxprobe/gdal-native/lib/cmake" "$W/gbxprobe/gdal-native/lib/pkgconfig"
: > "$W/gbxprobe/__init__.py"
printf 'Metadata-Version: 2.1\nName: gbxprobe\nVersion: 0\n' > "$W/gbxprobe-0.dist-info/METADATA"
printf 'Wheel-Version: 1.0\nGenerator: verify-native\nRoot-Is-Purelib: false\nTag: cp312-cp312-linux_%s\n' "$ARCH" > "$W/gbxprobe-0.dist-info/WHEEL"
$PY -m wheel pack "$W" --dest-dir "$WORK" > "$WORK/wheel-pack.txt" 2>&1 || fail "could not build the throwaway wheel: $(tail -c 300 "$WORK/wheel-pack.txt")"
WHL="$WORK/gbxprobe-0-cp312-cp312-linux_${ARCH}.whl"
auditwheel show "$WHL" 2>&1 | sed -e 's/^/    auditwheel: /'
auditwheel show --json "$WHL" > "$WORK/auditwheel.json" 2> /dev/null
$PY - "$WORK/auditwheel.json" "$ARCH" <<'PYEOF' || fail "auditwheel does not report consistency with manylinux_2_28_${ARCH}: $(head -c 400 "$WORK/auditwheel.json")"
import json, sys
r = json.load(open(sys.argv[1]))
tag = f"manylinux_2_28_{sys.argv[2]}"
assert r.get("overall_tag") == tag, r.get("overall_tag")
assert not r.get("external_libs"), r.get("external_libs")
PYEOF
ok "auditwheel: the tree is consistent with manylinux_2_28_${ARCH}, no external shared libraries"

# --- 5 JNI: the Java classes and the JNI library from the same build. JniSmoke is compiled first and put on the class
# path next to the jar: the source-file launcher loads it in a separate class loader, and a library loaded by System.load
# there is invisible to the classes in the jar (UnsatisfiedLinkError on the first native call).
JAR="$R/java/gdal-${GDAL_VERSION}.jar"
mkdir -p "$WORK/jni"
"$JAVA_HOME/bin/javac" -cp "$JAR" -d "$WORK/jni" "$HERE/JniSmoke.java" > "$WORK/javac.txt" 2>&1 || fail "javac JniSmoke: $(head -c 400 "$WORK/javac.txt")"
# a JVM under Rosetta can deadlock while idle: bound the run, and say so when it times out
timeout 600 "$JAVA_HOME/bin/java" -cp "$JAR:$WORK/jni" JniSmoke "$R/lib" "$R/share" "$GDAL_VERSION" > "$WORK/jni.txt" 2>&1
[ $? -ne 124 ] || fail "JniSmoke timed out after 600 s (on an emulated architecture the JVM can deadlock: run the command again once)"
grep -q "JNI_SMOKE_OK ${GDAL_VERSION}" "$WORK/jni.txt" || fail "JniSmoke did not print JNI_SMOKE_OK ${GDAL_VERSION}: $(head -c 600 "$WORK/jni.txt")"
if grep -q "Native library load failed" "$WORK/jni.txt"; then nl="the bindings' own 'Native library load failed' line appears (expected and harmless: their loadLibrary finds nothing on java.library.path, the absolute System.load already bound the library)"; else nl="no 'Native library load failed' line"; fi
ok "JNI smoke: $(grep JNI_SMOKE_OK "$WORK/jni.txt"); $nl"

# --- 6 osgeo from the same build: the version, the library it loaded, a FileGDB write and read (defect Task 19 Step 1)
WANT="$GDAL_VERSION" ROOT="$R" PYTHONPATH="$R/python" $PY -c "
from osgeo import gdal, ogr, osr; import os, tempfile
gdal.UseExceptions(); ogr.UseExceptions(); osr.UseExceptions()
want = os.environ['WANT']; root = os.environ['ROOT']
assert gdal.__version__ == want, gdal.__version__
assert gdal.VersionInfo('RELEASE_NAME') == want, gdal.VersionInfo('RELEASE_NAME')
mapped = sorted({l.split()[-1] for l in open('/proc/self/maps') if 'libgdal.so' in l})
assert mapped and all(m.startswith(root + '/lib/') for m in mapped), mapped
d = os.path.join(tempfile.mkdtemp(), 't.gdb'); drv = ogr.GetDriverByName('OpenFileGDB')
ds = drv.CreateDataSource(d); sr = osr.SpatialReference(); sr.ImportFromEPSG(4326)
lyr = ds.CreateLayer('p', sr, ogr.wkbPoint); f = ogr.Feature(lyr.GetLayerDefn())
f.SetGeometry(ogr.CreateGeometryFromWkt('POINT (1 2)')); lyr.CreateFeature(f); ds = None
back = ogr.Open(d)  # keep the dataset: a layer of a dataset that was garbage-collected is a dangling pointer
assert back.GetLayer(0).GetFeatureCount() == 1
srs = osr.SpatialReference(); assert srs.ImportFromEPSG(27700) == 0
print('osgeo', gdal.__version__, 'libgdal from', mapped[0])
" > "$WORK/osgeo.txt" 2>&1 || fail "osgeo / FileGDB write: $(tail -c 600 "$WORK/osgeo.txt")"
ok "$(cat "$WORK/osgeo.txt") (FileGDB write and read, EPSG:27700 via PROJ data)"

# --- 7 HTTPS from our libcurl + OpenSSL: no CA path is compiled in, so trust arrives only through GDAL_CURL_CA_BUNDLE.
# A local server with a throwaway CA stands in for the network. The CA is also put in the SYSTEM trust store, so a
# system curl accepts the server (control) while our libcurl must still refuse it until the variable is set.
TLS="$WORK/tls"
mkdir -p "$TLS/www"
(
    cd "$TLS" || exit 1
    openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 2 -subj "/CN=gbx-verify-ca"
    openssl req -newkey rsa:2048 -nodes -keyout srv.key -out srv.csr -subj "/CN=localhost"
    printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n' > ext.cnf
    openssl x509 -req -in srv.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out srv.pem -days 2 -extfile ext.cnf
) > "$WORK/tls.log" 2>&1 || fail "could not make the test CA: $(tail -c 400 "$WORK/tls.log")"
PYTHONPATH="$R/python" $PY -c "
from osgeo import gdal
gdal.UseExceptions(); ds = gdal.GetDriverByName('GTiff').Create('$TLS/www/t.tif', 64, 48, 1); ds = None" || fail "could not write the test GeoTIFF"
cat > "$WORK/server.py" <<'EOF'
import http.server, ssl, sys
d, cert, key, portfile = sys.argv[1:5]
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=d, **k)
    def log_message(self, *a):
        pass
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
open(portfile, "w").write(str(srv.server_address[1]))
srv.serve_forever()
EOF
$PY "$WORK/server.py" "$TLS/www" "$TLS/srv.pem" "$TLS/srv.key" "$TLS/port" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
disown "$SERVER_PID"
for _ in $(seq 1 50); do [ -s "$TLS/port" ] && break; sleep 0.2; done
[ -s "$TLS/port" ] || fail "the local HTTPS server did not start: $(tail -c 300 "$WORK/server.log")"
PORT="$(cat "$TLS/port")"
# the system bundle at the three places a distribution keeps it (Ubuntu, RHEL, OpenSSL's cert.pem), now containing the test CA
# (and /opt/_internal/certs.pem, the file this image's own curl reads)
for sys_bundle in /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/cert.pem /opt/_internal/certs.pem; do
    mkdir -p "$(dirname "$sys_bundle")" && { cat /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem; echo; cat "$TLS/ca.pem"; } > "$sys_bundle" || fail "could not write the system bundle $sys_bundle"
done
curl -sS --max-time 10 -o /dev/null "https://localhost:$PORT/t.tif" 2> "$WORK/syscurl.txt" || fail "control: the system curl does not trust the server through the system store: $(cat "$WORK/syscurl.txt")"
cat > "$WORK/vsicurl.py" <<'EOF'
import ctypes, os, sys
from osgeo import gdal
gdal.UseExceptions()
port, root = sys.argv[1], sys.argv[2]
gdal.SetConfigOption("CPL_CURL_VERBOSE", "YES")
try:
    ds = gdal.Open(f"/vsicurl/https://localhost:{port}/t.tif")
    print("OPENED", ds.RasterXSize, ds.RasterYSize)
except RuntimeError as e:
    print("REFUSED", str(e).replace("\n", " ")[:200])
v = ctypes.CDLL(os.path.join(root, "lib", "libcurl.so.4"))
v.curl_version.restype = ctypes.c_char_p
print("CURL", v.curl_version().decode())
maps = sorted({l.split()[-1] for l in open("/proc/self/maps") if "libssl" in l or "libcrypto" in l})
print("TLS_LIBS", " ".join(maps))
EOF
PYTHONPATH="$R/python" $PY "$WORK/vsicurl.py" "$PORT" "$R" > "$WORK/vsicurl-noca.txt" 2>&1
grep -q "^REFUSED" "$WORK/vsicurl-noca.txt" || fail "without GDAL_CURL_CA_BUNDLE our libcurl opened an HTTPS URL whose CA only the system store trusts (a CA path is compiled in): $(head -c 400 "$WORK/vsicurl-noca.txt")"
grep -qi "certificate" "$WORK/vsicurl-noca.txt" || fail "the refusal does not name a certificate problem: $(head -c 400 "$WORK/vsicurl-noca.txt")"
GDAL_CURL_CA_BUNDLE="$TLS/ca.pem" PYTHONPATH="$R/python" $PY "$WORK/vsicurl.py" "$PORT" "$R" > "$WORK/vsicurl-ca.txt" 2>&1
grep -q "^OPENED 64 48" "$WORK/vsicurl-ca.txt" || fail "with GDAL_CURL_CA_BUNDLE the /vsicurl/ HTTPS read failed: $(grep -v '^\*\|^[<>]' "$WORK/vsicurl-ca.txt" | head -c 500)"
grep "^CURL" "$WORK/vsicurl-ca.txt" | grep -q "OpenSSL/4" || fail "our libcurl is not built on OpenSSL 4: $(grep '^CURL' "$WORK/vsicurl-ca.txt")"
tls="$(grep '^TLS_LIBS' "$WORK/vsicurl-ca.txt")"
case "$tls" in *"$R/lib/libssl.so.4"*) ;; *) fail "the TLS libraries in use are not ours: $tls" ;; esac
case "$tls" in *"libssl.so.1.1"*|*"libssl.so.3"*) fail "a system libssl was loaded as well: $tls" ;; esac
ok "HTTPS /vsicurl/ read: refused without a CA although the system store trusts it, read with GDAL_CURL_CA_BUNDLE; $(grep '^CURL' "$WORK/vsicurl-ca.txt")"

# --- 8 the dev kit: a C program against include/ + lib-dev/, through pkg-config, through gdal-config, and through cmake
[ -f "$R/include/gdal.h" ] && [ -f "$R/include/proj.h" ] && [ -f "$R/include/geos_c.h" ] || fail "include/ lacks gdal.h, proj.h or geos_c.h"
cat > "$WORK/t.c" <<'EOF'
#include <gdal.h>
#include <proj.h>
#include <geos_c.h>
#include <stdio.h>
int main(void) { GDALAllRegister(); printf("GDAL %s PROJ %d.%d.%d GEOS %s\n", GDALVersionInfo("RELEASE_NAME"), PROJ_VERSION_MAJOR, PROJ_VERSION_MINOR, PROJ_VERSION_PATCH, GEOS_VERSION); return 0; }
EOF
want_line="GDAL ${GDAL_VERSION} PROJ ${PROJ_VERSION} GEOS ${GEOS_VERSION}"
PKG_CONFIG_PATH="$R/lib/pkgconfig" pkg-config --exists gdal proj geos || fail "pkg-config cannot find gdal, proj and geos"
# shellcheck disable=SC2046
cc "$WORK/t.c" -o "$WORK/t-pc" $(PKG_CONFIG_PATH="$R/lib/pkgconfig" pkg-config --cflags --libs gdal proj geos) -Wl,-rpath,"$R/lib" 2> "$WORK/cc.txt" || fail "pkg-config build: $(head -c 500 "$WORK/cc.txt")"
got="$(env -i "$WORK/t-pc")" && [ "$got" = "$want_line" ] || fail "the pkg-config program printed '$got', wanted '$want_line'"
# shellcheck disable=SC2046
cc "$WORK/t.c" -o "$WORK/t-gc" $("$R/bin/gdal-config" --cflags) $("$R/bin/geos-config" --cflags) $("$R/bin/gdal-config" --libs) $("$R/bin/geos-config" --clibs) -lproj -Wl,-rpath,"$R/lib" 2> "$WORK/cc.txt" || fail "gdal-config build: $(head -c 500 "$WORK/cc.txt")"
got="$(env -i "$WORK/t-gc")" && [ "$got" = "$want_line" ] || fail "the gdal-config program printed '$got', wanted '$want_line'"
for flags in "gdal-config --libs" "geos-config --clibs" "nc-config --libs"; do
    case "$("$R/bin/${flags%% *}" ${flags#* })" in *"-L$R/lib-dev"*) ;; *) fail "'$flags' does not point -L at $R/lib-dev: $("$R/bin/${flags%% *}" ${flags#* })" ;; esac
done
[ "$("$R/bin/gdal-config" --prefix)" = "$R" ] || fail "gdal-config --prefix is '$("$R/bin/gdal-config" --prefix)', wanted $R"
mkdir -p "$WORK/cm"
printf 'cmake_minimum_required(VERSION 3.20)\nproject(t C)\nfind_package(GDAL CONFIG REQUIRED)\nadd_executable(t t.c)\ntarget_link_libraries(t PRIVATE GDAL::GDAL)\nset_target_properties(t PROPERTIES BUILD_RPATH "%s/lib")\n' "$R" > "$WORK/cm/CMakeLists.txt"
printf '#include <gdal.h>\n#include <stdio.h>\nint main(void) { GDALAllRegister(); printf("GDAL %%s\\n", GDALVersionInfo("RELEASE_NAME")); return 0; }\n' > "$WORK/cm/t.c"
{ cmake -S "$WORK/cm" -B "$WORK/cm/b" -G Ninja -DCMAKE_PREFIX_PATH="$R" && cmake --build "$WORK/cm/b"; } > "$WORK/cmake.txt" 2>&1 || fail "cmake find_package(GDAL CONFIG) against the tree: $(tail -c 700 "$WORK/cmake.txt")"
[ "$(env -i "$WORK/cm/b/t")" = "GDAL ${GDAL_VERSION}" ] || fail "the cmake-built program did not report GDAL ${GDAL_VERSION}"
ok "dev kit: C program built against include/ + lib-dev/ via pkg-config, via gdal-config and via cmake find_package(GDAL), run with no library path: $want_line"

echo "VERIFY OK arch=$ARCH gdal=$GDAL_VERSION raster_drivers=$nr vector_drivers=$nv"
