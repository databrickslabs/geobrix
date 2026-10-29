#!/bin/bash
# verify-native.sh [root] [debuginfo.tar.xz]: the oracle for `gbx:build:native --target stage-native`. Runs in the
# toolchain image with the staged tree mounted (default /gdal-native), its debuginfo tarball (default /debuginfo.tar.xz)
# and this directory mounted at /verify, under `--network none`.
# Prints `ok: <check>` per check and stops at the first `VERIFY FAILED: <what>`; the last line of a good run is `VERIFY OK`.
#   1 versions, tools      2 licence guard      3 required drivers
#   4 ELF closure, hashed SONAMEs (none equals a system name), auditwheel, and libgdal loading after the host's sqlite and curl
#   5 JNI smoke            6 osgeo against the one GDAL: FileGDB, iconv code pages (CP1252, CP932), GRIB AEC, GDAL_DATA
#   7 HTTPS through GDAL_CURL_CA_BUNDLE (no compiled-in CA path)
#   8 the dev kit: headers, pkg-config, gdal-config, cmake and a C program built against include/ + lib-dev/
#   9 libjpeg-turbo SIMD     10 JPEG XL: the JPEGXL driver, GTiff COMPRESS=JXL lossless and lossy (Byte, UInt16, Float32)
#  11 REGEXP (PCRE2): SQLite dialect, SQLite SQL, a GPKG through INDIRECT_SQLITE   12 muparser: `gdal raster calc`
#  13 the PROJ grids: files, licences, OSTN15 against Ordnance Survey's test points (and not the Helmert fallback),
#     OSGM15 and EGM96 heights both ways, PROJ_NETWORK off      14 projsync      15 curl with HTTP/2 (nghttp2)
#  16 the HDF5 filter plugins     17 split debug symbols: none in the tree, a debug file per build-id, one address symbolized
set -uo pipefail

R="$(cd "${1:-/gdal-native}" && pwd)"
DEBUGINFO="${2:-/debuginfo.tar.xz}"
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

for d in lib lib-dev bin share/gdal share/proj java python/osgeo python/osgeo_utils licenses include lib/pkgconfig lib/cmake hdf5-plugins; do
    [ -d "$R/$d" ] || fail "the tree has no $d/"
done
[ -f "$R/MANIFEST.json" ] || fail "no MANIFEST.json"
[ -f "$R/lib/libgdalalljni.so" ] || fail "no lib/libgdalalljni.so"
# the SONAMEs are hashed (libgdal-<hash>.so.39): the stager recorded original -> hashed in MANIFEST.json
sonm() { $PY -c 'import json, sys; print(next(v for k, v in json.load(open(sys.argv[1]))["sonames"].items() if k.startswith(sys.argv[2])))' "$R/MANIFEST.json" "$1" 2> /dev/null; }
LIBGDAL="$(sonm libgdal.so.)"; LIBCURL="$(sonm libcurl.so.)"; LIBSSL="$(sonm libssl.so.)"
[ -n "$LIBGDAL" ] && [ -n "$LIBCURL" ] && [ -n "$LIBSSL" ] || fail "MANIFEST.json has no SONAME map (sonames) for libgdal, libcurl and libssl"
[ -f "$R/lib/$LIBGDAL" ] && [ -f "$R/lib/$LIBCURL" ] && [ -f "$R/lib/$LIBSSL" ] || fail "MANIFEST.json names $LIBGDAL, $LIBCURL, $LIBSSL, which are not all in lib/"
[ -f "$R/java/gdal-${GDAL_VERSION}.jar" ] || fail "no java/gdal-${GDAL_VERSION}.jar"
if [ -n "$(find "$R/lib" -maxdepth 1 -type l)" ]; then fail "lib/ holds symlinks: $(find "$R/lib" -maxdepth 1 -type l -printf '%f ' | head -c 200)"; fi

# --- 1 versions and tools (no GDAL_DATA, PROJ_DATA or LD_LIBRARY_PATH set: the tree finds its own data and libraries)
v="$("$R/bin/gdalinfo" --version 2>&1)" || fail "gdalinfo --version: $v"
# GDAL 3.13 prints `GDAL 3.13.3 "Iowa City", released 2026/08/13`: the version, then the release name
case "$v" in "GDAL ${GDAL_VERSION},"*|"GDAL ${GDAL_VERSION} "*) ;; *) fail "gdalinfo reports '$v', wanted 'GDAL ${GDAL_VERSION}' first" ;; esac
"$R/bin/projinfo" EPSG:27700 > "$WORK/projinfo.txt" 2>&1 || fail "projinfo EPSG:27700: $(head -c 300 "$WORK/projinfo.txt")"
grep -q "tmerc" "$WORK/projinfo.txt" || fail "projinfo EPSG:27700 printed no transverse Mercator definition"
"$R/bin/ogrinfo" --version | grep -q "^GDAL ${GDAL_VERSION}[ ,]" || fail "ogrinfo version"
ok "gdalinfo '${v%%,*}', projinfo EPSG:27700 (the tools find the GDAL and PROJ data relative to the tree; osgeo and the JNI need GDAL_DATA set, step 6)"

# --- 2 licence guard: libraries, NEEDED, driver lists, licence files
BANNED='poppler|podofo|pdfium|rttopo|spatialite|mysql|mariadb|FileGDBAPI|ecw|ltidsdk|kdu|lura|grok|x265|odbc|clntsh|sfcgal|CGAL|lcms2_fast_float|lcms2_threaded'
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
ADDED="nghttp2 pcre2 muparser highway brotli lcms2 libjxl proj-grids"
for dep in gdal proj geos libgeotiff libtiff sqlite openssl curl zlib libdeflate zstd xz lz4 c-blosc libpng libjpeg-turbo libwebp lerc expat openjpeg libaec hdf5 netcdf-c $ADDED; do
    [ -n "$(find "$R/licenses/$dep" -type f -size +0 2> /dev/null | head -n 1)" ] || fail "licenses/$dep is missing or empty"
    n_lic=$((n_lic + 1))
done
ok "licence guard part 1: $n_lic licence directories non-empty; $nr raster and $nv vector drivers, none of ECW/MrSID/KAK/Lura/Grok/FileGDB-SDK/MySQL/ODBC/MSSQL/PGeo/OCI"
# the libraries this build adds: permissive licences only (no GPL family), libjxl's patent grant and netCDF-C's embedded bzip2
# licence (its bzip2 filter plugin) present
for dep in $ADDED; do
    if grep -rlE "GNU (Lesser |Library |Affero )?General Public License" "$R/licenses/$dep" > /dev/null; then
        fail "licenses/$dep names a GPL-family licence: $(grep -rlE 'GNU (Lesser |Library |Affero )?General Public License' "$R/licenses/$dep" | head -3)"
    fi
done
[ -s "$R/licenses/libjxl/PATENTS" ] || fail "licenses/libjxl/PATENTS (the patent grant) is missing"
[ -s "$R/licenses/netcdf-c/BZIP2_LICENSE" ] || fail "licenses/netcdf-c/BZIP2_LICENSE (the bzip2 filter plugin) is missing"
grep -q "BSD-2-Clause" "$R/licenses/proj-grids/NOTICE.txt" && grep -q "Public-Domain" "$R/licenses/proj-grids/NOTICE.txt" || fail "licenses/proj-grids/NOTICE.txt does not list the grids' licences"
ok "licence guard part 2: the added libraries ($ADDED) carry a licence and none is GPL-family (libjxl PATENTS, netCDF-C BZIP2_LICENSE present; lcms2's GPL plugins are banned names)"

# --- 3 required drivers
REQUIRED=(GTiff COG VRT MEM netCDF HDF5 GRIB JP2OpenJPEG Zarr PNG JPEG WEBP MBTiles GPKG OpenFileGDB "ESRI Shapefile" GeoJSON GeoJSONSeq FlatGeobuf MVT PMTiles CSV KML GML JPEGXL)
for d in "${REQUIRED[@]}"; do
    grep -qxF "$d" "$WORK/drivers.txt" || fail "required driver $d is missing"
done
ok "the ${#REQUIRED[@]} required drivers are registered (JPEGXL included)"

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
# hashed names: a bundled library shares no SONAME with a system library (the loader reuses an already-loaded library of the
# same SONAME, so a bundled libsqlite3.so.0 or libcurl.so.4 would be shadowed by, or would shadow, the host's)
import json
hashed = re.compile(r"^.+-[0-9a-f]{8}\.so(\..*)?$")
manifest = json.load(open(os.path.join(root, "MANIFEST.json")))
system = set()
for d in ("/lib64", "/usr/lib64", "/lib", "/usr/lib", "/usr/local/lib"):
    for dp, _dirs, files in os.walk(d):
        system.update(files)
for so, rel in sorted(sonames.items()):
    if os.path.basename(rel) == "libgdalalljni.so":
        continue
    if not hashed.match(so):
        fail(f"{rel} has SONAME {so}, which has no -<sha256[:8]> infix (it would share a name with a system library)")
    if so in system or policy.match(so):
        fail(f"bundled SONAME {so} is also the name of a system library in this image")
mapped = manifest.get("sonames", {})
if sorted(mapped.values()) != sorted(os.path.basename(r) for s, r in sonames.items() if os.path.basename(r) != "libgdalalljni.so"):
    fail("MANIFEST.json sonames (original -> hashed) does not match the libraries in lib/")
for old in mapped:
    if old in libs:
        fail(f"the original name {old} is still in lib/")
print(f"ok: ELF closure: {n} ELF files, each RUNPATH $ORIGIN-relative to lib/, each NEEDED in lib/ or the manylinux set, "
      f"{len(sonames)} SONAMEs unique and equal to the file names; {len(mapped)} libraries have hashed SONAMEs "
      f"(<stem>-<sha256[:8]>.so<rest>), none equal to a system library name; the JNI keeps libgdalalljni.so")
PYEOF

W="$WORK/gbxprobe-0"
mkdir -p "$W/gbxprobe/gdal-native" "$W/gbxprobe-0.dist-info"
cp -a "$R/lib" "$R/bin" "$R/python" "$R/hdf5-plugins" "$W/gbxprobe/gdal-native/"
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

# libgdal loaded after the HOST's sqlite and curl (what a Python process that imported sqlite3 and used libcurl looks like).
# With shared SONAMEs this fails to load here (undefined symbol: sqlite3_error_offset / curl_multi_poll) or runs on the host's
# SQLite and curl; with hashed names GDAL keeps its own.
cat > "$WORK/hostfirst.py" <<'EOF'
import ctypes, os, sys
root, libgdal, libcurl = os.environ["ROOT"], os.environ["LIBGDAL"], os.environ["LIBCURL"]
host = {}
for name in ("libsqlite3.so.0", "libcurl.so.4"):
    path = "/usr/lib64/" + name
    ctypes.CDLL(path)
    host[name] = path
ctypes.CDLL(f"{root}/lib/{libgdal}")
sys.path.insert(0, f"{root}/python")
os.environ["GDAL_DATA"] = f"{root}/share/gdal"
from osgeo import gdal, ogr
gdal.UseExceptions(); ogr.UseExceptions()
ds = ogr.GetDriverByName("GPKG").CreateDataSource(os.environ["TMP"] + "/h.gpkg")
rs = ds.ExecuteSQL("SELECT sqlite_version()")
version = rs.GetNextFeature().GetField(0)
ds.ReleaseResultSet(rs)
assert version == os.environ["SQLITE_VERSION"], f"GDAL runs on SQLite {version}, not {os.environ['SQLITE_VERSION']}"
gdal.VSIStatL("/vsicurl/http://127.0.0.1:9/x")   # first curl use: GDAL compares the curl it was built against with the one it runs on
maps = sorted({l.split()[-1] for l in open("/proc/self/maps")})
ours = [m for m in maps if m.startswith(root + "/lib/") and ("libcurl" in m or "libsqlite3" in m)]
assert len(ours) == 2, ours
for name in host:   # the host copies are mapped too (as their real files, libsqlite3.so.0.8.6 ...), next to ours
    assert any(os.path.basename(m).startswith(name.split(".so")[0] + ".so") and not m.startswith(root) for m in maps), f"the host {name} was not loaded"
print("sqlite", version, "libcurl and libsqlite3 from", sorted(os.path.basename(m) for m in ours))
EOF
TMP="$WORK" ROOT="$R" LIBGDAL="$LIBGDAL" LIBCURL="$LIBCURL" SQLITE_VERSION="$SQLITE_VERSION" $PY "$WORK/hostfirst.py" > "$WORK/hostfirst.txt" 2>&1 || fail "libgdal after the host's libsqlite3.so.0 and libcurl.so.4: $(tail -c 500 "$WORK/hostfirst.txt")"
grep -q "Runtime failure is likely" "$WORK/hostfirst.txt" && fail "GDAL runs against a different curl than it was built with: $(grep -m1 'Runtime failure' "$WORK/hostfirst.txt")"
ok "libgdal loads after the host's libsqlite3.so.0 and libcurl.so.4, runs on its own SQLite ${SQLITE_VERSION} and curl ${CURL_VERSION} (hashed SONAMEs)"

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

# --- 6 osgeo from the same build: the version, the library it loaded, a FileGDB write and read (the FileGDB writer relies on it), the
# code pages that need iconv, GRIB's AEC (CCSDS) support, and the GDAL_DATA that osgeo has to be given (the JNI: JniSmoke)
cat > "$WORK/osgeo_check.py" <<'EOF'
import os, struct, tempfile
from osgeo import gdal, ogr, osr
gdal.UseExceptions(); ogr.UseExceptions(); osr.UseExceptions()
want = os.environ['WANT']; root = os.environ['ROOT']
assert gdal.__version__ == want, gdal.__version__
assert gdal.VersionInfo('RELEASE_NAME') == want, gdal.VersionInfo('RELEASE_NAME')
mapped = sorted({l.split()[-1] for l in open('/proc/self/maps') if 'libgdal' in l})
assert mapped and all(m.startswith(root + '/lib/') for m in mapped), mapped
tmp = tempfile.mkdtemp()
d = os.path.join(tmp, 't.gdb'); drv = ogr.GetDriverByName('OpenFileGDB')
ds = drv.CreateDataSource(d); sr = osr.SpatialReference(); sr.ImportFromEPSG(4326)
lyr = ds.CreateLayer('p', sr, ogr.wkbPoint); f = ogr.Feature(lyr.GetLayerDefn())
f.SetGeometry(ogr.CreateGeometryFromWkt('POINT (1 2)')); lyr.CreateFeature(f); ds = None
back = ogr.Open(d)  # keep the dataset: a layer of a dataset that was garbage-collected is a dangling pointer
assert back.GetLayer(0).GetFeatureCount() == 1
srs = osr.SpatialReference(); assert srs.ImportFromEPSG(27700) == 0
# iconv (glibc's): a CP1252 Shapefile is written and read back; a CP932 DBF (Shift-JIS, .cpg CP932) decodes to text
sdir = os.path.join(tmp, 'cp1252')
ds = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(sdir)
lyr = ds.CreateLayer('cp', None, ogr.wkbPoint, ['ENCODING=CP1252'])
fd = ogr.FieldDefn('name', ogr.OFTString); fd.SetWidth(32); lyr.CreateField(fd)
f = ogr.Feature(lyr.GetLayerDefn()); f.SetField('name', 'caf\u00e9 \u20ac \u00f1'); f.SetGeometry(ogr.CreateGeometryFromWkt('POINT (1 2)'))
lyr.CreateFeature(f); f = None; ds = None
back = ogr.Open(sdir)
assert back.GetLayer(0).GetNextFeature().GetField('name') == 'caf\u00e9 \u20ac \u00f1'
text = '\u6771\u4eac'  # Tokyo
rec = text.encode('cp932')
dbf = struct.pack('<B3BIHH', 3, 126, 10, 28, 1, 65, 11) + bytes(20)
dbf += b'NAME'.ljust(11, b'\0') + b'C' + bytes(4) + bytes([10, 0]) + bytes(14) + b'\r' + b' ' + rec.ljust(10, b' ') + b'\x1a'
open(os.path.join(tmp, 'jp.dbf'), 'wb').write(dbf); open(os.path.join(tmp, 'jp.cpg'), 'w').write('CP932')
back = ogr.Open(os.path.join(tmp, 'jp.dbf'))
got = back.GetLayer(0).GetNextFeature().GetField('NAME')
assert got == text, repr(got)
assert gdal.GetDriverByName('GRIB').GetMetadataItem('HAVE_AEC') == 'YES', 'GRIB was built without libaec (CCSDS)'
print('osgeo', gdal.__version__, 'libgdal from', mapped[0])
EOF
WANT="$GDAL_VERSION" ROOT="$R" PYTHONPATH="$R/python" $PY "$WORK/osgeo_check.py" > "$WORK/osgeo.txt" 2>&1 || fail "osgeo check: $(tail -c 700 "$WORK/osgeo.txt")"
# osgeo and the JNI do not find share/gdal on their own (the tools do), and GDAL fixes its data search path at the first lookup
# (importing osgeo.gdal registers the drivers, which does one), so SetConfigOption("GDAL_DATA") after the import has no effect:
# osgeo needs GDAL_DATA in the environment before the import, or gdal.PushFinderLocation(<share/gdal>); the JNI needs the
# config option before AllRegister() (JniSmoke does exactly that). Each case is its own process.
PYTHONPATH="$R/python" $PY -c "
from osgeo import gdal
assert gdal.FindFile('gdal', 'osmconf.ini') is None" > "$WORK/osgeo-nodata.txt" 2>&1 || fail "osgeo finds share/gdal without GDAL_DATA, or fails: $(tail -c 300 "$WORK/osgeo-nodata.txt")"
GDAL_DATA="$R/share/gdal" PYTHONPATH="$R/python" $PY -c "
import os
from osgeo import gdal
got = gdal.FindFile('gdal', 'osmconf.ini')
assert got == os.environ['GDAL_DATA'] + '/osmconf.ini', got" > "$WORK/osgeo-data.txt" 2>&1 || fail "osgeo does not find share/gdal with GDAL_DATA in the environment: $(tail -c 300 "$WORK/osgeo-data.txt")"
ROOT="$R" PYTHONPATH="$R/python" $PY -c "
import os
from osgeo import gdal
gdal.PushFinderLocation(os.environ['ROOT'] + '/share/gdal')
got = gdal.FindFile('gdal', 'osmconf.ini')
assert got == os.environ['ROOT'] + '/share/gdal/osmconf.ini', got" > "$WORK/osgeo-push.txt" 2>&1 || fail "osgeo does not find share/gdal after PushFinderLocation: $(tail -c 300 "$WORK/osgeo-push.txt")"
ok "$(cat "$WORK/osgeo.txt") (FileGDB write and read, EPSG:27700 via PROJ data, CP1252 Shapefile write/read and CP932 DBF decode via iconv, GRIB HAVE_AEC=YES; share/gdal found with GDAL_DATA in the environment or PushFinderLocation, not without)"

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
v = ctypes.CDLL(os.path.join(root, "lib", os.environ["LIBCURL"]))
v.curl_version.restype = ctypes.c_char_p
print("CURL", v.curl_version().decode())
maps = sorted({l.split()[-1] for l in open("/proc/self/maps") if "libssl" in l or "libcrypto" in l})
print("TLS_LIBS", " ".join(maps))
EOF
LIBCURL="$LIBCURL" PYTHONPATH="$R/python" $PY "$WORK/vsicurl.py" "$PORT" "$R" > "$WORK/vsicurl-noca.txt" 2>&1
grep -q "^REFUSED" "$WORK/vsicurl-noca.txt" || fail "without GDAL_CURL_CA_BUNDLE our libcurl opened an HTTPS URL whose CA only the system store trusts (a CA path is compiled in): $(head -c 400 "$WORK/vsicurl-noca.txt")"
grep -qi "certificate" "$WORK/vsicurl-noca.txt" || fail "the refusal does not name a certificate problem: $(head -c 400 "$WORK/vsicurl-noca.txt")"
GDAL_CURL_CA_BUNDLE="$TLS/ca.pem" LIBCURL="$LIBCURL" PYTHONPATH="$R/python" $PY "$WORK/vsicurl.py" "$PORT" "$R" > "$WORK/vsicurl-ca.txt" 2>&1
grep -q "^OPENED 64 48" "$WORK/vsicurl-ca.txt" || fail "with GDAL_CURL_CA_BUNDLE the /vsicurl/ HTTPS read failed: $(grep -v '^\*\|^[<>]' "$WORK/vsicurl-ca.txt" | head -c 500)"
grep "^CURL" "$WORK/vsicurl-ca.txt" | grep -q "OpenSSL/4" || fail "our libcurl is not built on OpenSSL 4: $(grep '^CURL' "$WORK/vsicurl-ca.txt")"
tls="$(grep '^TLS_LIBS' "$WORK/vsicurl-ca.txt")"
case "$tls" in *"$R/lib/$LIBSSL"*) ;; *) fail "the TLS libraries in use are not ours: $tls" ;; esac
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

# The rows below use osgeo from the tree, with its data directories given (as the payload activation gives them).
export GDAL_DATA="$R/share/gdal" PROJ_DATA="$R/share/proj"
pyo() { PYTHONPATH="$R/python" $PY "$@"; }

# --- 9 libjpeg-turbo SIMD (NEON on aarch64, NASM-built SSE2/AVX2 on x86_64): the SIMD dispatcher is compiled in (only it reads
# JSIMD_FORCENONE), and it is in use: forcing the scalar path with JSIMD_FORCENONE=1 makes JPEG-in-TIFF decoding slower
LIBJPEG="$(sonm libjpeg.so.)"
grep -qa JSIMD_FORCENONE "$R/lib/$LIBJPEG" || fail "libjpeg-turbo ($LIBJPEG) has no SIMD dispatcher: it was built WITH_SIMD=OFF"
cat > "$WORK/jpeg_simd.py" <<'EOF'
import os, statistics, time
import numpy as np
from osgeo import gdal
gdal.UseExceptions()
n = 2048
y, x = np.mgrid[0:n, 0:n].astype(np.float32)
img = np.clip(np.stack([127 + 100 * np.sin(x / 97.0) * np.cos(y / 61.0), 127 + 100 * np.sin((x + y) / 151.0),
                        127 + 60 * np.cos(x / 37.0)]) + np.random.default_rng(0).normal(0, 6, (3, n, n)), 0, 255).astype(np.uint8)
src = gdal.GetDriverByName("MEM").Create("", n, n, 3, gdal.GDT_Byte)
for b in range(3):
    src.GetRasterBand(b + 1).WriteArray(img[b])
opts = ["COMPRESS=JPEG", "PHOTOMETRIC=YCBCR", "JPEG_QUALITY=85", "TILED=YES"]
enc, dec = [], []
for i in range(4):
    t = time.perf_counter(); gdal.GetDriverByName("GTiff").CreateCopy("/vsimem/j.tif", src, options=opts).Close(); e = time.perf_counter() - t
    t = time.perf_counter(); ds = gdal.Open("/vsimem/j.tif"); ds.ReadRaster(); ds = None; d = time.perf_counter() - t
    if i:
        enc.append(e); dec.append(d)
print(f"{statistics.median(enc):.4f} {statistics.median(dec):.4f}")
EOF
simd="$(pyo "$WORK/jpeg_simd.py" 2>&1)" || fail "JPEG-in-TIFF timing: $simd"
scalar="$(JSIMD_FORCENONE=1 pyo "$WORK/jpeg_simd.py" 2>&1)" || fail "JPEG-in-TIFF timing (JSIMD_FORCENONE=1): $scalar"
# the gain is checked on the better of encode and decode (under x86_64 emulation decoding gains least), and neither may be slower
ratio="$($PY -c 'import sys; s, f = [list(map(float, a.split())) for a in sys.argv[1:3]]; print(f"{f[0] / s[0]:.2f} {f[1] / s[1]:.2f}")' "$simd" "$scalar")"
$PY -c 'import sys; e, d = map(float, sys.argv[1].split()); sys.exit(0 if max(e, d) >= 1.15 and min(e, d) >= 0.95 else 1)' "$ratio" \
    || fail "JPEG-in-TIFF is not faster with SIMD than with JSIMD_FORCENONE=1 (encode/decode s: SIMD $simd, scalar $scalar)"
ok "libjpeg-turbo SIMD: the dispatcher is compiled in, and 2048x2048 RGB JPEG-in-TIFF encodes ${ratio% *}x and decodes ${ratio#* }x faster than with JSIMD_FORCENONE=1 (encode/decode s, median of 3: SIMD $simd, scalar $scalar)"

# --- 10 JPEG XL: the JPEGXL driver writes and reads a .jxl, and the GTiff JXL codec (GDAL's internal libtiff) round-trips
# Byte, UInt16 and Float32 losslessly, and lossily (JXL_LOSSLESS=NO) to a smaller file with a small error. Lossy JXL is
# perceptual: a Float32 band is nominally 0..1 (the same field scaled to 0..1000 comes back up to 92% off), so the lossy
# Float32 row is 0..1; lossless is exact at any range.
cat > "$WORK/jxl.py" <<'EOF'
import numpy as np
from osgeo import gdal
gdal.UseExceptions()
n = 256
y, x = np.mgrid[0:n, 0:n].astype(np.float64)
base = 0.5 + 0.4 * np.sin(x / 13.0) * np.cos(y / 17.0) + np.random.default_rng(1).normal(0, 0.01, (n, n))
out = []
for name, dt, scale in (("Byte", gdal.GDT_Byte, 255), ("UInt16", gdal.GDT_UInt16, 65535), ("Float32", gdal.GDT_Float32, 1.0)):
    a = (np.clip(base, 0, 1) * scale).astype({"Byte": np.uint8, "UInt16": np.uint16, "Float32": np.float32}[name])
    src = gdal.GetDriverByName("MEM").Create("", n, n, 1, dt)
    src.GetRasterBand(1).WriteArray(a)
    sizes = {}
    for mode, opts in (("lossless", ["COMPRESS=JXL", "JXL_LOSSLESS=YES"]), ("lossy", ["COMPRESS=JXL", "JXL_LOSSLESS=NO", "JXL_DISTANCE=1.0"])):
        path = f"/vsimem/{name}-{mode}.tif"
        gdal.GetDriverByName("GTiff").CreateCopy(path, src, options=opts).Close()
        ds = gdal.Open(path)
        assert ds.GetMetadataItem("COMPRESSION", "IMAGE_STRUCTURE") == "JXL", (name, mode, ds.GetMetadata("IMAGE_STRUCTURE"))
        b = ds.GetRasterBand(1).ReadAsArray()
        sizes[mode] = gdal.VSIStatL(path).size
        err = float(np.abs(b.astype(np.float64) - a.astype(np.float64)).max()) / scale
        if mode == "lossless":
            assert np.array_equal(a, b), (name, "lossless round trip differs", err)
        else:
            assert 0 < err < 0.05, (name, "lossy error", err)
    assert sizes["lossy"] < sizes["lossless"], (name, sizes)
    out.append(f"{name} {sizes['lossless']}/{sizes['lossy']} B")
src = gdal.GetDriverByName("MEM").Create("", 64, 64, 3, gdal.GDT_Byte)
for i in range(3):
    src.GetRasterBand(i + 1).Fill(40 * (i + 1))
gdal.GetDriverByName("JPEGXL").CreateCopy("/vsimem/t.jxl", src, options=["LOSSLESS=YES"]).Close()
back = gdal.Open("/vsimem/t.jxl")
assert back.GetDriver().ShortName == "JPEGXL" and back.RasterCount == 3 and back.GetRasterBand(2).Checksum() == src.GetRasterBand(2).Checksum()
print("; ".join(out))
EOF
jxl="$(pyo "$WORK/jxl.py" 2>&1)" || fail "JPEG XL: $(tail -c 600 <<< "$jxl")"
ok "JPEG XL: the JPEGXL driver writes and reads a .jxl; GTiff COMPRESS=JXL 256x256 lossless exact and lossy smaller with a small error (lossless/lossy bytes: $jxl)"

# --- 11 REGEXP (PCRE2): in the SQLite dialect over any OGR source (a Shapefile), in a SQLite database's own SQL, through
# VectorTranslate -dialect SQLite, and on a GPKG through the INDIRECT_SQLITE dialect. GDAL 3.13.3 compiles PCRE2 into its
# SQLite driver only (ogr/ogrsf_frmts/sqlite/CMakeLists.txt): a GPKG's own SQL has REGEXP only once that driver has run
# in the process (it registers its SQL functions as an SQLite auto-extension). Reported from a fresh process, not asserted.
cat > "$WORK/regexp.py" <<'EOF'
import sys
from osgeo import gdal, ogr
gdal.UseExceptions(); ogr.UseExceptions()
tmp, mode = sys.argv[1], sys.argv[2]
def make(drv, path):
    ds = ogr.GetDriverByName(drv).CreateDataSource(path)
    lyr = ds.CreateLayer("t", None, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for v in ("cafe", "car", "bar", "Cat"):
        f = ogr.Feature(lyr.GetLayerDefn()); f.SetField("name", v); f.SetGeometry(ogr.CreateGeometryFromWkt("POINT (0 0)")); lyr.CreateFeature(f)
    ds = None
def select(path, dialect):
    ds = ogr.Open(path)
    try:
        rs = ds.ExecuteSQL("SELECT name FROM t WHERE name REGEXP '^ca' ORDER BY name", dialect=dialect)
        got = [r.GetField(0) for r in rs]
        ds.ReleaseResultSet(rs)
        return got
    except RuntimeError as e:
        return str(e).splitlines()[0]
    finally:
        ds = None
want = ["cafe", "car"]
if mode == "fresh":  # nothing but a GPKG in this process
    make("GPKG", f"{tmp}/fresh.gpkg")
    native = select(f"{tmp}/fresh.gpkg", "")
    print("REGEXP available" if native == want else "no REGEXP")
    sys.exit(0)
for drv, path in (("ESRI Shapefile", f"{tmp}/r_shp"), ("SQLite", f"{tmp}/r.sqlite"), ("GPKG", f"{tmp}/r.gpkg")):
    make(drv, path)
got = {"Shapefile, SQLite dialect": select(f"{tmp}/r_shp", "SQLite"), "SQLite database": select(f"{tmp}/r.sqlite", ""),
       "GPKG, INDIRECT_SQLITE": select(f"{tmp}/r.gpkg", "INDIRECT_SQLITE")}
assert all(v == want for v in got.values()), got
opts = gdal.VectorTranslateOptions(format="GeoJSON", SQLStatement="SELECT name FROM t WHERE name REGEXP '^ca'", SQLDialect="SQLite")
gdal.VectorTranslate(f"{tmp}/r.geojson", f"{tmp}/r_shp", options=opts).Close()
gj = ogr.Open(f"{tmp}/r.geojson")  # keep the dataset: a layer of a collected dataset is a dangling pointer
n = gj.GetLayer(0).GetFeatureCount()
assert n == 2, n
native = select(f"{tmp}/r.gpkg", "")
print(f"{want} from {', '.join(got)} and VectorTranslate -dialect SQLite; a GPKG's own SQL after that: "
      + ("REGEXP available" if native == want else "no REGEXP"))
EOF
rx="$(pyo "$WORK/regexp.py" "$WORK" main 2>&1)" || fail "REGEXP: $(tail -c 500 <<< "$rx")"
fresh="$(pyo "$WORK/regexp.py" "$WORK" fresh 2>&1)" || fail "REGEXP (a GPKG in a fresh process): $(tail -c 300 <<< "$fresh")"
ok "REGEXP (PCRE2 ${PCRE2_VERSION}): $rx; a GPKG's own SQL in a fresh process: $fresh (GDAL builds PCRE2 into its SQLite driver only)"

# --- 12 muparser: `gdal raster calc --dialect=muparser` on a 2-band tile, (A-B)/(A+B) against numpy
cat > "$WORK/calc.py" <<'EOF'
import subprocess, sys
import numpy as np
from osgeo import gdal
gdal.UseExceptions()
tool, tmp = sys.argv[1], sys.argv[2]
rng = np.random.default_rng(2)
a = rng.uniform(1, 100, (64, 48)).astype(np.float32)
b = rng.uniform(1, 100, (64, 48)).astype(np.float32)
ds = gdal.GetDriverByName("GTiff").Create(f"{tmp}/ab.tif", 48, 64, 2, gdal.GDT_Float32)
ds.GetRasterBand(1).WriteArray(a); ds.GetRasterBand(2).WriteArray(b); ds = None
r = subprocess.run([tool, "raster", "calc", "-i", f"A={tmp}/ab.tif", "--calc", "(A[1]-A[2])/(A[1]+A[2])",
                    "--dialect", "muparser", "--ot", "Float32", "-o", f"{tmp}/ndvi.tif"], capture_output=True, text=True)
assert r.returncode == 0, r.stderr
out = gdal.Open(f"{tmp}/ndvi.tif")  # keep the dataset while its band is read
got = out.GetRasterBand(1).ReadAsArray()
want = (a - b) / (a + b)
err = float(np.abs(got - want).max())
assert got.shape == want.shape and err < 1e-6, err
print(f"{err:.1e}")
EOF
calc="$(pyo "$WORK/calc.py" "$R/bin/gdal" "$WORK" 2>&1)" || fail "gdal raster calc --dialect=muparser: $(tail -c 500 <<< "$calc")"
ok "muparser ${MUPARSER_VERSION}: gdal raster calc --dialect=muparser (A[1]-A[2])/(A[1]+A[2]) on a 2-band 48x64 tile equals numpy (max error $calc)"

# --- 13 the PROJ grids. Files: in share/proj with the pinned sha256 and the licence their copyright tag states. OSTN15:
# Ordnance Survey's published test points (OSTN15_TestInput/Output_OSGBtoETRS.txt in OS's OSTN15-NTv2.zip) transform
# EPSG:27700 -> ETRS89 to within a centimetre, and with the grid hidden PROJ falls back to a metre-level Helmert.
# Geoids: OSGM15 (ETRS89 -> ODN height) and EGM96 (WGS 84 -> EGM96 height), at a grid node the height moves by exactly
# the grid's value, both ways. PROJ_NETWORK is off (proj.ini), so nothing is fetched from the CDN.
for k in OSTN15 OSGM15 EGM96; do
    f="PROJGRID_${k}_FILE"; s="PROJGRID_${k}_SHA256"
    [ "$(sha256sum "$R/share/proj/${!f}" | cut -d' ' -f1)" = "${!s}" ] || fail "share/proj/${!f} is not the pinned file"
done
grep -qE "^network *= *off" "$R/share/proj/proj.ini" || fail "share/proj/proj.ini does not set network = off"
mkdir -p "$WORK/proj-nogrid"
find "$R/share/proj" -maxdepth 1 -type f ! -name "$PROJGRID_OSTN15_FILE" -exec cp {} "$WORK/proj-nogrid/" \;
cat > "$WORK/grids.py" <<'EOF'
import math, os, sys
import numpy as np
from osgeo import gdal, osr
gdal.UseExceptions(); osr.UseExceptions()
share = os.environ["PROJ_DATA"]
mode = sys.argv[1]
# (E, N) OSGB36 -> (lat, lon) ETRS89, from Ordnance Survey's OSTN15 NTv2 test data (TP01, TP09, TP16, TP26, TP40)
TP = [((91492.146, 11318.804), (49.92226393730, -6.29977752014)), ((530624.974, 178388.464), (51.48936564950, -0.11992557180)),
      ((357455.843, 383290.436), (53.34480280190, -2.64049320810)), ((256340.925, 664697.269), (55.85399952950, -4.29649016251)),
      ((395999.668, 1138728.951), (60.13308091660, -2.07382822798))]
def crs(code):
    s = osr.SpatialReference(); s.SetFromUserInput(code); s.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER); return s
ct = osr.CoordinateTransformation(crs("EPSG:27700"), crs("EPSG:4258"))
errs = []
for (e, n), (lat, lon) in TP:
    x, y, _ = ct.TransformPoint(e, n)
    errs.append(math.hypot((y - lat) * 111320.0, (x - lon) * 111320.0 * math.cos(math.radians(lat))))
if mode == "nogrid":
    print(f"{max(errs):.3f}")
    sys.exit(0)
assert max(errs) < 0.01, errs
assert osr.GetPROJEnableNetwork() is False
lic = {}
for name, want in (("uk_os_OSTN15_NTv2_OSGBtoETRS.tif", "BSD License"), ("uk_os_OSGM15_GB.tif", "BSD License"), ("us_nga_egm96_15.tif", "Public Domain")):
    tag = gdal.Open(os.path.join(share, name)).GetMetadataItem("TIFFTAG_COPYRIGHT") or ""
    assert want in tag, (name, tag)
    lic[name] = want
# a geoid at one of its nodes: h -> H moves by exactly the grid value N (H = h - N), and back
def node(grid):
    ds = gdal.Open(os.path.join(share, grid))
    gt, band = ds.GetGeoTransform(), ds.GetRasterBand(1)
    c, r = ds.RasterXSize // 2, ds.RasterYSize // 2
    v = band.ReadAsArray(c, r, 1, 1)[0, 0]
    while v == band.GetNoDataValue():
        c += 3; v = band.ReadAsArray(c, r, 1, 1)[0, 0]
    return gt[0] + (c + 0.5) * gt[1], gt[3] + (r + 0.5) * gt[5], float(v)
heights = []
for grid, src, dst in (("uk_os_OSGM15_GB.tif", "EPSG:4937", "EPSG:4258+5701"), ("us_nga_egm96_15.tif", "EPSG:4979", "EPSG:4326+5773")):
    lon, lat, n_ = node(grid)
    fwd = osr.CoordinateTransformation(crs(src), crs(dst))
    inv = osr.CoordinateTransformation(crs(dst), crs(src))
    h = 100.0
    _, _, hh = fwd.TransformPoint(lon, lat, h)
    _, _, back = inv.TransformPoint(lon, lat, hh)
    assert abs(hh - (h - n_)) < 1e-3 and abs(n_) > 1, (grid, h, hh, n_)
    assert abs(back - h) < 1e-3, (grid, back)
    heights.append(f"{grid.split('_')[2]} at ({lat:.4f}, {lon:.4f}): N {n_:.3f} m, h 100 -> H {hh:.3f} -> h {back:.3f}")
print(f"OSTN15 max error {max(errs) * 1000:.2f} mm over {len(TP)} OS test points; " + "; ".join(heights) + f"; licences {lic}")
EOF
grids="$(pyo "$WORK/grids.py" grid 2>&1)" || fail "PROJ grids: $(tail -c 700 <<< "$grids")"
helmert="$(PROJ_DATA="$WORK/proj-nogrid" pyo "$WORK/grids.py" nogrid 2>&1)" || fail "PROJ without the OSTN15 grid: $(tail -c 400 <<< "$helmert")"
$PY -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0.5 else 1)' "$helmert" || fail "without the OSTN15 grid the OS test points are still within ${helmert} m: the grid is not what is used"
ok "PROJ grids (OSTN15, OSGM15, EGM96; pinned sha256, network = off): $grids; with OSTN15 hidden, PROJ's Helmert fallback misses by up to $helmert m"

# --- 14 projsync: runs and lists its options; listing the CDN's files needs the network, which this run has none of
# (projsync prints its usage and exits 1 for --help: the usage is the check)
ps="$("$R/bin/projsync" --help 2>&1)"; rc=$?
{ grep -q -- "--list-files" <<< "$ps" && grep -q -- "--target-dir" <<< "$ps" && grep -q -- "--source-id" <<< "$ps"; } || fail "projsync --help: exit $rc: $(head -c 300 <<< "$ps")"
ok "projsync runs and prints its usage (--list-files, --source-id, --target-dir; exit $rc); --list-files not run: --network none (_selfcheck.natives() runs it where there is network)"

# --- 15 curl with HTTP/2: GDAL's libcurl is built with nghttp2 (with GDAL_HTTP_VERSION unset, curl offers h2 by ALPN)
cv="$(LIBCURL="$LIBCURL" ROOT="$R" $PY -c 'import ctypes, os
v = ctypes.CDLL(os.path.join(os.environ["ROOT"], "lib", os.environ["LIBCURL"])); v.curl_version.restype = ctypes.c_char_p
print(v.curl_version().decode())' 2>&1)"
grep -q "nghttp2/${NGHTTP2_VERSION}" <<< "$cv" || fail "libcurl has no nghttp2 ${NGHTTP2_VERSION}: $cv"
ok "curl HTTP/2: $cv"

# --- 16 the HDF5 filter plugins: netCDF-C's zstd, blosc and bzip2 (and the rest) in hdf5-plugins/, closure checked in 4
plugins="$(ls "$R/hdf5-plugins" | tr '\n' ' ')"
for p in zstd blosc bzip2; do [ -f "$R/hdf5-plugins/lib__nch5$p.so" ] || fail "hdf5-plugins/ has no lib__nch5$p.so ($plugins)"; done
ok "HDF5 filter plugins in hdf5-plugins/: $plugins"

# --- 17 split debug symbols: no shipped ELF file holds debug sections; each one's build-id (MANIFEST.json build_ids) has its
# .build-id/<xx>/<rest>.debug in the debuginfo tarball; and that file symbolizes an address of the shipped, patched libgdal
[ -f "$DEBUGINFO" ] || fail "no debuginfo tarball at $DEBUGINFO"
tar -tJf "$DEBUGINFO" > "$WORK/debuginfo.txt" || fail "cannot list $DEBUGINFO"
$PY - "$R" "$WORK/debuginfo.txt" <<'PYEOF' || exit 1
import json, os, re, subprocess, sys
root, listing = sys.argv[1], sys.argv[2]
have = {l.strip().removeprefix("./") for l in open(listing) if l.strip().endswith(".debug")}
ids = json.load(open(os.path.join(root, "MANIFEST.json")))["build_ids"]
n = 0
for dp, _d, files in os.walk(root):
    for name in files:
        path = os.path.join(dp, name)
        if os.path.islink(path) or open(path, "rb").read(4) != b"\x7fELF":
            continue
        rel = os.path.relpath(path, root)
        m = re.search(r"Build ID: ([0-9a-f]+)", subprocess.run(["readelf", "-n", path], capture_output=True, text=True).stdout)
        if not m or ids.get(rel) != m.group(1):
            sys.exit(f"VERIFY FAILED: {rel}: build-id {m.group(1) if m else None}, MANIFEST.json says {ids.get(rel)}")
        if f".build-id/{m.group(1)[:2]}/{m.group(1)[2:]}.debug" not in have:
            sys.exit(f"VERIFY FAILED: {rel}: no .build-id/{m.group(1)[:2]}/{m.group(1)[2:]}.debug in the debuginfo tarball")
        secs = subprocess.run(["readelf", "-S", "-W", path], capture_output=True, text=True).stdout
        if re.search(r"\]\s+\.debug_|\]\s+\.symtab\s", secs):
            sys.exit(f"VERIFY FAILED: {rel} still holds debug sections or a symbol table")
        n += 1
print(f"ok: debug symbols: {n} ELF files stripped (no .debug_* or .symtab), each build-id in MANIFEST.json with its debug file in the tarball ({len(have)} files)")
PYEOF
gid="$($PY -c 'import json, sys; print(json.load(open(sys.argv[1]))["build_ids"]["lib/" + sys.argv[2]])' "$R/MANIFEST.json" "$LIBGDAL")"
mkdir -p "$WORK/dbg" && tar -xJf "$DEBUGINFO" -C "$WORK/dbg" "./.build-id/${gid:0:2}/${gid:2}.debug" 2> /dev/null || tar -xJf "$DEBUGINFO" -C "$WORK/dbg" ".build-id/${gid:0:2}/${gid:2}.debug" \
    || fail "cannot extract libgdal's debug file .build-id/${gid:0:2}/${gid:2}.debug"
addr="$(readelf --dyn-syms -W "$R/lib/$LIBGDAL" | awk '$8 == "GDALOpenEx" && !a {a = $2} END {print a}')"
[ -n "$addr" ] || fail "no GDALOpenEx in libgdal's dynamic symbols"
sym="$(addr2line -f -C -e "$WORK/dbg/.build-id/${gid:0:2}/${gid:2}.debug" "0x$addr" | tr '\n' ' ')"
case "$sym" in GDALOpenEx*gcore/*:[0-9]*) ;; *) fail "addr2line of GDALOpenEx (0x$addr) with libgdal's debug file gives '$sym'" ;; esac
ok "symbolized: libgdal 0x$addr -> $sym (debug file .build-id/${gid:0:2}/${gid:2}.debug, $(stat -c %s "$WORK/dbg/.build-id/${gid:0:2}/${gid:2}.debug") bytes)"

echo "VERIFY OK arch=$ARCH gdal=$GDAL_VERSION raster_drivers=$nr vector_drivers=$nv"
