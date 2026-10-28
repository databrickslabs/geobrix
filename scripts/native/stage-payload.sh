#!/bin/bash
# stage-payload.sh: turn the build prefix (/opt/gbx-native) into the relocatable tree gdal-native/ at /stage/gdal-native:
#   lib/        one file per SONAME (a wheel holds no symlinks), RUNPATH $ORIGIN, with libgdalalljni.so; lib/pkgconfig, lib/cmake.
#               Every library but the JNI has a hashed name (libgdal-<sha256[:8]>.so.39), so it shares no SONAME with a system one
#   lib-dev/    symlinks for linking only (libgdal.so, libgdal.so.39 and libgdal-<hash>.so.39 -> ../lib/libgdal-<hash>.so.39); never packaged
#   bin/        gdalinfo ogrinfo projinfo, and gdal-config geos-config nc-config made relocatable
#   include/    the headers of GDAL, PROJ, GEOS and every library built for them
#   share/      gdal, proj
#   java/       gdal-<GDAL_VERSION>.jar
#   python/     osgeo, osgeo_utils (RUNPATH $ORIGIN/../../lib)
#   licenses/   one directory per library built from source
#   MANIFEST.json   versions, revision, arch, the SONAME map (original -> hashed), driver lists, build seconds, sizes, sha256 of every file
# Runs in the build container after build-gdal.sh. Every check that can fail prints `STAGE FAILED: <what>` and exits 1.
set -euo pipefail
source /recipes/pins.conf

P=/opt/gbx-native
OUT=/stage/gdal-native
PY=/opt/python/cp312-cp312/bin/python
fail() { echo "STAGE FAILED: $*"; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT"/{lib,lib-dev,bin,share,java,python,licenses}

# --- lib/: the libraries the roots need, one file per SONAME. The roots are what a GDAL, PROJ, GEOS or netCDF consumer loads.
$PY - "$P" "$OUT/lib" <<'PYEOF'
import os, shutil, subprocess, sys

prefix, dest = sys.argv[1], sys.argv[2]

def readelf(path):
    return subprocess.run(["readelf", "-d", path], capture_output=True, text=True, check=True).stdout

def soname_of(path):
    for line in readelf(path).splitlines():
        if "(SONAME)" in line:
            return line.split("[", 1)[1].rsplit("]", 1)[0]
    return None

def needed_of(path):
    return [l.split("[", 1)[1].rsplit("]", 1)[0] for l in readelf(path).splitlines() if "(NEEDED)" in l]

# every real shared library in the prefix, by SONAME; two files with one SONAME is an error
libs = {}
libdir = os.path.join(prefix, "lib")
for name in sorted(os.listdir(libdir)):
    path = os.path.join(libdir, name)
    if os.path.islink(path) or not os.path.isfile(path) or ".so" not in name:
        continue
    so = soname_of(path)
    if so is None:
        sys.exit(f"STAGE FAILED: {name} has no SONAME")
    if so in libs:
        sys.exit(f"STAGE FAILED: {name} and {os.path.basename(libs[so])} both have SONAME {so}")
    libs[so] = path
jni = os.path.join(libdir, "jni", "libgdalalljni.so")
if not os.path.isfile(jni):
    sys.exit("STAGE FAILED: no lib/jni/libgdalalljni.so (Java bindings not built)")
site = os.path.join(libdir, "python3.12", "site-packages", "osgeo")
if not os.path.isdir(site):
    sys.exit("STAGE FAILED: no osgeo in the prefix (Python bindings not built)")

# the closure of the roots over NEEDED, inside the prefix
roots = [jni]
for r in ("libgdal.so", "libproj.so", "libgeos_c.so", "libnetcdf.so", "libhdf5_hl.so"):
    matches = [s for s in libs if s.startswith(r)]
    if not matches:
        sys.exit(f"STAGE FAILED: no {r}.* in the prefix")
    roots.append(libs[matches[0]])
roots += [os.path.join(site, f) for f in os.listdir(site) if f.endswith(".so")]
keep, todo = {}, list(roots)
while todo:
    path = todo.pop()
    for n in needed_of(path):
        if n in libs and n not in keep:
            keep[n] = libs[n]
            todo.append(libs[n])
for so, path in sorted(keep.items()):
    shutil.copy2(path, os.path.join(dest, so))
shutil.copy2(jni, os.path.join(dest, "libgdalalljni.so"))
left = sorted(set(libs) - set(keep))
print(f"lib: {len(keep) + 1} libraries shipped, {len(left)} built but not needed ({' '.join(left)})")
PYEOF

# --- python/, java/, share/, bin/, include/, licenses/
SITE="$P/lib/python3.12/site-packages"
for pkg in osgeo osgeo_utils; do
    [ -d "$SITE/$pkg" ] || fail "no $pkg in $SITE"
    cp -R "$SITE/$pkg" "$OUT/python/"
done
find "$OUT/python" -name __pycache__ -prune -exec rm -rf {} +
# the Maven artifacts go next to the classes jar (sources, pom): the classes jar is the one we ship
[ -f "$P/share/java/gdal-${GDAL_VERSION}.jar" ] || fail "no $P/share/java/gdal-${GDAL_VERSION}.jar: $(ls "$P/share/java" 2>&1 | tr '\n' ' ')"
cp "$P/share/java/gdal-${GDAL_VERSION}.jar" "$OUT/java/gdal-${GDAL_VERSION}.jar"
cp -R "$P/share/gdal" "$P/share/proj" "$OUT/share/"
for b in gdalinfo ogrinfo projinfo gdal-config geos-config nc-config; do
    [ -f "$P/bin/$b" ] || fail "no $P/bin/$b"
    cp "$P/bin/$b" "$OUT/bin/$b"
done
cp -R "$P/include" "$OUT/include"
cp -R "$P/share/licenses/." "$OUT/licenses/"
for d in "$OUT"/licenses/*/; do
    [ -n "$(find "$d" -type f -size +0 | head -n 1)" ] || fail "licence directory $(basename "$d") is empty"
done
# build-time metadata the shipped tree does not need
find "$OUT" \( -name '*.la' -o -name '*.a' \) -delete

# --- pkgconfig and cmake: from lib/pkgconfig, lib/cmake and the few packages that install under the prefix root
mkdir -p "$OUT/lib/pkgconfig" "$OUT/lib/cmake"
cp "$P"/lib/pkgconfig/*.pc "$OUT/lib/pkgconfig/"
cp -R "$P"/lib/cmake/. "$OUT/lib/cmake/"
[ -d "$P/cmake" ] && { mkdir -p "$OUT/lib/cmake/hdf5"; cp -R "$P"/cmake/. "$OUT/lib/cmake/hdf5/"; }
[ -d "$P/share/cmake" ] && for d in "$P"/share/cmake/*/; do mkdir -p "$OUT/lib/cmake/$(basename "$d")"; cp -R "$d". "$OUT/lib/cmake/$(basename "$d")/"; done

chmod -R u+w "$OUT"

# --- relocatable text: scripts, .pc and cmake files name the build prefix and the versioned library files
$PY - "$P" "$OUT" <<'PYEOF'
import os, re, subprocess, sys

prefix, out = sys.argv[1], sys.argv[2]

# every library name in the prefix (real files and links) -> the SONAME file that exists in lib/
names = {}
libdir = os.path.join(prefix, "lib")
for name in os.listdir(libdir):
    path = os.path.join(libdir, name)
    if ".so" not in name or not os.path.isfile(path):
        continue
    for line in subprocess.run(["readelf", "-d", os.path.realpath(path)], capture_output=True, text=True).stdout.splitlines():
        if "(SONAME)" in line:
            so = line.split("[", 1)[1].rsplit("]", 1)[0]
            if os.path.exists(os.path.join(out, "lib", so)):
                names[name] = so
versioned = sorted((n for n in names if re.search(r"\.so\.\d", n) and n != names[n]), key=len, reverse=True)

def to_soname(text):
    # libgdal.so.39.3.13.3 -> libgdal.so.39: the file that exists after staging
    for n in versioned:
        text = re.sub(re.escape(n) + r"(?![\w.])", names[n], text)
    return text

def is_elf(path):
    with open(path, "rb") as f:
        return f.read(4) == b"\x7fELF"

left = []
for root, _d, files in os.walk(out):
    for f in files:
        path = os.path.join(root, f)
        rel = os.path.relpath(path, out)
        if os.path.islink(path) or not rel.startswith(("lib/pkgconfig/", "lib/cmake/", "bin/")) or is_elf(path):
            continue
        text = open(path, encoding="utf-8", errors="surrogateescape").read()
        new = text
        if rel.startswith("lib/pkgconfig/"):
            # link against lib-dev (the unversioned names) and take the prefix from the .pc file's own location
            # prefix first (gdal.pc uses ${prefix} in lines above its own prefix= line), then libdir and the paths in lib-dev
            new = re.sub(r"(?m)^prefix=.*\n", "", new)
            new = re.sub(r"(?m)^libdir=.*$", "libdir=${prefix}/lib-dev", new)
            new = new.replace(prefix + "/lib", "${prefix}/lib-dev").replace(prefix, "${prefix}")
            new = "prefix=${pcfiledir}/../..\n" + new
        elif rel.startswith("lib/cmake/"):
            new = to_soname(new)
            if prefix in new:
                new = new.replace(prefix + "/lib/", "${_GBX_ROOT}/lib-dev/").replace(prefix, "${_GBX_ROOT}")
                new = 'get_filename_component(_GBX_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)\n' + new
        else:  # bin/: shell scripts (the ELF tools were skipped above)
            if not text.startswith("#!"):
                continue
            if prefix in new:
                head, _, rest = new.partition("\n")
                new = head + '\nGBX_ROOT="$(cd "$(dirname "$0")/.." && pwd)"\n' + rest
                # libdir is where -L points: the linker names are in lib-dev (geos-config derives it as ${exec_prefix}/lib)
                new = re.sub(r"(?m)^libdir=.*$", 'libdir="$GBX_ROOT/lib-dev"', new)
                new = new.replace(prefix + "/lib", "$GBX_ROOT/lib-dev").replace(prefix, "$GBX_ROOT")
                if re.search(r"'[^']*\$GBX_ROOT", new):
                    sys.exit(f"STAGE FAILED: {rel} uses the root inside single quotes")
        if new != text:
            open(path, "w", encoding="utf-8", errors="surrogateescape").write(new)
        if prefix in new:
            left.append(rel)
if left:
    sys.exit("STAGE FAILED: the build prefix is still named in " + ", ".join(sorted(left)))
PYEOF

# --- strip, then hashed SONAMEs (rename-sonames.py also rebuilds lib-dev/), then RUNPATH relative to lib/ for every ELF
# file (set-rpath.py fails on any it cannot set). The hash is taken from the stripped bytes.
while IFS= read -r f; do
    if head -c 4 "$f" | grep -q 'ELF'; then strip --strip-unneeded "$f"; fi
done < <(find "$OUT/lib" "$OUT/bin" "$OUT/python" -type f)
$PY /recipes/rename-sonames.py --tree "$OUT" --map-out /stage/sonames.json
$PY /recipes/set-rpath.py --tree "$OUT" --tree-at gdal-native --lib-at gdal-native/lib

# --- MANIFEST.json (last: it records the final bytes)
$PY - "$OUT" <<'PYEOF'
import hashlib, json, os, re, subprocess, sys

out = sys.argv[1]
env = {"PATH": os.environ["PATH"]}
renamed = json.load(open(os.path.join(os.path.dirname(out), "sonames.json")))

def pins():
    d = {}
    for line in open("/recipes/pins.conf"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            d[k] = v.strip('"')
    return d

p = pins()
deps = ["GDAL", "PROJ", "GEOS", "LIBGEOTIFF", "LIBTIFF", "SQLITE", "OPENSSL", "CURL", "ZLIB", "LIBDEFLATE", "ZSTD", "XZ", "LZ4",
        "CBLOSC", "LIBPNG", "LIBJPEG_TURBO", "LIBWEBP", "LERC", "EXPAT", "OPENJPEG", "LIBAEC", "HDF5", "NETCDF_C"]
versions = {d.lower(): p[d + "_VERSION"] for d in deps}

def formats(tool):
    res = subprocess.run([os.path.join(out, "bin", tool), "--formats"], capture_output=True, text=True, env=env)
    names = []
    for line in res.stdout.splitlines():
        m = re.match(r"^\s+(.+?) -([^()]*?)- \(([^)]*)\): ", line)
        if m:
            names.append(m.group(1))
    return sorted(set(names))

raster, vector = formats("gdalinfo"), formats("ogrinfo")
secs = {}
for line in open("/opt/gbx-native/share/gbx/build-seconds.txt"):
    k, v = line.split()
    secs[k] = secs.get(k, 0) + int(v)

files = {}
for root, _d, names in os.walk(out):
    for n in sorted(names):
        path = os.path.join(root, n)
        rel = os.path.relpath(path, out)
        if rel == "MANIFEST.json" or os.path.islink(path):
            continue
        with open(path, "rb") as f:
            files[rel] = hashlib.sha256(f.read()).hexdigest()

def size(sub):
    return sum(os.path.getsize(os.path.join(r, n)) for r, _d, ns in os.walk(os.path.join(out, sub)) for n in ns if not os.path.islink(os.path.join(r, n)))

manifest = {
    "name": "gdal-native",
    "native_revision": int(p["GBX_NATIVE_REVISION"]),
    "arch": os.uname().machine,
    "baseline": "manylinux_2_28",
    "versions": versions,
    "sonames": renamed["sonames"],
    "jni": renamed["jni"],
    "drivers": {
        "raster_count": len(raster), "vector_count": len(vector),
        "raster": raster, "vector": vector,
    },
    "build_seconds": {"dependencies": {k: v for k, v in secs.items() if k != "gdal"}, "gdal": secs.get("gdal", 0),
                      "dependencies_total": sum(v for k, v in secs.items() if k != "gdal")},
    "bytes": {"lib": size("lib"), "total": size(".")},
    "file_count": len(files),
    "files": files,
}
with open(os.path.join(out, "MANIFEST.json"), "w") as f:
    json.dump(manifest, f, indent=1, sort_keys=True)
    f.write("\n")
print(f"MANIFEST: {len(files)} files, {len(raster)} raster and {len(vector)} vector drivers, lib {size('lib')} bytes, total {size('.')} bytes")
PYEOF
echo "stage ok: $OUT"
