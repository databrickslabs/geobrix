#!/bin/bash
# verify-rust-native.sh [GDAL_NATIVE] [BINDINGS] [OUT]: the oracle for `gbx:build:native --target rust-native`. Runs in
# the rust-native image with --network none and
#   GDAL_NATIVE (ro, default /gdal-native)  the exported stage-native tree the extension was linked against
#   BINDINGS (ro, default /bindings)        the bindings output (vendor/: rasterio, pyproj, shapely ... on that tree)
#   OUT (ro, default /out)                  build-rust-native.sh's output: payload/_native/<arch>/rs/<module>.abi3.so
#   /wheelhouse-test (ro)                   the hashed test environment (test-requirements-<arch>.txt)
#   /verify (ro)                            scripts/native
# Prints `ok: <check>` per check and stops at the first `VERIFY FAILED: <what>`; the last line is `VERIFY OK ...`.
#   1 link: NEEDED holds the payload's hashed libgdal, libgeos_c and libproj (from MANIFEST.json), every NEEDED is in
#     lib/ or the manylinux policy set (glibc, libstdc++, libgcc_s), RUNPATH is $ORIGIN/../lib and there is no RPATH
#   2 the payload layout (_native/<arch>/{lib,rs}, _native/share, _vendor/<arch> with osgeo): ldd resolves every library
#     into _native/<arch>/lib or to a policy-set system library, nothing is "not found"
#   3 auditwheel show on a throwaway wheel of _native/<arch>/{lib,rs}: manylinux_2_28_<arch>, no external library
#   4 scripts/native/rust_native_smoke.py, once per import order (bindings first, probe first)
set -uo pipefail

GN="$(cd "${1:-/gdal-native}" && pwd)"
B="${2:-/bindings}"
OUT="${3:-/out}"
ARCH="$(uname -m)"
PY=/opt/python/cp312-cp312/bin/python
T=/t
fail() { echo "VERIFY FAILED: $*"; exit 1; }
ok() { echo "ok: $*"; }
unset LD_LIBRARY_PATH GDAL_DATA PROJ_DATA PROJ_LIB PYTHONPATH LIBRARY_PATH PKG_CONFIG_PATH PKG_CONFIG_LIBDIR
export PYTHONDONTWRITEBYTECODE=1

[ -f "$GN/MANIFEST.json" ] && [ -d "$GN/lib" ] || fail "$GN is not a gdal-native tree"
[ -d "$B/vendor" ] || fail "no $B/vendor (gbx:build:native --target bindings)"
SO="$(ls "$OUT/payload/_native/$ARCH/rs/"*.abi3.so 2> /dev/null)"
[ -n "$SO" ] && [ "$(printf '%s\n' "$SO" | wc -l)" -eq 1 ] || fail "not one extension in $OUT/payload/_native/$ARCH/rs/"
NAME="$(basename "$SO")"

# --- 1 link
echo "readelf -d $NAME:"
readelf -d "$SO" | grep -E '\((NEEDED|RUNPATH|RPATH|SONAME)\)' | sed -e 's/^ */    /'
$PY - "$SO" "$GN" <<'PYEOF' || exit 1
import json, os, re, subprocess, sys

so, gn = sys.argv[1], sys.argv[2]
policy = re.compile(r"^(libc|libm|libdl|librt|libpthread|libstdc\+\+|libgcc_s|libutil|libresolv)\.so\.\d+$|^ld-linux-[\w-]+\.so\.\d+$")
out = subprocess.run(["readelf", "-d", so], capture_output=True, text=True).stdout
get = lambda tag: [l.split("[", 1)[1].rsplit("]", 1)[0] for l in out.splitlines() if f"({tag})" in l]
needed, rpath, runpath = get("NEEDED"), get("RPATH"), get("RUNPATH")
sonames = json.load(open(os.path.join(gn, "MANIFEST.json")))["sonames"]
libs = set(os.listdir(os.path.join(gn, "lib")))

def fail(msg):
    print("VERIFY FAILED: " + msg)
    sys.exit(1)

want = {}
for stem in ("libgdal.so.", "libgeos_c.so.", "libproj.so."):
    hits = [v for k, v in sonames.items() if k.startswith(stem)]
    if len(hits) != 1:
        fail(f"MANIFEST.json maps {len(hits)} libraries for {stem}*")
    want[stem.split(".")[0]] = hits[0]
for lib, hashed in want.items():
    if hashed not in needed:
        fail(f"NEEDED {needed} lacks the payload's {lib} ({hashed}): linked elsewhere, or statically")
for dep in needed:
    if dep in sonames:
        fail(f"NEEDED {dep} is an original SONAME: the loader would take a system copy")
    if dep not in libs and not policy.match(dep):
        fail(f"NEEDED {dep}: not in the payload's lib/ and not in the manylinux policy set")
if rpath:
    fail(f"RPATH {rpath}: a build path or an RPATH that outranks LD_LIBRARY_PATH")
if runpath != ["$ORIGIN/../lib"]:
    fail(f"RUNPATH {runpath}, wanted ['$ORIGIN/../lib'] (_native/<arch>/rs/ beside lib/)")
ours = [d for d in needed if d in libs]
print(f"ok: link: NEEDED = {', '.join(ours)} from the payload + {', '.join(d for d in needed if d not in libs)} (policy set); "
      f"RUNPATH $ORIGIN/../lib, no RPATH")
PYEOF

# --- 2 the payload layout and ldd
rm -rf "$T"
mkdir -p "$T/gbx/_native/$ARCH/rs" "$T/gbx/_vendor"
cp -a "$GN/lib" "$T/gbx/_native/$ARCH/lib"
rm -rf "$T/gbx/_native/$ARCH/lib/pkgconfig" "$T/gbx/_native/$ARCH/lib/cmake"
cp -a "$GN/share" "$T/gbx/_native/share"
cp -a "$B/vendor" "$T/gbx/_vendor/$ARCH"
cp -a "$GN/python/osgeo" "$T/gbx/_vendor/$ARCH/osgeo"
$PY /verify/set-rpath.py --tree "$T/gbx/_vendor/$ARCH/osgeo" --tree-at "_vendor/$ARCH/osgeo" --lib-at "_native/$ARCH/lib" > /dev/null \
    || fail "set-rpath.py on osgeo"
cp "$SO" "$T/gbx/_native/$ARCH/rs/"
P="$T/gbx/_native/$ARCH/rs/$NAME"
echo "ldd $NAME (in _native/$ARCH/rs/):"
ldd "$P" 2>&1 | sed -e 's/^\s*/    /'
$PY - "$P" "$T/gbx/_native/$ARCH/lib" <<'PYEOF' || exit 1
import os, re, subprocess, sys

so, lib = sys.argv[1], sys.argv[2]
policy = re.compile(r"^(libc|libm|libdl|librt|libpthread|libstdc\+\+|libgcc_s|libutil|libresolv)\.so\.\d+$|^ld-linux-[\w-]+\.so\.\d+$")
rows = subprocess.run(["ldd", so], capture_output=True, text=True).stdout.splitlines()
ours = system = 0
for row in rows:
    row = row.strip()
    if "not found" in row:
        print(f"VERIFY FAILED: ldd: {row}")
        sys.exit(1)
    m = re.match(r"(\S+) => (\S+)", row)
    if not m:
        continue  # linux-vdso, the dynamic loader itself
    name, path = m.groups()
    if os.path.dirname(os.path.realpath(path)) == os.path.realpath(lib):
        ours += 1
    elif policy.match(name):
        system += 1
    else:
        print(f"VERIFY FAILED: ldd: {name} resolves to {path}, outside the payload and not glibc/libstdc++")
        sys.exit(1)
print(f"ok: ldd: {ours} libraries resolve into the payload's lib/, {system} to the system, all of them glibc, libstdc++ or libgcc_s")
PYEOF

# --- 3 auditwheel on a throwaway wheel of _native/<arch>/{lib,rs}
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
W="$WORK/gbxrsprobe-0"
mkdir -p "$W/gbxrsprobe/_native/$ARCH" "$W/gbxrsprobe-0.dist-info"
cp -a "$T/gbx/_native/$ARCH/lib" "$T/gbx/_native/$ARCH/rs" "$W/gbxrsprobe/_native/$ARCH/"
: > "$W/gbxrsprobe/__init__.py"
printf 'Metadata-Version: 2.1\nName: gbxrsprobe\nVersion: 0\n' > "$W/gbxrsprobe-0.dist-info/METADATA"
printf 'Wheel-Version: 1.0\nGenerator: verify-rust-native\nRoot-Is-Purelib: false\nTag: cp312-cp312-linux_%s\n' "$ARCH" > "$W/gbxrsprobe-0.dist-info/WHEEL"
$PY -m wheel pack "$W" --dest-dir "$WORK" > "$WORK/wheel-pack.txt" 2>&1 || fail "could not build the throwaway wheel: $(tail -c 300 "$WORK/wheel-pack.txt")"
WHL="$WORK/gbxrsprobe-0-cp312-cp312-linux_${ARCH}.whl"
auditwheel show --json "$WHL" > "$WORK/auditwheel.json" 2> /dev/null
$PY - "$WORK/auditwheel.json" "$ARCH" <<'PYEOF' || fail "auditwheel does not report manylinux_2_28_${ARCH} with no external library: $(head -c 400 "$WORK/auditwheel.json")"
import json, sys
r = json.load(open(sys.argv[1]))
assert r.get("overall_tag") == f"manylinux_2_28_{sys.argv[2]}", r.get("overall_tag")
assert not r.get("external_libs"), r.get("external_libs")
PYEOF
auditwheel show "$WHL" 2>&1 | grep -E "is consistent with|platform tag" | sed -e 's/^/    auditwheel: /'
ok "auditwheel: _native/$ARCH/lib + rs/$NAME are consistent with manylinux_2_28_$ARCH, no external shared libraries"

# --- 4 the smoke, in a venv of the hashed test environment, the payload active through the environment
"$PY" -m venv "$T/venv"
"$T/venv/bin/python" -m pip install -q --disable-pip-version-check --no-index --find-links /wheelhouse-test --require-hashes \
    -r "/verify/test-requirements-$ARCH.txt" || fail "the smoke venv"
for order in bindings-first probe-first; do
    echo "== smoke ($order)"
    GDAL_DATA="$T/gbx/_native/share/gdal" PROJ_DATA="$T/gbx/_native/share/proj" PYTHONPATH="$T/gbx/_vendor/$ARCH" \
        "$T/venv/bin/python" /verify/rust_native_smoke.py --probe "$P" --order "$order" || fail "the smoke ($order)"
done
echo "VERIFY OK arch=$ARCH extension=$NAME"
