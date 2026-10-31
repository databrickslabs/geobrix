#!/bin/bash
# build-rust-native.sh [CRATE] [GDAL_NATIVE] [OUT]: build a PyO3 crate that calls GDAL, PROJ or GEOS against the payload.
# Runs in the rust-native image (gbx:build:native --target rust-native) with --network none and
#   CRATE (rw, default /src)              the crate; cargo's target/ goes to $CARGO_TARGET_DIR inside the container
#   /vendor (ro)                          its crates, vendored on the host (scripts/native/cargo-vendor.sh)
#   GDAL_NATIVE (ro, default /gdal-native) the exported stage-native tree: the dev kit (include/, lib/pkgconfig/, lib-dev/)
#   OUT (rw, default /out)                wheel/ (as maturin wrote it) and payload/_native/<arch>/rs/<module>.abi3.so
#
# The environment below is the recipe for every GeoBrix Rust crate that links the native stack:
#   PKG_CONFIG_LIBDIR = PKG_CONFIG_PATH = <root>/lib/pkgconfig
#                     only the payload's .pc files are seen; gdal-sys, proj-sys, geos-sys and libsqlite3-sys probe
#                     gdal, proj, geos and sqlite3 there, and the payload's versions decide their bindings
#   LIBRARY_PATH = <root>/lib-dev
#                     the linker names (libgdal.so -> ../lib/libgdal-<hash>.so.39), so the extension records the hashed
#                     SONAMEs; also covers a crate that passes a bare -L<root>/lib
#   GDAL_HOME, GDAL_LIB_DIR, GDAL_INCLUDE_DIR, GDAL_VERSION, GDAL_STATIC, GEOS_LIB_DIR, GEOS_VERSION unset
#                     each makes a -sys crate skip pkg-config (GDAL_HOME sends the linker to lib/, which has no libgdal.so)
#   _PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC=0
#                     proj-sys stops (panics) instead of building its bundled PROJ from source when pkg-config finds none
#   LIBCLANG_PATH, BINDGEN_EXTRA_CLANG_ARGS  set by the image: bindgen (gdal-sys `bindgen`; 0.12.0 has no 3.13 bindings)
# maturin builds with --auditwheel skip: a repair would copy libgdal and the rest into the wheel, a second GDAL in the
# process. set-rpath.py then gives the extension RUNPATH $ORIGIN/../lib: its place is _native/<arch>/rs/, beside
# _native/<arch>/lib/ (the env wheel's layout; `gbx:build:wheel --extra-payload <OUT>/payload` packs it there).
# Prints `BUILD FAILED: <what>` and exits 1 on a problem; the last line of a good run is `RUST_NATIVE_BUILT ...`.
set -euo pipefail

CRATE="${1:-/src}"
GN="${2:-/gdal-native}"
OUT="${3:-/out}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCH="$(uname -m)"
fail() { echo "BUILD FAILED: $*"; exit 1; }
[ -f "$CRATE/Cargo.toml" ] && [ -f "$CRATE/pyproject.toml" ] || fail "no Cargo.toml and pyproject.toml in $CRATE"
for d in include lib/pkgconfig lib-dev; do [ -d "$GN/$d" ] || fail "$GN has no $d/ (not a gdal-native dev kit)"; done
[ -n "${LIBCLANG_PATH:-}" ] || fail "LIBCLANG_PATH is not set (build in the rust-native image)"

cp /recipes/cargo-offline.toml "$CARGO_HOME/config.toml"
unset GDAL_HOME GDAL_LIB_DIR GDAL_INCLUDE_DIR GDAL_VERSION GDAL_STATIC GEOS_LIB_DIR GEOS_VERSION
export PKG_CONFIG_LIBDIR="$GN/lib/pkgconfig" PKG_CONFIG_PATH="$GN/lib/pkgconfig" LIBRARY_PATH="$GN/lib-dev"
export _PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC=0
echo "dev kit: gdal $(pkg-config --modversion gdal), proj $(pkg-config --modversion proj), geos $(pkg-config --modversion geos), sqlite3 $(pkg-config --modversion sqlite3); $(tr "\n" " " < /opt/clang-libs-version.txt)"

rm -rf "$OUT/wheel" "$OUT/payload"
mkdir -p "$OUT/wheel" "$OUT/payload/_native/$ARCH/rs"
t0=$(date +%s)
# GBX_CARGO_FEATURES: the crate's cargo features (e.g. gdal for rust/gbxrs), or none
(cd "$CRATE" && maturin build --release --offline --locked --compatibility linux --auditwheel skip -i "$PY" \
    ${GBX_CARGO_FEATURES:+--features "$GBX_CARGO_FEATURES"} --out "$OUT/wheel") \
    || fail "maturin build"
seconds=$(($(date +%s) - t0))
whl="$(ls "$OUT"/wheel/*-cp312-abi3-linux_"$ARCH".whl 2> /dev/null | head -n 1 || true)"
[ -n "$whl" ] || fail "no *-cp312-abi3-linux_$ARCH.whl in $OUT/wheel"
unzip -q -o -j "$whl" '*.abi3.so' -d "$OUT/payload/_native/$ARCH/rs"
so="$(ls "$OUT/payload/_native/$ARCH/rs/"*.abi3.so)"
[ "$(printf '%s\n' "$so" | wc -l)" -eq 1 ] || fail "the wheel holds more than one extension: $so"
$PY "$HERE/set-rpath.py" --tree "$OUT/payload/_native/$ARCH/rs" --tree-at "_native/$ARCH/rs" --lib-at "_native/$ARCH/lib" \
    || fail "set-rpath.py"
echo "RUST_NATIVE_BUILT arch=$ARCH extension=$(basename "$so") wheel=$(basename "$whl") cargo_seconds=$seconds"
