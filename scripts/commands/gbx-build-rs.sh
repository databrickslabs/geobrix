#!/bin/bash
# gbx:build:rs - Build the shipped Rust module (rust/gbxrs) offline in the native build images, per architecture.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
source "$PROJECT_ROOT/scripts/native/fetch-lib.sh"

case "$(uname -m)" in
    arm64|aarch64) ARCH="aarch64" ;;
    *) ARCH="x86_64" ;;
esac
FEATURES=""
CHECK=0
LOG_PATH=""
CRATE="rust/gbxrs"

usage() {
    show_banner "Build the Rust module (rust/gbxrs)"
    echo "Usage: bash scripts/commands/gbx-build-rs.sh [--arch x86_64|aarch64|both] [--features gdal] [--check] [--log <path>]"
    echo ""
    echo "  --arch      default: this machine's architecture. The other one runs under emulation and is slow."
    echo "  --features  default: none. 'gdal' builds decode_window against the payload's libgdal (the rust-native image and the"
    echo "              stage-native dev kit); the output is build/rs/out-gdal/<arch>/payload/_native/<arch>/rs/."
    echo "              Without it the module is built in the toolchain image: build/rs/payload/_native/<arch>/rs/."
    echo "  --check     do not build: run cargo fmt --check, clippy -D warnings (all targets) and cargo test in the toolchain image,"
    echo "              then clippy and cargo deny (licenses bans sources, and advisories) with the gdal feature in the rust-native image"
    echo "  --log       write the output to this log file"
}

needs_value() { echo "$1 needs a value" >&2; usage >&2; exit 2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch)     [[ $# -ge 2 ]] || needs_value "$1"; ARCH="$2"; shift 2 ;;
        --features) [[ $# -ge 2 ]] || needs_value "$1"; FEATURES="$2"; shift 2 ;;
        --check)    CHECK=1; shift ;;
        --log)      [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help)  usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done
case "$ARCH" in x86_64|aarch64|both) ;; *) echo "--arch must be x86_64, aarch64 or both" >&2; exit 1 ;; esac
case "$FEATURES" in ""|gdal) ;; *) echo "--features must be empty or gdal" >&2; exit 1 ;; esac

setup_log_file "$LOG_PATH"
docker info > /dev/null 2>&1 || { echo "PRECONDITION MISSING: Docker is not running; gbx:build:rs not run"; exit 1; }
cd "$PROJECT_ROOT"
VENDOR="$GBX_NATIVE_BUILD/vendor/gbxrs"

build_arch() {
    local a="$1" plat h out tag t0 work
    case "$a" in x86_64) plat=linux/amd64 ;; aarch64) plat=linux/arm64 ;; esac
    h="$(python3 scripts/native/pins.py tag-hash "$a")"
    t0=$(date +%s)
    work="$GBX_NATIVE_BUILD/work/gbxrs-$a"
    rm -rf "$work"
    mkdir -p "$work"
    cp -R "$CRATE" "$work/crate"
    rm -rf "$work/crate/target"
    if [ "$FEATURES" = gdal ]; then
        local sn="$GBX_NATIVE_BUILD/out/$a/stage-native/gdal-native"
        tag="geobrix-native-build:rust-native-$h-$a"
        out="$PROJECT_ROOT/build/rs/out-gdal/$a"
        [ -d "$sn/include" ] || { echo "PRECONDITION MISSING: $sn (build --target stage-native first); the module with gdal was not built"; return 1; }
        docker image inspect "$tag" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: image $tag (gbx:build:native --target rust-native --arch $a); the module with gdal was not built"; return 1; }
        mkdir -p "$out"
        echo "== build $a: $CRATE --features gdal against $sn in $tag (--network none)"
        docker run --rm --network none --platform "$plat" -e GBX_CARGO_FEATURES=gdal \
            -v "$work/crate:/src" -v "$VENDOR:/vendor:ro" -v "$sn:/gdal-native:ro" -v "$out:/out" \
            -v "$PROJECT_ROOT/scripts/native:/verify:ro" "$tag" bash /verify/build-rust-native.sh /src /gdal-native /out 2>&1 \
            | tee "$out/build-crate.txt"
    else
        tag="geobrix-native-build:toolchain-$h-$a"
        out="$PROJECT_ROOT/build/rs/out/$a"
        docker image inspect "$tag" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: image $tag (gbx:build:native --target toolchain --arch $a); the module was not built"; return 1; }
        mkdir -p "$out" "build/rs/payload/_native/$a/rs"
        echo "== build $a: $CRATE in $tag (--network none)"
        docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$VENDOR:/vendor:ro" -v "$out:/out" "$tag" \
            bash -lc 'cp /recipes/cargo-offline.toml $CARGO_HOME/config.toml && cd /src && \
                      maturin build --release --offline --locked --compatibility manylinux_2_28 -i /opt/python/cp312-cp312/bin/python --out /out'
        unzip -o -j "$out"/gbxrs-*-cp312-abi3-manylinux_2_28_"$a".whl '_gbxrs*.so' -d "build/rs/payload/_native/$a/rs"
    fi
    [ ! -e "$work/crate/target" ] || { echo "BUILD FAILED: the offline build left $work/crate/target (CARGO_TARGET_DIR not honoured)"; return 1; }
    echo "RS_BUILT arch=$a features=${FEATURES:-none} seconds=$(($(date +%s) - t0)) out=$out"
}

# --check: the crate's gates, offline in the same images (advisories need the network for the public database)
check_arch() {
    local a="$1" plat h work sn
    case "$a" in x86_64) plat=linux/amd64 ;; aarch64) plat=linux/arm64 ;; esac
    h="$(python3 scripts/native/pins.py tag-hash "$a")"
    work="$GBX_NATIVE_BUILD/work/gbxrs-check-$a"
    sn="$GBX_NATIVE_BUILD/out/$a/stage-native/gdal-native"
    local offline='cp /recipes/cargo-offline.toml $CARGO_HOME/config.toml && cd /src'
    local env_gdal='GN=/gdal-native; unset GDAL_HOME GDAL_LIB_DIR GDAL_INCLUDE_DIR GDAL_VERSION GDAL_STATIC; export PKG_CONFIG_LIBDIR=$GN/lib/pkgconfig PKG_CONFIG_PATH=$GN/lib/pkgconfig LIBRARY_PATH=$GN/lib-dev'
    for tag in "geobrix-native-build:toolchain-$h-$a" "geobrix-native-build:rust-native-$h-$a"; do
        docker image inspect "$tag" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: image $tag (gbx:build:native --target ${tag#*:}); the checks were not run"; return 1; }
    done
    [ -d "$sn/include" ] || { echo "PRECONDITION MISSING: $sn (build --target stage-native first); the checks were not run"; return 1; }
    rm -rf "$work"
    mkdir -p "$work"
    cp -R "$CRATE" "$work/crate"
    rm -rf "$work/crate/target"
    echo "== check $a: fmt, clippy and test (no features) in the toolchain image (--network none)"
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$VENDOR:/vendor:ro" "geobrix-native-build:toolchain-$h-$a" \
        bash -lc "$offline && cargo fmt --check && cargo clippy --release --all-targets --offline --locked -- -D warnings && cargo test --release --offline --locked"
    echo "== check $a: clippy with --features gdal, and cargo deny over the gdal graph, in the rust-native image"
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$VENDOR:/vendor:ro" -v "$sn:/gdal-native:ro" \
        -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" "geobrix-native-build:rust-native-$h-$a" \
        bash -lc "$offline && $env_gdal && cargo clippy --release --all-targets --features gdal --offline --locked -- -D warnings && cargo deny --all-features --offline --locked --config /deny.toml check licenses bans sources"
    docker run --rm --platform "$plat" -v "$work/crate:/src" -v "$VENDOR:/vendor:ro" -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" \
        "geobrix-native-build:rust-native-$h-$a" \
        bash -lc "$offline && cargo deny --all-features --locked --config /deny.toml check -A index-failure advisories"
    echo "RS_CHECKED arch=$a"
}

# the crates are vendored on the host (the build is offline); re-vendor whenever Cargo.lock is newer than the vendor tree
if [ ! -d "$VENDOR" ] || [ "$CRATE/Cargo.lock" -nt "$VENDOR" ]; then
    bash scripts/native/cargo-vendor.sh "$CRATE" "$VENDOR"
    touch "$VENDOR"
fi
run_one=build_arch
[ "$CHECK" = 1 ] && run_one=check_arch
if [ "$ARCH" = both ]; then
    $run_one aarch64
    $run_one x86_64
else
    $run_one "$ARCH"
fi
