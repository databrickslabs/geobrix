#!/bin/bash
# gbx:build:native - Build the GeoBrix native stack in the pinned manylinux_2_28 container
# (scripts/native/build.Dockerfile): the toolchain image (C/C++, Java, Python, Rust), then the natives and bindings.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
source "$PROJECT_ROOT/scripts/native/fetch-lib.sh"

case "$(uname -m)" in
    arm64|aarch64) ARCH="aarch64" ;;
    *) ARCH="x86_64" ;;
esac
TARGET="bindings"
LOG_PATH=""
NO_CACHE=()

usage() {
    show_banner "Build the native stack in the manylinux_2_28 container"
    echo "Usage: bash scripts/commands/gbx-build-native.sh [--arch x86_64|aarch64|both] [--target toolchain|stage-native|bindings] [--no-cache] [--log <path>]"
    echo ""
    echo "  --arch    x86_64, aarch64 or both (default: this machine's). 'both' builds aarch64 first, then x86_64;"
    echo "            the non-native one runs under emulation and is much slower."
    echo "  --target  toolchain    the build container: Python 3.12 tools, OpenJDK 17, SWIG, Ant, Rust, cargo-deny; verified by an"
    echo "                         offline abi3 PyO3 build, an import of the wheel and an offline cargo-deny check"
    echo "            stage-native the GDAL stack staged for the wheel"
    echo "            bindings     the Python bindings built against it (default)"
    echo "  --no-cache  Rebuild every layer (a cold build; the seconds it reports are then the full cost)"
    echo "  --log     Log file (filename -> test-logs/<name>; relative -> under test-logs/; absolute as-is)"
    echo ""
    echo "Image: geobrix-native-build:<target>-<first 12 of sha256 of scripts/native/pins.conf>-<arch>"
    echo "Output: build/native/out/<arch>/<target>/. Everything the build needs is fetched on the host first"
    echo "(scripts/native/prefetch.sh) and the container runs with --network none where it builds Rust."
    echo "Needs no dev container and no container lock. Never pushes an image."
}

# Under `set -u` a trailing option would die on an unbound $2 with no explanation.
needs_value() { echo "$1 needs a value" >&2; usage; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch) [[ $# -ge 2 ]] || needs_value "$1"; ARCH="$2"; shift 2 ;;
        --target) [[ $# -ge 2 ]] || needs_value "$1"; TARGET="$2"; shift 2 ;;
        --no-cache) NO_CACHE=(--no-cache); shift ;;
        --log) [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$ARCH" in x86_64|aarch64|both) ;; *) echo "--arch must be x86_64, aarch64 or both" >&2; exit 1 ;; esac
case "$TARGET" in toolchain|stage-native|bindings) ;; *) echo "--target must be toolchain, stage-native or bindings" >&2; exit 1 ;; esac

DOCKERFILE="$PROJECT_ROOT/scripts/native/build.Dockerfile"
show_banner "Native build: $TARGET ($ARCH)"
setup_log_file "$LOG_PATH"
docker info > /dev/null 2>&1 || { echo "PRECONDITION MISSING: Docker is not running; gbx:build:native not run"; exit 1; }
grep -Eq "^FROM .* AS $TARGET\$" "$DOCKERFILE" || {
    echo "PRECONDITION MISSING: build.Dockerfile has no '$TARGET' stage yet; gbx:build:native not run"; exit 1; }

cd "$PROJECT_ROOT"
PINS12="$(sha256_of scripts/native/pins.conf | cut -c1-12)"
python3 scripts/native/pins.py validate > /dev/null || { python3 scripts/native/pins.py validate; exit 1; }

# A copy-on-write clone where the filesystem has one (APFS, btrfs, xfs), an ordinary copy elsewhere.
clone() { if [ "$(uname -s)" = Darwin ]; then cp -cR "$1" "$2"; else cp -R --reflink=auto "$1" "$2"; fi; }

# build/native/ctx/<arch>/: the whole build context, so the container sees nothing else.
assemble_ctx() {
    local a="$1" ctx="$GBX_NATIVE_BUILD/ctx/$1"
    rm -rf "$ctx"
    mkdir -p "$ctx/wheelhouse" "$ctx/bin"
    clone scripts/native "$ctx/recipes"
    rm -rf "$ctx/recipes/__pycache__" "$ctx/recipes/rust-smoke/target"
    cp rust-toolchain.toml "$ctx/rust-toolchain.toml"
    clone "$GBX_NATIVE_BUILD/src" "$ctx/src"
    clone "$GBX_NATIVE_BUILD/wheelhouse/$a" "$ctx/wheelhouse/$a"
    clone "$GBX_NATIVE_BUILD/bin/$a" "$ctx/bin/$a"
    clone "$GBX_NATIVE_BUILD/sdists" "$ctx/sdists"
}

# What the toolchain image must be, checked offline inside it, then the Rust round trip the pins exist for.
verify_toolchain() {
    local a="$1" plat="$2" tag="$3"
    local work="$GBX_NATIVE_BUILD/work/$a" vendor="$GBX_NATIVE_BUILD/vendor/rust-smoke"
    local host_cargo="${GBX_HOST_CARGO_HOME:-$HOME/.cache/geobrix/cargo}/bin/cargo"
    local offline='cp /recipes/cargo-offline.toml $CARGO_HOME/config.toml && cd /src'
    local wheel

    echo "== verify $a: tools and pins in $tag"
    docker run --rm --network none --platform "$plat" \
        -e GBX_ARCH="$a" -e WANT_RUST="$(pin RUST_TOOLCHAIN)" -e WANT_MATURIN="$(pin MATURIN_VERSION)" \
        -e WANT_NUMPY="$(pin NUMPY_BUILD_VERSION)" -e WANT_SWIG="$(pin SWIG_VERSION)" -e WANT_ANT="$(pin ANT_VERSION)" \
        -e WANT_JDK="$(pin JDK_MAJOR)" -e WANT_DENY="$(pin CARGO_DENY_VERSION)" "$tag" bash -lc '
        set -e
        test "$(uname -m)" = "$GBX_ARCH"
        test "$(rustc --version | cut -d" " -f2)" = "$WANT_RUST"
        test "$(maturin --version | cut -d" " -f2)" = "$WANT_MATURIN"
        $PY -c "import numpy, sys; assert numpy.__version__ == sys.argv[1], numpy.__version__" "$WANT_NUMPY"
        swig -version | grep -q "SWIG Version $WANT_SWIG"
        ant -version | grep -q "version $WANT_ANT"
        test "$JAVA_HOME" = /usr/lib/jvm/java-17-openjdk
        "$JAVA_HOME/bin/javac" -version 2>&1 | grep -q "javac $WANT_JDK\."
        cargo-deny --version | grep -q "$WANT_DENY"
        rustup target list --installed | grep -q "^$GBX_ARCH-unknown-linux-gnu\$"
        for t in cmake ninja auditwheel patchelf; do $t --version > /dev/null; done
        test -f /recipes/cargo-offline.toml
        # no package index in any layer: no pip or uv configuration, and pip reports none
        test "$(cat /etc/pip.conf ~/.pip/pip.conf ~/.config/pip/pip.conf ~/.config/uv/uv.toml 2>/dev/null | wc -c | tr -d " ")" = 0
        test -z "$($PY -m pip config list)"
        echo "tools ok: rustc $WANT_RUST, maturin $WANT_MATURIN, numpy $WANT_NUMPY, swig $WANT_SWIG, ant $WANT_ANT, jdk $WANT_JDK"
    '

    echo "== verify $a: Rust smoke, built offline into an abi3 wheel"
    [ -x "$host_cargo" ] || bash scripts/native/install-host-rust.sh
    bash scripts/native/cargo-vendor.sh scripts/native/rust-smoke "$vendor"
    rm -rf "$work"
    mkdir -p "$work/out"
    clone scripts/native/rust-smoke "$work/crate"
    rm -rf "$work/crate/target"
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$work/out:/out" "$tag" \
        bash -lc "$offline && maturin build --release --offline --compatibility manylinux_2_28 -i /opt/python/cp312-cp312/bin/python --out /out"
    wheel="$(ls "$work"/out/*-cp312-abi3-manylinux_2_28_"$a".whl 2> /dev/null | head -n 1 || true)"
    [ -n "$wheel" ] || { echo "VERIFY FAILED: no *-cp312-abi3-manylinux_2_28_$a.whl in $work/out"; return 1; }
    echo "wheel: $(basename "$wheel")"
    docker run --rm --network none --platform "$plat" -v "$work/out:/out:ro" "$tag" bash -lc \
        '$PY -m venv /tmp/v && /tmp/v/bin/pip install -q --no-index /out/*.whl && /tmp/v/bin/python -c "import gbx_rust_smoke as m; assert m.smoke() == \"ok\"; print(\"smoke: \" + m.smoke())"'

    echo "== verify $a: cargo deny"
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" "$tag" \
        bash -lc "$offline && cargo deny --offline --locked --config /deny.toml check licenses bans sources"
    # Advisories need the public advisory database, so this run has the network. The yanked check needs the crates
    # index, which a vendored source does not have: it is reported, not silently skipped (`yanked = "deny"` in
    # deny.toml applies to any run that can reach an index).
    docker run --rm --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" "$tag" \
        bash -lc "$offline && cargo deny --locked --config /deny.toml check -A index-failure advisories"
    echo "NOTE: yanked crates are not checked in the container (a vendored source has no crates index); the advisory database was"
}

build_arch() {
    local a="$1" plat upper tag out args=() flag kv t0 t1 t2 size
    case "$a" in x86_64) plat=linux/amd64; upper=X86_64 ;; aarch64) plat=linux/arm64; upper=AARCH64 ;; esac
    tag="geobrix-native-build:$TARGET-$PINS12-$a"
    out="$GBX_NATIVE_BUILD/out/$a/$TARGET"

    t0=$(date +%s)
    bash scripts/native/prefetch.sh --arch "$a"
    assemble_ctx "$a"
    # the pins as build args, the per-architecture pins under the single names the Dockerfile reads
    while read -r flag kv; do args+=("$flag" "$kv"); done < <(python3 scripts/native/pins.py build-args)
    args+=(--build-arg "MANYLINUX_IMAGE=$(pin "MANYLINUX_IMAGE_$upper")" --build-arg "GBX_ARCH=$a"
           --build-arg "RUSTUP_INIT_SHA256=$(pin "RUSTUP_INIT_SHA256_$upper")" --build-arg "CARGO_DENY_SHA256=$(pin "CARGO_DENY_SHA256_$upper")")

    t1=$(date +%s)
    echo "== build $a: docker buildx build --target $TARGET ($plat)"
    docker buildx build --platform "$plat" -f "$DOCKERFILE" "${args[@]}" ${NO_CACHE[@]+"${NO_CACHE[@]}"} --target "$TARGET" -t "$tag" --load "$GBX_NATIVE_BUILD/ctx/$a"
    if grep -Eq "^FROM .* AS export-$TARGET\$" "$DOCKERFILE"; then
        rm -rf "$out"
        mkdir -p "$out"
        docker buildx build --platform "$plat" -f "$DOCKERFILE" "${args[@]}" --target "export-$TARGET" --output "type=local,dest=$out" "$GBX_NATIVE_BUILD/ctx/$a"
    fi
    t2=$(date +%s)
    size="$(docker image inspect --format '{{.Size}}' "$tag")"
    mkdir -p "$out"
    echo "NATIVE_BUILD arch=$a target=$TARGET build_seconds=$((t2 - t1)) prefetch_seconds=$((t1 - t0)) image=$tag image_bytes=$size" | tee "$out/build.txt"

    case "$TARGET" in
        toolchain) verify_toolchain "$a" "$plat" "$tag" ;;
        *) echo "VERIFY FAILED: no verify for target $TARGET"; return 1 ;;
    esac
    echo "NATIVE_VERIFIED arch=$a target=$TARGET total_seconds=$(($(date +%s) - t0)) image=$tag"
}

if [ "$ARCH" = both ]; then ARCHES=(aarch64 x86_64); else ARCHES=("$ARCH"); fi
for a in "${ARCHES[@]}"; do build_arch "$a"; done
