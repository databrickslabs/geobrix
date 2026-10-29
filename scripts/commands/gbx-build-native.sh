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
    echo "Usage: bash scripts/commands/gbx-build-native.sh [--arch x86_64|aarch64|both] [--target toolchain|stage-native|bindings|rust-native] [--no-cache] [--log <path>]"
    echo ""
    echo "  --arch    x86_64, aarch64 or both (default: this machine's). 'both' builds aarch64 first, then x86_64;"
    echo "            the non-native one runs under emulation and is much slower."
    echo "  --target  toolchain    the build container: Python 3.12 tools, OpenJDK 17, SWIG, Ant, Rust, cargo-deny; verified by an"
    echo "                         offline abi3 PyO3 build, an import of the wheel and an offline cargo-deny check"
    echo "            stage-native GDAL, PROJ, GEOS and every library they need, built from source, with the JNI library, the Java"
    echo "                         classes and osgeo from the same build: build/native/out/<arch>/stage-native/gdal-native/. Verified by"
    echo "                         scripts/native/verify-native.sh (licence guard, drivers, ELF closure, auditwheel, JNI, osgeo, HTTPS, dev kit)"
    echo "            bindings     rasterio, pyogrio, pyproj, shapely and netCDF4 built from their sdists against that tree:"
    echo "                         build/native/out/<arch>/bindings/{vendor,dist,wheels}. Verified by scripts/native/test-bindings.sh, which"
    echo "                         runs each binding's own test suite against it and writes p11.json (default)"
    echo "            rust-native  the toolchain plus libclang, for Rust crates that call the payload's GDAL, PROJ and GEOS; verified by"
    echo "                         building rust/gbxrs-native-probe against the stage-native tree (scripts/native/build-rust-native.sh),"
    echo "                         its links, closure and a smoke beside rasterio and osgeo (scripts/native/verify-rust-native.sh), and"
    echo "                         cargo deny: build/native/out/<arch>/rust-native/payload/_native/<arch>/rs/_gbx_native_probe.abi3.so"
    echo "  --no-cache  Rebuild every layer (a cold build; the seconds it reports are then the full cost)"
    echo "  --log     Log file (filename -> test-logs/<name>; relative -> under test-logs/; absolute as-is)"
    echo ""
    echo "Image: geobrix-native-build:<target>-<first 12 of sha256 of pins.conf's KEY=VALUE lines + the toolchain stage of build.Dockerfile + the files it copies>-<arch>"
    echo "Output: build/native/out/<arch>/<target>/. Sources, wheels and tool binaries are fetched and checksummed on"
    echo "the host first (scripts/native/prefetch.sh). The image build still downloads the Rust toolchain (rustup) and the"
    echo "JDK (dnf); Rust crates build with --network none from vendored sources."
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
case "$TARGET" in toolchain|stage-native|bindings|rust-native) ;; *) echo "--target must be toolchain, stage-native, bindings or rust-native" >&2; exit 1 ;; esac

DOCKERFILE="$PROJECT_ROOT/scripts/native/build.Dockerfile"
show_banner "Native build: $TARGET ($ARCH)"
setup_log_file "$LOG_PATH"
docker info > /dev/null 2>&1 || { echo "PRECONDITION MISSING: Docker is not running; gbx:build:native not run"; exit 1; }
grep -Eq "^FROM .* AS $TARGET\$" "$DOCKERFILE" || {
    echo "PRECONDITION MISSING: build.Dockerfile has no '$TARGET' stage yet; gbx:build:native not run"; exit 1; }

cd "$PROJECT_ROOT"
# The image tag: first 12 of a sha256 over pins.conf's KEY=VALUE lines (comments and blank lines dropped), the toolchain
# stage of build.Dockerfile and the files that stage copies (so an edit to the toolchain, not only to a pin, makes a new
# tag and a new toolchain image; a comment edit makes none). Later targets reuse the tag of the toolchain they are
# built on. rust-pins.conf is not hashed: no image GDAL is built in reads it.
tag_hash() { python3 scripts/native/pins.py tag-hash "$1"; }
python3 scripts/native/pins.py validate > /dev/null || { python3 scripts/native/pins.py validate; exit 1; }

# A copy-on-write clone where the filesystem has one (APFS, btrfs, xfs), an ordinary copy elsewhere.
clone() { if [ "$(uname -s)" = Darwin ]; then cp -cR "$1" "$2"; else cp -R --reflink=auto "$1" "$2"; fi; }

# build/native/ctx/<arch>/: the whole build context, so the container sees nothing else.
assemble_ctx() {
    local a="$1" ctx="$GBX_NATIVE_BUILD/ctx/$1"
    rm -rf "$ctx"
    mkdir -p "$ctx/wheelhouse" "$ctx/wheelhouse-bindings" "$ctx/bin"
    clone scripts/native "$ctx/recipes"
    rm -rf "$ctx/recipes/__pycache__" "$ctx/recipes/rust-smoke/target"
    # the stages copy the pins without their comments: a comment edit must not miss the cache and rebuild GDAL
    rm -f "$ctx/recipes/pins.conf"
    python3 scripts/native/pins.py canonical > "$ctx/recipes/pins.conf"
    cp rust-toolchain.toml "$ctx/rust-toolchain.toml"
    clone "$GBX_NATIVE_BUILD/src" "$ctx/src"
    clone "$GBX_NATIVE_BUILD/wheelhouse/$a" "$ctx/wheelhouse/$a"
    clone "$GBX_NATIVE_BUILD/wheelhouse-bindings/$a" "$ctx/wheelhouse-bindings/$a"
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
        -e WANT_JDK="$(pin JDK_MAJOR)" -e WANT_DENY="$(pin CARGO_DENY_VERSION)" -e WANT_NASM="$(pin NASM_VERSION)" "$tag" bash -lc '
        set -e
        test "$(uname -m)" = "$GBX_ARCH"
        # NASM (libjpeg-turbo SIMD) on x86_64 only; aarch64 needs none
        if [ "$GBX_ARCH" = x86_64 ]; then nasm -v | grep -q "version $WANT_NASM "; else ! command -v nasm > /dev/null; fi
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
    # the offline form leaves the mounted crate clean: CARGO_TARGET_DIR points inside the container
    [ ! -e "$work/crate/target" ] || { echo "VERIFY FAILED: the offline build left $work/crate/target (CARGO_TARGET_DIR not honoured)"; return 1; }
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
    echo "NOTE: yanked crates are NOT checked here (a vendored source has no crates index); advisories ran against the public database."
    echo "      The CI job .github/workflows/rust-deny.yml runs the full check, yanked crates included, and is the gate."
}

# The split debug symbols as exported (debuginfo/.build-id/<xx>/<rest>.debug) into one tarball per architecture and
# target, beside the tree and never inside it (no wheel reads it). Unpack it anywhere and point a debugger at it by
# build-id. A build-ids.txt (`<path> <build-id>`, the bindings) must have a debug file for every line. The name carries
# the arch and the first 12 hex of ANCHOR, the build-id of the build's key file (libgdal for gdal-native, rasterio's
# _base module for the bindings): each version's GitHub release attaches these tarballs as assets under these names.
pack_debuginfo() {
    local a="$1" out="$2" name="$3" anchor="$4" tarball id missing=0
    [[ "$anchor" =~ ^[0-9a-f]{12,}$ ]] || { echo "BUILD FAILED: no build-id to name the $TARGET debuginfo tarball by ('$anchor')"; return 1; }
    tarball="$out/$name-$a-${anchor:0:12}-debuginfo.tar.xz"
    [ -d "$out/debuginfo/.build-id" ] || { echo "BUILD FAILED: the $TARGET export has no debuginfo/.build-id"; return 1; }
    if [ -f "$out/build-ids.txt" ]; then
        while read -r _ id; do
            [ -f "$out/debuginfo/.build-id/${id:0:2}/${id:2}.debug" ] || { echo "BUILD FAILED: no debug file for build-id $id"; missing=1; }
        done < "$out/build-ids.txt"
        [ "$missing" = 0 ] || return 1
    fi
    # no extended attributes (macOS adds com.apple.provenance, which GNU tar warns about on extraction)
    COPYFILE_DISABLE=1 tar --no-xattrs -C "$out/debuginfo" -cJf "$tarball" .build-id
    rm -rf "$out/debuginfo"
    echo "NATIVE_DEBUGINFO arch=$a tarball=$(basename "$tarball") bytes=$(wc -c < "$tarball" | tr -d ' ')" | tee -a "$out/build.txt"
}

# gdal-native/ as exported, mounted read-only into the toolchain image with no network: the oracle is verify-native.sh
# (the debuginfo tarball beside it is mounted too: it checks every build-id has its debug file and symbolizes one address).
verify_stage_native() {
    local a="$1" plat="$2" out="$3" h="$4" dbg
    dbg="$(ls "$out"/gdal-native-*-"$a"-*debuginfo.tar.xz)"
    echo "== verify $a: $out/gdal-native in geobrix-native-build:toolchain-$h-$a (--network none)"
    docker run --rm --network none --platform "$plat" -v "$out/gdal-native:/gdal-native:ro" -v "$dbg:/debuginfo.tar.xz:ro" \
        -v "$PROJECT_ROOT/scripts/native:/verify:ro" \
        "geobrix-native-build:toolchain-$h-$a" bash /verify/verify-native.sh /gdal-native /debuginfo.tar.xz 2>&1 | tee "$out/verify.txt"
}

# The bindings against gdal-native as exported by stage-native (the tree the bindings were linked against), with the
# hashed test environment, the sdists (for their test directories) and the PyPI wheels (the baseline), --network none.
# test-bindings.sh prints `ok: <check>` / `TEST FAILED: <what>` and writes /bindings/p11.json.
verify_bindings() {
    local a="$1" plat="$2" out="$3" h="$4"
    local sn="$GBX_NATIVE_BUILD/out/$a/stage-native/gdal-native"
    [ -d "$sn" ] || { echo "PRECONDITION MISSING: $sn (build --target stage-native first); the bindings were not tested"; return 1; }
    echo "== test $a: $out against $sn in geobrix-native-build:toolchain-$h-$a (--network none)"
    docker run --rm --network none --platform "$plat" \
        -v "$sn:/gdal-native:ro" -v "$out:/bindings" -v "$GBX_NATIVE_BUILD/sdists:/sdists:ro" \
        -v "$GBX_NATIVE_BUILD/wheelhouse-test/$a:/wheelhouse-test:ro" -v "$GBX_NATIVE_BUILD/wheelhouse-pypi/$a:/wheelhouse-pypi:ro" \
        -v "$PROJECT_ROOT/scripts/native:/verify:ro" "geobrix-native-build:toolchain-$h-$a" bash /verify/test-bindings.sh /gdal-native /bindings 2>&1 \
        | tee "$out/test.txt"
}

# A Rust crate against gdal-native as exported by stage-native (the dev kit), in the rust-native image (the toolchain plus
# libclang), --network none: build-rust-native.sh builds the probe crate (the recipe a kernel crate follows), then
# verify-rust-native.sh checks its NEEDED and RUNPATH, ldd and auditwheel closure, and runs the smoke beside rasterio and
# osgeo (the bindings' vendor tree); then cargo deny, as for the toolchain's smoke crate.
verify_rust_native() {
    local a="$1" plat="$2" out="$3" tag="$4"
    local sn="$GBX_NATIVE_BUILD/out/$a/stage-native/gdal-native" bn="$GBX_NATIVE_BUILD/out/$a/bindings"
    local crate=rust/gbxrs-native-probe vendor="$GBX_NATIVE_BUILD/vendor/gbxrs-native-probe" work="$GBX_NATIVE_BUILD/work/rust-native-$a"
    local offline='cp /recipes/cargo-offline.toml $CARGO_HOME/config.toml && cd /src' t0
    [ -d "$sn" ] || { echo "PRECONDITION MISSING: $sn (build --target stage-native first); the Rust crate was not built"; return 1; }
    [ -d "$bn/vendor" ] || { echo "PRECONDITION MISSING: $bn/vendor (build --target bindings first); the Rust crate was not built"; return 1; }

    echo "== verify $a: $crate built against $sn in $tag (--network none)"
    bash scripts/native/cargo-vendor.sh "$crate" "$vendor"
    rm -rf "$work"
    mkdir -p "$work"
    clone "$crate" "$work/crate"
    rm -rf "$work/crate/target"
    t0=$(date +%s)
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$sn:/gdal-native:ro" \
        -v "$out:/out" -v "$PROJECT_ROOT/scripts/native:/verify:ro" "$tag" bash /verify/build-rust-native.sh /src /gdal-native /out 2>&1 \
        | tee "$out/build-crate.txt"
    [ ! -e "$work/crate/target" ] || { echo "VERIFY FAILED: the offline build left $work/crate/target (CARGO_TARGET_DIR not honoured)"; return 1; }
    echo "RUST_NATIVE_CRATE arch=$a crate=$crate build_seconds=$(($(date +%s) - t0))" | tee -a "$out/build.txt"

    echo "== verify $a: links, closure and the smoke beside rasterio and osgeo (--network none)"
    t0=$(date +%s)
    docker run --rm --network none --platform "$plat" -v "$sn:/gdal-native:ro" -v "$bn:/bindings:ro" -v "$out:/out:ro" \
        -v "$GBX_NATIVE_BUILD/wheelhouse-test/$a:/wheelhouse-test:ro" -v "$PROJECT_ROOT/scripts/native:/verify:ro" "$tag" \
        bash /verify/verify-rust-native.sh /gdal-native /bindings /out 2>&1 | tee "$out/verify.txt"
    echo "RUST_NATIVE_VERIFY arch=$a verify_seconds=$(($(date +%s) - t0))" | tee -a "$out/build.txt"

    echo "== verify $a: cargo deny"
    docker run --rm --network none --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" "$tag" \
        bash -lc "$offline && cargo deny --offline --locked --config /deny.toml check licenses bans sources"
    docker run --rm --platform "$plat" -v "$work/crate:/src" -v "$vendor:/vendor:ro" -v "$PROJECT_ROOT/deny.toml:/deny.toml:ro" "$tag" \
        bash -lc "$offline && cargo deny --locked --config /deny.toml check -A index-failure advisories"
    echo "NOTE: yanked crates are NOT checked here (a vendored source has no crates index); the CI job .github/workflows/rust-deny.yml is the gate."
}

build_arch() {
    local a="$1" plat upper tag out h args=() flag kv t0 t1 t2 size
    case "$a" in x86_64) plat=linux/amd64; upper=X86_64 ;; aarch64) plat=linux/arm64; upper=AARCH64 ;; esac
    h="$(tag_hash "$a")"
    tag="geobrix-native-build:$TARGET-$h-$a"
    out="$GBX_NATIVE_BUILD/out/$a/$TARGET"

    t0=$(date +%s)
    bash scripts/native/prefetch.sh --arch "$a"
    assemble_ctx "$a"
    # the pins as build args, the per-architecture pins under the single names the Dockerfile reads
    while read -r flag kv; do args+=("$flag" "$kv"); done < <(python3 scripts/native/pins.py build-args)
    args+=(--build-arg "MANYLINUX_IMAGE=$(pin "MANYLINUX_IMAGE_$upper")" --build-arg "GBX_ARCH=$a"
           --build-arg "RUSTUP_INIT_SHA256=$(pin "RUSTUP_INIT_SHA256_$upper")" --build-arg "CARGO_DENY_SHA256=$(pin "CARGO_DENY_SHA256_$upper")")
    # rust-pins.conf feeds only the rust-native stage
    [ "$TARGET" != rust-native ] || args+=(--build-arg "CLANG_LIBS_VERSION=$(pin CLANG_LIBS_VERSION)")

    t1=$(date +%s)
    # The later targets build on the toolchain image of these pins, and are verified inside it (the stage-native image
    # also holds the build prefix). It is built here when it is missing; an existing one is used as it is, so a
    # toolchain-stage change needs `--target toolchain` first. `--build-context toolchain=...` makes the Dockerfile's
    # `FROM toolchain` read that image instead of building the stage again.
    if [ "$TARGET" != toolchain ]; then
        if ! docker image inspect "geobrix-native-build:toolchain-$h-$a" > /dev/null 2>&1; then
            echo "== build $a: docker buildx build --target toolchain ($plat; no toolchain image for these pins yet)"
            docker buildx build --platform "$plat" -f "$DOCKERFILE" "${args[@]}" ${NO_CACHE[@]+"${NO_CACHE[@]}"} --target toolchain \
                -t "geobrix-native-build:toolchain-$h-$a" --load "$GBX_NATIVE_BUILD/ctx/$a"
        fi
        args+=(--build-context "toolchain=docker-image://geobrix-native-build:toolchain-$h-$a")
    fi
    echo "== build $a: docker buildx build --target $TARGET ($plat)"
    docker buildx build --platform "$plat" -f "$DOCKERFILE" "${args[@]}" ${NO_CACHE[@]+"${NO_CACHE[@]}"} --target "$TARGET" -t "$tag" --load "$GBX_NATIVE_BUILD/ctx/$a"
    if grep -Eq "^FROM .* AS export-$TARGET\$" "$DOCKERFILE"; then
        rm -rf "$out"
        mkdir -p "$out"
        # The export reads the image just built (a named build context for the $TARGET stage), never the build cache:
        # after a --no-cache build the cache also holds the previous build's layers, and the export picked those, so
        # the tree written out was not the image's.
        docker buildx build --platform "$plat" -f "$DOCKERFILE" "${args[@]}" --build-context "$TARGET=docker-image://$tag" \
            --target "export-$TARGET" --output "type=local,dest=$out" "$GBX_NATIVE_BUILD/ctx/$a"
    fi
    t2=$(date +%s)
    size="$(docker image inspect --format '{{.Size}}' "$tag")"
    mkdir -p "$out"
    echo "NATIVE_BUILD arch=$a target=$TARGET build_seconds=$((t2 - t1)) prefetch_seconds=$((t1 - t0)) image=$tag image_bytes=$size" | tee "$out/build.txt"

    case "$TARGET" in
        toolchain) verify_toolchain "$a" "$plat" "$tag" ;;
        stage-native) pack_debuginfo "$a" "$out" "gdal-native-$(pin GDAL_VERSION)-gbx$(pin GBX_NATIVE_REVISION)" \
                "$(python3 -c 'import json, sys; m = json.load(open(sys.argv[1]))["build_ids"]
print(next((v for k, v in m.items() if k.startswith("lib/libgdal-")), ""))' "$out/gdal-native/MANIFEST.json")" \
            && verify_stage_native "$a" "$plat" "$out" "$h" ;;
        bindings) pack_debuginfo "$a" "$out" "bindings-gbx$(pin GBX_NATIVE_REVISION)" \
                "$(awk '$1 ~ /^rasterio\/_base[.]/ {print $2}' "$out/build-ids.txt" | head -n 1)" \
            && verify_bindings "$a" "$plat" "$out" "$h" ;;
        rust-native) verify_rust_native "$a" "$plat" "$out" "$tag" ;;
        *) echo "VERIFY FAILED: no verify for target $TARGET"; return 1 ;;
    esac
    echo "NATIVE_VERIFIED arch=$a target=$TARGET total_seconds=$(($(date +%s) - t0)) image=$tag"
}

if [ "$ARCH" = both ]; then ARCHES=(aarch64 x86_64); else ARCHES=("$ARCH"); fi
for a in "${ARCHES[@]}"; do build_arch "$a"; done

# The GDAL Java classes the JAR bundles (scripts/native/maven) must be the ones this build gives: a stage-native build
# that changes them fails here until they are re-staged and re-pinned (java_jar.py stage, GDAL_JAVA_JAR_SHA256). The
# check needs both architectures' stage-native output (an older one of the other architecture is fine: the classes
# do not depend on it).
if [ "$TARGET" = stage-native ]; then
    if [ -f "$GBX_NATIVE_BUILD/out/aarch64/stage-native/gdal-native/MANIFEST.json" ] \
        && [ -f "$GBX_NATIVE_BUILD/out/x86_64/stage-native/gdal-native/MANIFEST.json" ]; then
        python3 "$PROJECT_ROOT/scripts/native/java_jar.py" stage --check --root "$PROJECT_ROOT" || {
            echo "VERIFY FAILED: re-stage the GDAL Java classes (python3 scripts/native/java_jar.py stage) and re-pin GDAL_JAVA_JAR_SHA256"
            exit 1
        }
    else
        echo "NOTE: GDAL's Java classes not checked: java_jar.py stage --check needs both architectures' stage-native output"
    fi
fi
