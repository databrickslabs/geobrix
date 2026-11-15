#!/bin/bash

# --- CONFIGURATION ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# This checkout's container settings (the image tag may differ per worktree), then the platform and
# image-tag resolution shared with the start scripts. The container file comes first: the tag is read
# when gbx-platform.sh is sourced.
source "$SCRIPT_DIR/gbx-container.sh"
gbx_load_local_env "$PROJECT_ROOT" || exit 1
source "$SCRIPT_DIR/gbx-platform.sh"
STAGES=(
    "base"
    "system-deps"
    "hadoop-builder"
    "gdal-builder"
    "final"
)

# --- COLORS FOR FEEDBACK ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# --- HELP MENU ---
usage() {
    echo -e "${CYAN}Geobrix Smart Builder${NC}"
    echo -e "Usage: $0 [OPTIONS] [-- DOCKER_FLAGS]"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  -h, --help        Show this help message and exit"
    echo -e "  --platform <p>    linux/arm64 or linux/amd64 (default: \$GBX_DOCKER_PLATFORM, else this host's"
    echo -e "                    architecture). Each platform gets its own image tag and checkpoint tags."
    echo -e "  -p, --pull        Force pull latest ubuntu:24.04 (default: true)"
    echo -e "  --no-pull         Do not pull base image (faster, may miss security updates)"
    echo -e "  --overlay gbxnative"
    echo -e "                    Do not build the base image. Layer the env6 wheel's natives (the payload: GDAL, PROJ,"
    echo -e "                    GEOS and the bindings built against them), the rasterio distribution and the pinned Rust"
    echo -e "                    toolchain over the existing base image (geobrix-dev:ubuntu24-gdal311-spark-<arch>, which"
    echo -e "                    stays as the fallback) and tag geobrix-dev:ubuntu24-gbxnative-spark-<arch>. Needs the"
    echo -e "                    env6 wheel built (gbx:build:wheel --env 6) and scripts/native/prefetch.sh run for the arch."
    echo -e "                    No package index is used; the only network use is rustup's verified toolchain download."
    echo ""
    echo -e "${YELLOW}Package index:${NC}"
    echo -e "  Export PIP_INDEX_URL to build through a private mirror. It reaches the build as a BuildKit"
    echo -e "  secret (never a --build-arg), so it is not recorded in the image history."
    echo ""
    echo -e "${YELLOW}Docker Flags:${NC}"
    echo -e "  Any arguments provided to this script (or following --) are passed"
    echo -e "  directly to 'docker build'. Use this for things like --no-cache."
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 --no-cache                     Build everything from scratch"
    echo -e "  $0 --progress=plain               Show detailed build output"
    echo -e "  $0 --build-arg CORES=4            Override the CPU cores used (default=2)"
    echo -e "  $0 --platform linux/amd64         Build the amd64 image (default: the host's architecture)"
    echo -e "  $0 --overlay gbxnative            Add the env wheel's natives and Rust to the existing base image"
    exit 0
}

# --- ARGUMENT PARSING ---
EXTRA_ARGS=()
FORCE_PULL=true
PLATFORM_ARG=""
OVERLAY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        -p|--pull)
            FORCE_PULL=true
            shift
            ;;
        --no-pull)
            FORCE_PULL=false
            shift
            ;;
        --platform)
            if [ $# -lt 2 ]; then echo "ERROR: --platform needs a value (linux/arm64 or linux/amd64)" >&2; exit 2; fi
            PLATFORM_ARG="$2"
            shift 2
            ;;
        --platform=*)
            PLATFORM_ARG="${1#--platform=}"
            shift
            ;;
        --overlay)
            if [ $# -lt 2 ]; then echo "ERROR: --overlay needs a value (gbxnative)" >&2; exit 2; fi
            OVERLAY="$2"
            shift 2
            ;;
        --overlay=*)
            OVERLAY="${1#--overlay=}"
            shift
            ;;
        --) # End of our internal options, treat everything else as docker flags
            shift
            EXTRA_ARGS+=("$@")
            break
            ;;
        *) # Catch-all for docker flags (like --no-cache)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# --- HELPER FUNCTIONS ---
log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# --- PLATFORM AND TAGS ---
# The platform is passed to every stage build, so the image is native to it; one tag per
# architecture means the arm64 and amd64 images coexist.
PLATFORM="$(gbx_docker_platform "$PLATFORM_ARG")" || exit 2
ARCH="$(gbx_platform_arch "$PLATFORM")"
FINAL_TAG="$(gbx_dev_image_tag "$PLATFORM")"
case "$OVERLAY" in
    "") ;;
    gbxnative) FINAL_TAG="$(gbx_dev_native_image "$PLATFORM")" ;;
    *) echo "ERROR: unknown overlay '$OVERLAY' (the only one is gbxnative)" >&2; exit 2 ;;
esac

# The platform is chosen above, and the package index travels as a secret (below): a docker flag
# that sets either again would build one thing and tag another, or put the index in the history.
gbx_check_docker_flags "${EXTRA_ARGS[@]}" || exit 2

# --- WHAT THE IMAGE IS EXPECTED TO HOLD (compared in step 4) ---
EXPECT_GDAL="$(sed -n 's/^ARG GDAL_VERSION=//p' "$PROJECT_ROOT/scripts/docker/Dockerfile" | head -1)"
EXPECT_PROJ="9.4.0"           # noble libproj-dev
EXPECT_GEOS="3.12.1"          # noble libgeos-dev
EXPECT_RASTERIO_GDAL="3.12.1" # the GDAL vendored in the rasterio wheel below, not the system one
# EXPECT_RASTERIO_GDAL was read off this rasterio release's manylinux wheel. A new pin in the lock may vendor
# another GDAL, so the constants above are not trusted for it until someone has measured them again.
EXPECT_RASTERIO_GDAL_VERIFIED_FOR="rasterio==1.5.0"
LOCK_FILE="$PROJECT_ROOT/python/geobrix/requirements-dev-container.txt"
if [ "$ARCH" = "arm64" ]; then
    LOCK_FILE="$PROJECT_ROOT/python/geobrix/requirements-dev-container-aarch64.txt"
fi
# Where the native overlay puts the payload (GBX_NATIVE_ROOT in Dockerfile.gbxnative).
OVERLAY_PAYLOAD_ROOT="/opt/gbx-native"

if [ -z "$OVERLAY" ]; then
    LOCKED_RASTERIO="$(sed -n 's/^\(rasterio==[^ ]*\).*/\1/p' "$LOCK_FILE" | head -1)"
    if [ "$LOCKED_RASTERIO" != "$EXPECT_RASTERIO_GDAL_VERIFIED_FOR" ]; then
        log_error "The dev lock pins ${LOCKED_RASTERIO:-no rasterio}, but the probe's expected rasterio GDAL (${EXPECT_RASTERIO_GDAL}) was verified for ${EXPECT_RASTERIO_GDAL_VERIFIED_FOR}: re-verify the probe constants (build_smart.sh), then update EXPECT_RASTERIO_GDAL_VERIFIED_FOR."
        exit 1
    fi
fi

# --- ERROR HANDLING ---
set -e
SECRET_FILE=""
cleanup() { if [ -n "$SECRET_FILE" ]; then rm -f "$SECRET_FILE"; fi; }
trap cleanup EXIT
trap 'log_error "Build failed at stage: ${CURRENT_STAGE}"; exit 1' ERR

# --- INITIALIZATION ---
START_TIME=$(date +%s)
log_info "Starting smart build for ${FINAL_TAG} (${PLATFORM})..."

# Forward host PIP_INDEX_URL into the build if set. Leave unset to use public PyPI; export it to
# point at a private mirror if your network needs one. It goes in as a BuildKit secret from a
# private file that exists only for the duration of the build, never as a --build-arg or ENV
# (either is recorded in `docker history`). The URL is never printed.
#
# BuildKit does not key its layer cache on a secret, so a changed index alone would reuse the cached pip
# configuration layer. A non-secret fingerprint of the index is therefore also passed as the build-arg
# GBX_PIP_INDEX_FPR, to the final stage only (the stage with the pip steps): a changed index rebuilds that
# stage and nothing before it (GDAL stays cached). The fingerprint is the first 12 hex characters of an
# HMAC-SHA256 of the index keyed with a random per-machine salt (32 bytes, kept 0600 in
# ${GBX_PIP_INDEX_SALT_FILE:-~/.cache/geobrix/pip-index-fpr-salt} and never printed), so the value, which
# `docker history` shows, cannot be used to confirm a guessed index. The same index gives the same
# fingerprint on this machine, so the cache still holds; another machine has another salt.
# The salt is valid when it is exactly 64 hex characters (32 bytes). Anything else, a truncated write or a file
# someone edited, is replaced with a warning that names the file and never prints its content. A new salt is
# written to a private temp file and linked into place (ln fails when the name exists), so two builds starting
# together cannot both win: each reads the file back and uses whichever salt is there.
salt_is_valid() {
    local v
    v="$(cat "$1" 2>/dev/null)" || return 1
    [[ "$v" =~ ^[0-9a-f]{64}$ ]]
}
index_fingerprint() {
    local salt_file="${GBX_PIP_INDEX_SALT_FILE:-${XDG_CACHE_HOME:-$HOME/.cache}/geobrix/pip-index-fpr-salt}" tmp
    if ! salt_is_valid "$salt_file"; then
        # a directory made here is private; one that already exists is left as it is
        if [ ! -d "$(dirname "$salt_file")" ]; then
            mkdir -p "$(dirname "$salt_file")" && chmod 700 "$(dirname "$salt_file")" || return 1
        fi
        if [ -e "$salt_file" ]; then   # present but invalid (also a zero-byte file): ln would fail on it forever
            log_warn "The salt file $salt_file is not 64 hex characters; making a new one (the package index fingerprint changes once)." >&2
            rm -f "$salt_file" || return 1
        fi
        tmp="$(umask 077; mktemp "$salt_file.XXXXXX")" || return 1
        ( umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$tmp" ) || { rm -f "$tmp"; return 1; }
        ln "$tmp" "$salt_file" 2>/dev/null || true   # a build that got there first keeps its salt
        rm -f "$tmp"
        salt_is_valid "$salt_file" || return 1       # the winner's, read back
    fi
    chmod 600 "$salt_file" 2>/dev/null || true
    GBX_SALT_FILE="$salt_file" python3 -c '
import hashlib, hmac, os
salt = bytes.fromhex(open(os.environ["GBX_SALT_FILE"]).read().strip())
print(hmac.new(salt, os.environ["PIP_INDEX_URL"].encode(), hashlib.sha256).hexdigest()[:12])
'
}
SECRET_ARGS=()
FINAL_STAGE_ARGS=()
# The overlay builds with no package index (its pip steps are --no-index), so it takes no secret.
if [ -n "${PIP_INDEX_URL:-}" ] && [ -z "$OVERLAY" ]; then
    log_info "Using PIP_INDEX_URL from host environment for the build (as a build secret)."
    SECRET_FILE=$(mktemp "${TMPDIR:-/tmp}/gbx-dev-pip-index.XXXXXX")
    chmod 600 "$SECRET_FILE"
    printf '%s' "$PIP_INDEX_URL" > "$SECRET_FILE"
    SECRET_ARGS=(--secret "id=pip_index_url,src=$SECRET_FILE")
    INDEX_FPR="$(index_fingerprint)" && [ -n "$INDEX_FPR" ] || {
        log_error "Could not compute the package index fingerprint (needs python3 and a writable salt file); nothing was built."
        exit 1
    }
    FINAL_STAGE_ARGS=(--build-arg "GBX_PIP_INDEX_FPR=${INDEX_FPR}")
fi

# --- THE NATIVE OVERLAY (--overlay gbxnative) ---
# The base dev image plus the env6 wheel's natives: its payload (the same bytes the wheel ships: GDAL, PROJ, GEOS,
# HDF5, netCDF and the bindings built against them, and the JNI), the rasterio distribution built against that
# GDAL, and the pinned Rust toolchain (scripts/docker/Dockerfile.gbxnative). The build context is assembled here
# from what the native build left on this machine, every external file checked against its pin; nothing is
# downloaded by this script, and the image build uses no package index.
overlay_fail() { log_error "$1" >&2; exit 1; }

native_pin() { python3 "$PROJECT_ROOT/scripts/native/pins.py" get "$1"; }

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# The payload architecture directory for this build's platform: aarch64 | x86_64.
native_arch() {
    case "$ARCH" in
        arm64) echo aarch64 ;;
        amd64) echo x86_64 ;;
        *) return 1 ;;
    esac
}

# maturin's requirement with its hashes, as the build requirements lock records them (pip --require-hashes).
maturin_requirement() { # <lock file> <version>
    python3 - "$1" "$2" <<'PY'
import re
import sys

lock, version = sys.argv[1], sys.argv[2]
lines = open(lock).read().splitlines()
for i, line in enumerate(lines):
    if re.match(rf"maturin=={re.escape(version)}\s*\\?$", line):
        out = [line.rstrip(" \\")]
        j = i
        while lines[j].rstrip().endswith("\\"):
            j += 1
            out.append(lines[j].strip().rstrip(" \\"))
        print(" ".join(out))
        sys.exit(0)
sys.exit(1)
PY
}

# assemble_overlay_context: build/devimage/<native arch>/ with payload/, rust/ and dist/. Prints its path.
assemble_overlay_context() {
    local na up src ctx init want wheel dist
    na="$(native_arch)" || overlay_fail "no native payload for architecture $ARCH"
    up="$(printf '%s' "$na" | tr '[:lower:]' '[:upper:]')"
    src="$PROJECT_ROOT/build/env-wheel/env6/payload"
    ctx="$PROJECT_ROOT/build/devimage/$na"

    [ -f "$src/_native/$na/MANIFEST.json" ] && [ -d "$src/_vendor/$na" ] && [ -d "$src/_vendor/common" ] && [ -d "$src/_native/share" ] \
        || overlay_fail "PRECONDITION MISSING: no env6 payload for $na at build/env-wheel/env6/payload; build it first (gbx:build:wheel --env 6)."
    dist="$(ls "$PROJECT_ROOT/build/native/out/$na/bindings/dist"/rasterio-*.whl 2>/dev/null)"
    [ -n "$dist" ] && [ "$(printf '%s\n' "$dist" | wc -l | tr -d ' ')" = 1 ] \
        || overlay_fail "PRECONDITION MISSING: not exactly one rasterio distribution wheel in build/native/out/$na/bindings/dist (gbx:build:native --target bindings)."
    init="$PROJECT_ROOT/build/native/bin/$na/rustup-init"
    want="$(native_pin "RUSTUP_INIT_SHA256_$up")"
    [ -f "$init" ] && [ "$(sha256_file "$init")" = "$want" ] \
        || overlay_fail "PRECONDITION MISSING: build/native/bin/$na/rustup-init is absent or is not the pinned build; run scripts/native/prefetch.sh --arch $na."
    wheel="$(ls "$PROJECT_ROOT/build/native/wheelhouse/$na"/maturin-"$(native_pin MATURIN_VERSION)"-*.whl 2>/dev/null | head -1)"
    [ -n "$wheel" ] \
        || overlay_fail "PRECONDITION MISSING: no maturin wheel in build/native/wheelhouse/$na; run scripts/native/prefetch.sh --arch $na."
    local req
    req="$(maturin_requirement "$PROJECT_ROOT/scripts/native/build-requirements-$na.txt" "$(native_pin MATURIN_VERSION)")" \
        || overlay_fail "scripts/native/build-requirements-$na.txt has no hashed maturin==$(native_pin MATURIN_VERSION)"

    rm -rf "$ctx" || overlay_fail "could not remove the old build context $ctx"
    mkdir -p "$ctx/payload/_native" "$ctx/payload/_vendor" "$ctx/rust/wheels" "$ctx/dist" \
        || overlay_fail "could not make the build context $ctx"
    # This runs in a command substitution, where `set -e` does not reach: every step checks itself.
    cp -R "$src/_native/$na" "$src/_native/share" "$ctx/payload/_native/" || overlay_fail "could not copy the payload's _native into the context"
    cp -R "$src/_vendor/$na" "$src/_vendor/common" "$ctx/payload/_vendor/" || overlay_fail "could not copy the payload's _vendor into the context"
    cp "$init" "$ctx/rust/rustup-init" || overlay_fail "could not copy rustup-init into the context"
    cp "$PROJECT_ROOT/rust-toolchain.toml" "$ctx/rust/rust-toolchain.toml" || overlay_fail "could not copy rust-toolchain.toml into the context"
    cp "$wheel" "$ctx/rust/wheels/" || overlay_fail "could not copy the maturin wheel into the context"
    printf '%s\n' "$req" > "$ctx/rust/maturin.req" || overlay_fail "could not write maturin.req"
    cp "$dist" "$ctx/dist/" || overlay_fail "could not copy the rasterio distribution into the context"
    echo "$ctx"
}

build_overlay() {
    local base na ctx
    base="$(gbx_dev_base_image "$PLATFORM")"
    docker image inspect "$base" >/dev/null 2>&1 \
        || overlay_fail "PRECONDITION MISSING: the base image $base is not built here; build it first (bash scripts/commands/gbx-docker-rebuild.sh --platform $PLATFORM)."
    na="$(native_arch)"
    ctx="$(assemble_overlay_context)" || exit 1
    log_info "Building the native overlay on ${base} (context ${ctx#"$PROJECT_ROOT"/})..."
    DOCKER_BUILDKIT=1 docker build \
        --platform "$PLATFORM" \
        -t "$FINAL_CHECKPOINT" \
        -f "$PROJECT_ROOT/scripts/docker/Dockerfile.gbxnative" \
        --build-arg "BASE_IMAGE=$base" \
        --build-arg "GBX_ARCH=$na" \
        --build-arg "RUST_TOOLCHAIN=$(native_pin RUST_TOOLCHAIN)" \
        --build-arg "RUSTUP_INIT_SHA256=$(native_pin "RUSTUP_INIT_SHA256_$(printf '%s' "$na" | tr '[:lower:]' '[:upper:]')")" \
        --build-arg "MATURIN_VERSION=$(native_pin MATURIN_VERSION)" \
        "${EXTRA_ARGS[@]}" "$ctx"
}

# The checkpoint image the index check and the probe read, then tag.
if [ -n "$OVERLAY" ]; then
    FINAL_CHECKPOINT="geobrix-checkpoint:${OVERLAY}-${ARCH}"
    CURRENT_STAGE="overlay"
    build_overlay
else
    FINAL_CHECKPOINT="geobrix-checkpoint:final-${ARCH}"
    # --- STEP 1: PULL LATEST BASE ---
    if [ "$FORCE_PULL" = true ]; then
        log_info "Checking for latest Ubuntu 24.04 security patches..."
        docker pull --platform "$PLATFORM" ubuntu:24.04
    fi

    # --- STEP 2: MULTI-STAGE BUILD LOOP ---
    for STAGE in "${STAGES[@]}"; do
        CURRENT_STAGE=$STAGE
        STAGE_TAG="geobrix-checkpoint:${STAGE}-${ARCH}"

        # We always use the --pull flag on the 'base' stage to link to the latest
        # Ubuntu image we just pulled locally.
        PULL_FLAG=""
        if [ "$STAGE" == "base" ] && [ "$FORCE_PULL" = true ]; then
            PULL_FLAG="--pull"
        fi

        STAGE_ARGS=()
        if [ "$STAGE" == "final" ]; then
            STAGE_ARGS=("${FINAL_STAGE_ARGS[@]}")
        fi

        log_info "Building stage: ${YELLOW}${STAGE}${NC}..."
        STAGE_START=$(date +%s)

        # Note: Using "${EXTRA_ARGS[@]}" preserves spaces in arguments.
        # Context is the project root so the Dockerfile's COPY can reach
        # python/geobrix/requirements-dev-container{,-aarch64}.{in,txt} for the hash-pinned
        # pip install. Dockerfile path is explicit via -f.
        DOCKER_BUILDKIT=1 docker build \
            $PULL_FLAG \
            --platform "$PLATFORM" \
            --target "$STAGE" \
            -t "$STAGE_TAG" \
            -f "$PROJECT_ROOT/scripts/docker/Dockerfile" \
            "${SECRET_ARGS[@]}" \
            "${STAGE_ARGS[@]}" \
            "${EXTRA_ARGS[@]}" "$PROJECT_ROOT"

        STAGE_END=$(date +%s)
        log_success "Stage ${STAGE} completed in $((STAGE_END - STAGE_START))s."
    done

fi

# --- STEP 3: CHECK THE IMAGE RECORDS NO PACKAGE INDEX, THEN TAG ---
# The index must not be in the image's history or environment. Mirrors the check in gbx-dcs-build.sh,
# and never prints the URL (patterns are read from a pipe, not placed on a command line).
# Returns 0 clean, 1 an index was found, 2 the history or config could not be read (fails closed).
verify_image_holds_no_index() {
    local meta host
    meta="$( { docker history --no-trunc "$1" && docker image inspect "$1"; } 2>/dev/null )" || return 2
    [ -n "$meta" ] || return 2
    # An ARG, ENV or RUN that assigns PIP_INDEX_URL / UV_INDEX_URL a value (a script that only READS the
    # secret, as PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)", holds no value).
    if printf '%s\n' "$meta" \
        | sed 's|PIP_INDEX_URL="\$(cat /run/secrets/pip_index_url)"||g' \
        | grep -qE '(PIP|UV)_INDEX_URL=[^[:space:]]'; then
        return 1
    fi
    if [ -n "${PIP_INDEX_URL:-}" ]; then
        host="$(printf '%s' "$PIP_INDEX_URL" | sed -E 's#^[A-Za-z][A-Za-z0-9+.-]*://##; s#^[^@/]*@##; s#[/:?].*$##')"
        # The public index appears in the Dockerfile's own default; it is no private mirror to leak.
        case "$host" in pypi.org|files.pythonhosted.org) return 0 ;; esac
        if printf '%s\n' "$meta" | grep -qF -f <(printf '%s\n' "$PIP_INDEX_URL"; [ -n "$host" ] && printf '%s\n' "$host"); then
            return 1
        fi
    fi
    return 0
}

verify_rc=0
verify_image_holds_no_index "$FINAL_CHECKPOINT" || verify_rc=$?
if [ "$verify_rc" -eq 1 ]; then
    docker rmi "$FINAL_CHECKPOINT" >/dev/null 2>&1 || true
    log_error "The package index was recorded in the image history or config; image not tagged as ${FINAL_TAG}."
    exit 1
elif [ "$verify_rc" -ne 0 ]; then
    log_error "Could not read the image history or config to verify it holds no package index; image not tagged as ${FINAL_TAG}."
    exit 1
fi
log_info "Image history and config hold no package index."

# --- STEP 4: PROBE THE NATIVE STACK, THEN TAG ---
# Modelled on the checks in gbx-dcs-build.sh: run a throwaway container from the finished image (never the
# running geobrix-dev, and with no network) and compare what it reports (scripts/docker/gbx-native-probe.sh) with
# what the image is meant to hold. Any difference leaves the image untagged (the checkpoint stays for inspection).
#
# The base image: the heavyweight tier uses the system GDAL (built from source in the gdal-builder stage, version
# from the Dockerfile) with PROJ and GEOS from noble apt; the lightweight tier's rasterio, pyogrio, pyproj and
# shapely are PyPI wheels with their own libraries and must match the architecture's lock.
# The native overlay: everything is the env wheel payload's, and the pins (scripts/native/pins.conf) say which.
# native_stack_failures <probe output>: one line per expectation that does not hold (none: all hold).
native_stack_failures() {
    local out="$1" want got name soname
    value() { printf '%s\n' "$out" | sed -n "s/^$1=//p" | head -1; }
    expect() { # <label> <want> <got>
        if [ -z "$3" ] || [ "$2" != "$3" ]; then echo "$1: want '$2', got '${3:-nothing}'"; fi
    }
    expect "GDAL" "$EXPECT_GDAL" "$(value gdal)"
    expect "PROJ" "$EXPECT_PROJ" "$(value proj)"
    expect "GEOS" "$EXPECT_GEOS" "$(value geos)"
    # The JNI's libgdal: the soname is the one the JNI file asks for (not a constant here), and it must resolve to
    # the GDAL built into /usr/local, not to some other libgdal of that name (an apt copy, say).
    soname="$(value jni_soname)"
    case "$soname" in
        libgdal.so.*) ;;
        *) echo "libgdalalljni.so needs no libgdal (readelf gave '${soname:-nothing}')" ;;
    esac
    if [ "$(value jni_libgdal)" != "/usr/local/lib/${soname:-libgdal.so}" ]; then
        echo "libgdalalljni.so does not resolve ${soname:-libgdal} in /usr/local/lib (ldd gave '$(value jni_libgdal)')"
    fi
    if [ "$(value jni_unresolved)" != "0" ]; then
        echo "libgdalalljni.so has unresolved libraries: '$(value jni_unresolved)'"
    fi
    expect "rasterio's GDAL" "$EXPECT_RASTERIO_GDAL" "$(value rasterio_gdal)"
    case ",$(value rasterio_tags)," in
        *manylinux*) ;;
        *) echo "rasterio is not a manylinux wheel (wheel tags: '$(value rasterio_tags)'); built from source against the system GDAL?" ;;
    esac
    if [ "$(value rasterio_libs)" != "true" ]; then
        echo "rasterio has no vendored rasterio.libs directory"
    fi
    for name in pyogrio pyproj shapely; do
        want="$(sed -n "s/^$name==\([^ ]*\).*/\1/p" "$LOCK_FILE" | head -1)"
        got="$(value "$name")"
        if [ -z "$want" ]; then echo "$name is not pinned in $(basename "$LOCK_FILE")"; else expect "$name (lock)" "$want" "$got"; fi
    done
    return 0
}

# native_overlay_failures <probe output> <payload MANIFEST.json>: the same for the native overlay, against the pins.
native_overlay_failures() {
    local out="$1" manifest="$2" na name soname
    value() { printf '%s\n' "$out" | sed -n "s/^$1=//p" | head -1; }
    expect() { # <label> <want> <got>
        if [ -z "$3" ] || [ "$2" != "$3" ]; then echo "$1: want '$2', got '${3:-nothing}'"; fi
    }
    na="$(native_arch)"
    expect "GDAL (payload manifest)" "$(native_pin GDAL_VERSION)" "$(value gdal)"
    expect "PROJ (payload manifest)" "$(native_pin PROJ_VERSION)" "$(value proj)"
    expect "GEOS (payload manifest)" "$(native_pin GEOS_VERSION)" "$(value geos)"
    # what the bindings load is what the pins name, and it is the payload's
    expect "osgeo's GDAL" "$(native_pin GDAL_VERSION)" "$(value osgeo_gdal)"
    expect "rasterio's GDAL" "$(native_pin GDAL_VERSION)" "$(value rasterio_gdal)"
    expect "pyogrio's GDAL" "$(native_pin GDAL_VERSION)" "$(value pyogrio_gdal)"
    expect "pyproj's PROJ" "$(native_pin PROJ_VERSION)" "$(value pyproj_proj)"
    expect "shapely's GEOS" "$(native_pin GEOS_VERSION)" "$(value shapely_geos)"
    for name in osgeo rasterio pyogrio pyproj shapely; do
        if [ "$(value "${name}_from_payload")" != "true" ]; then
            echo "$name is not imported from the payload (from_payload '$(value "${name}_from_payload")', error '$(value "${name}_error")')"
        fi
    done
    expect "payload native revision" "$(native_pin GBX_NATIVE_REVISION)" "$(value native_revision)"
    expect "rasterio distribution" "$(native_pin RASTERIO_VERSION)+gbx$(native_pin GBX_NATIVE_REVISION)" "$(value rasterio_dist)"
    if [ -n "$(value pypi_libs)" ]; then
        echo "a process importing the bindings maps a PyPI wheel's libraries: $(value pypi_libs)"
    fi
    # the JNI: its libgdal is the payload's (the soname is whatever the JNI file asks for, and the manifest must
    # know it), the link the JVM loads is the payload's own file, and nothing is unresolved
    soname="$(value jni_soname)"
    case "$soname" in
        libgdal*) ;;
        *) echo "libgdalalljni.so needs no libgdal (readelf gave '${soname:-nothing}')" ;;
    esac
    if [ -n "$soname" ] && ! python3 -c '
import json, sys
sys.exit(0 if sys.argv[2] in json.load(open(sys.argv[1]))["sonames"].values() else 1)
' "$manifest" "$soname" 2>/dev/null; then
        echo "the JNI needs $soname, which the payload's MANIFEST does not list among its sonames"
    fi
    expect "HDF5 plugin directory is a link to the payload's filter plugins" "${OVERLAY_PAYLOAD_ROOT}/_native/${na}/hdf5-plugins" "$(value hdf5_plugin_dir)"
    expect "libgdalalljni.so resolves libgdal in the payload" "${OVERLAY_PAYLOAD_ROOT}/_native/${na}/lib/${soname}" "$(value jni_libgdal)"
    expect "/usr/lib/libgdalalljni.so is a link to the payload's JNI" "${OVERLAY_PAYLOAD_ROOT}/_native/${na}/lib/libgdalalljni.so" "$(value jni_link)"
    if [ "$(value jni_unresolved)" != "0" ]; then
        echo "libgdalalljni.so has unresolved libraries: '$(value jni_unresolved)'"
    fi
    return 0
}

# run_native_probe <image> [probe arguments]: the probe in a throwaway container with no network; sets PROBE_OUT
# and PROBE_ERR_TAIL (the last lines of its stderr, URLs removed) and returns docker's status.
run_native_probe() {
    local image="$1" err rc=0 extra=()
    shift
    if [ $# -gt 0 ]; then extra=(-- "$@"); fi
    err="$(mktemp "${TMPDIR:-/tmp}/gbx-probe-err.XXXXXX")" || return 1
    PROBE_OUT="$(docker run --rm -i --network none --platform "$PLATFORM" --entrypoint bash "$image" -s "${extra[@]}" < "$SCRIPT_DIR/gbx-native-probe.sh" 2>"$err")" || rc=$?
    PROBE_ERR_TAIL="$(tail -n 20 "$err" | sed -E 's#[A-Za-z][A-Za-z0-9+.-]*://[^ ]+#<url>#g')"
    rm -f "$err"
    return "$rc"
}

probe_rc=0
if [ -n "$OVERLAY" ]; then
    run_native_probe "$FINAL_CHECKPOINT" --payload "$OVERLAY_PAYLOAD_ROOT" || probe_rc=$?
else
    run_native_probe "$FINAL_CHECKPOINT" || probe_rc=$?
fi
if [ "$probe_rc" -ne 0 ] || [ -z "$PROBE_OUT" ]; then
    log_error "Could not run the native-stack probe in the image (exit $probe_rc); image not tagged as ${FINAL_TAG}."
    if [ -n "$PROBE_ERR_TAIL" ]; then
        log_error "probe stderr (last lines):"
        printf '%s\n' "$PROBE_ERR_TAIL" | sed 's/^/    /' >&2
    fi
    exit 1
fi
if [ -n "$OVERLAY" ]; then
    NATIVE_FAILURES="$(native_overlay_failures "$PROBE_OUT" "$PROJECT_ROOT/build/devimage/$(native_arch)/payload/_native/$(native_arch)/MANIFEST.json")"
else
    NATIVE_FAILURES="$(native_stack_failures "$PROBE_OUT")"
fi
if [ -n "$NATIVE_FAILURES" ]; then
    while IFS= read -r line; do log_error "native stack: $line"; done <<< "$NATIVE_FAILURES"
    log_error "The image's native stack is not what it should be; image not tagged as ${FINAL_TAG} (checkpoint ${FINAL_CHECKPOINT} kept for inspection)."
    exit 1
fi
if [ -n "$OVERLAY" ]; then
    log_info "Native stack as expected: GDAL $(native_pin GDAL_VERSION), PROJ $(native_pin PROJ_VERSION), GEOS $(native_pin GEOS_VERSION) from the payload; the JVM's JNI link and every binding are the payload's; no PyPI library is mapped."
else
    log_info "Native stack as expected: GDAL ${EXPECT_GDAL}, PROJ ${EXPECT_PROJ}, GEOS ${EXPECT_GEOS}; JNI resolves its libgdal in /usr/local/lib; rasterio is a manylinux wheel; pyogrio, pyproj and shapely match the lock."
fi

log_info "Applying final tag: ${YELLOW}${FINAL_TAG}${NC}..."
docker tag "$FINAL_CHECKPOINT" "$FINAL_TAG"

# --- SUMMARY ---
END_TIME=$(date +%s)
TOTAL_RUNTIME=$((END_TIME - START_TIME))

echo -e "\n${GREEN}===========================================${NC}"
log_success "Smart Build Complete!"
log_info "Final Image: ${FINAL_TAG}"
log_info "Total Build Time: $((TOTAL_RUNTIME / 60))m $((TOTAL_RUNTIME % 60))s"
echo -e "${GREEN}===========================================${NC}"