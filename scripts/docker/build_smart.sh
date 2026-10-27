#!/bin/bash

# --- CONFIGURATION ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Platform and image-tag resolution shared with the start scripts.
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
    exit 0
}

# --- ARGUMENT PARSING ---
EXTRA_ARGS=()
FORCE_PULL=true
PLATFORM_ARG=""

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

# The platform is chosen above, and the package index travels as a secret (below): a docker flag
# that sets either again would build one thing and tag another, or put the index in the history.
gbx_check_docker_flags "${EXTRA_ARGS[@]}" || exit 2

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
index_fingerprint() {
    local salt_file="${GBX_PIP_INDEX_SALT_FILE:-${XDG_CACHE_HOME:-$HOME/.cache}/geobrix/pip-index-fpr-salt}"
    if [ ! -s "$salt_file" ]; then
        # a directory made here is private; one that already exists is left as it is
        if [ ! -d "$(dirname "$salt_file")" ]; then
            mkdir -p "$(dirname "$salt_file")" && chmod 700 "$(dirname "$salt_file")" || return 1
        fi
        ( umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$salt_file" ) || return 1
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
if [ -n "${PIP_INDEX_URL:-}" ]; then
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

FINAL_CHECKPOINT="geobrix-checkpoint:final-${ARCH}"
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
# running geobrix-dev) and compare what it reports (scripts/docker/gbx-native-probe.sh) with what the image
# is meant to hold. The heavyweight tier uses the system GDAL (built from source in the gdal-builder stage,
# version from the Dockerfile) with PROJ and GEOS from noble apt; the lightweight tier's rasterio, pyogrio,
# pyproj and shapely are PyPI wheels with their own libraries and must match the architecture's lock. Any
# difference leaves the image untagged (the checkpoint stays for inspection).
EXPECT_GDAL="$(sed -n 's/^ARG GDAL_VERSION=//p' "$PROJECT_ROOT/scripts/docker/Dockerfile" | head -1)"
EXPECT_PROJ="9.4.0"           # noble libproj-dev
EXPECT_GEOS="3.12.1"          # noble libgeos-dev
EXPECT_RASTERIO_GDAL="3.12.1" # the GDAL vendored in the rasterio 1.5.0 manylinux wheel, not the system one
LOCK_FILE="$PROJECT_ROOT/python/geobrix/requirements-dev-container.txt"
if [ "$ARCH" = "arm64" ]; then
    LOCK_FILE="$PROJECT_ROOT/python/geobrix/requirements-dev-container-aarch64.txt"
fi

# native_stack_failures <probe output>: one line per expectation that does not hold (none: all hold).
native_stack_failures() {
    local out="$1" want got name
    value() { printf '%s\n' "$out" | sed -n "s/^$1=//p" | head -1; }
    expect() { # <label> <want> <got>
        if [ -z "$3" ] || [ "$2" != "$3" ]; then echo "$1: want '$2', got '${3:-nothing}'"; fi
    }
    expect "GDAL" "$EXPECT_GDAL" "$(value gdal)"
    expect "PROJ" "$EXPECT_PROJ" "$(value proj)"
    expect "GEOS" "$EXPECT_GEOS" "$(value geos)"
    case "$(value jni_libgdal)" in
        */libgdal.so.37) ;;
        *) echo "libgdalalljni.so does not resolve libgdal.so.37 (ldd gave '$(value jni_libgdal)')" ;;
    esac
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

probe_rc=0
PROBE_OUT="$(docker run --rm -i --platform "$PLATFORM" --entrypoint bash "$FINAL_CHECKPOINT" -s < "$SCRIPT_DIR/gbx-native-probe.sh" 2>/dev/null)" || probe_rc=$?
if [ "$probe_rc" -ne 0 ] || [ -z "$PROBE_OUT" ]; then
    log_error "Could not run the native-stack probe in the image (exit $probe_rc); image not tagged as ${FINAL_TAG}."
    exit 1
fi
NATIVE_FAILURES="$(native_stack_failures "$PROBE_OUT")"
if [ -n "$NATIVE_FAILURES" ]; then
    while IFS= read -r line; do log_error "native stack: $line"; done <<< "$NATIVE_FAILURES"
    log_error "The image's native stack is not what it should be; image not tagged as ${FINAL_TAG} (checkpoint ${FINAL_CHECKPOINT} kept for inspection)."
    exit 1
fi
log_info "Native stack as expected: GDAL ${EXPECT_GDAL}, PROJ ${EXPECT_PROJ}, GEOS ${EXPECT_GEOS}; JNI resolves libgdal.so.37; rasterio is a manylinux wheel; pyogrio, pyproj and shapely match the lock."

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