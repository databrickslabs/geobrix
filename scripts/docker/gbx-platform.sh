#!/bin/bash
# gbx-platform.sh: which CPU platform the geobrix-dev image is built and run for, and what the
# image is called. SOURCED (never executed) by build_smart.sh, start_docker.sh,
# start_docker_with_volumes.sh, extras/geobrix_docker.sh and scripts/commands/gbx-docker-*.sh,
# so the build and the run side cannot disagree about the platform or the tag.
#
# Platform: an explicit value (a --platform flag) wins, then $GBX_DOCKER_PLATFORM, then the
# host's own architecture (uname -m). There is no amd64 default: an arm64 host builds and runs
# a native arm64 image (an amd64 image on an arm64 Mac runs under Rosetta, about 10x slower).
# Accepted spellings: linux/arm64, linux/amd64, arm64, amd64, aarch64, x86_64.
#
# Tag: one image per architecture, so both coexist and falling back is instant:
#   geobrix-dev:ubuntu24-gdal311-spark-arm64
#   geobrix-dev:ubuntu24-gdal311-spark-amd64
# The un-suffixed geobrix-dev:ubuntu24-gdal311-spark is the pre-arm64 (amd64) tag. It is never
# written any more; gbx_dev_run_image reads it only as a fallback for an amd64 run.
#
# Bash 3.2 compatible (macOS /bin/bash).

GBX_DEV_IMAGE_REPO="geobrix-dev"
GBX_DEV_IMAGE_TAG="${GBX_DEV_IMAGE_TAG:-ubuntu24-gdal311-spark}"   # a worktree's .gbx-local.env may name its own (gbx-container.sh)
GBX_DEV_LEGACY_IMAGE="${GBX_DEV_IMAGE_REPO}:${GBX_DEV_IMAGE_TAG}"

# gbx_normalize_platform <spelling>  ->  linux/arm64 | linux/amd64 (status 2 for anything else)
gbx_normalize_platform() {
    local v="${1#linux/}"
    case "$v" in
        arm64|arm64/v8|aarch64) echo "linux/arm64" ;;
        amd64|x86_64|x86-64) echo "linux/amd64" ;;
        *) return 2 ;;
    esac
}

# gbx_host_platform  ->  the platform of this machine (uname -m)
gbx_host_platform() {
    local m
    m="$(uname -m)"
    # A shell started under Rosetta reports x86_64 on an Apple Silicon Mac; the CPU says what the host is.
    if [ "$m" = "x86_64" ] && [ "$(uname -s)" = "Darwin" ] && [ "$(sysctl -n hw.optional.arm64 2>/dev/null)" = "1" ]; then
        m="arm64"
    fi
    gbx_normalize_platform "$m" || {
        echo "ERROR: unsupported host architecture '$m' (expected arm64/aarch64 or x86_64/amd64); set GBX_DOCKER_PLATFORM" >&2
        return 2
    }
}

# gbx_docker_platform [explicit]  ->  explicit, else $GBX_DOCKER_PLATFORM, else the host platform
gbx_docker_platform() {
    local raw="${1:-}" source="--platform"
    if [ -z "$raw" ] && [ -n "${GBX_DOCKER_PLATFORM:-}" ]; then
        raw="$GBX_DOCKER_PLATFORM"
        source="GBX_DOCKER_PLATFORM"
    fi
    if [ -z "$raw" ]; then
        gbx_host_platform
        return $?
    fi
    gbx_normalize_platform "$raw" || {
        echo "ERROR: unsupported platform '$raw' from $source (expected linux/arm64 or linux/amd64)" >&2
        return 2
    }
}

# gbx_platform_arch <platform>  ->  arm64 | amd64
gbx_platform_arch() {
    local p
    p="$(gbx_normalize_platform "$1")" || return 2
    echo "${p#linux/}"
}

# gbx_dev_image_tag <platform>  ->  geobrix-dev:ubuntu24-gdal311-spark-<arch>
gbx_dev_image_tag() {
    local a
    a="$(gbx_platform_arch "$1")" || return 2
    echo "${GBX_DEV_IMAGE_REPO}:${GBX_DEV_IMAGE_TAG}-${a}"
}

# gbx_dev_run_image <platform>  ->  the image to run for that platform: the per-arch tag when it
# exists locally; for amd64 only, the pre-arm64 un-suffixed tag when that image really is amd64;
# otherwise the per-arch tag (so `docker run` reports the missing image by its real name).
gbx_dev_run_image() {
    local tag arch
    tag="$(gbx_dev_image_tag "$1")" || return 2
    arch="$(gbx_platform_arch "$1")"
    if docker image inspect "$tag" >/dev/null 2>&1; then
        echo "$tag"
    elif [ "$arch" = "amd64" ] && [ "$(docker image inspect --format '{{.Architecture}}' "$GBX_DEV_LEGACY_IMAGE" 2>/dev/null)" = "amd64" ]; then
        echo "$GBX_DEV_LEGACY_IMAGE"
    else
        echo "$tag"
    fi
}

# gbx_container_arch <container>  ->  amd64 | arm64 (the architecture of the image it runs from)
gbx_container_arch() {
    local image
    image="$(docker inspect --format '{{.Image}}' "$1" 2>/dev/null)" || return 1
    docker image inspect --format '{{.Architecture}}' "$image" 2>/dev/null
}

# gbx_require_dev_image <platform>  ->  the image to run (gbx_dev_run_image); when it does not exist
# locally, says how to build it (or how to run the other architecture) on stderr and returns 1
# rather than letting `docker run` try to pull a local-only tag from a registry.
gbx_require_dev_image() {
    local image arch other
    image="$(gbx_dev_run_image "$1")" || return 2
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        arch="$(gbx_platform_arch "$1")"
        other="amd64"
        [ "$arch" = "amd64" ] && other="arm64"
        {
            echo "ERROR: image $image not found for $(gbx_normalize_platform "$1")."
            echo "  Build it:    bash scripts/commands/gbx-docker-rebuild.sh --platform $(gbx_normalize_platform "$1")"
            echo "  Or run the $other image instead: GBX_DOCKER_PLATFORM=linux/$other (an image of the other"
            echo "  architecture runs under emulation, about 10x slower)."
        } >&2
        return 1
    fi
    echo "$image"
}

# gbx_check_docker_flags <docker build flags...>  ->  status 2 (and says why) for a flag build_smart.sh owns:
# a platform (it is chosen with --platform / GBX_DOCKER_PLATFORM, and the tag follows it) or the package index
# as a build-arg (it travels as a BuildKit secret; a build-arg is recorded in the image history).
gbx_check_docker_flags() {
    local a
    for a in "$@"; do
        case "$a" in
            --platform|--platform=*)
                echo "ERROR: pass the platform as --platform <p> before any '--' (or set GBX_DOCKER_PLATFORM), not as a docker flag." >&2
                return 2
                ;;
            PIP_INDEX_URL=*|--build-arg=PIP_INDEX_URL=*)
                echo "ERROR: do not pass the package index as a build-arg (it would be recorded in the image history). Export PIP_INDEX_URL; it is passed as a BuildKit secret." >&2
                return 2
                ;;
        esac
    done
}
