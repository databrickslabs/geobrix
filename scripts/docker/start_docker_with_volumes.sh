#!/bin/bash
# Start this checkout's dev container with the Volumes mount for sample data testing
# Run from anywhere: bash scripts/docker/start_docker_with_volumes.sh [--privileged] [--platform linux/arm64|linux/amd64]
# The checkout is the one this script lives in. The container name, ports and extra mounts are the main
# checkout's (geobrix-dev, 5005/8888/4040, <checkout>/sample-data/Volumes at /Volumes) unless a linked
# worktree's .gbx-local.env says otherwise (gbx-container.sh).
#
# Ports: 5005 = JDWP remote debug (attach your IDE's "Remote JVM Debug" to localhost:5005);
#        8888 = Jupyter's default port, kept for an ad-hoc `jupyter` server (none starts by default);
#        4040 = Spark UI. Goal: testbook + kernel work; Jupyter Lab not required.
# JDWP is opt-in per run: `mvn test -DagentLib='${jdwp.agent}' ...` (pom.xml property) puts the
# agent on the forked test JVM only. To instead attach it to EVERY JVM in the container (the old
# always-on default; a second concurrent JVM fails because both bind 5005, which is why gbx
# commands unset it), add this line to the docker run below:
#   -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
# Optional: --privileged for kernel/ZMQ or entropy issues (see README-DOCKER.md).
# Platform: --platform linux/arm64|linux/amd64, else $GBX_DOCKER_PLATFORM, else this host's
# architecture; the image is geobrix-dev:ubuntu24-gdal311-spark-<arch> (see gbx-platform.sh).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/gbx-container.sh"
gbx_load_local_env "$REPO_ROOT" || exit 1
source "$SCRIPT_DIR/gbx-platform.sh"

PRIVILEGED=""
PLATFORM_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) echo "usage: $0 [--privileged] [--platform linux/arm64|linux/amd64]  (platform: else \$GBX_DOCKER_PLATFORM, else this host)"; exit 0 ;;
        --privileged) PRIVILEGED="--privileged"; shift ;;
        --platform)
            if [ $# -lt 2 ]; then echo "ERROR: --platform needs a value (linux/arm64 or linux/amd64)" >&2; exit 2; fi
            PLATFORM_ARG="$2"; shift 2 ;;
        --platform=*) PLATFORM_ARG="${1#--platform=}"; shift ;;
        *) echo "ERROR: unknown option: $1 (usage: $0 [--privileged] [--platform linux/arm64|linux/amd64])" >&2; exit 2 ;;
    esac
done

PLATFORM="$(gbx_docker_platform "$PLATFORM_ARG")" || exit 2
IMAGE="$(gbx_require_dev_image "$PLATFORM")" || exit 1

# Create the sample-data directory if it doesn't exist
mkdir -p "$GBX_DEV_VOLUMES_DIR/main/default/geobrix_samples"

gbx_dev_run_args "$REPO_ROOT"
docker run --platform "$PLATFORM" "${GBX_DEV_RUN_ARGS[@]}" \
$PRIVILEGED \
-itd "$IMAGE" /bin/bash

echo "✅ Docker container $GBX_DEV_CONTAINER started ($PLATFORM, image $IMAGE) with Volumes mounted to: $GBX_DEV_VOLUMES_DIR"
echo "   In container: /Volumes -> host: $GBX_DEV_VOLUMES_DIR"
echo ""
echo "To connect: docker exec -it $GBX_DEV_CONTAINER /bin/bash"
echo "To test Essential Bundle: docker exec -it $GBX_DEV_CONTAINER python3 /root/geobrix/scripts/test-essential-bundle.py"
echo "To test Complete Bundle: docker exec -it $GBX_DEV_CONTAINER python3 /root/geobrix/scripts/test-complete-bundle.py"
