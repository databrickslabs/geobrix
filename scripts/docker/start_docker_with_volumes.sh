#!/bin/bash
# Start Docker container with Volumes mount for sample data testing
# Run from project root: bash scripts/docker/start_docker_with_volumes.sh [--privileged] [--platform linux/arm64|linux/amd64]
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

# Create sample-data directory if it doesn't exist
mkdir -p sample-data/Volumes/main/default/geobrix_samples

docker run --platform "$PLATFORM" --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/geobrix \
-v $PWD/sample-data/Volumes:/Volumes \
$PRIVILEGED \
-itd "$IMAGE" /bin/bash

echo "✅ Docker container started ($PLATFORM, image $IMAGE) with Volumes mounted to: $PWD/sample-data/Volumes"
echo "   In container: /Volumes -> host: $PWD/sample-data/Volumes"
echo ""
echo "To connect: docker exec -it geobrix-dev /bin/bash"
echo "To test Essential Bundle: docker exec -it geobrix-dev python3 /root/geobrix/scripts/test-essential-bundle.py"
echo "To test Complete Bundle: docker exec -it geobrix-dev python3 /root/geobrix/scripts/test-complete-bundle.py"
