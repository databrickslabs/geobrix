#!/bin/bash
# Start Docker container with Volumes mount for sample data testing
# Run from project root: sh scripts/docker/start_docker_with_volumes.sh [--privileged]
#
# Ports: 5005=JDWP, 8888, 4040=Spark UI. Goal: testbook + kernel work; Jupyter Lab not required.
# Optional: --privileged for kernel/ZMQ or entropy issues (see README-DOCKER.md).

PRIVILEGED=""
[[ "$1" == "--privileged" ]] && PRIVILEGED="--privileged"

# Create sample-data directory if it doesn't exist
mkdir -p sample-data/Volumes/main/default/geobrix_samples

docker run --platform linux/amd64 --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/geobrix \
-v $PWD/sample-data/Volumes:/Volumes \
-e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
$PRIVILEGED \
-itd geobrix-dev:ubuntu24-gdal311-spark /bin/bash

echo "✅ Docker container started with Volumes mounted to: $PWD/sample-data/Volumes"
echo "   In container: /Volumes -> host: $PWD/sample-data/Volumes"
echo ""
echo "To connect: docker exec -it geobrix-dev /bin/bash"
echo "To test Essential Bundle: docker exec -it geobrix-dev python3 /root/geobrix/scripts/test-essential-bundle.py"
echo "To test Complete Bundle: docker exec -it geobrix-dev python3 /root/geobrix/scripts/test-complete-bundle.py"
