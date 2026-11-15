#!/bin/bash
# Starts this checkout's dev container (geobrix-dev for the main checkout; a linked worktree names its
# own in .gbx-local.env, see gbx-container.sh) with the repo bind-mounted at /root/geobrix.
# The mount source is the repository root this script lives in (not $PWD), so the container is always
# bound to the repository root regardless of which directory you launch from, and a stale/deleted CWD
# can't dangle the mount. Run from anywhere:
#   bash scripts/docker/start_docker.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Guard against binding to an ephemeral agent worktree (.claude/worktrees/*), which get
# auto-cleaned out from under a long-lived container and dangle the mount -> exec fails with
# "current working directory is outside of container mount namespace root".
case "$REPO_ROOT" in
  */.claude/worktrees/*)
    echo "⚠️  Top-level resolves to a temporary worktree:" >&2
    echo "      $REPO_ROOT" >&2
    echo "    These get auto-cleaned and will dangle the container mount." >&2
    echo "    cd into the main checkout before starting the dev container." >&2
    exit 1
    ;;
esac

# Ports: 5005 = JDWP remote debug (attach your IDE's "Remote JVM Debug" to localhost:5005);
#        8888 = Jupyter's default port, kept for an ad-hoc `jupyter` server (none starts by default);
#        4040 = Spark UI.
# JDWP is opt-in per run: `mvn test -DagentLib='${jdwp.agent}' ...` (pom.xml property) puts the
# agent on the forked test JVM only. To instead attach it to EVERY JVM in the container (the old
# always-on default; a second concurrent JVM fails because both bind 5005, which is why gbx
# commands unset it), add this line to the docker run below:
#   -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
#
# Platform: the host's architecture, or $GBX_DOCKER_PLATFORM (linux/arm64 | linux/amd64). The image is
# geobrix-dev:ubuntu24-gdal311-spark-<arch> (see gbx-platform.sh).
# Container name, ports and mounts: gbx-container.sh (the defaults above are the main checkout's).
source "$SCRIPT_DIR/gbx-container.sh"
gbx_load_local_env "$REPO_ROOT" || exit 1
source "$SCRIPT_DIR/gbx-platform.sh"
PLATFORM="$(gbx_docker_platform)" || exit 2
IMAGE="$(gbx_require_dev_image "$PLATFORM")" || exit 1

gbx_dev_run_args "$REPO_ROOT" --no-volumes
docker run --platform "$PLATFORM" "${GBX_DEV_RUN_ARGS[@]}" \
-itd "$IMAGE" /bin/bash
