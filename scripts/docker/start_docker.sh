#!/bin/bash
# Starts the geobrix-dev container with the repo bind-mounted at /root/geobrix.
# The mount source is resolved from git (not $PWD) so the container is always bound
# to the repository root regardless of which subdirectory you launch from, and so a
# stale/deleted CWD can't dangle the mount. Run from anywhere inside the repo:
#   bash scripts/docker/start_docker.sh

# Resolve the repository top-level via git. Errors clearly if not inside a git work tree.
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ] || [ ! -d "$REPO_ROOT" ]; then
  echo "❌ Not inside a git work tree (or top-level not found). cd into the geobrix repo and retry." >&2
  exit 1
fi

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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
source "$SCRIPT_DIR/gbx-platform.sh"
PLATFORM="$(gbx_docker_platform)" || exit 2
IMAGE="$(gbx_require_dev_image "$PLATFORM")" || exit 1

docker run --platform "$PLATFORM" --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v "$REPO_ROOT":/root/geobrix \
-itd "$IMAGE" /bin/bash
