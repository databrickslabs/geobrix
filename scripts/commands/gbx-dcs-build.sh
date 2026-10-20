#!/bin/bash
# gbx:dcs:build - Build the GeoBrix Databricks Container Services image (dedicated clusters) from
# the built JAR (target/) and wheel (python/geobrix/dist/), then verify the image's contents.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ARCH="arm64"
TARGET="release"
LOG_PATH=""
VER=""
REPO="${GBX_DCS_REPO:-mjohns797/geobrix}"

usage() {
    show_banner "DCS: build the GeoBrix image"
    echo "Usage: bash scripts/commands/gbx-dcs-build.sh [--arch arm64|amd64] [--target base|release] [--log <path>]"
    echo ""
    echo "  --arch    arm64 (default; Graviton, databricksruntime/environment:v6-standard-arm)"
    echo "            amd64 (x86, databricksruntime/environment:v6-standard)"
    echo "  --target  release (default): base + wheel + JAR, tagged <repo>:<ver>-env6-<arm|x86>-rc"
    echo "            base: DCS env 6 + GDAL 3.11.4 only, tagged <repo>:base-env6-<arm|x86>"
    echo "  --log     Log file (filename -> test-logs/<name>)"
    echo ""
    echo "Env: GBX_DCS_REPO (default mjohns797/geobrix); PIP_INDEX_URL (optional package index for the"
    echo "     wheel install, passed to the build as a BuildKit secret so it never lands in the image)."
    echo "Publishing is separate and run by the release owner: gbx:dcs:publish."
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch) ARCH="$2"; shift 2 ;;
        --target) TARGET="$2"; shift 2 ;;
        --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$ARCH" in
    arm64) BASE_IMAGE="databricksruntime/environment:v6-standard-arm"; SUFFIX="env6-arm" ;;
    amd64) BASE_IMAGE="databricksruntime/environment:v6-standard"; SUFFIX="env6-x86" ;;
    *) echo "--arch must be arm64 or amd64" >&2; exit 1 ;;
esac
case "$TARGET" in base|release) ;; *) echo "--target must be base or release" >&2; exit 1 ;; esac

show_banner "DCS: build the GeoBrix image ($TARGET, $ARCH)"
setup_log_file "$LOG_PATH"
docker info > /dev/null 2>&1 || { echo "PRECONDITION MISSING: Docker is not running; gbx:dcs:build not run"; exit 1; }

# No EXIT trap here: setup_log_file owns it (it waits for the log tee). Clean up explicitly instead.
CTX=$(mktemp -d)
SECRET_FILE=""
cleanup() { rm -rf "$CTX"; if [ -n "$SECRET_FILE" ]; then rm -f "$SECRET_FILE"; fi; }
die() { cleanup; echo "$1"; exit 1; }
cp "$PROJECT_ROOT/scripts/docker/dcs/Dockerfile" "$CTX/Dockerfile" || die "PRECONDITION MISSING: could not stage the Dockerfile; gbx:dcs:build not run"

if [ "$TARGET" = "release" ]; then
    shopt -s nullglob
    JARS=("$PROJECT_ROOT"/target/geobrix-*-jar-with-dependencies.jar)
    WHLS=("$PROJECT_ROOT"/python/geobrix/dist/geobrix-*-py3-none-any.whl)
    shopt -u nullglob
    [ ${#JARS[@]} -eq 1 ] || die "PRECONDITION MISSING: expected exactly one target/geobrix-*-jar-with-dependencies.jar, found ${#JARS[@]}; gbx:dcs:build not run"
    [ ${#WHLS[@]} -eq 1 ] || die "PRECONDITION MISSING: expected exactly one python/geobrix/dist/geobrix-*-py3-none-any.whl, found ${#WHLS[@]}; gbx:dcs:build not run"
    JAR="${JARS[0]}"; WHL="${WHLS[0]}"
    VER=$(basename "$JAR" | sed -nE 's/^geobrix-(.+)-jar-with-dependencies\.jar$/\1/p')
    WVER=$(basename "$WHL" | sed -nE 's/^geobrix-(.+)-py3-none-any\.whl$/\1/p')
    [ "$VER" = "$WVER" ] || die "PRECONDITION MISSING: JAR version $VER != wheel version $WVER; gbx:dcs:build not run"
    python3 "$PROJECT_ROOT/scripts/ci/check_jar_packaging.py" "$JAR" || die "PRECONDITION MISSING: rebuild the JAR (mvn clean package); gbx:dcs:build not run"
    cp "$JAR" "$WHL" "$CTX/" || die "PRECONDITION MISSING: could not stage the JAR and wheel; gbx:dcs:build not run"
    TAG="$REPO:${VER}-${SUFFIX}-rc"
else
    TAG="$REPO:base-${SUFFIX}"
fi

# A package index (if any) goes in as a BuildKit secret from a private file outside the build context,
# so it never lands in the image history; the file exists only for the duration of the build.
if [ -n "${PIP_INDEX_URL:-}" ]; then
    SECRET_FILE=$(mktemp "${TMPDIR:-/tmp}/gbx-dcs-pip-index.XXXXXX") || die "PRECONDITION MISSING: could not create a temp file for the package index; gbx:dcs:build not run"
    chmod 600 "$SECRET_FILE"
    printf '%s' "$PIP_INDEX_URL" > "$SECRET_FILE"
fi

build_rc=0
DOCKER_BUILDKIT=1 docker build --platform "linux/$ARCH" --target "$TARGET" \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    ${VER:+--build-arg GEOBRIX_VERSION="$VER"} \
    ${SECRET_FILE:+--secret id=pip_index_url,src="$SECRET_FILE"} \
    -t "$TAG" "$CTX" || build_rc=$?
cleanup
[ "$build_rc" -eq 0 ] || { echo "BUILD FAILED: docker build exited $build_rc for $TAG"; exit "$build_rc"; }

# Verify what the cluster will see.
docker run --rm --platform "linux/$ARCH" --entrypoint sh "$TAG" -c '
    set -e
    gdalinfo --version | grep -q "GDAL 3.11.4"
    test -e /usr/lib/libgdalalljni.so
' || { echo "VERIFY FAILED: GDAL 3.11.4 / libgdalalljni.so missing in $TAG"; exit 1; }
if [ "$TARGET" = "release" ]; then
    JAR_SHA=$(shasum -a 256 "$JAR" | cut -d" " -f1)
    docker run --rm --platform "linux/$ARCH" --entrypoint sh "$TAG" -c "
        set -e
        n=\$(find / -xdev -name 'geobrix-*-jar-with-dependencies.jar' 2>/dev/null | wc -l)
        [ \"\$n\" -eq 1 ] || { echo \"expected 1 GeoBrix JAR, found \$n\"; exit 1; }
        echo \"$JAR_SHA  /databricks/jars/$(basename "$JAR")\" | sha256sum -c -
        test ! -e /scala-jars/user/$(basename "$JAR")
        test ! -e /opt/geobrix
        /databricks/python3/bin/python -c 'import importlib.metadata as m; v=m.version(\"geobrix\"); assert v == \"$VER\", v'
    " || { echo "VERIFY FAILED: image contents in $TAG"; exit 1; }
fi

# The image history must not carry a package index (this image is published to a public registry).
# Only the layers this build added are inspected: the public base image's own history already names its
# build-time index host (UV_DEFAULT_INDEX). History lists newest first, so ours are the top entries.
# Counts only; never print the URL.
HIST_ALL=$(docker history --no-trunc --format '{{.CreatedBy}}' "$TAG") || { echo "VERIFY FAILED: could not read the history of $TAG"; exit 1; }
HIST_BASE=$(docker history --no-trunc --format '{{.CreatedBy}}' "$BASE_IMAGE") || { echo "VERIFY FAILED: could not read the history of $BASE_IMAGE"; exit 1; }
OWN_LAYERS=$(( $(printf '%s\n' "$HIST_ALL" | wc -l) - $(printf '%s\n' "$HIST_BASE" | wc -l) ))
HIST=$(printf '%s\n' "$HIST_ALL" | awk -v n="$OWN_LAYERS" 'NR <= n')
# The Dockerfile reads the secret as PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)"; that text holds no value.
N_VALUE=$(printf '%s\n' "$HIST" | sed 's|PIP_INDEX_URL="\$(cat /run/secrets/pip_index_url)"||g' | grep -c 'PIP_INDEX_URL=[^[:space:]]' || true)
N_HOST=0
if [ -n "${PIP_INDEX_URL:-}" ]; then
    INDEX_HOST=$(printf '%s' "$PIP_INDEX_URL" | sed -E 's#^[A-Za-z][A-Za-z0-9+.-]*://([^/@]*@)?([^/:]+).*#\2#')
    if [ -n "$INDEX_HOST" ]; then N_HOST=$(printf '%s\n' "$HIST" | grep -cF -- "$INDEX_HOST" || true); fi
fi
if [ "$N_VALUE" -ne 0 ] || [ "$N_HOST" -ne 0 ]; then
    echo "VERIFY FAILED: package index found in the history of $TAG (PIP_INDEX_URL values: $N_VALUE, configured index host: $N_HOST)"
    exit 1
fi

echo "IMAGE=$TAG"
echo "IMAGE_ID=$(docker image inspect --format '{{.Id}}' "$TAG")"
