#!/bin/bash
# gbx:dcs:build - Build the GeoBrix Databricks Container Services image (dedicated clusters) from the built env6
# wheel (dist/env6/) and the per-arch lock (python/geobrix/envs/env6/dcs-<arch>.lock), then verify the image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ARCH="arm64"
RELOCK=0
LOG_PATH=""
REPO="${GBX_DCS_REPO:-mjohns797/geobrix}"

usage() {
    show_banner "DCS: build the GeoBrix image"
    echo "Usage: bash scripts/commands/gbx-dcs-build.sh [--arch arm64|amd64] [--relock] [--log <path>]"
    echo ""
    echo "  --arch    arm64 (default; Graviton, databricksruntime/environment:v6-standard-arm)"
    echo "            amd64 (x86, databricksruntime/environment:v6-standard)"
    echo "  --relock  first regenerate the lock (python/geobrix/envs/env6/dcs-<arch>.lock and its base list) from"
    echo "            dcs.in, the base image and the built rasterio wheel (needs uv and Docker)"
    echo "  --log     Log file (filename -> test-logs/<name>)"
    echo ""
    echo "Image: <repo>:<ver>-env6-<arm|x86>-rc, tagged only after every check passes."
    echo "Env: GBX_DCS_REPO (default mjohns797/geobrix). The locked wheels are fetched on this machine through the"
    echo "     package index pip is configured with (PIP_INDEX_URL or pip config); the image build needs none."
    echo "Publishing is separate and run by the release owner: gbx:dcs:publish."
}

# Under `set -u` a trailing `--arch` would die on an unbound $2 with no explanation.
needs_value() { echo "$1 needs a value" >&2; usage; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch) [[ $# -ge 2 ]] || needs_value "$1"; ARCH="$2"; shift 2 ;;
        --relock) RELOCK=1; shift ;;
        --log) [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$ARCH" in
    arm64) BASE_IMAGE="databricksruntime/environment:v6-standard-arm"; SUFFIX="env6-arm"; MARCH="aarch64" ;;
    amd64) BASE_IMAGE="databricksruntime/environment:v6-standard"; SUFFIX="env6-x86"; MARCH="x86_64" ;;
    *) echo "--arch must be arm64 or amd64" >&2; exit 1 ;;
esac

show_banner "DCS: build the GeoBrix image ($ARCH)"
setup_log_file "$LOG_PATH"
docker info > /dev/null 2>&1 || { echo "PRECONDITION MISSING: Docker is not running; gbx:dcs:build not run"; exit 1; }

# No EXIT trap here: setup_log_file owns it (it waits for the log tee). Clean up explicitly instead.
# The template names TMPDIR explicitly: a bare `mktemp -d` ignores it on macOS.
CTX=$(mktemp -d "${TMPDIR:-/tmp}/gbx-dcs-build.XXXXXX")
# The image is built and verified under a temporary local tag and only then given its final tag, so a
# failed check never leaves a <ver>-rc tag on an image that did not pass, and never moves a good one.
# Cleanup removes this temporary tag only.
TAG="gbx-dcs-build-tmp:$$"
cleanup() { rm -rf "$CTX"; }
die() { cleanup; echo "$1"; exit 1; }
# Ctrl-C or SIGTERM: remove the build context and the temporary tag, then exit with the signal's conventional
# status. INT and TERM only: the EXIT trap stays setup_log_file's and still runs on this exit. (bash runs the
# trap when the foreground docker command returns; Ctrl-C reaches that command too, since it signals the whole
# foreground group.)
# SIGPIPE is ignored for the same reason: with --log, stdout is a pipe into tee, and a group signal kills
# the tee as well. The next write to that pipe (bash 3.2 reports the killed docker to it before it runs the
# trap) would then kill the shell with SIGPIPE, skipping the clean-up and the exit status. Ignored, the write
# just fails. The clean-up itself writes nothing, so it is done before the closing message.
trap '' PIPE
interrupted() { cleanup; docker rmi "$TAG" > /dev/null 2>&1 || true; echo "Interrupted: removed the build's temporary files" 2> /dev/null || true; exit "$1"; }
trap 'interrupted 130' INT
trap 'interrupted 143' TERM
verify_fail() { docker rmi "$TAG" > /dev/null 2>&1 || true; echo "$1"; exit 1; }

DCS_DIR="$PROJECT_ROOT/scripts/docker/dcs"
ENV_DIR="$PROJECT_ROOT/python/geobrix/envs/env6"
cp "$DCS_DIR/Dockerfile" "$CTX/Dockerfile" || die "PRECONDITION MISSING: could not stage the Dockerfile; gbx:dcs:build not run"
command -v python3 > /dev/null 2>&1 || die "PRECONDITION MISSING: python3 is not on PATH (it reads the wheel, checks and hashes the JAR, and fetches the locked wheels); gbx:dcs:build not run"

# The env6 wheel: its payload is the image's GDAL stack, and it carries the JAR the image puts in /databricks/jars.
shopt -s nullglob
WHLS=("$PROJECT_ROOT"/dist/env6/geobrix-*+env6-py3-none-any.whl)
shopt -u nullglob
[ ${#WHLS[@]} -eq 1 ] || die "PRECONDITION MISSING: expected exactly one dist/env6/geobrix-*+env6-py3-none-any.whl (gbx:build:wheel --env 6), found ${#WHLS[@]}; gbx:dcs:build not run"
WHL="${WHLS[0]}"
WVER=$(basename "$WHL" | sed -nE 's/^geobrix-(.+)-py3-none-any\.whl$/\1/p')
VER="${WVER%+env6}"
mkdir "$CTX/geobrix-wheel" && cp "$WHL" "$CTX/geobrix-wheel/" || die "PRECONDITION MISSING: could not stage the wheel; gbx:dcs:build not run"
# The JAR is the wheel's own copy, so the image's JAR and the wheel's are one file (mode 0644 in the context).
JAR_NAME=$(python3 -c '
import os, shutil, sys, zipfile
wheel, ctx = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(wheel) as z:
    names = [n for n in z.namelist() if n.startswith("databricks/labs/gbx/_jar/") and n.endswith("-jar-with-dependencies.jar")]
    if len(names) != 1:
        sys.exit("expected one JAR in the wheel, found %d" % len(names))
    dest = os.path.join(ctx, os.path.basename(names[0]))
    with z.open(names[0]) as src, open(dest, "wb") as out:
        shutil.copyfileobj(src, out)
os.chmod(dest, 0o644)
print(os.path.basename(names[0]))
' "$WHL" "$CTX") || die "PRECONDITION MISSING: could not take the JAR out of $(basename "$WHL"); gbx:dcs:build not run"
JAR_VER=$(printf '%s' "$JAR_NAME" | sed -nE 's/^geobrix-(.+)-jar-with-dependencies\.jar$/\1/p')
[ "$JAR_VER" = "$VER" ] || die "PRECONDITION MISSING: the wheel's JAR version $JAR_VER != wheel version $VER; gbx:dcs:build not run"
# Check and hash the copy in the build context: it is the file the image gets.
CTX_JAR="$CTX/$JAR_NAME"
python3 "$PROJECT_ROOT/scripts/ci/check_jar_packaging.py" "$CTX_JAR" || die "PRECONDITION MISSING: rebuild the wheel with a clean JAR (mvn clean package, gbx:build:wheel); gbx:dcs:build not run"
JAR_SHA=$(python3 -c '
import hashlib, sys
h = hashlib.sha256()
with open(sys.argv[1], "rb") as f:
    for block in iter(lambda: f.read(1 << 20), b""):
        h.update(block)
print(h.hexdigest())
' "$CTX_JAR") || die "PRECONDITION MISSING: could not hash the JAR; gbx:dcs:build not run"
FINAL_TAG="$REPO:${VER}-${SUFFIX}-rc"

# The lock and the wheels it installs beyond the base image (staged from the cache under build/dcs/, fetched
# on this machine when missing; the image build itself needs no package index).
if [ "$RELOCK" -eq 1 ]; then
    python3 "$DCS_DIR/dcs_lock.py" --root "$PROJECT_ROOT" relock "$MARCH" || die "RELOCK FAILED: dcs-$MARCH.lock not regenerated; gbx:dcs:build not run"
fi
[ -f "$ENV_DIR/dcs-$MARCH.lock" ] && [ -f "$ENV_DIR/dcs-base-$MARCH.txt" ] \
    || die "PRECONDITION MISSING: python/geobrix/envs/env6/dcs-$MARCH.lock or dcs-base-$MARCH.txt (gbx:dcs:build --relock); gbx:dcs:build not run"
# The base image is the digest the lock was made from (the tag is mutable): the build, the pull and the history
# check all use it, so a base that moved under its tag cannot get into the image unnoticed.
BASE_IMAGE=$(python3 "$DCS_DIR/dcs_lock.py" --root "$PROJECT_ROOT" base-image "$MARCH") || die "PRECONDITION MISSING: the base list pins no image digest (gbx:dcs:build --relock); gbx:dcs:build not run"
cp "$ENV_DIR/dcs-$MARCH.lock" "$CTX/dcs.lock" || die "PRECONDITION MISSING: could not stage the lock; gbx:dcs:build not run"
python3 "$DCS_DIR/dcs_lock.py" --root "$PROJECT_ROOT" wheelhouse "$MARCH" "$CTX/wheelhouse" || die "WHEELHOUSE FAILED: the locked wheels were not staged for $MARCH; gbx:dcs:build not run"
RASTERIO=$(python3 "$DCS_DIR/dcs_lock.py" --root "$PROJECT_ROOT" rasterio-pin) \
    || die "PRECONDITION MISSING: scripts/native/pins.conf (RASTERIO_VERSION); gbx:dcs:build not run"

build_rc=0
DOCKER_BUILDKIT=1 docker build --progress plain --platform "linux/$ARCH" \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    --build-arg GBX_ARCH="$MARCH" \
    --build-arg GEOBRIX_VERSION="$VER" \
    -t "$TAG" "$CTX" || build_rc=$?
cleanup
[ "$build_rc" -eq 0 ] || { echo "BUILD FAILED: docker build exited $build_rc for $FINAL_TAG"; exit "$build_rc"; }

# The checks below read $BASE_IMAGE (its history), but docker build leaves the base in BuildKit's cache, not the
# local image store ("No such image" on a host that never pulled it). Pull it for the build's platform so the
# checks never depend on a previous manual pull.
docker pull --platform "linux/$ARCH" "$BASE_IMAGE" > /dev/null || verify_fail "VERIFY FAILED: could not pull $BASE_IMAGE for linux/$ARCH (image not tagged as $FINAL_TAG)"

# Verify what the cluster will see. verify_image.py is the oracle (see its header): the payload's stack, the JNI
# and plugin links, osgeo, the lock, OpenSSL 1.1, [vizx]. No network; the script, the lock and the base list are
# mounted read-only, so none of them is in the image.
docker run --rm --platform "linux/$ARCH" --network none \
    -v "$DCS_DIR:/mnt/dcs:ro" \
    -v "$ENV_DIR/dcs-$MARCH.lock:/mnt/dcs.lock:ro" \
    -v "$ENV_DIR/dcs-base-$MARCH.txt:/mnt/dcs-base.txt:ro" \
    -e GBX_RASTERIO="$RASTERIO" \
    --entrypoint /databricks/python3/bin/python "$TAG" /mnt/dcs/verify_image.py \
    || verify_fail "VERIFY FAILED: the image does not carry the payload's stack and the lock, see the lines above (not tagged as $FINAL_TAG)"

# Exactly one GeoBrix JAR on the cluster's classpath, the wheel's own copy beside it is the same file, and the
# installed wheel is this one.
docker run --rm --platform "linux/$ARCH" --network none --entrypoint sh "$TAG" -c "
    set -e
    n=\$(find / -xdev -name 'geobrix-*-jar-with-dependencies.jar' -not -path '*/databricks/labs/gbx/_jar/*' 2>/dev/null | wc -l)
    [ \"\$n\" -eq 1 ] || { echo \"expected 1 GeoBrix JAR outside the wheel, found \$n\"; exit 1; }
    site=\$(/databricks/python3/bin/python -c 'import sysconfig; print(sysconfig.get_paths()[\"purelib\"])')
    echo \"$JAR_SHA  /databricks/jars/$JAR_NAME\" | sha256sum -c -
    echo \"$JAR_SHA  \$site/databricks/labs/gbx/_jar/$JAR_NAME\" | sha256sum -c -
    [ \"\$(stat -c %a /databricks/jars/$JAR_NAME)\" = 644 ]
    test ! -e /scala-jars/user/$JAR_NAME
    test ! -e /opt/geobrix
    /databricks/python3/bin/python -c 'import importlib.metadata as m; v=m.version(\"geobrix\"); assert v == \"$WVER\", v'
" || verify_fail "VERIFY FAILED: image contents (not tagged as $FINAL_TAG)"

# The image history must not carry a package index (this image is published to a public registry).
# Only the layers this build added are inspected: the public base image's own history already names its
# build-time index host (UV_DEFAULT_INDEX). History lists newest first, so ours are the top entries.
# Counts only; never print the URL.
HIST_ALL=$(docker history --no-trunc --format '{{.CreatedBy}}' "$TAG") || verify_fail "VERIFY FAILED: could not read the history of the image (not tagged as $FINAL_TAG)"
HIST_BASE=$(docker history --no-trunc --format '{{.CreatedBy}}' "$BASE_IMAGE") || verify_fail "VERIFY FAILED: could not read the history of $BASE_IMAGE (image not tagged as $FINAL_TAG)"
OWN_LAYERS=$(( $(printf '%s\n' "$HIST_ALL" | wc -l) - $(printf '%s\n' "$HIST_BASE" | wc -l) ))
# A count of zero or less would make the check below inspect nothing and pass; fail instead.
[ "$OWN_LAYERS" -gt 0 ] || verify_fail "VERIFY FAILED: could not isolate this build's layers from the base image (not tagged as $FINAL_TAG)"
HIST=$(printf '%s\n' "$HIST_ALL" | awk -v n="$OWN_LAYERS" 'NR <= n')
N_VALUE=$(printf '%s\n' "$HIST" | grep -c 'PIP_INDEX_URL=[^[:space:]]' || true)
N_HOST=0
if [ -n "${PIP_INDEX_URL:-}" ]; then
    INDEX_HOST=$(printf '%s' "$PIP_INDEX_URL" | sed -E 's#^[A-Za-z][A-Za-z0-9+.-]*://([^/@]*@)?([^/:]+).*#\2#')
    if [ -n "$INDEX_HOST" ]; then N_HOST=$(printf '%s\n' "$HIST" | grep -cF -- "$INDEX_HOST" || true); fi
fi
if [ "$N_VALUE" -ne 0 ] || [ "$N_HOST" -ne 0 ]; then
    verify_fail "VERIFY FAILED: package index found in the history of the image (PIP_INDEX_URL values: $N_VALUE, configured index host: $N_HOST; not tagged as $FINAL_TAG)"
fi

# Every check passed: give the image its final tag, then drop the temporary one (that tag only).
docker tag "$TAG" "$FINAL_TAG" || verify_fail "BUILD FAILED: could not tag the verified image as $FINAL_TAG"
docker rmi "$TAG" > /dev/null
echo "IMAGE=$FINAL_TAG"
echo "IMAGE_ID=$(docker image inspect --format '{{.Id}}' "$FINAL_TAG")"
