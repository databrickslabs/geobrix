#!/bin/bash
# gbx:dcs:publish - Push the GeoBrix DCS image built by gbx:dcs:build. Run by the release owner with
# their own Docker Hub login; this command never logs in or handles credentials.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ARCH="arm64"; MODE=""; VER=""; LOG_PATH=""
REPO="${GBX_DCS_REPO:-mjohns797/geobrix}"

usage() {
    show_banner "DCS: publish the GeoBrix image"
    echo "Usage: bash scripts/commands/gbx-dcs-publish.sh (--candidate | --release) [--arch arm64|amd64] [--version <ver>] [--log <path>]"
    echo ""
    echo "  --candidate  push <repo>:<ver>-env6-<arm|x86>-rc (for on-cluster verification)"
    echo "  --release    re-tag the pushed, verified -rc candidate as <ver>-env6-<arm|x86> and env6-<arm|x86>, push both"
    echo "               (refuses an -rc image that was not pushed; do not rebuild between --candidate and --release)"
    echo "  --version    default: parsed from the single target/geobrix-<ver>-jar-with-dependencies.jar;"
    echo "               required when target/ holds none or more than one"
    echo "Requires: you are logged in to Docker Hub (docker login) as an account that can push to <repo>."
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --candidate) MODE="candidate"; shift ;;
        --release) MODE="release"; shift ;;
        --arch) ARCH="$2"; shift 2 ;;
        --version) VER="$2"; shift 2 ;;
        --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done
[ -n "$MODE" ] || { usage; exit 1; }
case "$ARCH" in arm64) SUFFIX="env6-arm" ;; amd64) SUFFIX="env6-x86" ;; *) echo "--arch must be arm64 or amd64" >&2; exit 1 ;; esac
if [ -z "$VER" ]; then
    # Same rule as gbx:dcs:build: exactly one JAR, otherwise the version is ambiguous and must be explicit.
    shopt -s nullglob
    JARS=("$PROJECT_ROOT"/target/geobrix-*-jar-with-dependencies.jar)
    shopt -u nullglob
    [ ${#JARS[@]} -eq 1 ] || { echo "PRECONDITION MISSING: expected exactly one target/geobrix-*-jar-with-dependencies.jar to read the version from, found ${#JARS[@]}; pass --version <ver>; not published"; exit 1; }
    VER=$(basename "${JARS[0]}" | sed -nE 's/^geobrix-(.+)-jar-with-dependencies\.jar$/\1/p')
fi
[ -n "$VER" ] || { echo "PRECONDITION MISSING: could not read a version from the JAR name; pass --version <ver>; not published"; exit 1; }

show_banner "DCS: publish $REPO ($MODE, $VER, $ARCH)"
setup_log_file "$LOG_PATH"
RC="$REPO:${VER}-${SUFFIX}-rc"
docker image inspect "$RC" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: local image $RC (run gbx:dcs:build); not published"; exit 1; }

push() {
    docker push "$1" || { echo "Push of $1 failed. If this is an authentication error, run 'docker login' yourself and retry."; exit 1; }
}

# The <repo>@sha256:... entry in the image's RepoDigests; empty if the image was never pushed to <repo>.
repo_digest() {
    docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$1" 2>/dev/null \
        | awk -v p="$REPO@" '!done && index($0, p) == 1 { print; done = 1 }' || true
}

print_digest() {
    local d
    d=$(repo_digest "$1")
    [ -n "$d" ] || { echo "No $REPO@sha256 digest on $1 after the push; check the push output above"; exit 1; }
    echo "DIGEST=$d"
}

if [ "$MODE" = "candidate" ]; then
    push "$RC"
    print_digest "$RC"
else
    # A fresh gbx:dcs:build has no digest for this repo until it is pushed, so an -rc image without one is
    # not the candidate that was pushed and verified (for example, it was rebuilt after --candidate).
    [ -n "$(repo_digest "$RC")" ] || { echo "PRECONDITION MISSING: $RC has no $REPO@sha256 digest, so it is not a pushed candidate; push it as the candidate and verify it first (rebuilding after --candidate requires a new candidate push); not published"; exit 1; }
    docker tag "$RC" "$REPO:${VER}-${SUFFIX}"
    docker tag "$RC" "$REPO:${SUFFIX}"
    push "$REPO:${VER}-${SUFFIX}"
    push "$REPO:${SUFFIX}"
    print_digest "$REPO:${VER}-${SUFFIX}"
fi
