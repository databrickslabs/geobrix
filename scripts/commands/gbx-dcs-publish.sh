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
    echo "  --release    tag the verified -rc image as <ver>-env6-<arm|x86> and env6-<arm|x86>, push both"
    echo "  --version    default: parsed from target/geobrix-<ver>-jar-with-dependencies.jar"
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
    JAR=$(ls "$PROJECT_ROOT"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1 || true)
    VER=$(basename "${JAR:-none}" | sed -nE 's/^geobrix-(.+)-jar-with-dependencies\.jar$/\1/p')
fi
[ -n "$VER" ] || { echo "PRECONDITION MISSING: no --version and no target/ JAR to read it from; not published"; exit 1; }

show_banner "DCS: publish $REPO ($MODE, $VER, $ARCH)"
setup_log_file "$LOG_PATH"
RC="$REPO:${VER}-${SUFFIX}-rc"
docker image inspect "$RC" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: local image $RC (run gbx:dcs:build); not published"; exit 1; }

push() {
    docker push "$1" || { echo "Push of $1 failed. If this is an authentication error, run 'docker login' yourself and retry."; exit 1; }
}

if [ "$MODE" = "candidate" ]; then
    push "$RC"
    echo "DIGEST=$(docker image inspect --format '{{index .RepoDigests 0}}' "$RC")"
else
    docker tag "$RC" "$REPO:${VER}-${SUFFIX}"
    docker tag "$RC" "$REPO:${SUFFIX}"
    push "$REPO:${VER}-${SUFFIX}"
    push "$REPO:${SUFFIX}"
    echo "DIGEST=$(docker image inspect --format '{{index .RepoDigests 0}}' "$REPO:${VER}-${SUFFIX}")"
fi
