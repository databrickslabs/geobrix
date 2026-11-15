#!/bin/bash
# gbx:dcs:publish - Push the GeoBrix DCS image built by gbx:dcs:build. Run by the release owner with
# their own Docker Hub login; this command never logs in or handles credentials.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ARCH="arm64"; MODE=""; VER=""; LOG_PATH=""; CONFIRM=""; EXPECT_DIGEST=""
REPO="${GBX_DCS_REPO:-mjohns797/geobrix}"

usage() {
    show_banner "DCS: publish the GeoBrix image"
    echo "Usage: bash scripts/commands/gbx-dcs-publish.sh (--candidate | --release --confirm <ver>) [--expect-digest sha256:<hex>]"
    echo "                                               [--arch arm64|amd64] [--version <ver>] [--log <path>]"
    echo ""
    echo "  --candidate      push <repo>:<ver>-env6-<arm|x86>-rc (for on-cluster verification); prints its DIGEST="
    echo "  --release        re-tag the pushed, verified -rc candidate as <ver>-env6-<arm|x86> and env6-<arm|x86>, push both"
    echo "                   (refuses an -rc image that was not pushed; do not rebuild between --candidate and --release)"
    echo "  --confirm <ver>  required with --release and must equal the version being released: pass it only after"
    echo "                   verifying the candidate on a cluster"
    echo "  --expect-digest  sha256:<hex>; refuse unless the local -rc image's <repo>@ digest is exactly this. For a"
    echo "                   release, pass the digest --candidate printed (the one you verified)"
    echo "  --version        default: parsed from the single target/geobrix-<ver>-jar-with-dependencies.jar;"
    echo "                   required when target/ holds none or more than one"
    echo "Requires: you are logged in to Docker Hub (docker login) as an account that can push to <repo>."
}

# Under `set -u` a trailing `--arch` would die on an unbound $2 with no explanation.
needs_value() { echo "$1 needs a value" >&2; usage; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --candidate) MODE="candidate"; shift ;;
        --release) MODE="release"; shift ;;
        --confirm) [[ $# -ge 2 ]] || needs_value "$1"; CONFIRM="$2"; shift 2 ;;
        --expect-digest) [[ $# -ge 2 ]] || needs_value "$1"; EXPECT_DIGEST="$2"; shift 2 ;;
        --arch) [[ $# -ge 2 ]] || needs_value "$1"; ARCH="$2"; shift 2 ;;
        --version) [[ $# -ge 2 ]] || needs_value "$1"; VER="$2"; shift 2 ;;
        --log) [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done
[ -n "$MODE" ] || { usage; exit 1; }
if [ -n "$CONFIRM" ] && [ "$MODE" != "release" ]; then echo "--confirm applies to --release only" >&2; exit 1; fi
if [ -n "$EXPECT_DIGEST" ] && ! printf '%s' "$EXPECT_DIGEST" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
    echo "--expect-digest must look like sha256:<64 hex characters> (the part after @ in the DIGEST= line --candidate printed)" >&2; exit 1
fi
case "$ARCH" in arm64) SUFFIX="env6-arm" ;; amd64) SUFFIX="env6-x86" ;; *) echo "--arch must be arm64 or amd64" >&2; exit 1 ;; esac
# The log starts before anything that can refuse on a missing precondition, so the refusal is in the log.
setup_log_file "$LOG_PATH"
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
# Publishing the release tags is the step users pull, so it needs explicit consent, given before any
# docker tag or push: the version to release, typed again after the candidate was verified on a cluster.
if [ "$MODE" = "release" ]; then
    [ -n "$CONFIRM" ] || { echo "REFUSED: --release publishes $REPO:${VER}-${SUFFIX} and $REPO:${SUFFIX}; verify the -rc candidate on a dedicated cluster first, then re-run with --confirm $VER; nothing was tagged or pushed"; exit 1; }
    [ "$CONFIRM" = "$VER" ] || { echo "REFUSED: --confirm $CONFIRM does not match the version being released ($VER); re-run with --confirm $VER after verifying the candidate on a cluster; nothing was tagged or pushed"; exit 1; }
fi
RC="$REPO:${VER}-${SUFFIX}-rc"
docker image inspect "$RC" > /dev/null 2>&1 || { echo "PRECONDITION MISSING: local image $RC (run gbx:dcs:build); not published"; exit 1; }

# A release pushes two tags, one after the other; PUSHED lists those that went through, so a failure on
# the second says exactly what is already out there.
PUSHED=""
push() {
    if ! docker push "$1"; then
        echo "Push of $1 failed. If this is an authentication error, run 'docker login' yourself and retry."
        if [ "$MODE" = "release" ]; then
            if [ -n "$PUSHED" ]; then
                echo "PARTIAL RELEASE: already pushed in this run:$PUSHED; $1 was not pushed."
            else
                echo "Release incomplete: no release tag was pushed."
            fi
            echo "It is safe to re-run the same --release command: it re-tags the same -rc image and pushes again, and a tag that is already pushed is unchanged."
        fi
        exit 1
    fi
    PUSHED="$PUSHED $1"
}

# The <repo>@sha256:... entry in the image's RepoDigests; empty if the image was never pushed to <repo>.
repo_digest() {
    docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$1" 2>/dev/null \
        | awk -v p="$REPO@" '!done && index($0, p) == 1 { print; done = 1 }' || true
}

# Refuse unless the local -rc image carries exactly the digest the caller expects (before any tag or push).
check_expected_digest() {
    [ -n "$EXPECT_DIGEST" ] || return 0
    local have
    have=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$RC" 2>/dev/null || true)
    if ! printf '%s\n' "$have" | grep -Fxq -- "$REPO@$EXPECT_DIGEST"; then
        echo "REFUSED: $RC does not have the expected digest $EXPECT_DIGEST (local $REPO@ digest: $(repo_digest "$RC" | sed 's/^[^@]*@//;s/^$/none, the image was not pushed/')); nothing was tagged or pushed"
        exit 1
    fi
}

print_digest() {
    local d
    d=$(repo_digest "$1")
    [ -n "$d" ] || { echo "No $REPO@sha256 digest on $1 after the push; check the push output above"; exit 1; }
    echo "DIGEST=$d"
}

if [ "$MODE" = "candidate" ]; then
    check_expected_digest
    push "$RC"
    print_digest "$RC"
else
    # A fresh gbx:dcs:build has no digest for this repo until it is pushed, so an -rc image without one is
    # not the candidate that was pushed and verified (for example, it was rebuilt after --candidate).
    [ -n "$(repo_digest "$RC")" ] || { echo "PRECONDITION MISSING: $RC has no $REPO@sha256 digest, so it is not a pushed candidate; push it as the candidate and verify it first (rebuilding after --candidate requires a new candidate push); not published"; exit 1; }
    check_expected_digest
    echo "RELEASING=$(repo_digest "$RC")"
    docker tag "$RC" "$REPO:${VER}-${SUFFIX}"
    docker tag "$RC" "$REPO:${SUFFIX}"
    push "$REPO:${VER}-${SUFFIX}"
    push "$REPO:${SUFFIX}"
    print_digest "$REPO:${VER}-${SUFFIX}"
fi
