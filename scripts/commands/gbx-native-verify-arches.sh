#!/bin/bash
# gbx:native:verify-arches - The two-architecture native gate. CI builds and tests on x86_64 only; this checks both
# architectures' native builds and env wheels locally, then writes the stamp CI checks (scripts/native/arch-gate.json).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

LOG_PATH=""
ARCHES=(aarch64 x86_64)
STAMP=scripts/native/arch-gate.json

usage() {
    show_banner "Two-architecture native gate"
    echo "Usage: bash scripts/commands/gbx-native-verify-arches.sh [--log <path>]"
    echo ""
    echo "Verifies what is built, builds nothing, for aarch64 and x86_64 (the one not native to this machine under"
    echo "emulation), then writes $STAMP, which CI checks (native_inputs.py check-stamp):"
    echo "  1  provenance: build/native/out/<arch>/stage-native is the native build in dist/env5's and dist/env6's"
    echo "     wheels, and those wheels were built from HEAD's native inputs (native_inputs.py provenance)"
    echo "  2  scripts/native/verify-native.sh (rows 1-18) on each architecture's stage-native tree, --network none"
    echo "  3  gbx:build:wheel --env 5 --verify <arch> and --env 6 --verify <arch>, for each architecture"
    echo "  4  $STAMP: the native-input hash of HEAD (native_inputs.py hash --set gate), the date, the GDAL"
    echo "     version and each check's passed / failed counts; written only when every check passed"
    echo ""
    echo "  --log   Log file (filename -> test-logs/<name>; relative -> under test-logs/; absolute as-is)"
    echo ""
    echo "Needs Docker, both toolchain images, both stage-native trees and both env wheels (gbx:build:native"
    echo "--arch both --target stage-native and --target bindings, gbx:build:wheel --env 5 and --env 6). No dev"
    echo "container, no container lock. Each check's log: test-logs/arch-gate/. Commit $STAMP."
}

needs_value() { echo "$1 needs a value" >&2; usage; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --log) [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Two-architecture native gate (aarch64, x86_64)"
setup_log_file "$LOG_PATH"
T0=$(date +%s)

# Every precondition before any check.
missing() { echo "PRECONDITION MISSING: $1; gbx:native:verify-arches not run"; exit 1; }
docker info > /dev/null 2>&1 || missing "Docker is not running"
for a in "${ARCHES[@]}"; do
    img="geobrix-native-build:toolchain-$(python3 scripts/native/pins.py tag-hash "$a")-$a"
    docker image inspect "$img" > /dev/null 2>&1 || missing "$img (bash scripts/commands/gbx-build-native.sh --arch $a --target toolchain)"
    [ -f "build/native/out/$a/stage-native/gdal-native/MANIFEST.json" ] \
        || missing "build/native/out/$a/stage-native (bash scripts/commands/gbx-build-native.sh --arch $a --target stage-native)"
    ls build/native/out/"$a"/stage-native/gdal-native-*-"$a"-*-debuginfo.tar.xz > /dev/null 2>&1 \
        || missing "the $a debuginfo tarball beside build/native/out/$a/stage-native/gdal-native"
done
for env in 5 6; do
    ls dist/env"$env"/*.whl > /dev/null 2>&1 || missing "dist/env$env's wheel (bash scripts/commands/gbx-build-wheel.sh --env $env)"
done

WORK="$PROJECT_ROOT/test-logs/arch-gate"
rm -rf "$WORK"
mkdir -p "$WORK"

echo "== 1 provenance"
python3 scripts/native/native_inputs.py provenance | tee "$WORK/provenance.txt"
BUILT_FROM="$(sed -n 's/^BUILT_FROM=//p' "$WORK/provenance.txt")"

STAMP_ARGS=()
for a in "${ARCHES[@]}"; do
    case "$a" in x86_64) plat=linux/amd64 ;; aarch64) plat=linux/arm64 ;; esac
    img="geobrix-native-build:toolchain-$(python3 scripts/native/pins.py tag-hash "$a")-$a"
    out="$PROJECT_ROOT/build/native/out/$a/stage-native"
    dbg="$(ls "$out"/gdal-native-*-"$a"-*-debuginfo.tar.xz | head -n 1)"
    echo "== 2 verify-native $a ($img, --network none)"
    t=$(date +%s)
    # as gbx:build:native verifies the tree it exported (the debuginfo tarball beside it is mounted too)
    docker run --rm --network none --platform "$plat" -v "$out/gdal-native:/gdal-native:ro" -v "$dbg:/debuginfo.tar.xz:ro" \
        -v "$PROJECT_ROOT/scripts/native:/verify:ro" "$img" bash /verify/verify-native.sh /gdal-native /debuginfo.tar.xz 2>&1 \
        | tee "$WORK/verify-native-$a.txt" || true
    echo "ARCH_GATE_STEP verify-native arch=$a seconds=$(($(date +%s) - t))"
    STAMP_ARGS+=(--verify-native "$a=$WORK/verify-native-$a.txt")
done

for env in 5 6; do
    for a in "${ARCHES[@]}"; do
        report="build/env-wheel/env$env/verify-$a.txt"
        rm -f "$report"  # a failed run must not leave an older report to be counted
        echo "== 3 env$env wheel --verify $a"
        t=$(date +%s)
        bash scripts/commands/gbx-build-wheel.sh --env "$env" --verify "$a" 2>&1 | tee "$WORK/env$env-verify-$a.log" || true
        echo "ARCH_GATE_STEP env$env-verify arch=$a seconds=$(($(date +%s) - t))"
        STAMP_ARGS+=(--env-wheel "$env:$a=$PROJECT_ROOT/$report")
    done
done

echo "== 4 $STAMP"
if python3 scripts/native/native_inputs.py write-stamp --rev HEAD --built-from "$BUILT_FROM" "${STAMP_ARGS[@]}"; then
    echo "ARCH_GATE_OK stamp=$STAMP seconds=$(($(date +%s) - T0))"
else
    echo "ARCH_GATE_FAILED seconds=$(($(date +%s) - T0)) (logs: test-logs/arch-gate/)"
    exit 1
fi
