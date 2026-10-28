#!/bin/bash
# prefetch.sh --arch x86_64|aarch64: fill build/native/ (gitignored) on the host with everything the build
# container needs, so the container builds from its context and never reaches for the network.
#   src/                every pinned source tarball, sha256-checked (shared by both architectures)
#   bin/<arch>/         rustup-init and cargo-deny.tar.gz, sha256-checked
#   wheelhouse/<arch>/  the hashed build requirements as manylinux_2_28 wheels (pip download --require-hashes)
#   sdists/             the Python-binding sdists (task N4 fills it)
# Stops with `PREFETCH FAILED: <file> sha256 ...` on a checksum mismatch. The package index is whatever the
# host's pip is configured for; this script never reads, passes or prints it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/fetch-lib.sh"

usage() {
    echo "Usage: bash scripts/native/prefetch.sh --arch x86_64|aarch64"
    echo "Fills build/native/{src,bin/<arch>,wheelhouse/<arch>,sdists} from scripts/native/pins.conf, checking every sha256."
}

ARCH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch) [[ $# -ge 2 ]] || { echo "--arch needs a value" >&2; exit 2; }; ARCH="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done
case "$ARCH" in
    x86_64) UPPER=X86_64 ;;
    aarch64) UPPER=AARCH64 ;;
    *) echo "--arch must be x86_64 or aarch64" >&2; exit 2 ;;
esac

OUT="$GBX_NATIVE_BUILD"
mkdir -p "$OUT/src" "$OUT/bin/$ARCH" "$OUT/wheelhouse/$ARCH" "$OUT/sdists"

while IFS=$'\t' read -r file sha url url2; do
    fetch_verified "$OUT/src/$file" "$sha" "$url" "$url2" || exit 1
done < <(python3 "$GBX_NATIVE_DIR/pins.py" sources)

fetch_verified "$OUT/bin/$ARCH/rustup-init" "$(pin "RUSTUP_INIT_SHA256_$UPPER")" \
    "$(pin_url RUSTUP_URL "$ARCH-unknown-linux-gnu")" || exit 1
fetch_verified "$OUT/bin/$ARCH/cargo-deny.tar.gz" "$(pin "CARGO_DENY_SHA256_$UPPER")" \
    "$(pin_url CARGO_DENY_URL "$ARCH-unknown-linux-musl")" || exit 1

# Wheels: stdout is dropped and stderr has every URL and every host pip names redacted, because the index may be an
# internal one (pip prints a connection error's host without a scheme: `host='...'`, `Connection to ...`).
# --no-deps: the lock names every transitive wheel, and the host's own Python (older than 3.12) would
# otherwise evaluate environment markers such as `tomli; python_version < "3.11"` for itself.
set +e
python3 -m pip download -q --disable-pip-version-check --require-hashes --no-deps --only-binary :all: \
    --platform "manylinux_2_28_$ARCH" --platform "manylinux2014_$ARCH" \
    --python-version 3.12 --implementation cp \
    -r "$GBX_NATIVE_DIR/build-requirements-$ARCH.txt" -d "$OUT/wheelhouse/$ARCH" 2>&1 > /dev/null \
    | sed -E "s#[a-zA-Z+]+://[^ ]+#<url>#g; s#host='[^']*'#host='<host>'#g; s#Connection to [^ ]+#Connection to <host>#g"
rc=${PIPESTATUS[0]}
set -e
if [ "$rc" -ne 0 ]; then
    echo "PREFETCH FAILED: wheelhouse/$ARCH (pip download exited $rc)" >&2
    exit 1
fi
echo "prefetch: $ARCH ok ($(ls "$OUT/src" | wc -l | tr -d ' ') sources, $(ls "$OUT/wheelhouse/$ARCH" | wc -l | tr -d ' ') wheels)"
