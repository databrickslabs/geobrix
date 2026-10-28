#!/bin/bash
# prefetch.sh --arch x86_64|aarch64: fill build/native/ (gitignored) on the host with everything the build
# container needs, so the container builds from its context and never reaches for the network.
#   src/                every pinned source tarball, sha256-checked (shared by both architectures)
#   bin/<arch>/         rustup-init and cargo-deny.tar.gz, sha256-checked
#   wheelhouse/<arch>/  the hashed build requirements as manylinux_2_28 wheels (pip download --require-hashes)
#   sdists/             the five Python-binding sdists, sha256-checked against pins.conf (<X>_SDIST_SHA256), and rasterio's
#                       tests/data archive (test-data.conf)
#   wheelhouse-bindings/<arch>/  the sdists' own build requirements (bindings-requirements-<arch>.txt, hashed)
#   wheelhouse-test/<arch>/      the probe's test environment: test-requirements{,-newclick}-<arch>.txt (hashed)
#   wheelhouse-pypi/<arch>/      the same five versions as PyPI binary wheels: the baseline the probe compares with
# Stops with `PREFETCH FAILED: <file> sha256 ...` on a checksum mismatch. The package index is whatever the
# host's pip is configured for; this script never reads, passes or prints it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/fetch-lib.sh"

usage() {
    echo "Usage: bash scripts/native/prefetch.sh --arch x86_64|aarch64"
    echo "Fills build/native/{src,bin/<arch>,wheelhouse/<arch>,sdists,wheelhouse-{bindings,test,pypi}/<arch>} from scripts/native/pins.conf, checking every sha256."
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
mkdir -p "$OUT/src" "$OUT/bin/$ARCH" "$OUT/wheelhouse/$ARCH" "$OUT/sdists" \
    "$OUT/wheelhouse-bindings/$ARCH" "$OUT/wheelhouse-test/$ARCH" "$OUT/wheelhouse-pypi/$ARCH"

while IFS=$'\t' read -r file sha url url2; do
    fetch_verified "$OUT/src/$file" "$sha" "$url" "$url2" || exit 1
done < <(python3 "$GBX_NATIVE_DIR/pins.py" sources)

fetch_verified "$OUT/bin/$ARCH/rustup-init" "$(pin "RUSTUP_INIT_SHA256_$UPPER")" \
    "$(pin_url RUSTUP_URL "$ARCH-unknown-linux-gnu")" || exit 1
fetch_verified "$OUT/bin/$ARCH/cargo-deny.tar.gz" "$(pin "CARGO_DENY_SHA256_$UPPER")" \
    "$(pin_url CARGO_DENY_URL "$ARCH-unknown-linux-musl")" || exit 1

# Wheels: stdout is dropped and stderr has every URL and every host pip names redacted, because the index may be an
# internal one (pip prints a connection error's host without a scheme: `host='...'`, `Connection to ...`).
# --no-deps: a lock names every transitive wheel, and the host's own Python (older than 3.12) would
# otherwise evaluate environment markers such as `tomli; python_version < "3.11"` for itself.
# pip_download LABEL DEST PIP-ARGS...: binary wheels for manylinux_2_28 (and older tags), cp312
pip_download() {
    local label="$1" dest="$2" rc
    shift 2
    set +e
    python3 -m pip download -q --disable-pip-version-check --no-deps --only-binary :all: \
        --platform "manylinux_2_28_$ARCH" --platform "manylinux2014_$ARCH" \
        --python-version 3.12 --implementation cp "$@" -d "$dest" 2>&1 > /dev/null \
        | sed -E "s#[a-zA-Z+]+://[^ ]+#<url>#g; s#host='[^']*'#host='<host>'#g; s#Connection to [^ ]+#Connection to <host>#g"
    rc=${PIPESTATUS[0]}
    set -e
    if [ "$rc" -ne 0 ]; then
        echo "PREFETCH FAILED: $label (pip download exited $rc)" >&2
        exit 1
    fi
}
pip_download "wheelhouse/$ARCH" "$OUT/wheelhouse/$ARCH" --require-hashes -r "$GBX_NATIVE_DIR/build-requirements-$ARCH.txt"
pip_download "wheelhouse-bindings/$ARCH" "$OUT/wheelhouse-bindings/$ARCH" --require-hashes -r "$GBX_NATIVE_DIR/bindings-requirements-$ARCH.txt"
pip_download "wheelhouse-test/$ARCH" "$OUT/wheelhouse-test/$ARCH" --require-hashes -r "$GBX_NATIVE_DIR/test-requirements-$ARCH.txt"
pip_download "wheelhouse-test/$ARCH (newclick)" "$OUT/wheelhouse-test/$ARCH" --require-hashes -r "$GBX_NATIVE_DIR/test-requirements-newclick-$ARCH.txt"

# The five binding sdists: the sdist is the file pinned, from the project's own path on the public file host
# (the one a package index redirects to). A changed sum stops the build, as for the C sources.
BINDINGS=(RASTERIO PYOGRIO PYPROJ SHAPELY NETCDF4)
for dep in "${BINDINGS[@]}"; do
    file="$(pin "${dep}_SDIST_FILE")"
    name="${file%%-[0-9]*}"
    fetch_verified "$OUT/sdists/$file" "$(pin "${dep}_SDIST_SHA256")" \
        "https://files.pythonhosted.org/packages/source/${name:0:1}/$name/$file" || exit 1
done

# rasterio's tests/data (its sdist ships none): the project's tag archive, sha256-checked against test-data.conf
# shellcheck disable=SC1091
source "$GBX_NATIVE_DIR/test-data.conf"
if [ "$RASTERIO_TESTDATA_VERSION" != "$(pin RASTERIO_VERSION)" ]; then
    echo "PREFETCH FAILED: test-data.conf is for rasterio $RASTERIO_TESTDATA_VERSION, pins.conf says $(pin RASTERIO_VERSION)" >&2
    exit 1
fi
fetch_verified "$OUT/sdists/rasterio-$RASTERIO_TESTDATA_VERSION-testdata.tar.gz" "$RASTERIO_TESTDATA_SHA256" "$RASTERIO_TESTDATA_URL" || exit 1

# The baselines: the same versions as PyPI wheels. When a version has no binary wheel for this architecture (netCDF4
# 1.7.5.1 has none), the newest binary wheel at or below it is used, and the test says which one it compared with.
rm -f "$OUT/wheelhouse-pypi/$ARCH"/*.whl
for dep in "${BINDINGS[@]}"; do
    file="$(pin "${dep}_SDIST_FILE")"
    name="${file%%-[0-9]*}"
    pip_download "wheelhouse-pypi/$ARCH ($name)" "$OUT/wheelhouse-pypi/$ARCH" "$name<=$(pin "${dep}_VERSION")"
done
echo "prefetch: $ARCH ok ($(ls "$OUT/src" | wc -l | tr -d ' ') sources, $(ls "$OUT/wheelhouse/$ARCH" | wc -l | tr -d ' ') wheels, $(ls "$OUT/sdists" | wc -l | tr -d ' ') sdists and test archives, $(ls "$OUT/wheelhouse-pypi/$ARCH" | wc -l | tr -d ' ') baseline wheels)"
