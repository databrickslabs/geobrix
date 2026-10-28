#!/bin/bash
# cargo-vendor.sh <crate-dir> <out-dir>           vendor a crate's locked dependencies (cargo vendor --locked)
# cargo-vendor.sh --generate-lockfile <crate-dir> write the crate's Cargo.lock through the same source
#
# Runs on the HOST: the build container is offline and reads <out-dir> through cargo-offline.toml.
# Where the crates come from, first source that works:
#   1. GBX_CARGO_REGISTRY (an internal sparse registry, from the untracked .gbx-local.env) as a source replacement
#   2. cargo's default registry (index.crates.io)
#   3. cargo's git index (CARGO_REGISTRIES_CRATES_IO_PROTOCOL=git)
# `cargo vendor` ignores [source] configuration unless it is given --respect-source-config, so the vendor
# runs pass it (generate-lockfile always reads the config). A cargo config of the host that already replaces
# crates-io (~/.cargo/config.toml, or any .cargo/config.toml above the crate) makes source 1 stop with
# "already defined"; source 2 then runs under that config.
# The registry address is never printed or written: cargo's stdout (its config snippet) is discarded, its
# stderr has every URL and the registry host replaced, and Cargo.lock keeps the canonical crates-io source.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
source "$ROOT/scripts/docker/gbx-container.sh"
gbx_load_local_env "$ROOT" 2> /dev/null || true

export RUSTUP_HOME="${GBX_HOST_RUSTUP_HOME:-$HOME/.cache/geobrix/rustup}"
export CARGO_HOME="${GBX_HOST_CARGO_HOME:-$HOME/.cache/geobrix/cargo}"
export PATH="$CARGO_HOME/bin:$PATH"

usage() {
    echo "Usage: bash scripts/native/cargo-vendor.sh <crate-dir> <out-dir>"
    echo "       bash scripts/native/cargo-vendor.sh --generate-lockfile <crate-dir>"
}
case "${1:-}" in
    -h|--help) usage; exit 0 ;;
    --generate-lockfile) [[ $# -eq 2 ]] || { usage >&2; exit 2; }; MODE=lock; CRATE="$2" ;;
    *) [[ $# -eq 2 ]] || { usage >&2; exit 2; }; MODE=vendor; CRATE="$1"; OUT="$2" ;;
esac
[ -f "$CRATE/Cargo.toml" ] || { echo "cargo-vendor: no Cargo.toml in $CRATE" >&2; exit 1; }
command -v cargo > /dev/null 2>&1 || { echo "cargo-vendor: no cargo; run bash scripts/native/install-host-rust.sh first" >&2; exit 1; }

# Every URL, and the registry's own host (a resolver error names a host without a scheme), becomes <url>.
REGISTRY_HOST=""
if [ -n "${GBX_CARGO_REGISTRY:-}" ]; then
    REGISTRY_HOST="${GBX_CARGO_REGISTRY#*://}"; REGISTRY_HOST="${REGISTRY_HOST%%/*}"
fi
redact() {
    if [ -n "$REGISTRY_HOST" ]; then
        sed -E "s#[a-zA-Z+]+://[^ ]+#<url>#g; s#$(printf '%s' "$REGISTRY_HOST" | sed 's/[.[\*^$]/\\&/g')#<url>#g"
    else
        sed -E 's#[a-zA-Z+]+://[^ ]+#<url>#g'
    fi
}

# run_cargo <mirror|plain|git> <cargo args>: stdout dropped, stderr redacted, cargo's exit status returned.
run_cargo() {
    local source="$1" rc
    shift
    set +e
    (
        cd "$CRATE"
        case "$source" in
            mirror) cargo "$@" --config 'source.crates-io.replace-with="gbx-mirror"' \
                        --config "source.gbx-mirror.registry=\"$GBX_CARGO_REGISTRY\"" ;;
            plain) cargo "$@" ;;
            git) CARGO_REGISTRIES_CRATES_IO_PROTOCOL=git cargo "$@" ;;
        esac
    ) 2>&1 > /dev/null | redact | awk 'NR <= 40 { print } END { if (NR > 40) print "... (" NR - 40 " more lines)" }'
    rc=${PIPESTATUS[0]}
    set -e
    return "$rc"
}

sources=()
[ -n "${GBX_CARGO_REGISTRY:-}" ] && sources+=(mirror)
sources+=(plain git)

for source in "${sources[@]}"; do
    if [ "$MODE" = vendor ]; then
        mkdir -p "$(dirname "$OUT")"
        out_abs="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
        ok=0; run_cargo "$source" vendor --locked --versioned-dirs --respect-source-config "$out_abs" || ok=$?
    else
        ok=0; run_cargo "$source" generate-lockfile || ok=$?
    fi
    if [ "$ok" -eq 0 ]; then
        echo "cargo-vendor: $MODE ok (source: $source)"
        exit 0
    fi
    echo "cargo-vendor: source '$source' failed (exit $ok)" >&2
done
echo "cargo-vendor: no crate source worked" >&2
exit 1
