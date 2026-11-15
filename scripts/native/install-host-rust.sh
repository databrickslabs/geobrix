#!/bin/bash
# install-host-rust.sh: the pinned Rust toolchain on the host (Apple Silicon macOS, or Linux x86_64/aarch64 for
# CI), for `cargo vendor`, `cargo generate-lockfile` and quick iteration. Nothing built here is ever shipped
# (shipped modules build in the manylinux_2_28 container). It touches no shell profile: the toolchain lives
# under ~/.cache/geobrix and cargo-vendor.sh puts it on PATH for itself.
#   rustup-init (pinned version, sha256-checked) -> RUSTUP_HOME / CARGO_HOME under ~/.cache/geobrix
#   `rustup toolchain install` in the repo reads rust-toolchain.toml (channel, components, targets)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/fetch-lib.sh"

case "${1:-}" in
    -h|--help) echo "Usage: bash scripts/native/install-host-rust.sh"; echo "Installs the toolchain pinned by rust-toolchain.toml / pins.conf into ~/.cache/geobrix/{rustup,cargo}."; exit 0 ;;
    "") ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
esac
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) TRIPLE=aarch64-apple-darwin; KEY=AARCH64_APPLE_DARWIN ;;
    Linux-x86_64) TRIPLE=x86_64-unknown-linux-gnu; KEY=X86_64 ;;
    Linux-aarch64) TRIPLE=aarch64-unknown-linux-gnu; KEY=AARCH64 ;;
    *) echo "install-host-rust.sh: no pinned rustup-init for $(uname -s) $(uname -m)" >&2; exit 1 ;;
esac

export RUSTUP_HOME="${GBX_HOST_RUSTUP_HOME:-$HOME/.cache/geobrix/rustup}"
export CARGO_HOME="${GBX_HOST_CARGO_HOME:-$HOME/.cache/geobrix/cargo}"
WANT="$(pin RUST_TOOLCHAIN)"
INIT="$GBX_NATIVE_BUILD/host/rustup-init"

fetch_verified "$INIT" "$(pin "RUSTUP_INIT_SHA256_$KEY")" "$(pin_url RUSTUP_URL "$TRIPLE")"
chmod +x "$INIT"
if [ ! -x "$CARGO_HOME/bin/rustup" ]; then
    "$INIT" -q -y --no-modify-path --profile minimal --default-toolchain none
fi
cd "$GBX_NATIVE_ROOT"
"$CARGO_HOME/bin/rustup" toolchain install
GOT="$("$CARGO_HOME/bin/rustc" --version)"
case "$GOT" in
    *" $WANT "*) echo "host rust: $GOT (RUSTUP_HOME=$RUSTUP_HOME CARGO_HOME=$CARGO_HOME)" ;;
    *) echo "install-host-rust: rustc says '$GOT', pins.conf says RUST_TOOLCHAIN=$WANT" >&2; exit 1 ;;
esac
