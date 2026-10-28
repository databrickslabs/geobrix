#!/bin/bash
# cargo-deny-ci.sh [--list]: the supply-chain gate for every GeoBrix Rust crate: advisories, yanked crates, bans,
# licences and sources (policy: deny.toml), with the cargo-deny and Rust toolchain pinned in pins.conf.
#
# This is the one place the yanked check runs: the build container's check uses vendored sources, which have no
# crates index, so it cannot see yanks. Run on a Linux machine with network access to the public crates index
# (the GitHub workflow rust-deny.yml does). A crate directory is checked when it has a Cargo.lock; a Cargo.toml
# that is under no Cargo.lock fails the gate (a crate without a committed lock cannot be checked, so it never
# slips through unchecked). An unreachable crates index fails the gate too (index-failure is denied), so a
# yank check is never silently skipped.
#   --list   print the crate roots that would be checked and stop
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/fetch-lib.sh"
cd "$GBX_NATIVE_ROOT"

LIST=false
case "${1:-}" in
    -h|--help) sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    --list) LIST=true ;;
    "") ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
esac

# Every tracked or new (not ignored) Cargo.toml, mapped to the nearest directory at or above it with a Cargo.lock.
roots=""
missing=""
while IFS= read -r manifest; do
    dir="$(dirname "$manifest")"
    while [ ! -f "$dir/Cargo.lock" ] && [ "$dir" != "." ]; do dir="$(dirname "$dir")"; done
    if [ -f "$dir/Cargo.lock" ]; then
        case "$roots" in *"
$dir
"*) ;; *) roots="$roots
$dir
" ;; esac
    else
        missing="$missing $manifest"
    fi
done < <(git ls-files --cached --others --exclude-standard -- 'Cargo.toml' '*/Cargo.toml' | sort)

if [ -n "$missing" ]; then
    echo "cargo-deny-ci: no committed Cargo.lock above:$missing" >&2
    exit 1
fi
roots="$(printf '%s\n' "$roots" | sed '/^$/d' | sort)"
if [ -z "$roots" ]; then echo "cargo-deny-ci: no Rust crates in the repository"; exit 0; fi
if [ "$LIST" = true ]; then printf '%s\n' "$roots"; exit 0; fi

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) ARCH=x86_64 ;;
    Linux-aarch64) ARCH=aarch64 ;;
    *) echo "cargo-deny-ci: Linux x86_64/aarch64 only (the pinned cargo-deny is a Linux binary); use the CI job" >&2; exit 1 ;;
esac
UPPER="$(printf '%s' "$ARCH" | tr '[:lower:]' '[:upper:]')"

bash "$GBX_NATIVE_DIR/install-host-rust.sh"
export RUSTUP_HOME="${GBX_HOST_RUSTUP_HOME:-$HOME/.cache/geobrix/rustup}"
export CARGO_HOME="${GBX_HOST_CARGO_HOME:-$HOME/.cache/geobrix/cargo}"
export PATH="$CARGO_HOME/bin:$PATH"

TARBALL="$GBX_NATIVE_BUILD/bin/$ARCH/cargo-deny.tar.gz"
fetch_verified "$TARBALL" "$(pin "CARGO_DENY_SHA256_$UPPER")" "$(pin_url CARGO_DENY_URL "$ARCH-unknown-linux-musl")"
UNPACK="$GBX_NATIVE_BUILD/host/cargo-deny"
rm -rf "$UNPACK"
mkdir -p "$UNPACK"
tar -xzf "$TARBALL" -C "$UNPACK"
cp "$(find "$UNPACK" -name cargo-deny -type f | head -n 1)" "$CARGO_HOME/bin/cargo-deny"
chmod +x "$CARGO_HOME/bin/cargo-deny"
cargo deny --version | grep -q "$(pin CARGO_DENY_VERSION)"

failed=""
while IFS= read -r root; do
    echo "== cargo deny: $root"
    cargo deny --manifest-path "$root/Cargo.toml" --workspace --locked --config "$GBX_NATIVE_ROOT/deny.toml" \
        check -D index-failure advisories bans licenses sources || failed="$failed $root"
done <<< "$roots"
if [ -n "$failed" ]; then echo "cargo-deny-ci: FAILED for:$failed" >&2; exit 1; fi
echo "cargo-deny-ci: ok ($(printf '%s\n' "$roots" | wc -l | tr -d ' ') crate root(s))"
