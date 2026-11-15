#!/bin/bash
# cargo-deny-ci.sh [--list]: the supply-chain gate for every GeoBrix Rust crate: advisories, yanked crates, bans,
# licences and sources (policy: deny.toml), with the cargo-deny and Rust toolchain pinned in pins.conf.
#
# This is the one place the yanked check runs: the build container's check uses vendored sources, which have no
# crates index, so it cannot see yanks. Run on a Linux machine that can read the crates index (the GitHub workflow
# rust-deny.yml does). A crate directory is checked when it has a Cargo.lock; a Cargo.toml that is under no
# Cargo.lock fails the gate (a crate without a committed lock cannot be checked, so it never slips through
# unchecked). An unreachable crates index fails the gate too (index-failure is denied), so a yank check is never
# silently skipped.
#
# Where the crates come from: cargo's own configuration. The hardened CI runner does not reach index.crates.io, so
# the workflow writes a config.toml into CARGO_HOME (GBX_HOST_CARGO_HOME, below) that replaces crates-io with the
# org's JFrog mirror (.github/actions/jfrog-cargo). cargo and cargo-deny both read the crates through that
# replacement (cargo-deny runs `cargo fetch`, then reads the index entries cargo cached), so the yanked check
# works through it too. Without such a config, crates.io itself is used.
# Before cargo runs, the hosts this stage needs are probed and the ones that do not answer are named, because
# cargo's own errors ("download of config.json failed") never say which host failed.
#   --list    print the crate roots that would be checked and stop
#   --probe   probe the hosts the check needs, name the ones that do not answer, and stop (no install, no Linux check)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/fetch-lib.sh"
cd "$GBX_NATIVE_ROOT"

LIST=false
PROBE=false
case "${1:-}" in
    -h|--help) sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    --list) LIST=true ;;
    --probe) PROBE=true ;;
    "") ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
esac

export RUSTUP_HOME="${GBX_HOST_RUSTUP_HOME:-$HOME/.cache/geobrix/rustup}"
export CARGO_HOME="${GBX_HOST_CARGO_HOME:-$HOME/.cache/geobrix/cargo}"
export PATH="$CARGO_HOME/bin:$PATH"

# registry_host: the host of the registry that $CARGO_HOME/config.toml puts in place of crates.io (the line
# `registry = "sparse+https://HOST/..."` under the source table), or nothing when crates.io is used as is.
registry_host() {
    local line
    [ -f "$CARGO_HOME/config.toml" ] || return 0
    while IFS= read -r line; do
        if [[ "$line" =~ ^registry[[:space:]]*=[[:space:]]*\"(sparse\+)?https?://([^/\"]+) ]]; then
            echo "${BASH_REMATCH[2]}"
            return 0
        fi
    done < "$CARGO_HOME/config.toml"
}

# probe_hosts: one HTTPS request per host this stage needs. Any HTTP answer (a 401 or 403 too) means the host is
# reachable; a refused or reset connection (a TLS reset is how the runner group drops a host that is not on its
# allowlist) means it is not. Every unreachable host is named, and the return value is 1 if there was one.
probe_hosts() {
    local host url why err bad=0 mirror targets
    mirror="$(registry_host)"
    if [ -n "$mirror" ]; then
        targets="$mirror|https://$mirror/|crates mirror: index and crate files"
    else
        targets="index.crates.io|https://index.crates.io/config.json|crates.io index
static.crates.io|https://static.crates.io/|crate files"
    fi
    targets="$targets
github.com|https://github.com/RustSec/advisory-db|advisory database"
    while IFS='|' read -r host url why; do
        if err="$(curl -sS -o /dev/null --connect-timeout 15 --max-time 30 "$url" 2>&1)"; then
            echo "cargo-deny-ci: network: $host ok ($why)"
        else
            echo "cargo-deny-ci: network: $host UNREACHABLE ($why): ${err#curl: }" >&2
            bad=1
        fi
    done <<< "$targets"
    if [ "$bad" -ne 0 ]; then
        echo "cargo-deny-ci: cannot reach the host(s) named UNREACHABLE. On the CI runner the crates must come through the JFrog mirror (.github/actions/jfrog-cargo configures it); the advisory database is on github.com." >&2
    fi
    return "$bad"
}
if [ "$PROBE" = true ]; then probe_hosts; exit $?; fi

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

TARBALL="$GBX_NATIVE_BUILD/bin/$ARCH/cargo-deny.tar.gz"
fetch_verified "$TARBALL" "$(pin "CARGO_DENY_SHA256_$UPPER")" "$(pin_url CARGO_DENY_URL "$ARCH-unknown-linux-musl")"
UNPACK="$GBX_NATIVE_BUILD/host/cargo-deny"
rm -rf "$UNPACK"
mkdir -p "$UNPACK"
tar -xzf "$TARBALL" -C "$UNPACK"
cp "$(find "$UNPACK" -name cargo-deny -type f | head -n 1)" "$CARGO_HOME/bin/cargo-deny"
chmod +x "$CARGO_HOME/bin/cargo-deny"
cargo deny --version | grep -q "$(pin CARGO_DENY_VERSION)"

probe_hosts || exit 1

failed=""
# --all-features: a crate's optional dependencies (gbxrs: gdal-sys and bindgen behind `gdal`) are in the checked graph
while IFS= read -r root; do
    echo "== cargo deny: $root"
    cargo deny --manifest-path "$root/Cargo.toml" --workspace --all-features --locked --config "$GBX_NATIVE_ROOT/deny.toml" \
        check -D index-failure advisories bans licenses sources || failed="$failed $root"
done <<< "$roots"
if [ -n "$failed" ]; then echo "cargo-deny-ci: FAILED for:$failed" >&2; exit 1; fi
echo "cargo-deny-ci: ok ($(printf '%s\n' "$roots" | wc -l | tr -d ' ') crate root(s))"
