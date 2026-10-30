#!/bin/bash
# Sourced by prefetch.sh and install-host-rust.sh: pinned reads and checksum-checked downloads.
# Every value comes from scripts/native/pins.conf through pins.py (the one parser); nothing is cached
# without its sha256 matching, so a changed upstream file stops the build instead of entering it.

GBX_NATIVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GBX_NATIVE_ROOT="$(cd "$GBX_NATIVE_DIR/../.." && pwd)"
GBX_NATIVE_BUILD="${GBX_NATIVE_BUILD:-$GBX_NATIVE_ROOT/build/native}"

pin() { python3 "$GBX_NATIVE_DIR/pins.py" get "$1"; }

sha256_of() {
    if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# The pinned URL template with {triple} filled in: pin_url RUSTUP_URL aarch64-apple-darwin
pin_url() { pin "$1" | sed "s/{triple}/$2/"; }

# fetch_verified DEST SHA256 URL [URL2]: keep DEST when it already has the sum; otherwise download from the
# first source that answers. A download whose sum differs is a hard stop (no other source is tried): the
# upstream file changed, and the pin has to be re-verified by a person.
fetch_verified() {
    local dest="$1" want="$2" got url tmp="$1.part"
    shift 2
    if [ -f "$dest" ] && [ "$(sha256_of "$dest")" = "$want" ]; then return 0; fi
    mkdir -p "$(dirname "$dest")"
    for url in "$@"; do
        [ "$url" = single-source ] && continue
        rm -f "$tmp"
        if curl -fsSL --retry 3 --connect-timeout 20 --max-time 1200 -o "$tmp" "$url"; then
            got="$(sha256_of "$tmp")"
            if [ "$got" = "$want" ]; then mv "$tmp" "$dest"; return 0; fi
            rm -f "$tmp"
            echo "PREFETCH FAILED: $(basename "$dest") sha256 $got, pins.conf says $want (from $url)" >&2
            return 1
        fi
        rm -f "$tmp"
    done
    echo "PREFETCH FAILED: $(basename "$dest") could not be downloaded from any of its sources" >&2
    return 1
}

# fetch_sdist DEST SHA256 FILE: a Python binding's sdist (pins.conf <X>_SDIST_FILE / _SDIST_SHA256). With
# GBX_SDISTS_FROM=index (CI's builder sets it: the hardened runners reach PyPI only through the JFrog remote pip is
# configured for), index_fetch.py reads it from the package index pip is configured with and checks the sum; it never
# prints the index. Otherwise it comes from the project's own path on the public file host, through fetch_verified.
fetch_sdist() {
    local dest="$1" want="$2" file="$3" name
    name="${file%%-[0-9]*}"
    if [ "${GBX_SDISTS_FROM:-host}" = index ]; then
        python3 "$GBX_NATIVE_DIR/index_fetch.py" "$name" "$file" "$want" "$dest"
    else
        fetch_verified "$dest" "$want" "https://files.pythonhosted.org/packages/source/${name:0:1}/$name/$file"
    fi
}
