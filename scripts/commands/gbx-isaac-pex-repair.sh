#!/bin/bash
# gbx:isaac:pex-repair - Repair a broken Isaac Review (/review) install caused by
# dbexec's pex cache resolving a wrong-architecture `cryptography` wheel (e.g. a
# Linux x86-64 wheel on an arm64 Mac). `cryptography` is a stable-ABI (abi3) wheel,
# so the only platform-specific file is cryptography/hazmat/bindings/_rust.abi3.so;
# this swaps in the correct one (backing up the bad one to .bad.bak) and verifies.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

PEX_ROOT="${PEX_ROOT:-$HOME/.pex}"
WHEELS_DIR="$PEX_ROOT/installed_wheels"
# abi3 wheels are forward-compatible; 312 matches the dbexec python runtime.
PY_VERSION="312"
CHECK_ONLY=0
LOG_PATH=""

show_help() {
    show_banner "Isaac: Pex Repair (cryptography wheel arch fix)"
    echo -e "${CYAN}Repair /review when its dbexec pex got a wrong-arch cryptography wheel${NC}"
    echo -e "${CYAN}(dlopen '_rust.abi3.so ... slice is not valid mach-o file').${NC}"
    echo ""
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}bash scripts/commands/gbx-isaac-pex-repair.sh${NC} ${YELLOW}[OPTIONS]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--check${NC}        Diagnose only; report wrong-arch wheels and exit (no changes)."
    echo -e "  ${GREEN}--log <path>${NC}   Tee output to a log (filename -> test-logs/<name>)."
    echo -e "  ${GREEN}-h, --help${NC}     Show this help."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  bash scripts/commands/gbx-isaac-pex-repair.sh --check"
    echo -e "  bash scripts/commands/gbx-isaac-pex-repair.sh --log isaac-pex-repair.log"
    echo ""
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) show_help; exit 0 ;;
        --check) CHECK_ONLY=1; shift ;;
        --log) LOG_PATH="$(resolve_log_path "$2")"; shift 2 ;;
        *) echo -e "${RED}Unknown option: $1${NC}" >&2; show_help; exit 2 ;;
    esac
done

[[ -n "$LOG_PATH" ]] && setup_log_file "$LOG_PATH"
show_banner "Isaac: Pex Repair (cryptography wheel arch fix)"

OS="$(uname -s)"
ARCH="$(uname -m)"

# Only macOS is known to hit this (Linux ELF wheel on a Mach-O host). On Linux the
# native wheels are ELF and this failure mode does not apply.
if [[ "$OS" != "Darwin" ]]; then
    echo -e "${YELLOW}This repair targets macOS (Mach-O) hosts; detected '$OS'. Nothing to do.${NC}"
    exit 0
fi

# pip candidate platform tags for this Mac arch (first that downloads wins).
case "$ARCH" in
    arm64)  PLATFORMS=(macosx_11_0_arm64 macosx_10_12_universal2) ;;
    x86_64) PLATFORMS=(macosx_10_12_x86_64 macosx_10_9_x86_64 macosx_10_12_universal2) ;;
    *) echo -e "${RED}Unrecognized macOS arch '$ARCH'.${NC}" >&2; exit 2 ;;
esac

if [[ ! -d "$WHEELS_DIR" ]]; then
    echo -e "${YELLOW}No pex cache at ${WHEELS_DIR}.${NC}"
    echo -e "Run ${GREEN}/review${NC} (or gbx:review:round) once to build it, then re-run this if it crashes."
    exit 2
fi

# --- Detect wrong-arch cryptography _rust.abi3.so files ------------------------
# Broken on a Mac = the .so is not a Mach-O for this arch: either an ELF (Linux)
# binary, or a Mach-O whose arch is neither this host's nor a universal binary.
is_wrong_arch() {
    local so="$1" desc
    desc="$(file -b "$so" 2>/dev/null)"
    case "$desc" in
        *ELF*) return 0 ;;                       # Linux binary on a Mac -> broken
        *Mach-O*)
            case "$desc" in
                *universal*|*"$ARCH"*) return 1 ;;  # host arch or fat binary -> ok
                *) return 0 ;;                       # Mach-O, wrong single arch -> broken
            esac ;;
        *) return 0 ;;                           # unreadable / not a real .so -> broken
    esac
}

# bash 3.2 (macOS default) has no mapfile/readarray — read into the array portably.
ALL_SO=()
while IFS= read -r _so_line; do
    [[ -n "$_so_line" ]] && ALL_SO+=("$_so_line")
done < <(find "$WHEELS_DIR" -type f -path '*cryptography*/cryptography/hazmat/bindings/_rust.abi3.so' 2>/dev/null)

if [[ ${#ALL_SO[@]} -eq 0 ]]; then
    echo -e "${YELLOW}No cryptography _rust.abi3.so found under ${WHEELS_DIR}.${NC}"
    echo -e "Nothing to repair (Isaac's pex may not be built yet)."
    exit 0
fi

BROKEN=()
for so in "${ALL_SO[@]}"; do
    if is_wrong_arch "$so"; then
        BROKEN+=("$so")
        echo -e "${RED}✗ wrong-arch:${NC} $so"
        echo -e "    $(file -b "$so" 2>/dev/null)"
    else
        echo -e "${GREEN}✓ ok:${NC} $so ($(file -b "$so" 2>/dev/null | cut -d, -f1-2))"
    fi
done

if [[ ${#BROKEN[@]} -eq 0 ]]; then
    echo ""
    echo -e "${GREEN}All cryptography wheels are native ($ARCH). No repair needed.${NC}"
    exit 0
fi

if [[ "$CHECK_ONLY" -eq 1 ]]; then
    echo ""
    echo -e "${YELLOW}--check: ${#BROKEN[@]} wrong-arch wheel(s) found; run without --check to repair.${NC}"
    exit 3
fi

# --- Repair each broken wheel --------------------------------------------------
if ! command -v python3 >/dev/null 2>&1; then
    echo -e "${RED}ERROR: python3 not on PATH — needed to download the correct wheel.${NC}" >&2
    exit 2
fi

TMP_DL="$(mktemp -d -t gbx-isaac-pex-repair.XXXXXX)"
trap 'rm -rf "$TMP_DL"' EXIT

fetch_wheel() {  # $1=version ; echoes wheel path on success
    local ver="$1" plat whl
    for plat in "${PLATFORMS[@]}"; do
        rm -rf "$TMP_DL/$ver"; mkdir -p "$TMP_DL/$ver"
        if python3 -m pip download "cryptography==${ver}" \
                --platform "$plat" --python-version "$PY_VERSION" \
                --implementation cp --abi abi3 --only-binary=:all: --no-deps \
                -d "$TMP_DL/$ver" >/dev/null 2>&1; then
            whl="$(ls "$TMP_DL/$ver"/cryptography-*.whl 2>/dev/null | head -n1)"
            [[ -n "$whl" ]] && { echo "$whl"; return 0; }
        fi
    done
    return 1
}

FAILED=0
for so in "${BROKEN[@]}"; do
    # .../installed_wheels/<hash>/cryptography-<ver>/cryptography/hazmat/bindings/_rust.abi3.so
    wheel_dir="${so%%/cryptography/hazmat/bindings/_rust.abi3.so}"
    rel="cryptography/hazmat/bindings/_rust.abi3.so"
    ver="$(basename "$wheel_dir" | sed -E 's/^cryptography-([^-/]+).*/\1/')"
    echo ""
    show_separator
    echo -e "${CYAN}Repairing cryptography ${ver}${NC}  ($wheel_dir)"

    whl="$(fetch_wheel "$ver")"
    if [[ -z "$whl" ]]; then
        echo -e "${RED}  ✗ could not download a $ARCH cryptography==$ver wheel (tried: ${PLATFORMS[*]}).${NC}" >&2
        FAILED=1; continue
    fi
    echo -e "  downloaded $(basename "$whl")"

    if ! unzip -o -q "$whl" "$rel" -d "$TMP_DL/extract-$ver" 2>/dev/null; then
        echo -e "${RED}  ✗ wheel has no $rel — layout changed; repair manually.${NC}" >&2
        FAILED=1; continue
    fi
    new_so="$TMP_DL/extract-$ver/$rel"
    if is_wrong_arch "$new_so"; then
        echo -e "${RED}  ✗ downloaded .so is still not $ARCH-native; aborting this wheel.${NC}" >&2
        FAILED=1; continue
    fi

    chmod u+w "$(dirname "$so")" "$so" 2>/dev/null
    if [[ ! -e "$so.bad.bak" ]]; then
        cp -p "$so" "$so.bad.bak"
        echo -e "  backed up bad .so -> $(basename "$so").bad.bak"
    fi
    cp "$new_so" "$so"
    chmod 555 "$so" 2>/dev/null
    find "$wheel_dir" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null
    echo -e "${GREEN}  ✓ swapped in $ARCH .so:${NC} $(file -b "$so" | cut -d, -f1-2)"
done

# --- Verify --------------------------------------------------------------------
echo ""
show_separator
if [[ "$FAILED" -ne 0 ]]; then
    echo -e "${RED}One or more wheels could not be repaired (see above).${NC}" >&2
    exit 1
fi

if command -v dbexec >/dev/null 2>&1; then
    echo -e "${CYAN}Verifying: dbexec repo run isaac review --help${NC}"
    if (cd "$PROJECT_ROOT" && dbexec repo run isaac review --help >/dev/null 2>&1); then
        echo -e "${GREEN}✓ Isaac Review loads cleanly. /review is repaired.${NC}"
        exit 0
    fi
    echo -e "${RED}✗ isaac still fails to load after the swap — a different dependency may be mis-resolved.${NC}" >&2
    echo -e "Re-run with a fresh pex (remove ~/.pex and re-run /review) or inspect the traceback." >&2
    exit 1
fi

echo -e "${YELLOW}dbexec not on PATH — swapped the .so but could not verify. Try ${GREEN}/review${YELLOW} now.${NC}"
exit 0
