#!/bin/bash
# gbx:build:wheel - Build geobrix-<version>+env<N>-py3-none-any.whl: both architectures' native payload
# (gdal-native + the vendored bindings), the JAR, the tripwire extras, the runtime guard and the
# startup activation (scripts/native/build_env_wheel.py does the work).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ENV_N=""
JAR=""
EXTRA=()
LOG_PATH=""

usage() {
    show_banner "Build an env wheel with the native payload"
    echo "Usage: bash scripts/commands/gbx-build-wheel.sh --env 5|6 [--jar <path>] [--extra-payload <dir>] [--log <path>]"
    echo ""
    echo "  --env            5 (DBR 17.3, DBR 18, Serverless env 5) or 6 (DBR 19, Serverless env 6, SQL warehouses, UC)"
    echo "  --jar            the GeoBrix JAR to ship in _jar/ (default target/geobrix-<version>-jar-with-dependencies.jar)"
    echo "  --extra-payload  a tree merged into the payload under _native/<arch>/, e.g. build/rs/payload with"
    echo "                   _native/<arch>/rs/_gbxrs.abi3.so (the Rust kernels); repeatable"
    echo "  --log            Log file (filename -> test-logs/<name>; relative -> under test-logs/; absolute as-is)"
    echo ""
    echo "Inputs: build/native/out/<arch>/{stage-native,bindings}/ for x86_64 and aarch64 (gbx:build:native),"
    echo "python/geobrix/envs/env<N>/ and envs/pyproject.env.toml. The vendor-pure.txt wheels are fetched on the host"
    echo "through the configured pip, hash-checked (--require-hashes), into build/env-wheel/vendor-pure/env<N>/."
    echo "Output: dist/env<N>/geobrix-<version>+env<N>-py3-none-any.whl (the only wheel there) and"
    echo "build/env-wheel/env<N>/payload/ (the GBX_NATIVE_ROOT layout). Needs Docker for the auditwheel check in"
    echo "each architecture's toolchain image; no dev container and no container lock. Never uploads anything."
}

needs_value() { echo "$1 needs a value" >&2; usage; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env) [[ $# -ge 2 ]] || needs_value "$1"; ENV_N="$2"; shift 2 ;;
        --jar) [[ $# -ge 2 ]] || needs_value "$1"; JAR="$2"; shift 2 ;;
        --extra-payload) [[ $# -ge 2 ]] || needs_value "$1"; EXTRA+=(--extra-payload "$2"); shift 2 ;;
        --log) [[ $# -ge 2 ]] || needs_value "$1"; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$ENV_N" in 5|6) ;; *) echo "--env must be 5 or 6" >&2; usage; exit 1 ;; esac

cd "$PROJECT_ROOT"
VERSION="$(sed -nE 's/^__version__ = "([^"]+)"$/\1/p' python/geobrix/src/databricks/labs/gbx/__init__.py)"
JAR="${JAR:-target/geobrix-${VERSION}-jar-with-dependencies.jar}"
VENDOR_PURE="build/env-wheel/vendor-pure/env${ENV_N}"
show_banner "Env wheel: geobrix ${VERSION}+env${ENV_N}"
setup_log_file "$LOG_PATH"

command -v uv > /dev/null 2>&1 || { echo "PRECONDITION MISSING: uv is not installed; gbx:build:wheel not run"; exit 1; }
python3 -m pip --version > /dev/null 2>&1 || { echo "PRECONDITION MISSING: python3 has no pip (fetches the vendor-pure wheels); gbx:build:wheel not run"; exit 1; }

# Index output can carry the configured index's URL: every line from pip and uv is redacted.
redact() { sed -E 's#[A-Za-z][A-Za-z0-9+.-]*://[^[:space:]]+#<url>#g'; }

# The vendored pure-Python wheels: kept when the files on disk have the pinned sha256, else fetched.
mkdir -p "$VENDOR_PURE"
if ! python3 - "python/geobrix/envs/env${ENV_N}/vendor-pure.txt" "$VENDOR_PURE" <<'EOF'
import hashlib, pathlib, re, sys
pins, have = [], {}
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    line = line.split("#", 1)[0].strip()
    if line:
        spec, digest = line.split(" --hash=sha256:")
        pins.append((re.sub(r"[-_.]+", "_", spec.split("==")[0]).lower(), spec.split("==")[1], digest.strip()))
for w in pathlib.Path(sys.argv[2]).glob("*.whl"):
    have[hashlib.sha256(w.read_bytes()).hexdigest()] = w.name.lower()
sys.exit(0 if all(d in have and have[d].startswith(f"{n}-{v}-") for n, v, d in pins) else 1)
EOF
then
    echo "== fetch the vendor-pure.txt wheels (host pip, --require-hashes)"
    rm -f "$VENDOR_PURE"/*.whl
    python3 -m pip download -q --disable-pip-version-check --no-deps --only-binary :all: --python-version 3.12 \
        --require-hashes -r "python/geobrix/envs/env${ENV_N}/vendor-pure.txt" -d "$VENDOR_PURE" 2>&1 | redact
fi

T0=$(date +%s)
uv run -q --no-project --python 3.12 --with-requirements python/geobrix/requirements-build.txt \
    python scripts/native/build_env_wheel.py --env "$ENV_N" --jar "$JAR" --vendor-pure "$VENDOR_PURE" \
    ${EXTRA[@]+"${EXTRA[@]}"} 2>&1 | redact
echo "ENV_WHEEL env=${ENV_N} seconds=$(($(date +%s) - T0))"
