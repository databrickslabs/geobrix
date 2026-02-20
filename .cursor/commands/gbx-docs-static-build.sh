#!/bin/bash
# gbx:docs:static-build - Build docs for static zip (relative paths) and optionally zip to resources/beta-dist

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

SKIP_ZIP=false
LOG_FILE=""
OUTPUT_DIR=""

show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Docs Static Build (Offline Zip)")

Build documentation with relative paths for offline/local viewing; optionally zip to a folder.

USAGE:
    bash .cursor/commands/gbx-docs-static-build.sh [OPTIONS]

OPTIONS:
    --output <path>    Folder for the zip file (default: resources/static)
    --skip-zip         Only run 'npm run build:static-zip'; do not create zip
    --log <path>       Write build output to log file
    --help             Display this help message

EXAMPLES:
    # Build and zip to resources/static/geobrix-docs-<version>.zip
    bash .cursor/commands/gbx-docs-static-build.sh

    # Zip to a custom folder (zip name still uses version from docs/package.json)
    bash .cursor/commands/gbx-docs-static-build.sh --output resources/beta-dist/v0.2.0

    # Build only (no zip)
    bash .cursor/commands/gbx-docs-static-build.sh --skip-zip

NOTES:
    - Uses docs/package.json version for zip filename
    - Unzipped site works when opening index.html from any folder (e.g. Downloads; hash router used)

EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --skip-zip)
            SKIP_ZIP=true
            shift
            ;;
        --log)
            LOG_FILE=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$PROJECT_ROOT/resources/static"
else
    if [[ "$OUTPUT_DIR" != /* ]]; then
        OUTPUT_DIR="$PROJECT_ROOT/$OUTPUT_DIR"
    fi
fi

if [ -n "$LOG_FILE" ]; then
    setup_log_file "$LOG_FILE"
fi

print_banner "📚 GeoBrix: Docs Static Build (Offline Zip)"

cd "$PROJECT_ROOT/docs" || exit 1

print_separator
echo -e "${CYAN}Running npm run build:static-zip...${NC}"
print_separator

if ! npm run build:static-zip; then
    echo -e "${RED}❌ build:static-zip failed${NC}"
    exit 1
fi

if [ "$SKIP_ZIP" = true ]; then
    rm -rf "$PROJECT_ROOT/docs/build-static-zip"
    echo ""
    echo -e "${GREEN}✅ Static build complete (zip skipped). docs/build/ unchanged for serving.${NC}"
    exit 0
fi

VERSION=$(node -e "console.log(require('./package.json').version)")
ZIP_NAME="geobrix-docs-${VERSION}.zip"
mkdir -p "$OUTPUT_DIR"

print_separator
echo -e "${CYAN}Creating zip (build contents only): ${YELLOW}$OUTPUT_DIR/$ZIP_NAME${NC}"
print_separator

# Zip from the folder relativize script chose (nested build/ when present so zip root = index.html, assets, img)
ZIP_ROOT="$PROJECT_ROOT/docs/build-static-zip"
if [ -f "$PROJECT_ROOT/docs/build-static-zip/.static-zip-root" ]; then
    ZIP_ROOT=$(cat "$PROJECT_ROOT/docs/build-static-zip/.static-zip-root")
fi
cd "$ZIP_ROOT" || exit 1
zip -r "$OUTPUT_DIR/$ZIP_NAME" . -x "*.DS_Store" -x ".static-zip-root" || exit 1
cd "$PROJECT_ROOT" || exit 1
rm -rf "$PROJECT_ROOT/docs/build-static-zip"

echo ""
echo -e "${GREEN}✅ Static build and zip complete. docs/build/ unchanged for serving.${NC}"
echo -e "   Zip: ${YELLOW}$OUTPUT_DIR/$ZIP_NAME${NC}"
print_separator
