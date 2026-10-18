#!/bin/bash
# gbx:versions:bump - Bump the GeoBrix RELEASE version (e.g. 0.5.0 -> 0.5.1) across
# every source-of-truth, artifact-filename, banner and diagram-pill reference.
#
# This is NOT gbx:versions:audit (that tracks pinned Python *tooling* — pip/numpy/pyspark —
# and never touches the library version). This command changes the library's own version.
#
# It applies only the SAFE, targeted replacements (authoritative defs, wheel/JAR filenames,
# "Current version" banners, diagram pills). It then REPORTS every remaining bare occurrence
# of the old number so a human can judge the historically-scoped ones ("introduced in 0.5.0",
# "Requires GeoBrix 0.5.0+") that must stay true after the bump. It never rewrites those, and
# it never touches lock files or the standalone genie_map app version.
#
# The committed diagram PNGs are NOT regenerated here (that needs a headless-Chrome screenshot
# of the SVG) — the command prints the exact manual step at the end.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Bump GeoBrix release version"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:versions:bump${NC} ${YELLOW}--to <NEW> [--from <OLD>] [--dry-run] [--log <path>]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--to <NEW>${NC}     Target version, e.g. 0.5.1 (required)."
    echo -e "  ${GREEN}--from <OLD>${NC}   Source version. Auto-detected from pom.xml if omitted."
    echo -e "  ${GREEN}--dry-run${NC}      Show what would change; edit nothing."
    echo -e "  ${GREEN}--log <path>${NC}   Write output to a log file."
    echo -e "  ${GREEN}--help${NC}         Show this help."
    echo ""
    echo -e "${CYAN}What it changes (safe, targeted):${NC}"
    echo -e "  - Authoritative: ${YELLOW}pom.xml${NC} <version>, ${YELLOW}__init__.py${NC} __version__, ${YELLOW}docs/package.json${NC}"
    echo -e "  - Artifact filenames: ${YELLOW}geobrix-<VER>-py3-none-any.whl${NC} / ${YELLOW}geobrix-<VER>-jar-with-dependencies.jar${NC} everywhere"
    echo -e "  - Banners: ${YELLOW}Current version: <VER>${NC} (release notes), ${YELLOW}Current version **<VER>**${NC} (CLAUDE.md)"
    echo -e "  - Diagram pills: ${YELLOW}v<VER>${NC} in resources/images generators + committed .svg"
    echo ""
    echo -e "${CYAN}What it will NOT touch (reports for you to judge):${NC}"
    echo -e "  - Historically-scoped prose ('introduced in <OLD>', 'Requires GeoBrix <OLD>+') — stays true."
    echo -e "  - Lock files, the genie_map app version, third-party deps that share the number."
    echo -e "  - The committed diagram PNGs (manual Chrome re-render — printed at the end)."
    echo ""
}

NEW_VER=""
OLD_VER=""
DRY_RUN=0
LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --to)      NEW_VER="$2"; shift 2;;
        --from)    OLD_VER="$2"; shift 2;;
        --dry-run) DRY_RUN=1; shift;;
        --log)     LOG_PATH=$(resolve_log_path "$2"); shift 2;;
        --help|-h) show_help; exit 0;;
        *) echo -e "${RED}Unknown option: $1${NC}"; show_help; exit 1;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Bump GeoBrix release version"
setup_log_file "$LOG_PATH"

if [ -z "$NEW_VER" ]; then
    echo -e "${RED}--to <NEW> is required (e.g. --to 0.5.1).${NC}"; show_help; exit 1
fi

# Auto-detect OLD from pom.xml <version> if not supplied.
if [ -z "$OLD_VER" ]; then
    OLD_VER=$(grep -m1 -oE '<version>[0-9]+\.[0-9]+\.[0-9]+</version>' pom.xml \
              | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
    if [ -z "$OLD_VER" ]; then
        echo -e "${RED}Could not auto-detect current version from pom.xml — pass --from <OLD>.${NC}"; exit 1
    fi
fi

# Validate x.y.z form.
if ! echo "$NEW_VER" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo -e "${RED}--to '$NEW_VER' is not a valid x.y.z version.${NC}"; exit 1
fi

echo -e "${CYAN}Bumping${NC} ${YELLOW}${OLD_VER}${NC} ${CYAN}->${NC} ${GREEN}${NEW_VER}${NC}"
if [ "$OLD_VER" = "$NEW_VER" ]; then
    echo -e "${YELLOW}from == to; nothing to change. (Already at ${NEW_VER}.)${NC}"
fi
if [ "$DRY_RUN" -eq 1 ]; then
    echo -e "${YELLOW}(dry-run — no files will be modified)${NC}"
fi
show_separator

O_ESC=$(printf '%s' "$OLD_VER" | sed 's/\./\\./g')  # dots literal for grep/sed

CHANGED_FILES=()

# apply_sed <match-grep-pattern> <sed-expr> <file> <label>
# Applies an in-place sed only when <match-grep-pattern> is present; records the file.
# The match pattern is passed explicitly (not extracted from the sed expr) so patterns
# containing escaped slashes — e.g. <\/version> — are handled correctly.
apply_sed() {
    local match="$1" expr="$2" file="$3" label="$4"
    [ -f "$file" ] || return 0
    grep -qE "$match" "$file" 2>/dev/null || return 0
    echo -e "  ${GREEN}[${label}]${NC} $file"
    if [ "$DRY_RUN" -eq 0 ]; then
        # portable in-place edit (BSD + GNU sed)
        sed -i.gbxbak -E "$expr" "$file" && rm -f "${file}.gbxbak"
        CHANGED_FILES+=("$file")
    fi
}

echo -e "${CYAN}1. Authoritative version definitions${NC}"
apply_sed "<version>${O_ESC}</version>" \
    "s/<version>${O_ESC}<\/version>/<version>${NEW_VER}<\/version>/" \
    "pom.xml" "pom"
apply_sed "__version__ = \"${O_ESC}\"" \
    "s/__version__ = \"${O_ESC}\"/__version__ = \"${NEW_VER}\"/" \
    "python/geobrix/src/databricks/labs/gbx/__init__.py" "wheel"
apply_sed "\"version\": \"${O_ESC}\"" \
    "s/\"version\": \"${O_ESC}\"/\"version\": \"${NEW_VER}\"/" \
    "docs/package.json" "docs-site"
show_separator

echo -e "${CYAN}2. Artifact filenames (wheel / JAR) — repo-wide${NC}"
# Find files containing an old-versioned artifact name, excluding lock files and build output.
# (portable read loop — macOS ships bash 3.2, which lacks `mapfile`)
ART_FILES=()
while IFS= read -r f; do
    [ -n "$f" ] && ART_FILES+=("$f")
done < <(git grep -lE "geobrix-${O_ESC}-(py3-none-any\.whl|jar-with-dependencies\.jar)" \
    -- . \
    ':!:**/package-lock.json' ':!:**/pnpm-lock.yaml' ':!:docs/build*/**' \
    ':!:.superpowers/**' 2>/dev/null)
if [ ${#ART_FILES[@]} -eq 0 ]; then
    echo -e "  ${YELLOW}(no artifact-filename references at ${OLD_VER})${NC}"
fi
for f in "${ART_FILES[@]}"; do
    apply_sed "geobrix-${O_ESC}-(py3-none-any\.whl|jar-with-dependencies\.jar)" \
        "s/geobrix-${O_ESC}-py3-none-any\.whl/geobrix-${NEW_VER}-py3-none-any.whl/g; s/geobrix-${O_ESC}-jar-with-dependencies\.jar/geobrix-${NEW_VER}-jar-with-dependencies.jar/g" \
        "$f" "artifact"
done
show_separator

echo -e "${CYAN}3. Version banners${NC}"
apply_sed "Current version: ${O_ESC}" \
    "s/Current version: ${O_ESC}/Current version: ${NEW_VER}/g" \
    "docs/docs/release-notes.mdx" "release-notes-banner"
apply_sed "Current version \*\*${O_ESC}\*\*" \
    "s/Current version \*\*${O_ESC}\*\*/Current version **${NEW_VER}**/g" \
    "CLAUDE.md" "claude-md"
show_separator

echo -e "${CYAN}4. Diagram version pills (generators + committed SVG)${NC}"
PILL_FILES=(
    "resources/images/generators/rasterx-function-categories.py"
    "resources/images/generators/rasterx-tile-structure.py"
    "resources/images/generators/vapor-eyes-lakeflow-functions.py"
    "resources/images/diagrams/rasterx/rasterx-function-categories.svg"
    "resources/images/diagrams/rasterx/rasterx-function-categories_landscape.svg"
    "resources/images/diagrams/rasterx/rasterx-tile-structure.svg"
    "resources/images/diagrams/vapor-eyes/vapor-eyes-lakeflow-functions.svg"
)
for f in "${PILL_FILES[@]}"; do
    # Pill string is "v<VER>" — replace only the versioned token to avoid touching prose.
    apply_sed "v${O_ESC}" "s/v${O_ESC}/v${NEW_VER}/g" "$f" "pill"
done
show_separator

echo -e "${CYAN}5. Residual bare '${OLD_VER}' occurrences — JUDGE each (not auto-changed)${NC}"
echo -e "  ${BLUE}Historically-scoped prose ('introduced in ${OLD_VER}', 'Requires GeoBrix ${OLD_VER}+')${NC}"
echo -e "  ${BLUE}stays TRUE after the bump — do NOT change it. Lock files / genie_map app / shared${NC}"
echo -e "  ${BLUE}third-party pins are false positives.${NC}"
echo ""
RESID=$(git grep -nE "(^|[^0-9.])${O_ESC}([^0-9]|$)" -- . \
    ':!:pom.xml' \
    ':!:python/geobrix/src/databricks/labs/gbx/__init__.py' \
    ':!:docs/package.json' \
    ':!:**/package-lock.json' ':!:**/pnpm-lock.yaml' \
    ':!:docs/build*/**' ':!:.superpowers/**' \
    ':!:docs/docs/release-notes.mdx' \
    ':!:resources/images/generators/*' ':!:resources/images/diagrams/**' 2>/dev/null \
    | grep -vE "geobrix-${O_ESC}-(py3-none-any\.whl|jar-with-dependencies\.jar)" \
    | grep -vE "Current version \*\*${O_ESC}\*\*")
if [ -z "$RESID" ]; then
    echo -e "  ${GREEN}(none — clean)${NC}"
else
    echo "$RESID" | while IFS= read -r line; do
        printf '  %b%s%b\n' "${YELLOW}" "${line}" "${NC}"
    done
fi
show_separator

echo -e "${CYAN}Verify (after applying):${NC}"
echo -e "  1. ${YELLOW}pom.xml${NC}, ${YELLOW}__init__.py${NC}, ${YELLOW}docs/package.json${NC} all read ${GREEN}${NEW_VER}${NC}."
echo -e "  2. ${YELLOW}git grep -n \"${OLD_VER}-py3-none-any.whl\\|${OLD_VER}-jar-with-dependencies\"${NC} returns nothing."
echo -e "  3. ${YELLOW}git grep -n \"Current version: ${OLD_VER}\"${NC} returns nothing."
echo -e "  4. Build sanity: wheel builds as ${GREEN}geobrix-${NEW_VER}-py3-none-any.whl${NC}, JAR as ${GREEN}geobrix-${NEW_VER}-jar-with-dependencies.jar${NC}"
echo -e "     (the release workflow parses VERSION from the JAR name)."
echo -e "  5. Run affected tests: function-info, bindings/parity, and any JAR-path-pinned conftest/parity test."
show_separator

echo -e "${CYAN}Manual step — diagram PNGs (NOT done by this command):${NC}"
echo -e "  The docs site displays the ${YELLOW}.png${NC} of each pill diagram, not the .svg. After the SVG"
echo -e "  pill text is bumped above, re-render each PNG from its SVG (headless-Chrome screenshot per"
echo -e "  the generator header) so the site shows ${GREEN}v${NEW_VER}${NC}. A version bump alone leaves stale PNGs."
show_separator

echo -e "${CYAN}Release flow (after this PR merges to main):${NC}"
echo -e "  git tag -a v${NEW_VER} <merge-sha> && git push origin v${NEW_VER}"
echo -e "  gh release create v${NEW_VER} --latest --title \"GeoBrix v${NEW_VER} (Beta)\" --notes \"...\""
echo -e "  gh workflow run \"package geobrix artifacts\" -f ref=v${NEW_VER} -f attach_to_tag=v${NEW_VER}"
echo -e "  (6 assets; docs deploy auto-triggers on the docs-touching merge to main)."
show_separator

if [ "$DRY_RUN" -eq 0 ] && [ ${#CHANGED_FILES[@]} -gt 0 ]; then
    echo -e "${GREEN}Modified ${#CHANGED_FILES[@]} file(s).${NC} Review with ${YELLOW}git diff${NC}, then commit."
elif [ "$DRY_RUN" -eq 1 ]; then
    echo -e "${YELLOW}Dry-run complete — no files modified.${NC}"
else
    echo -e "${YELLOW}No files needed changes.${NC}"
fi

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi
