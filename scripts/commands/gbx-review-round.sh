#!/bin/bash
# gbx:review:round - Run Isaac Review on a SCOPED slice of the branch
# (default: the commits you're about to push) instead of the full
# branch-vs-main diff, which on a release branch is ~1000 files. Wraps
# `dbexec repo run isaac review` with a size guardrail so the expensive
# full-branch pass never fires by accident. Isaac Review is incremental
# (session cache + PR-finding reuse), so per-round scoping is cheap and
# cumulative across runs.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

# Empirically, isaac assembles the prompt from the FULL CONTENTS of every changed
# file plus a fixed rubric, so a review overflows the 200k context window at roughly
# ~150-200 changed files regardless of diff size (verified 2026-08-25: 175/263/948-file
# scopes all hit "Prompt is too long"; ~86 files fit). Keep rounds well under this;
# the silent-failure detection below is the real backstop.
LARGE_THRESHOLD=150
BASE=""
FULL=0
RULE=""
LOG_PATH=""
ALLOW_LARGE=0
KEEP_OUTPUTS=0
INCLUDE_GENERATED=0
PASSTHRU=()

show_help() {
    show_banner "Review: Round (Isaac Review, scoped)"
    echo -e "${CYAN}Run Isaac Review on a scoped slice instead of the full branch-vs-main diff.${NC}"
    echo ""
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}bash scripts/commands/gbx-review-round.sh${NC} ${YELLOW}[OPTIONS] [-- <extra isaac args>]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--base <ref>${NC}     Base ref to review HEAD against. Default: the branch's"
    echo -e "                   upstream (@{upstream}) = your unpushed commits. Use a"
    echo -e "                   historical SHA for a back-fill pass, or origin/main for"
    echo -e "                   the full release sweep (needs --allow-large)."
    echo -e "  ${GREEN}--full${NC}           Ignore Isaac's incremental session cache (re-review all)."
    echo -e "  ${GREEN}--rule <set>${NC}     Run a specific rule set / rule (isaac --run-rule)."
    echo -e "  ${GREEN}--allow-large${NC}    Proceed even if the scope exceeds ${LARGE_THRESHOLD} files."
    echo -e "  ${GREEN}--keep-outputs${NC}   Review .ipynb files WITH their stored outputs (default strips"
    echo -e "                   them: embedded plots overflow isaac's context and aren't code)."
    echo -e "  ${GREEN}--include-generated${NC}  Also review the generated lock files (requirements-dev-container*.txt,"
    echo -e "                   requirements*-ci.txt). Default: skipped (scripts/commands/isaac-review-local.yaml,"
    echo -e "                   Isaac opt_out.paths); they are hash-pinned compile output."
    echo -e "  ${GREEN}--log <path>${NC}     Tee output to a log (filename -> test-logs/<name>)."
    echo -e "  ${GREEN}-h, --help${NC}       Show this help."
    echo ""
    echo -e "${CYAN}Notes:${NC}"
    echo -e "  Extra isaac flags after ${YELLOW}--${NC} pass through (e.g. ${YELLOW}-- -f markdown -o report.md${NC})."
    echo -e "  Isaac runs via ${YELLOW}dbexec repo run isaac${NC} (the shell alias is not available in scripts)."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}# Review what you're about to push, then push:${NC}"
    echo -e "  bash scripts/commands/gbx-review-round.sh"
    echo -e "  ${YELLOW}# Review a specific historical slice:${NC}"
    echo -e "  bash scripts/commands/gbx-review-round.sh --base 776b6ec7"
    echo -e "  ${YELLOW}# Full release sweep to markdown (large; must opt in):${NC}"
    echo -e "  bash scripts/commands/gbx-review-round.sh --base origin/main --allow-large -- -f markdown -o test-logs/isaac-pr.md"
    echo ""
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) show_help; exit 0 ;;
        --base) BASE="$2"; shift 2 ;;
        --full) FULL=1; shift ;;
        --rule) RULE="$2"; shift 2 ;;
        --allow-large) ALLOW_LARGE=1; shift ;;
        --keep-outputs) KEEP_OUTPUTS=1; shift ;;
        --include-generated) INCLUDE_GENERATED=1; shift ;;
        --log) LOG_PATH="$(resolve_log_path "$2")"; shift 2 ;;
        --) shift; while [[ $# -gt 0 ]]; do PASSTHRU+=("$1"); shift; done ;;
        *) PASSTHRU+=("$1"); shift ;;
    esac
done

[[ -n "$LOG_PATH" ]] && setup_log_file "$LOG_PATH"
show_banner "Review: Round (Isaac Review, scoped)"

# Isaac runs via dbexec; the `isaac` alias is not available in a non-interactive script.
if ! command -v dbexec >/dev/null 2>&1; then
    echo -e "${RED}ERROR: dbexec not found on PATH — cannot run Isaac Review.${NC}" >&2
    exit 2
fi

cd "$PROJECT_ROOT" || exit 2

# Resolve the base ref (default: the branch's upstream = your unpushed commits).
if [[ -z "$BASE" ]]; then
    if ! BASE="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null)"; then
        echo -e "${RED}ERROR: no upstream configured for the current branch.${NC}" >&2
        echo -e "Pass ${GREEN}--base <ref>${NC} explicitly (e.g. --base origin/main, or a commit SHA)." >&2
        exit 2
    fi
fi

if ! git rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null 2>&1; then
    echo -e "${RED}ERROR: base ref '${BASE}' does not resolve to a commit.${NC}" >&2
    exit 2
fi

# Scope = merge-base(base, HEAD)...HEAD, matching isaac's --base semantics.
MERGE_BASE="$(git merge-base "$BASE" HEAD 2>/dev/null)"
FILE_COUNT="$(git diff --name-only "$MERGE_BASE" HEAD 2>/dev/null | grep -c . || true)"

if [[ "${FILE_COUNT:-0}" -eq 0 ]]; then
    echo -e "${YELLOW}Nothing to review: HEAD has no changes vs '${BASE}'.${NC}"
    echo -e "(For an earlier slice, pass ${GREEN}--base <older-ref>${NC}.)"
    exit 0
fi

echo -e "${CYAN}Scope:${NC} ${BASE}...HEAD  (${GREEN}${FILE_COUNT}${NC} file(s))"

if [[ "$FILE_COUNT" -gt "$LARGE_THRESHOLD" && "$ALLOW_LARGE" -ne 1 ]]; then
    echo "" >&2
    echo -e "${RED}REFUSING:${NC} scope is ${FILE_COUNT} files (> ${LARGE_THRESHOLD})." >&2
    echo -e "A diff this size (likely the full branch-vs-main) yields low-signal, high-cost reviews." >&2
    echo -e "Narrow it with ${GREEN}--base <recent-ref>${NC}, or pass ${GREEN}--allow-large${NC} to proceed intentionally." >&2
    exit 3
fi

ISAAC_ARGS=(review --base "$BASE")
REVIEW_DIR="$PROJECT_ROOT"

# Notebook outputs (base64 plots, multi-MB) are not reviewable code, and isaac loads
# every changed file in full, so one executed notebook can overflow the context window
# on its own. Unless --keep-outputs: review the same change in a scratch worktree where
# every changed .ipynb has its outputs stripped on BOTH sides. The stripped base is a
# local detached commit there (never pushed; it goes through the normal git hooks) and
# HEAD's versions are left uncommitted for `isaac review --uncommitted`. Sources and
# every non-notebook file are reviewed unchanged.
NOTEBOOKS=()
while IFS= read -r f; do [[ -n "$f" ]] && NOTEBOOKS+=("$f"); done \
    < <(git diff --name-only "$MERGE_BASE" HEAD -- '*.ipynb')
strip_outputs() {  # strip_outputs <dir> <paths...>: clear outputs/execution_count in place
    python3 -I - "$@" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
for rel in sys.argv[2:]:
    p = root / rel
    if not p.exists():
        continue
    nb = json.loads(p.read_text(encoding="utf-8"))
    for c in nb.get("cells", []):
        if c.get("cell_type") == "code":
            c["outputs"], c["execution_count"] = [], None
    p.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
PY
}
if [[ ${#NOTEBOOKS[@]} -gt 0 && "$KEEP_OUTPUTS" -ne 1 ]]; then
    HEAD_SHA="$(git rev-parse HEAD)"  # this checkout's HEAD; inside the worktree HEAD is the scratch base
    REVIEW_DIR="$(mktemp -d -t gbx-review-wt.XXXXXX)"
    trap 'git -C "$PROJECT_ROOT" worktree remove --force "$REVIEW_DIR" >/dev/null 2>&1' EXIT
    git worktree add --quiet --detach "$REVIEW_DIR" "$MERGE_BASE" || exit 2
    strip_outputs "$REVIEW_DIR" "${NOTEBOOKS[@]}"
    git -C "$REVIEW_DIR" commit -q -a --allow-empty \
        -m "chore(review): scratch base with notebook outputs stripped" \
        -m "Local and detached, never pushed: lets isaac review without multi-MB outputs." \
        >/dev/null || exit 2
    while IFS=$'\t' read -r status path; do
        if [[ "$status" == D ]]; then git -C "$REVIEW_DIR" rm -q -- "$path"
        else git -C "$REVIEW_DIR" checkout "$HEAD_SHA" -- "$path" || exit 2; fi
    done < <(git diff --name-status --no-renames "$MERGE_BASE" HEAD)
    strip_outputs "$REVIEW_DIR" "${NOTEBOOKS[@]}"
    ISAAC_ARGS=(review --uncommitted)
    echo -e "${CYAN}Notebook outputs stripped${NC} for review (${#NOTEBOOKS[@]} .ipynb; --keep-outputs to include them)."
fi
# Report every severity, and skip the generated lock files by default (Isaac opt_out.paths in the
# layered config). One config is passed: isaac-review-local.yaml (all severities + the opt_out), or with
# --include-generated isaac-review-severities.yaml (all severities only), so that mode does not fall back
# to Isaac's critical/major-only threshold. A config the caller passes after `--` (-c/--config) is
# theirs to manage, so none is added then.
HAS_OWN_CONFIG=0
for a in ${PASSTHRU[@]+"${PASSTHRU[@]}"}; do
    case "$a" in -c|--config|-c=*|--config=*) HAS_OWN_CONFIG=1 ;; esac
done
if [[ "$HAS_OWN_CONFIG" -ne 1 ]]; then
    if [[ "$INCLUDE_GENERATED" -eq 1 ]]; then
        ISAAC_ARGS+=(-c "$SCRIPT_DIR/isaac-review-severities.yaml")
    else
        ISAAC_ARGS+=(-c "$SCRIPT_DIR/isaac-review-local.yaml")
    fi
fi
[[ "$FULL" -eq 1 ]] && ISAAC_ARGS+=(--full)
[[ -n "$RULE" ]] && ISAAC_ARGS+=(--run-rule "$RULE")
[[ ${#PASSTHRU[@]} -gt 0 ]] && ISAAC_ARGS+=("${PASSTHRU[@]}")

echo -e "${CYAN}Running:${NC} dbexec repo run isaac ${ISAAC_ARGS[*]}"
show_separator

# Capture combined output so we can detect a SILENT failure: isaac exits 0 and
# reports "0 findings" even when its underlying model call errored (e.g. the diff
# overflows the context window -> {"is_error":true,...,"result":"Prompt is too long"}).
# A context-overflow "0 findings" is not a clean review — fail loudly instead.
SCAN_FILE="$(mktemp -t gbx-review-round.XXXXXX)"
(cd "$REVIEW_DIR" && dbexec repo run isaac "${ISAAC_ARGS[@]}") 2>&1 | tee "$SCAN_FILE"
rc=${PIPESTATUS[0]}

if grep -qE 'CERTIFICATE_EXPIRED|Missing/Expired Certificate' "$SCAN_FILE"; then
    echo -e "${YELLOW}Note:${NC} the Databricks client certificate is missing or expired (isaac's flag fetch failed);" >&2
    echo -e "run ${GREEN}dbcert${NC} on your laptop if the review below errored." >&2
fi

if grep -q 'No changes found' "$SCAN_FILE"; then
    rm -f "$SCAN_FILE"
    echo "" >&2
    echo -e "${RED}Isaac Review reviewed nothing:${NC} it found no changes, but the scope has ${FILE_COUNT} file(s)." >&2
    exit 4
fi

if grep -qE '"is_error"[[:space:]]*:[[:space:]]*true|Prompt is too long' "$SCAN_FILE"; then
    rm -f "$SCAN_FILE"
    echo "" >&2
    echo -e "${RED}Isaac Review FAILED silently:${NC} the model call errored (likely 'Prompt is too long' —" >&2
    echo -e "the ${FILE_COUNT}-file diff overflowed the context window). A '0 findings' result from this" >&2
    echo -e "run is ${RED}NOT${NC} trustworthy. Narrow the scope with ${GREEN}--base <recent-ref>${NC} so each pass fits," >&2
    echo -e "or review in smaller commit-range slices." >&2
    exit 4
fi

rm -f "$SCAN_FILE"
exit "$rc"
