#!/bin/bash
# gbx:bench:uc-probes - write the notebooks and statement files of the batched workspace pass, print the command of each run
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: UC probe files (batched workspace pass)"
    echo "Usage: bash scripts/commands/gbx-bench-uc-probes.sh [--catalog C] [--schema S] [--volume-dir DIR] [--jar PATH]"
    echo "         [--out-dir DIR] [--only RUN[,RUN]] [--dry-run] [--parse] [--log <path>] [--help]"
    echo "       bash scripts/commands/gbx-bench-uc-probes.sh --judge <results.json> [--catalog C] [--schema S] [--volume-dir DIR]"
    echo ""
    echo "Write every notebook and statement file the pass runs into one folder, with manifest.json and manifest.md"
    echo "(what each file is for, its runs and preconditions), check each file, and print the exact command of each run."
    echo "It touches no workspace and needs no login. The files are checked: notebooks are JSON whose code compiles and"
    echo "whose probe call fits its signature; statement files parse as the warehouse runner reads them, name only"
    echo "functions of the manifest, and qualify each to three levels. Exit 1 when a file fails a check."
    echo ""
    echo "Options:"
    echo "  --catalog C       Catalog of the scratch schemas (default geospatial_docs)"
    echo "  --schema S        Base name of the four scratch schemas S_w, S_s6, S_s6j, S_se (default geobrix_t28)"
    echo "  --volume-dir DIR  Volume folder that holds the env 6 wheel alone (default <volroot>/t28/<sha12 of HEAD>)"
    echo "  --jar PATH        The JAR on a Volume, for the Scala forms (default <volroot>/geobrix/0.6.0/jar/geobrix-0.6.0-jar-with-dependencies.jar)"
    echo "  --volroot DIR     Volume root of the flat env wheels (default /Volumes/geospatial_docs/geobrix/sample-data)"
    echo "  --out-dir DIR     Where the files go (default .superpowers/streamline/t28)"
    echo "  --profile NAME    Profile in the printed commands (default oauth-fe)"
    echo "  --ws-root DIR     Workspace folder in the printed commands (default /Users/<current user>/GeoBrix from --profile; required with no session)"
    echo "  --cluster-id ID   The running JAR-less cluster of probe P1; without it the two P1 notebooks are not written"
    echo "  --only RUNS       Only these runs (S6-0, S6-A..S6-H, W-A..W-E, D-A, D-B, STD-A, S5-A, END), any case"
    echo "  --dry-run         Build and check everything; write no file"
    echo "  --parse           Also run Spark's SQL parser over each statement (starts a local Spark; Databricks-only DDL is rejected)"
    echo "  --judge FILE      Read a results file written by gbx-uc-sql (w_slice, w_tierb, w_mech or w_engine_m6) and judge it"
    echo "  --log <path>      Write output to a log file (filename -> test-logs/<name>)"
    echo "  --help            Show this help"
    echo ""
    echo "Example:"
    echo "  bash scripts/commands/gbx-bench-uc-probes.sh --dry-run"
    echo "  bash scripts/commands/gbx-bench-uc-probes.sh --out-dir .superpowers/streamline/t28"
}

LOG_PATH=""
ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h) show_help; exit 0 ;;
        --log)
            [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
            LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --catalog|--schema|--volume-dir|--jar|--volroot|--out-dir|--profile|--ws-root|--cluster-id|--only|--judge)
            [[ $# -ge 2 && "$2" != --* ]] || { echo "Option $1 needs a value" >&2; exit 2; }
            ARGS+=("$1" "$2"); shift 2 ;;
        --dry-run|--parse) ARGS+=("$1"); shift ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 2 ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1
setup_log_file "$LOG_PATH"
# The generator needs the package and its curated host environment, not the Databricks SDK.
QUOTED=""
[[ ${#ARGS[@]} -gt 0 ]] && QUOTED="$(printf '%q ' "${ARGS[@]}")"   # printf with no words would print an empty argument
run_in_pyrx_venv "cd '$PROJECT_ROOT' && python -W ignore -m databricks.labs.gbx.bench.t28 $QUOTED"
exit $?
