#!/bin/bash
# gbx:ci:docs - Documentation tests menu (local run, CI status, trigger, watch, logs)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/run-doc-tests.sh" "$@"
