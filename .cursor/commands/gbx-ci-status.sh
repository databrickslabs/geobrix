#!/bin/bash
# gbx:ci:status - Check CI status and recent runs

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/check-ci-status.sh" "$@"
