#!/bin/bash
# gbx:ci:logs - Fetch CI run logs

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/fetch-ci-logs.sh" "$@"
