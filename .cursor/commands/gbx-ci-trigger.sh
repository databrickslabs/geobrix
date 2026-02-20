#!/bin/bash
# gbx:ci:trigger - Push and optionally trigger build main workflow

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/trigger-remote-tests.sh" "$@"
