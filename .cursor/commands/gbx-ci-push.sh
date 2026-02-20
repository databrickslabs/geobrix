#!/bin/bash
# gbx:ci:push - Push current branch and watch build main workflow

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/push-and-watch.sh"
