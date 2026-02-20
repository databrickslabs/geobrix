#!/bin/bash
# gbx:ci:watch - Watch CI run in real time

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/watch-ci.sh" "$@"
