#!/bin/bash
# gbx:ci:setup - Install and configure GitHub CLI for CI

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/ci/setup-gh-cli.sh" "$@"
