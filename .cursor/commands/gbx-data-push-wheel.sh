#!/bin/bash
# gbx:data:push-wheel - Build Python wheel and upload to GBX_BUNDLE_WHEEL_VOLUME_PATH (overwrite)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT" || exit 1
python notebooks/tests/push_wheel_to_volume.py
exit $?
