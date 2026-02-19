#!/bin/bash
# gbx:data:push-wheel - build JAR first (unless skip), then python3 -m build and upload wheel to GBX_ARTIFACT_VOLUME/ (overwrite if exists); set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 or GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 to skip

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT" || exit 1
python notebooks/tests/push_wheel_to_volume.py
exit $?
