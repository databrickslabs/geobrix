#!/bin/bash
# gbx:data:push-jar - mvn clean package -DskipTests then upload JAR to GBX_ARTIFACT_VOLUME/ (overwrite if exists); set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to skip

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT" || exit 1
python notebooks/tests/push_jar_to_volume.py
exit $?
