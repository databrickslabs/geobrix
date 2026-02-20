#!/bin/bash
# Run inside the geobrix-dev container.
# Sets Maven to use project-local .m2 (scripts/docker/m2) and skipScoverage as default.
# Same logic as steps [1] and [2] in docker_init.sh; no build step.
# Invoked by gbx-docker-start after container start so each launch keeps .m2 in project.

set -e

echo ""
echo "::: [Maven setup] .m2 in project + skipScoverage as default :::"
unset JAVA_TOOL_OPTIONS

if [ -f /usr/local/share/maven/conf/settings.xml ] && [ ! -f /usr/local/share/maven/conf/settings.xml.BAK ]; then
    mv /usr/local/share/maven/conf/settings.xml /usr/local/share/maven/conf/settings.xml.BAK
fi
cp /root/geobrix/scripts/docker/m2/settings.xml /usr/local/share/maven/conf
echo "    Active Maven profile(s):"
cd /root/geobrix && mvn help:active-profiles -q
echo "::: done :::"
echo ""
