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
# Fully qualified, version-pinned plugin coordinates: the short `help:` prefix does not resolve
# in geobrix-dev. Prefix lookup reads each repository's plugin-group maven-metadata.xml, and the
# db-maven-proxy mirror answers that request with an empty 105-byte stub, so Maven reports
# "No plugin found for prefix 'help'". The same convention is in scripts/ci/fetch-delta-test-jars.sh.
HELP_PLUGIN="3.5.1"
# No -q: the plugin reports through the INFO log, so -q printed an empty list under the heading above.
# Show just the profile lines on success; on failure show everything Maven said and stop.
cd /root/geobrix
if ! profiles_out="$(mvn -B "org.apache.maven.plugins:maven-help-plugin:${HELP_PLUGIN}:active-profiles" 2>&1)"; then
    echo "$profiles_out"
    exit 1
fi
echo "$profiles_out" | grep -E '^ - ' || echo "     (none)"
echo "::: done :::"
echo ""
