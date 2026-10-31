#!/bin/bash
# Run inside the geobrix-dev container.
# Sets Maven to use project-local .m2 (scripts/docker/m2) and skipScoverage as default.
# Same logic as steps [1] and [2] in docker_init.sh; no build step.
# Invoked by gbx-docker-start after container start so each launch keeps .m2 in project.

set -e

# The checkout as mounted, and the image Maven's conf dir (overridable for the scripts/ci tests).
ROOT="${GBX_CONTAINER_ROOT:-/root/geobrix}"
CONF="${GBX_MAVEN_CONF_DIR:-/usr/local/share/maven/conf}"
SETTINGS="$ROOT/scripts/docker/m2/settings.xml"
DEFAULT_SETTINGS="$ROOT/scripts/docker/extras/maven-settings.xml"

echo ""
echo "::: [Maven setup] .m2 in project + skipScoverage as default :::"
unset JAVA_TOOL_OPTIONS

# scripts/docker/m2/ is gitignored, so a new linked worktree has no settings.xml there: give it the
# tracked default. A settings.xml already there is kept. This comes first: the image's own settings
# are moved aside below, and without a replacement Maven would go to Maven Central, which is blocked.
if [ ! -f "$SETTINGS" ]; then
    if [ ! -f "$DEFAULT_SETTINGS" ]; then
        echo "    no $SETTINGS and no $DEFAULT_SETTINGS: the container's Maven settings were left unchanged" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$SETTINGS")"
    cp "$DEFAULT_SETTINGS" "$SETTINGS"
    echo "    $SETTINGS was missing: wrote the default (db-maven-proxy mirror, skipScoverage)"
fi
if [ -f "$CONF/settings.xml" ] && [ ! -f "$CONF/settings.xml.BAK" ]; then
    mv "$CONF/settings.xml" "$CONF/settings.xml.BAK"
fi
cp "$SETTINGS" "$CONF/settings.xml"
# Fully qualified, version-pinned plugin coordinates: the short `help:` prefix does not resolve
# in geobrix-dev. Prefix lookup reads each repository's plugin-group maven-metadata.xml, and the
# db-maven-proxy mirror answers that request with an empty 105-byte stub, so Maven reports
# "No plugin found for prefix 'help'". The same convention is in scripts/ci/fetch-delta-test-jars.sh.
HELP_PLUGIN="3.5.1"
# No -q: the plugin reports through the INFO log, so -q printed an empty list under the heading above.
# Show just the profile lines on success; on failure show everything Maven said and stop.
cd "$ROOT"
# GDAL's Java classes come from the native build (scripts/native/maven), in no repository: put them into the
# project-local repository, sha256-checked against their pin, with no Maven plugin, before Maven resolves anything.
echo "    GDAL Java classes:"
# The repository is the one the settings name (a settings.xml of the checkout's own may move it); the tracked
# default's /root/geobrix prefix is this checkout's $ROOT.
REPO="$(grep -v '<!--' "$SETTINGS" | sed -n 's:.*<localRepository>\(.*\)</localRepository>.*:\1:p' | head -1)"
case "$REPO" in
    /root/geobrix|/root/geobrix/*) REPO="$ROOT${REPO#/root/geobrix}" ;;
    "") REPO="$ROOT/scripts/docker/m2" ;;
esac
python3 "$ROOT/scripts/native/java_jar.py" install --repo "$REPO" || exit 1
echo "    Active Maven profile(s):"
if ! profiles_out="$(mvn -B "org.apache.maven.plugins:maven-help-plugin:${HELP_PLUGIN}:active-profiles" 2>&1)"; then
    echo "$profiles_out"
    exit 1
fi
echo "$profiles_out" | grep -E '^ - ' || echo "     (none)"
echo "::: done :::"
echo ""
