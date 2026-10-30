#!/bin/bash
# Run inside the dev container (gbx:docker:start runs it after the Maven setup, on every start), and on CI's runners
# (.github/actions/scala_build, before the first test JVM).
#
# The JAR's GDAL Java classes come from the native build (pom.xml gdal.java.version), and a JVM binds them to the
# first libgdalalljni.so on its library path: GDAL's classes load it by name, and GDALManager's fallback is
# /usr/lib/libgdalalljni.so (/lib is /usr/lib). The image's link there is its own GDAL 3.11 JNI, which those
# classes refuse. So that link points at the same build's JNI: the env6 payload's, which gbx:build:wheel --env 6
# extracts to build/env-wheel/env6/payload/. A JNI linked into a payload's _native/<arch>/lib takes that payload's
# GDAL and PROJ data (GDALManager). Made on every start, so a recreated container gets it again.
# With no payload yet, the link is left as it is and the fix is printed.
#
# CI: GBX_NATIVE_ROOT names the payload (scripts/native/jvm_payload.py lays it out from the cached x86_64 build), and
# GBX_JNI_REQUIRE=1 makes a missing payload an error: a JVM left on another GDAL's JNI aborts every GDAL suite.

set -u

ROOT="${GBX_CONTAINER_ROOT:-/root/geobrix}"
LINK="${GBX_JNI_LINK:-/usr/lib/libgdalalljni.so}"
ARCH="${GBX_JNI_ARCH:-$(uname -m)}"
PAYLOAD="${GBX_NATIVE_ROOT:-$ROOT/build/env-wheel/env6/payload}"
JNI="${PAYLOAD%/}/_native/$ARCH/lib/libgdalalljni.so"

echo ""
echo "::: [JVM GDAL] $LINK -> the payload's JNI ($PAYLOAD) :::"
if [ ! -f "$JNI" ]; then
    echo "    NOT LINKED: no $JNI (the payload is not built)."
    echo "    The JVM keeps the image's JNI ($(readlink -f "$LINK" 2>/dev/null || echo none)), which the JAR's GDAL classes refuse."
    if [ "${GBX_JNI_REQUIRE:-0}" = 1 ]; then
        echo "    GBX_JNI_REQUIRE=1: failing (CI's native payload step lays the payload out before this one)."
        echo "::: done :::"
        exit 1
    fi
    echo "    Fix: bash scripts/commands/gbx-build-wheel.sh --env 6, then bash scripts/commands/gbx-docker-start.sh"
    echo "::: done :::"
    exit 0
fi
ln -sfn "$JNI" "$LINK" || { echo "    could not link $LINK to $JNI" >&2; exit 1; }
echo "    $LINK -> $(readlink "$LINK")"
echo "::: done :::"
