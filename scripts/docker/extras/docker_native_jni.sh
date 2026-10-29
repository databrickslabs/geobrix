#!/bin/bash
# Run inside the dev container (gbx:docker:start runs it after the Maven setup, on every start).
#
# The JAR's GDAL Java classes come from the native build (pom.xml gdal.java.version), and a JVM binds them to the
# first libgdalalljni.so on its library path: GDAL's classes load it by name, and GDALManager's fallback is
# /usr/lib/libgdalalljni.so (/lib is /usr/lib). The image's link there is its own GDAL 3.11 JNI, which those
# classes refuse. So that link points at the same build's JNI: the env6 payload's, which gbx:build:wheel --env 6
# extracts to build/env-wheel/env6/payload/. A JNI linked into a payload's _native/<arch>/lib takes that payload's
# GDAL and PROJ data (GDALManager). Made on every start, so a recreated container gets it again.
# With no payload yet, the link is left as it is and the fix is printed.

set -u

ROOT="${GBX_CONTAINER_ROOT:-/root/geobrix}"
LINK="${GBX_JNI_LINK:-/usr/lib/libgdalalljni.so}"
ARCH="${GBX_JNI_ARCH:-$(uname -m)}"
JNI="$ROOT/build/env-wheel/env6/payload/_native/$ARCH/lib/libgdalalljni.so"

echo ""
echo "::: [JVM GDAL] $LINK -> the env6 payload's JNI :::"
if [ ! -f "$JNI" ]; then
    echo "    NOT LINKED: no $JNI (the env6 payload is not built in this checkout)."
    echo "    The JVM keeps the image's JNI ($(readlink -f "$LINK" 2>/dev/null || echo none)), which the JAR's GDAL classes refuse."
    echo "    Fix: bash scripts/commands/gbx-build-wheel.sh --env 6, then bash scripts/commands/gbx-docker-start.sh"
    echo "::: done :::"
    exit 0
fi
ln -sfn "$JNI" "$LINK" || { echo "    could not link $LINK to $JNI" >&2; exit 1; }
echo "    $LINK -> $(readlink "$LINK")"
echo "::: done :::"
