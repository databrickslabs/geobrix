# GeoBrix image for Databricks Container Services (DCS vNext) on DEDICATED clusters: the env6 wheel (its payload
# carries the one GDAL/PROJ/GEOS stack, its bindings and the JNI); its declared and [vizx] dependencies from a
# hashed per-arch lock with the base image's packages (click included) held, whose rasterio is our build against
# the same GDAL (no PyPI rasterio, no OpenSSL 1.1); exactly one JAR. No GDAL is compiled and no package index is
# needed: the wheels arrive in the build context (gbx:dcs:build stages them).
# Build with `gbx:dcs:build` (scripts/commands/gbx-dcs-build.sh); user docs: docs/docs/installation/dcs.mdx.
# DCS honors only FROM/ARG/RUN/ENV/WORKDIR/COPY.
ARG BASE_IMAGE=databricksruntime/environment:v6-standard-arm

FROM ${BASE_IMAGE} AS release
# x86_64 or aarch64: the payload's architecture directory
ARG GBX_ARCH
ARG GEOBRIX_VERSION
# The wheels are bind-mounted for the install, not copied, so no layer of the image holds them.
RUN --mount=type=bind,source=geobrix-wheel,target=/tmp/geobrix-wheel \
    --mount=type=bind,source=wheelhouse,target=/tmp/wh \
    --mount=type=bind,source=dcs.lock,target=/tmp/dcs.lock \
    py=/databricks/python3/bin/python \
 && $py -m pip install -q --disable-pip-version-check --no-cache-dir --no-index --no-deps /tmp/geobrix-wheel/geobrix-*.whl \
 && $py -m pip install -q --disable-pip-version-check --no-cache-dir --no-index --find-links /tmp/wh --require-hashes -r /tmp/dcs.lock \
 && $py -m pip check
# Point the system at the payload, so the JVM's legacy path, the GDAL tools that load by name and the netCDF-C
# plugin search all find the one GDAL stack:
# - the payload's libraries are registered system-wide (the JNI library is loaded through /usr/lib, and a link's
#   $ORIGIN is the link's directory, not the payload's);
# - /usr/lib/libgdalalljni.so is a link to the payload's JNI, which GDALManager follows into the payload for its data;
# - /usr/local/hdf5/lib/plugin, HDF5's and netCDF-C's default plugin directory, is a LINK to the payload's filter
#   plugins (a copy would lose their RUNPATH, $ORIGIN/../lib). The JVM cannot register a plugin directory itself,
#   so this is what lets the heavy tier read zstd, blosc and bzip2 netCDF-4 with no spark_env_vars setting.
RUN set -e; \
    gbx="$(/databricks/python3/bin/python -c 'import os, databricks.labs.gbx._native as n; print(os.path.dirname(os.path.dirname(n.__file__)))')"; \
    native="$gbx/_native/${GBX_ARCH}"; \
    test -f "$native/lib/libgdalalljni.so"; test -d "$native/hdf5-plugins"; \
    echo "$native/lib" > /etc/ld.so.conf.d/zz-gbx-native.conf && ldconfig; \
    ln -sf "$native/lib/libgdalalljni.so" /usr/lib/libgdalalljni.so; \
    mkdir -p /usr/local/hdf5/lib && ln -sfn "$native/hdf5-plugins" /usr/local/hdf5/lib/plugin
# Dedicated clusters load /databricks/jars; one GeoBrix JAR per cluster, so never also attach it as a library.
# The JAR is the wheel's own copy, staged mode 0644 in the build context.
COPY geobrix-${GEOBRIX_VERSION}-jar-with-dependencies.jar /databricks/jars/
