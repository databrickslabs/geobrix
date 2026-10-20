# syntax=docker/dockerfile:1
# GeoBrix image for Databricks Container Services (DCS vNext) on DEDICATED clusters.
# Bakes in native GDAL with its Java (JNI) bindings, the GeoBrix wheel and exactly one GeoBrix JAR,
# so both tiers run with no init script and no cluster library. Build with `gbx:dcs:build`
# (scripts/commands/gbx-dcs-build.sh); user docs: docs/docs/installation/dcs.mdx.
# DCS honors only FROM/ARG/RUN/ENV/WORKDIR/COPY.
ARG BASE_IMAGE=databricksruntime/environment:v6-standard-arm

# ---------- base: DCS env 6 + GDAL 3.11.4 (JNI) ----------
FROM ${BASE_IMAGE} AS base
ARG GDAL_VERSION=3.11.4
# Pinned SHA-256 of the GDAL source tarball; keep in lockstep with scripts/docker/Dockerfile.
ARG GDAL_SHA256=0fa36ee34d4451db586d2bf78ea0dbfa3b0dfae0516587f8130d21add0ac9dad

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ant autoconf automake build-essential cmake g++ git \
      libcurl4-gnutls-dev libgeos-dev libgeotiff-dev libhdf4-alt-dev libhdf5-dev \
      libjsoncpp-dev libnetcdf-dev libopenjp2-7-dev libpoppler-dev libpoppler-private-dev \
      libpq-dev libproj-dev libsnappy-dev libspatialite-dev libsqlite3-dev \
      libtiff-dev libtool libwebp-dev libxml2-dev ninja-build pkg-config \
      proj-bin proj-data sqlite3 swig unixodbc wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/src && \
    wget -q -O /tmp/gdal.tar.gz https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz && \
    echo "${GDAL_SHA256}  /tmp/gdal.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/gdal.tar.gz -C /usr/local/src/ && rm /tmp/gdal.tar.gz && \
    mkdir -p /usr/local/src/gdal-${GDAL_VERSION}/build && cd /usr/local/src/gdal-${GDAL_VERSION}/build && \
    cmake -G Ninja .. \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DGDAL_USE_JAVA=ON -DJAVA_HOME="${JAVA_HOME}" \
        -DJNI_INCLUDE_DIRS="${JAVA_HOME}/include;${JAVA_HOME}/include/linux" \
        -DBUILD_JAVA_BINDINGS=ON -DBUILD_PYTHON_BINDINGS=OFF \
        -DBUILD_TESTING=OFF && \
    cmake --build . --parallel 8 && cmake --install . && \
    echo /usr/local/lib > /etc/ld.so.conf.d/usr-local-gdal.conf && ldconfig && \
    cd / && rm -rf /usr/local/src/gdal-${GDAL_VERSION}

# The heavyweight JAR loads /usr/lib/libgdalalljni.so.
RUN ln -sf /usr/local/lib/libgdal.so /usr/lib/libgdal.so && \
    ln -sf /usr/local/lib/libgdal.so /usr/lib/libgdal.so.30 && \
    ln -sf /usr/local/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so && \
    mkdir -p /usr/lib/jni && ln -sf /usr/local/lib/jni/libgdalalljni.so /usr/lib/jni/libgdalalljni.so.30

# ---------- release: + GeoBrix wheel (lightweight deps) + exactly one GeoBrix JAR ----------
FROM base AS release
ARG GEOBRIX_VERSION
COPY geobrix-${GEOBRIX_VERSION}-py3-none-any.whl /tmp/geobrix-wheel/
# Hold every package the base image ships at its version; fail the build rather than drift.
# A package index, if one is needed, arrives as the BuildKit secret pip_index_url (never an ARG or ENV),
# so it is not recorded in the image history.
RUN --mount=type=secret,id=pip_index_url \
    if [ -s /run/secrets/pip_index_url ]; then export PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)"; fi && \
    /databricks/python3/bin/python -m pip list --format=freeze | sort > /tmp/base-constraints.txt && \
    /databricks/python3/bin/python -m pip install --no-cache-dir -c /tmp/base-constraints.txt \
      "geobrix[light_env6] @ file:///tmp/geobrix-wheel/geobrix-${GEOBRIX_VERSION}-py3-none-any.whl" && \
    /databricks/python3/bin/python -m pip check && \
    rm -rf /tmp/geobrix-wheel /tmp/base-constraints.txt
# Dedicated clusters load /databricks/jars; one GeoBrix JAR per cluster, so never also attach it as a library.
COPY geobrix-${GEOBRIX_VERSION}-jar-with-dependencies.jar /databricks/jars/
RUN chmod 0644 /databricks/jars/geobrix-${GEOBRIX_VERSION}-jar-with-dependencies.jar
