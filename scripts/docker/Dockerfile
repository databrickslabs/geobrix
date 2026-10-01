# --- GLOBAL ARGUMENTS ---
ARG BUILDPLATFORM=linux/amd64
ARG GDAL_VERSION=3.11.4
ARG PDAL_VERSION=2.8.2
# Pinned commit for PDAL ${PDAL_VERSION} (Labs lockdown policy: tags are mutable).
# To bump PDAL_VERSION, resolve the new tag with:
#   gh api repos/PDAL/PDAL/git/refs/tags/<version> --jq .object.sha
# and update PDAL_SHA in lockstep.
ARG PDAL_SHA=736fa0a66af4bed7105dff5fa152edf26bbb8a3a

# ==========================================
# STAGE 1: OS Base & Java
# e.g. `docker build --target base -t geobrix-base .`
# ==========================================
# Pinned by digest (Labs lockdown policy). To refresh, re-pull ubuntu:24.04 from
# Docker Hub and update the sha256 below. Tag comment retained for readability.
FROM --platform=${BUILDPLATFORM} ubuntu@sha256:c4a8d5503dfb2a3eb8ab5f807da5bc69a85730fb49b5cfca2330194ebcc41c7b AS base
# ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg && mkdir -p /usr/share/keyrings

# Azul Zulu signing key fingerprint (Labs lockdown policy: replace TOFU with
# explicit fingerprint pin). The downloaded key MUST present this fingerprint;
# any other key — including a different valid key on the same URL — fails the
# build. Cross-check on key bump:
#   gpg --show-keys --with-fingerprint --with-colons azul-repo.key
# Current fingerprint sourced from repos.azul.com 2026-05-14:
ARG AZUL_FPR=27BC0C8CB3D81623F59BDADCB1998361219BD9C9
RUN curl -fsSL https://repos.azul.com/azul-repo.key -o /tmp/azul-repo.key \
    && actual_fpr=$(gpg --show-keys --with-fingerprint --with-colons /tmp/azul-repo.key | awk -F: '/^fpr:/ {print $10; exit}') \
    && if [ "$actual_fpr" != "$AZUL_FPR" ]; then \
         echo "Azul key fingerprint mismatch: got=$actual_fpr expected=$AZUL_FPR" >&2; \
         exit 1; \
       fi \
    && gpg --dearmor < /tmp/azul-repo.key > /usr/share/keyrings/azul.gpg \
    && rm /tmp/azul-repo.key \
    && echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" > /etc/apt/sources.list.d/azul.list

RUN apt-get update && apt-get install -y zulu17-jdk --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Fix Java Pathing
RUN ln -s /usr/lib/jvm/zulu17-ca-amd64 /usr/lib/jvm/zulu-17
ENV JAVA_HOME=/usr/lib/jvm/zulu-17
ENV PATH=$JAVA_HOME/bin:$PATH

# ==========================================
# STAGE 2: System Dependencies
# e.g. `docker build --target system-deps -t geobrix-system-deps .`
# ==========================================
FROM base AS system-deps

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    software-properties-common && \
    add-apt-repository -y universe && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ant autoconf automake bash-completion build-essential cmake g++ git \
    libcurl4-gnutls-dev libgeos-dev libgeotiff-dev libhdf4-alt-dev libhdf5-dev \
    libjsoncpp-dev libnetcdf-dev libopenjp2-7-dev libpoppler-dev libpoppler-private-dev \
    libpq-dev libproj-dev libsnappy-dev libspatialite-dev libsqlite3-dev \
    libtiff-dev libtool libwebp-dev libxml2-dev ninja-build pkg-config \
    proj-bin python3-dev python3-pip python3-venv sqlite3 swig unixodbc wget && \
    rm -rf /var/lib/apt/lists/*

# Global Env Vars
ENV ROOTDIR=/usr/local
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV JUPYTER_ALLOW_INSECURE_WRITES=1
ENV CORES=2

WORKDIR $ROOTDIR/

# ==========================================
# STAGE 3: Hadoop Builder
# e.g. `docker build --target hadoop-builder -t geobrix-hadoop-builder .`
# ==========================================
FROM system-deps AS hadoop-builder
ENV HADOOP_VERSION=3.4.0
ENV HADOOP_HOME=/usr/local/hadoop
# Pinned SHA-512 of the Apache Hadoop 3.4.0 source tarball (Labs lockdown policy).
# Cross-checked against https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz.sha512
# To bump HADOOP_VERSION, update HADOOP_SHA512 in lockstep.
ENV HADOOP_SHA512=6f653c0109f97430047bd3677c50da7c8a2809d153b231794cf980b3208a6b4beff8ff1a03a01094299d459a3a37a3fe16731629987165d71f328657dbf2f24c

RUN mkdir -p $ROOTDIR/src \
    && wget -q -O /tmp/hadoop.tar.gz https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && echo "${HADOOP_SHA512}  /tmp/hadoop.tar.gz" | sha512sum -c - \
    && tar -xzf /tmp/hadoop.tar.gz -C $ROOTDIR/src/ \
    && rm /tmp/hadoop.tar.gz \
    && mv $ROOTDIR/src/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    ln -s $HADOOP_HOME/bin/hadoop /usr/local/bin/hadoop && \
    ln -s $HADOOP_HOME/bin/hdfs /usr/local/bin/hdfs && \
    ln -s $HADOOP_HOME/bin/yarn /usr/local/bin/yarn && \
    ln -s $HADOOP_HOME/bin/mapred /usr/local/bin/mapred && \
    echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# ==========================================
# STAGE 4: GDAL Builder (The Slowest Part)
# e.g. `docker build --target gdal-builder -t geobrix-gdal-builder .`
# ==========================================
FROM hadoop-builder AS gdal-builder
ARG GDAL_VERSION
# Pinned SHA-256 of the GDAL 3.11.4 source tarball (Labs lockdown policy).
# Upstream OSGeo only publishes MD5; this SHA-256 was computed locally after
# MD5-verifying the upstream download (md5: 9f4fa4b3be48fb60d5dd76fecb11a5f6).
# To bump GDAL_VERSION, update GDAL_SHA256 in lockstep.
ENV GDAL_SHA256=0fa36ee34d4451db586d2bf78ea0dbfa3b0dfae0516587f8130d21add0ac9dad
RUN wget -q -O /tmp/gdal.tar.gz https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && echo "${GDAL_SHA256}  /tmp/gdal.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/gdal.tar.gz -C $ROOTDIR/src/ \
    && rm /tmp/gdal.tar.gz

RUN mkdir -p src/gdal-${GDAL_VERSION}/build && cd src/gdal-${GDAL_VERSION}/build \
    && cmake -G Ninja .. \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DGDAL_USE_JAVA=ON -DJAVA_HOME="${JAVA_HOME}" \
        -DJNI_INCLUDE_DIRS="${JAVA_HOME}/include;${JAVA_HOME}/include/linux" \
        -DBUILD_JAVA_BINDINGS=ON -DBUILD_PYTHON_BINDINGS=OFF \
    && cmake --build . --parallel 8 && cmake --install . && ldconfig

# Redundant Symlinks as requested for project compatibility
RUN ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdal.so && \
    ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdal.so.30 && \
    ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so && \
    mkdir -p /usr/lib/jni && ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/jni/libgdalalljni.so.30

# ==========================================
# STAGE 5: PDAL Builder
# e.g. `docker build --target pdal-builder -t geobrix-pdal-builder .`
# ==========================================
FROM gdal-builder AS pdal-builder
ARG PDAL_VERSION
ARG PDAL_SHA
RUN mkdir -p /tmp/pdal && cd /tmp/pdal \
    && git init -q \
    && git remote add origin https://github.com/PDAL/PDAL.git \
    && git fetch --depth 1 -q origin ${PDAL_SHA} \
    && git checkout -q FETCH_HEAD \
    && mkdir -p build && cd build \
    && cmake -G Ninja .. \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DGDAL_CONFIG=/usr/local/bin/gdal-config \
        -DGDAL_INCLUDE_DIR=/usr/local/include \
        -DGDAL_LIBRARY=/usr/local/lib/libgdal.so \
        -DGEOTIFF_INCLUDE_DIR=/usr/include/geotiff \
        -DBUILD_PLUGIN_PYTHON=OFF -DBUILD_PLUGIN_GREYHOUND=OFF \
    && cmake --build . --parallel 2 && cmake --install . && ldconfig && \
    cd / && rm -rf /tmp/pdal

# ==========================================
# STAGE 6: Final Development Environment
# e.g. `docker build --target final -t geobrix-final .`
# ==========================================
FROM pdal-builder AS final

# --- Maven Installation (Pinned Checksum per Labs lockdown policy) ---
# Pinning the SHA-512 in-Dockerfile prevents a compromised archive.apache.org
# from serving both a bad tarball and a matching bad checksum. To bump
# MAVEN_VERSION, update MAVEN_SHA512 in lockstep.
ARG MAVEN_VERSION=3.9.9
ARG MAVEN_SHA512=a555254d6b53d267965a3404ecb14e53c3827c09c3b94b5678835887ab404556bfaf78dcfe03ba76fa2508649dca8531c74bca4d5846513522404d48e8c4ac8b
RUN mkdir -p $ROOTDIR/share/maven \
  && curl -fsSL -o /tmp/apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${MAVEN_SHA512}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C $ROOTDIR/share/maven --strip-components=1 \
  && ln -s $ROOTDIR/share/maven/bin/mvn $ROOTDIR/bin/mvn && rm /tmp/apache-maven.tar.gz

ENV MAVEN_HOME="$ROOTDIR/share/maven"
# Version pins — keep in sync with .github/actions/{scala,python}_build/action.yml,
# scripts/geobrix-gdal-init.sh, and notebooks/tests/requirements.txt.
# Run `gbx:versions:audit` after edits to confirm coverage.
# DBR-aligned values: https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
ENV SPARK_VERSION=4.0.0
ENV NUMPY_VERSION=2.1.3
ENV PANDAS_VERSION=2.2.3
ENV SETUPTOOLS_VERSION=74.0.0
ENV PIP_VERSION=25.0.1
ENV WHEEL_VERSION=0.45.1
ENV REQUESTS_VERSION=2.32.3
ENV IPYTHON_VERSION=8.30.0
ENV IPYKERNEL_VERSION=6.29.5
ENV IPYWIDGETS_VERSION=7.8.1
ENV JUPYTERLAB_VERSION=4.3.4
ENV JUPYTER_SERVER_VERSION=2.14.1
ENV JUPYTER_CORE_VERSION=5.7.2
ENV JUPYTER_CLIENT_VERSION=8.6.3
ENV TORNADO_VERSION=6.4.2
ENV NBFORMAT_VERSION=5.10.4
ENV NBCONVERT_VERSION=7.16.4
# Not in DBR; pinned to a stable PyPI version. build/pytest-cov also match CI.
# 1.5.0 (released 2026-04-30) is too new for some private PyPI mirrors that
# embargo recent releases; 1.4.4 is the broadly-available latest.
ENV BUILD_VERSION=1.4.4
ENV PYTEST_VERSION=8.4.2
ENV PYTEST_COV_VERSION=7.1.0
ENV GEOPANDAS_VERSION=1.1.3
# keplergl 0.3.4+ requires ipywidgets>=8.1.5, which conflicts with DBR's ipywidgets==7.8.1.
# 0.3.3 was never published on PyPI (gap between 0.3.2 stable and 0.3.4 alphas), so 0.3.2
# is the last keplergl compatible with the ipywidgets 7.x line. We prefer DBR alignment
# for ipywidgets over latest keplergl in the dev container.
ENV KEPLERGL_VERSION=0.3.2
# Intentional DBR divergence — DBR has pyzmq 26.2.0 but our Jupyter ZMQ workaround
# requires the older 25.1.1 (see "ZMQ fixes" section below). Re-evaluate on next bump.
ENV PYZMQ_VERSION=25.1.1

# --- Python Environment ---
#
# Pip index URL is injectable via --build-arg. Defaults to public PyPI;
# override to a private mirror if your build network requires one.
#
# Example:
#   docker build --build-arg PIP_INDEX_URL="$PIP_INDEX_URL" ...
# or set PIP_INDEX_URL in the host env before invoking scripts/docker/build_smart.sh.
# Default to public PyPI rather than empty string — an empty PIP_INDEX_URL
# env var causes pip to construct URLs with no host and fail with
# `Connection refused` against `/simple/<pkg>/` instead of falling back to
# the public index.
ARG PIP_INDEX_URL=https://pypi.org/simple/
RUN mkdir -p /etc/pip /root/.pip && \
    printf '[global]\ntimeout = 60\nindex-url = %s\n' "$PIP_INDEX_URL" \
      | tee /etc/pip.conf /root/.pip/pip.conf >/dev/null
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
# uv does NOT read pip.conf; it needs its own index env var. Mirror PIP_INDEX_URL
# into UV_INDEX_URL so `uv pip compile`/`uv pip install` use the same corp proxy pip
# does instead of hitting the blocked public pypi.org (Connection refused). Takes
# effect on the next image rebuild. CAVEAT for lock recompiles: the dev proxy
# (pypi-proxy.dev.databricks.com) is a ROLLING-RECENT mirror that prunes old releases,
# so it cannot reproduce runtime-matched locks (e.g. botocore 1.40.x). Authoritative
# recompiles must run against CI's full-retention JFrog mirror (db-pypi) — see the
# note in scripts/security/README.md; the dev proxy and public PyPI are both unsuitable.
ENV UV_INDEX_URL=${PIP_INDEX_URL}

# --- Hash-pinned Python install (Labs lockdown — supply chain) ---
#
# The full Python dependency closure for the dev container is locked with
# sha256 hashes in python/geobrix/requirements-dev-container.txt. pip refuses
# to install anything whose hash doesn't match. This protects against a
# compromised PyPI mirror substituting same-version-but-different-bytes
# wheels at docker-build time (the LiteLLM-class attack vector).
#
# Pinned versions in the .in source match the ENV block above (DBR 17.3 LTS
# aligned where applicable). GDAL is NOT in the lockfile — its Python wheel
# must build against the gdal-config from Stage 4 (source build).
#
# Regenerate via:
#   docker exec -it geobrix-dev bash -lc \
#     'cd /root/geobrix/python/geobrix && \
#      uv pip compile --generate-hashes --python-version 3.12 \
#         --output-file requirements-dev-container.txt requirements-dev-container.in'

# 1. Upgrade pip itself to a known version BEFORE consuming the hash-pinned
#    lockfile. Bootstrap is hash-pinned too (chicken-and-egg: a compromised
#    pip could otherwise ignore --require-hashes on the bulk install below).
#    --ignore-installed bypasses the Debian RECORD error. pip's --hash is only
#    valid for a requirement given via a file (newer bundled pip on a fresh
#    ubuntu:24.04 base rejects command-line --hash with "no such option:
#    --hash"), so materialize a one-line requirements file and install with
#    --require-hashes. Hashes match the pip==$PIP_VERSION entry in
#    requirements-dev-container.txt.
RUN printf 'pip==%s --hash=sha256:88f96547ea48b940a3a385494e181e29fb8637898f88d88737c5049780f196ea --hash=sha256:c46efd13b6aa8279f33f2864459c8ce587ea6a1a59ee20de055868d8f7688f7f\n' "$PIP_VERSION" > /tmp/pip-bootstrap.txt \
 && python3 -m pip install --upgrade --ignore-installed --break-system-packages \
        --require-hashes -r /tmp/pip-bootstrap.txt

# 2. Copy the lockfile (and its .in source for in-container regen if needed).
COPY --link python/geobrix/requirements-dev-container.in python/geobrix/requirements-dev-container.txt /tmp/

# 3. Install the full hash-pinned closure (~110 packages).
#    --ignore-installed: skip pip's pre-install uninstall pass. apt-installed
#    python3-wheel/python3-setuptools have no RECORD file so pip refuses to
#    uninstall them; without this flag pip aborts when an apt-managed package
#    version differs from the lockfile pin. Safe because --require-hashes
#    still cryptographically verifies every byte that lands on disk.
RUN pip3 install --require-hashes --ignore-installed -r /tmp/requirements-dev-container.txt --break-system-packages

# 4. Build the GDAL Python binding from sdist against the verified libgdal
#    headers from Stage 4.
#
#    Why this single package is not in the hash-pinned lockfile:
#    - The Python binding version is pinned by `gdal-config --version`, which
#      is whatever libgdal we source-built in Stage 4 (currently 3.11.4, and
#      that source tarball IS SHA-256-verified — see GDAL_SHA256 above). The
#      Python wheel/sdist on PyPI MUST match that native, so the version
#      string is dynamic in the same way GDAL_VERSION is.
#    - To bring this under --require-hashes we'd need a per-libgdal-version
#      hash for the matching PyPI sdist; OSGeo doesn't publish per-release
#      hashes for the PyPI sdist, and pinning them ourselves means another
#      file to bump in lockstep on every GDAL version bump.
#
#    Mitigation: --no-binary :all: forces pip to grab the source distribution
#    and compile it against our verified libgdal headers, rather than
#    accepting whatever pre-built manylinux wheel PyPI serves. A malicious
#    actor would have to ship a backdoored sdist (much harder than swapping
#    a wheel: the sdist build script runs `gdal-config` from our verified
#    Stage-4 install, and any byte that ships in libgdal.so came from the
#    verified source tarball). Residual risk: the sdist's Python wrapper
#    code itself is not byte-verified.
RUN pip3 install --no-binary :all: gdal==$(gdal-config --version) --break-system-packages

# ZMQ fixes
ENV ZMQ_TRANSPORT=tcp
# Add this to your main apt-get block
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    haveged \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

# Jupyter specific (final stage only: runtime dir + ipykernel registration).
# Bind to 0.0.0.0 so kernel ZMQ works reliably in Docker (no security
# requirement in dev). jupyterlab / jupyter_server / jupyter_core /
# jupyter_client / tornado / nbformat / nbconvert / testbook / pyzmq are all
# locked in requirements-dev-container.txt and were installed (and SHA-256
# verified) by the --require-hashes step above. No follow-up pip install
# here — adding one would re-resolve dependencies without hash enforcement
# and silently undo the lockfile's supply-chain guarantee.
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV JUPYTER_IP=0.0.0.0
ENV PYDEVD_DISABLE_FILE_VALIDATION=1
ENV JUPYTER_PLATFORM_DIRS=1
RUN mkdir -p /tmp/jupyter_runtime && chmod 1777 /tmp/jupyter_runtime
RUN python3 -m ipykernel install --name python3 --display-name "Python 3 (Geobrix)" --env JUPYTER_IP 0.0.0.0 --env JUPYTER_RUNTIME_DIR /tmp/jupyter_runtime

# Final refresh of the shared library cache
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/x86_64-linux-gnu
RUN ldconfig

WORKDIR /root/geobrix

# Copy the entrypoint script into the container
# this starts haveged (maybe more?)
COPY scripts/docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint to the custom script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Set the default command to /bin/bash, passed as an argument to the entrypoint script
CMD ["/bin/bash"]
