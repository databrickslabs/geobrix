# syntax=docker/dockerfile:1
# Build container for GeoBrix natives on manylinux_2_28. Every version arrives as a --build-arg from
# scripts/native/pins.conf; every source tarball, wheel and tool binary is pre-fetched on the host into the
# context (scripts/native/prefetch.sh), so the image build needs no package index and no source mirror.
# Two things are still downloaded while the image builds, both from public non-index hosts:
#   - the JDK and a few build packages, by dnf from the base image's distribution repositories;
#   - the Rust toolchain (rustc, cargo, std, clippy, rustfmt), by `rustup toolchain install` from
#     static.rust-lang.org; rustup checks the components against a manifest it fetches itself.
# Nothing is downloaded when the image runs: Rust crates build with --network none from vendored sources.
ARG MANYLINUX_IMAGE
FROM ${MANYLINUX_IMAGE} AS toolchain
ARG GBX_ARCH
ARG SWIG_FILE
ARG ANT_VERSION
ARG ANT_FILE
ARG ANT_SHA512
ARG RUSTUP_INIT_SHA256
ARG RUST_TOOLCHAIN
ARG CARGO_DENY_SHA256
ENV PY=/opt/python/cp312-cp312/bin/python
RUN dnf -y install java-17-openjdk-devel perl-IPC-Cmd perl-Time-Piece zip unzip which && dnf clean all \
 && rpm -q java-17-openjdk-devel > /opt/jdk-version.txt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
COPY wheelhouse/${GBX_ARCH}/ /wheelhouse/
# Only what the toolchain itself reads: an edit to any other script in scripts/native/ must not rebuild this stage
# (SWIG, Rust) or anything built on it (deps, gdal).
COPY recipes/build-requirements-${GBX_ARCH}.txt recipes/cargo-offline.toml /recipes/
RUN $PY -m pip install --no-index --find-links /wheelhouse --require-hashes -r /recipes/build-requirements-${GBX_ARCH}.txt \
 && for t in cmake ninja maturin auditwheel patchelf; do ln -sf /opt/python/cp312-cp312/bin/$t /usr/local/bin/$t; done
COPY src/${SWIG_FILE} src/${ANT_FILE} /src/
RUN mkdir -p /tmp/swig && tar -xf /src/${SWIG_FILE} -C /tmp/swig --strip-components=1 && cd /tmp/swig \
 && ./configure --prefix=/opt/swig --without-pcre && make -j"$(nproc)" && make install && cd / && rm -rf /tmp/swig \
 && echo "${ANT_SHA512}  /src/${ANT_FILE}" | sha512sum -c - && tar -xf /src/${ANT_FILE} -C /opt && ln -s /opt/apache-ant-${ANT_VERSION} /opt/ant
ENV PATH=/opt/swig/bin:/opt/ant/bin:/opt/rust/cargo/bin:$PATH RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo
# Keep cargo's target/ out of a crate that is mounted at /src (override with -e for a cache volume).
ENV CARGO_TARGET_DIR=/tmp/target
COPY bin/${GBX_ARCH}/ /tmp/bin/
COPY rust-toolchain.toml /opt/rust/rust-toolchain.toml
RUN echo "${RUSTUP_INIT_SHA256}  /tmp/bin/rustup-init" | sha256sum -c - && chmod +x /tmp/bin/rustup-init \
 && /tmp/bin/rustup-init -y --no-modify-path --profile minimal --default-toolchain none \
 && cd /opt/rust && rustup toolchain install && rustup default "${RUST_TOOLCHAIN}" && rustc --version | grep -q " ${RUST_TOOLCHAIN} " \
 && echo "${CARGO_DENY_SHA256}  /tmp/bin/cargo-deny.tar.gz" | sha256sum -c - \
 && tar -xzf /tmp/bin/cargo-deny.tar.gz -C /tmp/bin && find /tmp/bin -name cargo-deny -type f -exec cp {} /opt/rust/cargo/bin/ \; && rm -rf /tmp/bin

# What the toolchain image holds, one line each, written out by `--target export-toolchain`.
FROM toolchain AS manifest-toolchain
RUN { echo "arch $(uname -m)"; grep PRETTY_NAME /etc/os-release; echo "glibc $(ldd --version | head -1)"; \
      $PY --version; $PY -m pip freeze; echo "$(rpm -q java-17-openjdk-devel)"; javac -version 2>&1; \
      swig -version | grep -i "SWIG Version"; ant -version; rustup --version 2>&1 | head -1; rustc --version; cargo --version; \
      rustup target list --installed; cargo-deny --version; } > /toolchain-manifest.txt

FROM scratch AS export-toolchain
COPY --from=manifest-toolchain /toolchain-manifest.txt /

# ---------- deps: every library GDAL is linked against, built from the pinned sources into /opt/gbx-native ----------
# The sources are bind-mounted from the build context, so they never enter a layer, and every unpacked source tree and
# build directory is in RAM (a tmpfs mount at /tmp/gbx-build), so a layer holds only what was installed. One RUN per
# group keeps a failure late in the list from redoing the groups before it. Versions come from /recipes/pins.conf, the
# same file the image tag hashes.
FROM toolchain AS deps
COPY recipes/pins.conf recipes/build-lib.sh recipes/build-deps.sh /recipes/
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-deps.sh compress
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-deps.sh raster
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-deps.sh net
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-deps.sh science
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-deps.sh geo

# ---------- gdal: GDAL with its tools, the JNI library, the Java classes and osgeo ----------
FROM deps AS gdal
COPY recipes/build-gdal.sh /recipes/
RUN --mount=type=bind,source=src,target=/src --mount=type=tmpfs,target=/tmp/gbx-build,size=6442450944 bash /recipes/build-gdal.sh

# ---------- stage-native: the relocatable gdal-native/ tree (written to /stage/gdal-native) ----------
FROM gdal AS stage-native
COPY recipes/stage-payload.sh recipes/set-rpath.py recipes/rename-sonames.py /recipes/
RUN bash /recipes/stage-payload.sh

FROM scratch AS export-stage-native
COPY --from=stage-native /stage/gdal-native /gdal-native
