# --- GLOBAL ARGUMENTS ---
ARG BUILDPLATFORM=linux/amd64
ARG GDAL_VERSION=3.11.4
ARG PDAL_VERSION=2.8.2

# ==========================================
# STAGE 1: OS Base & Java
# e.g. `docker build --target base -t geobrix-base .`
# ==========================================
FROM --platform=${BUILDPLATFORM} ubuntu:24.04 AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg && mkdir -p /usr/share/keyrings

RUN curl -fsSL https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg \
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

RUN mkdir -p $ROOTDIR/src && wget -qO- https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
    tar -xzC $ROOTDIR/src/ && \
    mv $ROOTDIR/src/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
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
RUN wget -qO- https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz | tar -xzC $ROOTDIR/src/

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
RUN git clone --depth 1 -b ${PDAL_VERSION} https://github.com/PDAL/PDAL.git /tmp/pdal \
    && cd /tmp/pdal && mkdir -p build && cd build \
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

# --- Maven Installation (With Dynamic Checksum) ---
ARG MAVEN_VERSION=3.9.9
RUN mkdir -p $ROOTDIR/share/maven \
  && curl -fsSL -o /tmp/apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && curl -fsSL -o /tmp/apache-maven.tar.gz.sha512 https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512 \
  && echo "$(cat /tmp/apache-maven.tar.gz.sha512)  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C $ROOTDIR/share/maven --strip-components=1 \
  && ln -s $ROOTDIR/share/maven/bin/mvn $ROOTDIR/bin/mvn && rm /tmp/apache-maven.tar.gz*

ENV MAVEN_HOME="$ROOTDIR/share/maven"
ENV SPARK_VERSION=4.0.0
ENV NUMPY_VERSION=2.1.3
ENV SETUPTOOLS_VERSION=74.0.0

# --- Python Environment ---
# 1. Upgrade pip using --ignore-installed to bypass the Debian RECORD error
RUN python3 -m pip install --upgrade pip --ignore-installed --break-system-packages

# 2. Install base tools and NumPy 2.x
RUN pip3 install wheel \
    setuptools==$SETUPTOOLS_VERSION \
    numpy==$NUMPY_VERSION \
    --ignore-installed \
    --break-system-packages

# 3. Build Python GDAL against your source headers
RUN pip3 install gdal==$(gdal-config --version) --break-system-packages

# Build GDAL Python bindings against the source headers built in Stage 4
RUN pip3 install gdal==$(gdal-config --version) --break-system-packages

# Final library installs (build = PyPA python3 -m build for sdist/wheel)
RUN pip3 install black isort build py4j requests keplergl ipython pyspark==$SPARK_VERSION --break-system-packages \
    && pip3 install pytest pytest-cov pytest-mock nest-asyncio pytest-asyncio --break-system-packages \
    && pip3 install geopandas pystac-client planetary-computer rasterio rioxarray --break-system-packages \
    && pip3 install pdal --break-system-packages

# ZMQ fixes
ENV ZMQ_TRANSPORT=tcp
# Add this to your main apt-get block
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    haveged \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

# Jupyter specific (final stage only: runtime dir and binding)
# Bind to 0.0.0.0 so kernel ZMQ works reliably in Docker (no security requirement in dev)
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV JUPYTER_IP=0.0.0.0
ENV PYDEVD_DISABLE_FILE_VALIDATION=1
ENV JUPYTER_PLATFORM_DIRS=1
RUN mkdir -p /tmp/jupyter_runtime && chmod 1777 /tmp/jupyter_runtime
RUN python3 -m ipykernel install --name python3 --display-name "Python 3 (Geobrix)" --env JUPYTER_IP 0.0.0.0 --env JUPYTER_RUNTIME_DIR /tmp/jupyter_runtime
RUN pip3 install --upgrade jupyterlab jupyter_server jupyter_core jupyter_client tornado nbformat nbconvert testbook --break-system-packages
RUN pip3 install --no-cache-dir --break-system-packages pyzmq==25.1.1

# Final refresh of the shared library cache
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/x86_64-linux-gnu
RUN ldconfig

WORKDIR /root/geobrix

# Copy the entrypoint script into the container
# this starts haveged (maybe more?)
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint to the custom script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Set the default command to /bin/bash, passed as an argument to the entrypoint script
CMD ["/bin/bash"]
