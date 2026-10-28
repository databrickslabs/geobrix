# `resources/static/`

Committed binary assets that ship with GeoBrix releases. Reviewed at the commit
that adds or updates them; release workflows treat the bytes here as
authoritative and do no further rebuilding.

## `geobrix-gdal-platform-noble.tar.gz` (Git LFS)

The slowly-changing GDAL native install bundle: UbuntuGIS-PPA `.deb`s
(`libgdal36`, `libgdal-dev`, `gdal-bin`, `python3-gdal`, `libproj*`,
`libgeos*`, `proj-data`, `libspatialite`, `libnetcdf`, `libhdf5`, plus
transitive deps), the source-compiled GDAL Python wheel, the pip-toolchain
wheels (pip, setuptools, wheel, cython, numpy), and `libgdalalljni.so`.

The matching `geobrix-gdal-platform-noble.tar.gz.sha256` sidecar is
committed alongside.

This file is **the trust anchor for the cluster GDAL install** —
[`scripts/geobrix-gdal-init.sh`](../../scripts/geobrix-gdal-init.sh) verifies a
release-time repackage of these bytes (plus the per-release JAR) on every
cluster start.

### When to rebuild

- The pinned `GDAL_PPA_VERSION` in
  [`scripts/build-gdal-artifacts.sh`](../../scripts/build-gdal-artifacts.sh)
  changes.
- A new Ubuntu LTS becomes the DBR base image (e.g. noble → 26.04).
- A security advisory against one of the bundled libraries.

Otherwise leave it alone — it's reused across many GeoBrix releases.

### How to rebuild

The build is deliberately **local** (not CI) so the reviewer can reproduce
it byte-for-byte against the same Docker base image and confirm the
fingerprint check passed before the bytes are committed. From a host with
Docker:

```bash
# 1. Build the platform bundle in a fresh noble container.
#    --platform-only skips the per-release JAR step so the output is
#      reusable across many GeoBrix versions.
#    --out points UNDER the mounted /work tree so the tarball survives
#      the container exit.
#    --platform linux/amd64 is REQUIRED on Apple Silicon (M-series Macs)
#      so Docker emulates an x86_64 build host. The script refuses to run
#      on aarch64 because the resulting .debs would be ARM binaries that
#      can't install on the (amd64) Databricks cluster. Emulation makes
#      this build ~2-3x slower than native; budget 15-25 min on M-series
#      vs 5-10 min on an Intel/AMD Linux host. On native x86 hosts you
#      can drop the --platform flag.
mkdir -p dist
# If you're on the Databricks corp network, pypi.org is blocked — mount
# your host ~/.pip/pip.conf so the venv pip routes through
# the internal dev package proxy (or whatever mirror your .pip/pip.conf
# resolves to). On an unrestricted network you can drop the -v line.
docker run --rm --platform linux/amd64 \
    -v "$PWD":/work -w /work \
    -v "$HOME/.pip":/root/.pip:ro \
    ubuntu:24.04 bash -c '
        apt-get update && apt-get install -y sudo &&
        ./scripts/build-gdal-artifacts.sh \
            --jni scripts/gdal311/libgdalalljni.so \
            --out /work/dist/gdal-artifacts \
            --platform-only
    '
# Outputs (on the host, next to dist/gdal-artifacts/):
#   dist/geobrix-gdal-platform-noble.tar.gz
#   dist/geobrix-gdal-platform-noble.tar.gz.sha256

# 2. Move both files into resources/static/.
mv dist/geobrix-gdal-platform-noble.tar.gz        resources/static/
mv dist/geobrix-gdal-platform-noble.tar.gz.sha256 resources/static/

# 3. Open a PR. The reviewer:
#    - Reruns step 1 in their own container.
#    - Confirms `sha256sum resources/static/geobrix-gdal-platform-noble.tar.gz`
#      matches the rebuild and the committed sidecar.
#    - Eyeballs PACKAGES.txt inside the tarball for the libproj/libgeos/etc.
#      versions that just shifted.
```

<!--
The per-release static docs zip is NO LONGER committed here. It's built
fresh from docs/ during the release-cut workflow at
.github/workflows/package-geobrix-artifacts.yml (via `npm run build:static-zip`)
and attached directly to the release. A release IS the natural time to
cut a static docs snapshot, so committing the zip created a duplicated
artifact whose only purpose was to be re-attached.
-->

