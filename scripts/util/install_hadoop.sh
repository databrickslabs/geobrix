#!/usr/bin/env bash
# Standalone Hadoop installer for hosts that mirror the Dockerfile setup.
# Not referenced by the build; kept as a manual helper. Checksum-pinned per
# Labs lockdown policy — update HADOOP_VERSION and HADOOP_SHA512 in lockstep.
set -euo pipefail

HADOOP_VERSION="${HADOOP_VERSION:-3.4.0}"
HADOOP_SHA512="${HADOOP_SHA512:-6f653c0109f97430047bd3677c50da7c8a2809d153b231794cf980b3208a6b4beff8ff1a03a01094299d459a3a37a3fe16731629987165d71f328657dbf2f24c}"

tarball="hadoop-${HADOOP_VERSION}.tar.gz"
wget -q "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${tarball}"
echo "${HADOOP_SHA512}  ${tarball}" | sha512sum -c -
tar -xzf "${tarball}"
mv "hadoop-${HADOOP_VERSION}" /usr/local/hadoop
cp /usr/local/hadoop/lib/native/*.so /usr/lib/
rm "${tarball}"
