#!/bin/bash

sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
# - add ubuntugis PPA with GPG key
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update -y

# update to your actual volume path
VOL_DIR="/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix"

# install natives
# https://gdal.org/en/stable/api/python/python_bindings.html
# https://medium.com/@felipempfreelancer/install-gdal-for-python-on-ubuntu-24-04-9ed65dd39cac
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y libgdal-dev gdal-bin python3-gdal

# pip install GDAL (match deps to DBR17.3)
pip install --upgrade pip setuptools wheel cython
pip install wheel setuptools==74.0.0 numpy==2.1.3
export GDAL_CONFIG=/usr/bin/gdal-config
pip install --no-cache-dir --force-reinstall GDAL[numpy]=="$(gdal-config --version).*"

# copy JNI and JAR
cp $VOL_DIR/libgdalalljni.so /usr/lib/libgdalalljni.so
cp $VOL_DIR/geobrix-*-jar-with-dependencies.jar /databricks/jars