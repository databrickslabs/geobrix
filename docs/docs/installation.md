---
sidebar_position: 2
---

# Installation

GeoBrix currently offers heavy-weight, distributed APIs, primarily written in Scala for Spark with additional language bindings for PySpark and Spark SQL.

## Prerequisites

- Databricks Runtime (DBR) 17.x or later
- Classic Databricks cluster (not Serverless)
- GDAL native libraries

## Installation Steps

### 1. Download GeoBrix Artifacts

GeoBrix requires the following artifacts:

- **JAR file**: `geobrix-*-jar-with-dependencies.jar`
- **Shared Object**: `libgdalalljni.so` (GDAL native library)
- **Python Wheel**: `geobrix-*-py3-none-any.whl`

These are currently delivered via artifacts in the [beta-dist](https://github.com/databrickslabs/geobrix/tree/main/resources/beta-dist) directory.

### 2. Upload to Databricks Volume

1. Create or use an existing Databricks Volume
2. Upload the following files to your Volume (`*` for version):
   - `geobrix-*-jar-with-dependencies.jar`
   - `libgdalalljni.so`
   - `geobrix-*-py3-none-any.whl`

### 3. Create Init Script

GeoBrix requires GDAL natives, which are currently best installed via an init script on a classic cluster.

1. Copy the [geobrix-gdal-init.sh](https://github.com/databrickslabs/geobrix/blob/main/scripts/geobrix-gdal-init.sh) script
2. Modify the `VOL_DIR` variable to point to your Volume location where you uploaded the artifacts
3. Upload the modified init script to your Databricks Volume

Example init script:

```bash
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
```

### 4. Configure Cluster

#### Add Init Script

1. Go to your cluster configuration
2. Navigate to **Advanced Options** > **Init Scripts**
3. Add the init script path from your Volume

#### Add Libraries

1. In your cluster configuration, go to **Libraries**
2. Click **Install new**
3. Install the Python wheel:
   - Select **Upload** > **Python Whl**
   - Select the `dblabs_geobrix-0.1.0-py3-none-any.whl` file from your Volume, e.g. `VOL_DIR` location
4. Install the JAR:
   - The JAR is installed via the init script, so nothing further is needed

#### Cluster Configuration Example

```json
{
  "cluster_name": "geobrix-cluster",
  "spark_version": "17.3.x-scala2.13",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/volume_name/geobrix-gdal-init.sh"
      }
    }
  ]
}
```

### 5. Start the Cluster

Start or restart your cluster to ensure the init script runs and all libraries are loaded.

## Verification

To verify that GeoBrix is installed correctly:

### Python

```python
from databricks.labs.gbx.rasterx import functions as rx

# Register functions
rx.register(spark)

# List registered functions
spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_*'").show()
```

### SQL

```sql
-- List all GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';

-- Describe a specific function
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;
```

If you see the GeoBrix functions listed, your installation is successful!

## Troubleshooting

### GDAL Library Issues

If you encounter errors about missing GDAL libraries, please bear in mind that we currently use [UbuntuGIS](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable) PPA 
which from time-to-time might update GDAL version. If the major or minor version is changed, this will result in 
issues (requires us to recompile the JNI bindings):

1. Check that the init script ran successfully (check driver logs)
2. Verify the `libgdalalljni.so` file is in `/usr/lib` on the driver and executors
3. Add `/usr/lib` to `LD_LIBRARY_PATH` (uncommon)

### Function Registration Issues

If functions are not executing (uncommon):

1. Verify the JAR is in `VOL_DIR` used in the init script
2. Check that you've called the `.register(spark)` method for Spark SQL bindings
3. Restart the Python kernel or re-attach to cluster in the SQL notebook

### Permission Issues

If you encounter permission errors:

1. Ensure you have read access to the Volume
2. Check that the init script has execute permissions
3. Verify cluster policies allow init scripts

## Next Steps

- Follow the [Quick Start Guide](./quick-start.md) to begin using GeoBrix
- Explore the [Packages](./packages/overview.md) documentation
- Check out [Example Notebooks](./examples/overview.md)

