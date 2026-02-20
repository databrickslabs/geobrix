For tests to run in docker we need to copy the jar to site-packages/pyspark/jars.
Example: /root/geobrix/python/dev/lib/python3.12/site-packages/pyspark/jars/
spark.addArtifact does not work due to permissions issues.
Also there could be annoying warnings about jupyter_client which isn't something relevant to the pytest we do.
If those warnings occur use:  export JUPYTER_PLATFORM_DIRS=1

## Ports and binding

- The image sets **JUPYTER_IP=0.0.0.0** so the kernel ZMQ binds to all interfaces and works reliably in Docker (for **testbook** and notebook tests). No security requirement in this dev image.
- The container exposes **5005** (JDWP), **8888**, **4040** (Spark UI). Jupyter Lab is installed but not required; the goal is that **testbook** can start and talk to the kernel.

## Optional: privileged mode

If you hit kernel or ZMQ issues (e.g. entropy or device access), you can try running the container with `--privileged`. It is not required for normal use.
  ```bash
  docker run ... --privileged ...
  ```

> See scripts in the 'extras' dir for more.