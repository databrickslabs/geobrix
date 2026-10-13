For tests to run in docker we need to copy the jar to site-packages/pyspark/jars.
Example: /root/geobrix/python/dev/lib/python3.12/site-packages/pyspark/jars/
spark.addArtifact does not work due to permissions issues.
Also there could be annoying warnings about jupyter_client which isn't something relevant to the pytest we do.
If those warnings occur use:  export JUPYTER_PLATFORM_DIRS=1

## Ports and binding

- The image sets **JUPYTER_IP=0.0.0.0** so the kernel ZMQ binds to all interfaces and works reliably in Docker (for **testbook** and notebook tests). No security requirement in this dev image.
- The container exposes **5005** (JDWP remote debug), **8888** (Jupyter's default port, for an ad-hoc `jupyter` server), **4040** (Spark UI). JDWP is opt-in: no `JAVA_TOOL_OPTIONS` is set by default; debug a test run with `mvn test -DagentLib='${jdwp.agent}' ...` and attach to `localhost:5005` (see the comment in `start_docker.sh` to attach the agent to every JVM instead). Jupyter Lab is installed only as a transitive dependency of `ipywidgets` 7 and is not otherwise used; the goal is that **testbook** can start and talk to the kernel (`ipykernel`). Example-specific packages (e.g. `keplergl`) are installed per example when needed, not baked into the image.

## Optional: privileged mode

If you hit kernel or ZMQ issues (e.g. entropy or device access), you can try running the container with `--privileged`. It is not required for normal use.
  ```bash
  docker run ... --privileged ...
  ```

> See scripts in the 'extras' dir for more.