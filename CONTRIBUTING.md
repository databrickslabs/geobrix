We happily welcome contributions to GeoBrix. We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

**Scala code:** We follow the [Scala style guide](https://docs.scala-lang.org/style/) (Scalaguide). Style is enforced with [Scalastyle](scalastyle-config.xml); see [docs/scala-style-guide.md](docs/scala-style-guide.md). Run `gbx:lint:scalastyle` before pushing.

## Local development

GeoBrix is built to run on Databricks. The local loop below supports building it for Databricks.

- **Host virtual environment.** An editable install of `python/geobrix` is the fast inner loop. It has no env-wheel `osgeo`, so the functions that run only on the GDAL spec engine are not registered there, and their tests skip. Today these are `gbx_rst_slope`, `gbx_rst_mapalgebra` and `gbx_rst_transformcrs`; more will follow as functions convert. To list them, read `python/geobrix/src/databricks/labs/gbx/manifest/functions.json` for rows that have a `gdal` block and an engine-only `L3` level (`levels.engine_only` in `manager/levels.py`). The engine runs only when `engine.run.unavailable_reason()` returns `None`: `osgeo` is rasterio's GDAL, at the pin in the manifest.
- **Where to run them.** Use the dev container (`gbx:docker:start`), or run on Databricks with the env wheel. The container image (`scripts/docker/Dockerfile.gbxnative`) carries the env6 wheel's native payload, the same bytes Databricks runs.
- **CI.** The lightweight test shards also run on that payload (the `light-payload` job in `.github/workflows/build_main.yml`).
