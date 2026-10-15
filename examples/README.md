# GeoBrix Examples

Runnable examples for [GeoBrix](https://databrickslabs.github.io/geobrix/), organized to mirror the docs: example notebook series and Databricks Apps, plus the sample data and test harness shared across every example kind.

| Folder | What's inside | Docs |
|---|---|---|
| [`notebooks/`](notebooks/) | End-to-end example notebook series (Earth Observation, Wireless Coverage, Helios, Vapor Eyes, Orthomosaic, and more), each in its own subdirectory with a `README.md`. | [Examples overview](https://databrickslabs.github.io/geobrix/docs/examples/overview) |
| [`apps/`](apps/) | Databricks Apps examples, such as [`genie_map`](apps/genie_map/) — a kepler.gl map that answers natural-language questions over spatial gold tables through Genie. | [Genie Map](https://databrickslabs.github.io/geobrix/docs/examples/genie-map) |
| [`sample-data/`](sample-data/) | The `setup_sample_data.ipynb` notebook, which stages the GeoBrix sample-data bundles into a Unity Catalog Volume. | [Sample Data setup](https://databrickslabs.github.io/geobrix/docs/sample-data/setup) |
| [`tests/`](tests/) | The harness that validates and runs examples — locally in the GeoBrix dev container and on Databricks (classic clusters and Serverless). | [`tests/README.md`](tests/README.md) |

`sample-data/` and `tests/` sit alongside `notebooks/` and `apps/` rather than inside either, because they serve all example kinds.
