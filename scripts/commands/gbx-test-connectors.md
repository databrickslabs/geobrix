# gbx:test:connectors

Run the Lakeflow Connect connector tests (`connectors/lakeflow/`) in the isolated host venv `.venv-connectors/` (PySpark >= 4.1, which the community-connector framework requires). Dependencies re-sync only when their inputs change; the connector is reinstalled as a regular wheel every run (the layout pipelines use).

**Usage:** `bash scripts/commands/gbx-test-connectors.sh [--path <path>] [--resync] [--log <path>]`

**Options:**
- `--path <path>` — test path relative to `connectors/lakeflow/` (default `sources/geobrix/tests`)
- `--resync` — force re-install of the venv dependencies
- `--log <path>` — log file (default `test-logs/connectors-tests.log`)
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-test-connectors.sh`
- `bash scripts/commands/gbx-test-connectors.sh --path sources/geobrix/tests/test_connector.py --log conn.log`
