# Run Function-Info Tests

Checks `DESCRIBE FUNCTION` for every function with a JVM level, inside the **geobrix-dev** Docker container.

## Usage

```bash
bash scripts/commands/gbx-test-function-info.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it
- `--help` - Display help

## What it runs

The command runs one integration row of `python/geobrix/test/manager/test_contracts.py`, through `gbx:test:python --with-integration`. For every manifest row with an L1 companion, it registers the function and runs `DESCRIBE FUNCTION EXTENDED <name>`. The output must show `name(<usage args>)` as the row gives it, with optional parameters in brackets.

It needs the staged JAR (see `gbx:test:python` for how the JAR is staged). Container locking and logs follow `gbx:test:python`.

`function-info.json` is generated from the manifest by `gbx:manifest:gen` (or `gbx:docs:function-info`, which runs the same generator in Docker). This command does not regenerate it.

## Examples

```bash
gbx:test:function-info
gbx:test:function-info --log function-info-tests.log
```
