# Run ScalaStyle (Scala Lint)

Runs ScalaStyle on `src/main/scala` using the same config as CI (`scalastyle-config.xml`). Use this to catch style warnings and errors before pushing.

## Usage

```bash
bash .cursor/commands/gbx-lint-scalastyle.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file (filename → test-logs/filename.log)
- `--help` - Display help

## Examples

```bash
# Run ScalaStyle (in Docker)
gbx:lint:scalastyle

# Run and save output
gbx:lint:scalastyle --log scalastyle.log
```

## Notes

- Runs inside Docker container `geobrix-dev`.
- Same rules as CI: `scalastyle-config.xml`; CI fails on **errors** only (`failOnWarning: false`).
- Fix common issues: trailing whitespace, missing newline at EOF, non-ASCII in comments (use ASCII equivalents: `->` not `→`, `-` not `–`, `...` not `…`).
