# Verify Genie Map

Checks that the Genie Map app runs and answers. It waits until the app is RUNNING on ACTIVE compute, then sends one GET to its root with the profile's OAuth token. A 2xx answer passes.

## Usage

```bash
bash scripts/commands/gbx-app-verify.sh --profile <name> [OPTIONS]
```

## Options

- `--profile <name>` – Databricks config profile of the app's workspace (required; never chosen for you).
- `--app <name>` – The app name (default: the bundle's, `genie-map`).
- `--deploy` – Run `gbx:app:deploy --profile <name>` first: build, `bundle deploy`, run, Genie re-seed.
- `--local-checks` – Run `pnpm lint` and `pnpm test --run` in `examples/apps/genie_map` first (host).
- `--path <path>` – The URL path to GET (default `/`).
- `--wait-min N` – How long to wait for RUNNING, in minutes (default 10).
- `--poll-secs N` – Poll interval in seconds (default 15).
- `--log <path>` – Write output to a log file (filename → `test-logs/<name>`).
- `--help`, `-h` – Show the help.

## Examples

```bash
# Lint, test, deploy, then check the app answers
bash scripts/commands/gbx-app-verify.sh --profile <profile> --local-checks --deploy --log app.log

# Check an app that is already deployed
bash scripts/commands/gbx-app-verify.sh --profile <profile>
```

## Limits

- CRASHED, a compute state of ERROR or STOPPED, or a FAILED active deployment fails at once.
- A redirect fails the check: it is the login page, so the token was not accepted.
- The app reads the vapor-eyes gold tables. Run `gbx:test:sdp --bundle vapor_eyes_lf` before it, so they hold rows.
- The runner is `examples/tests/verify_app.py`; its tests are `scripts/ci/test_gbx_app_verify.py`.
