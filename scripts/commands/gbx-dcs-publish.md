# gbx:dcs:publish

Pushes the GeoBrix Databricks Container Services (DCS) image built by `gbx:dcs:build` to the registry. **Run by the release owner; never run by agents.** The command never runs `docker login` and never handles credentials: you must already be logged in to Docker Hub (`docker login`) as an account that can push to the repository. If a push fails with an authentication error, log in yourself and retry.

It publishes in two stages. `--candidate` pushes the `-rc` image so it can be verified on a dedicated cluster; `--release` re-tags the candidate image you pushed and verified (no rebuild) and pushes the release tags. Do not rebuild between `--candidate` and `--release`.

## Usage

```bash
bash scripts/commands/gbx-dcs-publish.sh (--candidate | --release) [--arch arm64|amd64] [--version <ver>] [--log <path>]
```

## Options

- `--candidate` - push `<repo>:<ver>-env6-<arm|x86>-rc`
- `--release` - re-tag the local `-rc` candidate as `<repo>:<ver>-env6-<arm|x86>` and `<repo>:env6-<arm|x86>`, then push both. Refuses when the `-rc` image has no `<repo>@sha256:...` digest (see Notes)
- `--arch arm64|amd64` - `arm64` (default; tags use `env6-arm`) or `amd64` (tags use `env6-x86`)
- `--version <ver>` - GeoBrix version. Default: parsed from the one `target/geobrix-<ver>-jar-with-dependencies.jar`; with none or more than one in `target/` the command refuses (`PRECONDITION MISSING ... pass --version <ver>`) rather than guess
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

## Environment

- `GBX_DCS_REPO` - image repository (default `mjohns797/geobrix`); must match the value used for `gbx:dcs:build`

## Examples

```bash
# Push the release candidate for on-cluster verification
bash scripts/commands/gbx-dcs-publish.sh --candidate

# After the candidate is verified, push the release tags
bash scripts/commands/gbx-dcs-publish.sh --release
```

## Notes

- The local `<repo>:<ver>-env6-<arm|x86>-rc` image from `gbx:dcs:build` must exist; otherwise the command prints `PRECONDITION MISSING: local image ...` and exits 1 without pushing.
- The last line is `DIGEST=<repo>@sha256:...`; if the pushed image has no such digest the command fails instead of printing an empty one. Record the release digest in the release notes.
- `--release` re-tags the candidate image you pushed and verified. A fresh `gbx:dcs:build` has no `<repo>@sha256:...` digest until it is pushed, so `--release` refuses an `-rc` image without one (`PRECONDITION MISSING ... push it as the candidate and verify it first`). If you rebuild after `--candidate`, push a new candidate and verify it again before `--release`.
