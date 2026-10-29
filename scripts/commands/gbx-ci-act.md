# Run GitHub Actions locally with act

Validate `.github/workflows/*.yml` changes before pushing by running them locally via [act](https://github.com/nektos/act). Catches ~80% of CI bugs (typos, missing env, action SHA pin issues, step ordering, composite-action structure errors) without a push-and-iterate loop.

## Usage

```bash
bash scripts/commands/gbx-ci-act.sh [act-arguments...]
```

## Examples

```bash
# List jobs across all workflows
gbx:ci:act -l

# Run one job from one workflow
gbx:ci:act -W .github/workflows/build_main.yml -j build

# Simulate a push event (runs all workflows triggered on push)
gbx:ci:act push

# Simulate a PR event
gbx:ci:act pull_request

# Pass any other arg through to act
gbx:ci:act --help
```

## First-time setup

```bash
brew install act
```

The first invocation builds `geobrix-ci-runner:local` (~5 min). Subsequent runs reuse the image.

The image bakes the Maven version the repo pins (`.mvn/wrapper/maven-wrapper.properties`). The command only builds the image when it is missing, so after that version changes an old image fails the pom's Maven check: `docker rmi geobrix-ci-runner:local`, then run again.

## What's wired

- **Runner image**: `catthehacker/ubuntu:runner-24.04` (digest-pinned). pip/Maven/npm registry URLs are build-arg injected from the host env (`PIP_INDEX_URL`, `MAVEN_MIRROR_URL`, `NPM_REGISTRY_URL`); set them to a private proxy if your network requires it, otherwise leave unset to use public registries.
- **Platform map**: `ubuntu-latest`, `ubuntu-24.04`, `ubuntu-22.04`, `larger`, `linux-ubuntu-latest` → `geobrix-ci-runner:local`
- **Workspace mirror**: the project is rsynced (hardlinks) into `.cache/act-workspace/`. Heavy, derived and untracked bulk data is left out; `sample-data/Volumes/` is excluded except for its **git-tracked** files (fixtures real CI checks out, such as `proj-grids/synthetic.gsb`), which are mirrored. The full `sample-data/Volumes` is still bind-mounted at `/Volumes`.
- **JFrog auth stub**: Real `.github/actions/jfrog-auth/action.yml` is bind-mounted over inside the act container with a no-op stub. Real `.github/` on disk is never modified.

## Coverage

| Catches | Doesn't catch |
|---|---|
| YAML syntax errors | JFrog OIDC token exchange (mocked) |
| Action SHA pin issues | `larger-runners` actually being larger |
| Step ordering | Real GitHub event payloads (head_sha, head_ref) |
| Composite action structure | Real secrets (CODECOV_TOKEN, REPO_ACCESS_TOKEN) |
| pip/Maven/npm install correctness | Org-level runner-group access policy |
| Matrix expansion | Protected env / branch protection gating |
| Conditional `if:` evaluation | |

For the gaps, push to a draft PR and let real CI exercise them.

## Notes

- The runner image is roughly 2 GB; cached locally after first build
- `act` reuses the image across runs; rebuild via `docker rmi geobrix-ci-runner:local && gbx:ci:act -l`
- Full mechanics in `scripts/ci-local/README.md`
