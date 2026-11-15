# Local GitHub Actions dry-run (`act`)

Validate `.github/workflows/*.yml` changes locally before pushing. Catches ~80% of CI bugs (typos, missing env, action SHA pin issues, step ordering, composite-action structure errors).

## What's here

| File | Purpose |
|---|---|
| `Dockerfile.gha-runner` | Runner image: `catthehacker/ubuntu:runner-24.04` digest-pinned. Pip/Maven/npm registry URLs are build-arg injected (defaults: public registries). Tagged `geobrix-ci-runner:local`. |
| `pip.conf` | Pip config skeleton baked into the runner image. `index-url` injected at build time from `$PIP_INDEX_URL`. |
| `maven-settings.xml` | Maven `<settings>` skeleton baked into the runner image. `<mirror>` injected at build time from `$MAVEN_MIRROR_URL`. |
| `npmrc` | npm registry config skeleton baked into the runner image; also covers yarn classic + pnpm. `registry=` injected at build time from `$NPM_REGISTRY_URL`. Used by `deploy-docs.yml`'s `npm ci`. |
| `jfrog-auth-stub/action.yml` | No-op composite action. Copied by `run-act.sh` into a sibling workspace mirror (`.cache/act-workspace/`) where it overlays the real one — see "How real `.github/` stays untouched" below. |
| `run-act.sh` | Pre-flight checks + image build + workspace-mirror prep + `act` invocation. |

The image bakes the Maven version the wrapper pins (`.mvn/wrapper/maven-wrapper.properties`), and `run-act.sh` writes an `./mvnw` into the mirror that runs it: real CI downloads the pinned Maven through JFrog, which this image cannot. An image built before that change has apt's older Maven and fails the pom's version check: `docker rmi geobrix-ci-runner:local`, then re-run. (`gbx:ci:act` only builds the image when it is missing, so it does not notice an old one.)

## Quick start

```bash
# One-time install
brew install act

# Use via the Cursor command (recommended)
gbx:ci:act -l                                          # list jobs
gbx:ci:act -W .github/workflows/build_main.yml -j build  # run one job
gbx:ci:act push                                        # simulate a push event

# Or directly
bash scripts/ci-local/run-act.sh -l
```

First run builds `geobrix-ci-runner:local` (~5 min). Subsequent runs reuse the image.

## How real `.github/` stays untouched

`act` parses workflow + composite-action YAML on the **host filesystem** (before any container starts), so a Docker-only overlay isn't enough. Instead we materialize a mirror at `.cache/act-workspace/` (gitignored):

```
PROJECT_ROOT/                       # real, never modified
├── .github/                        # CI uses this — untouched
└── ...

.cache/act-workspace/               # gitignored mirror (rebuilt on every run)
├── .github/                        # FRESH COPY of real .github/ ...
│   └── actions/
│       └── jfrog-auth/
│           └── action.yml          # ... with the stub overlaid here
├── pom.xml -> ../../pom.xml        # symlink
├── src/    -> ../../src/           # symlink
├── scripts/ -> ../../scripts/      # symlink
├── .git    -> ../../.git           # symlink (so git-aware actions work)
└── ...                             # all other top-level entries symlinked
```

`run-act.sh` `cd`s into the mirror, then invokes `act --bind`, so the bind-mounted workspace is the mirror. The stub composite emits a `::notice::` and exits; pip/Maven still find whatever registry was injected at image build time (a private mirror if you set `PIP_INDEX_URL` etc. before building, public registries otherwise).

The mirror is regenerated from scratch on every run — fast (~50 ms) because `.github/` is small (~100 KB) and everything else is symlinked.

## Deliberate local-act variations from CI

The mirror's composite actions are patched in two ways for local-act runs only. Both are act-environment workarounds, NOT changes to CI behavior:

| What | Why | Visible at runtime |
|---|---|---|
| `${{ matrix.X }}` → literal value | act doesn't propagate matrix context into composite-action `run:` blocks (nektos/act#2206-class issue). Pin values come from `gbx:versions:audit`. | `🔧 Rewriting matrix refs in mirror's composite actions...` |

The runner image also pre-installs `gdal[numpy]==3.11.4` into the tool-cache Python so the workflow's `pip install --no-build-isolation gdal[numpy]==X` step becomes a no-op. Reason: GDAL 3.11.4's `pyproject.toml` declares both `[project.license].file` and `[project.license].text`, which modern setuptools rejects (`invalid pyproject.toml config: project.license`). pip's "already satisfied" path skips the pyproject.toml validation entirely. See `Dockerfile.gha-runner`.

Both edits live only in `.cache/act-workspace/.github/actions/`. The real `.github/actions/{scala,python}_build/action.yml` files on disk are unchanged.

If a workflow change is suspected to be the source of a CI failure, check `gbx:versions:audit` and the run-act.sh patch list to make sure the variation isn't masking the real issue.

## What's covered

- Workflow YAML syntax + action SHA pin validity
- Step ordering (e.g. would `setup-python` actually be available before our pip step?)
- Composite action structure (`scala_build`, `python_build`)
- pip + Maven installs (route through whichever registry was build-arg-injected)
- Most matrix expansions
- Conditional `if:` evaluation

## What's NOT covered

- **JFrog OIDC**: no real GitHub OIDC issuer locally — the real `jfrog-auth` is bind-mounted out
- **`runs-on: { group: larger-runners, labels: larger }`**: treated as a label alias only — no actual "larger" machine; you get whatever Docker resources you allocate
- **Real GitHub event payloads**: `act` mocks `head_sha`, `head_ref`, etc. with placeholder values
- **Secrets**: `CODECOV_TOKEN`, `REPO_ACCESS_TOKEN` aren't set; workflow falls back to `GITHUB_TOKEN` via the `||` pattern, and codecov-action has `fail_ci_if_error: false` so it warns instead
- **Runner-group access policy**: org-level allowlist isn't simulated; if a workflow is gated by a protected env, that gating isn't replayed locally

## Iteration loop

```bash
# Edit a workflow…
vim .github/workflows/build_main.yml

# …and dry-run it
gbx:ci:act -W .github/workflows/build_main.yml -j build --pull=false

# When clean, push and let real CI take over (which exercises JFrog OIDC + larger runners)
git push
```

## Architecture (Apple Silicon caveat)

Real GitHub-hosted runners are **linux/amd64**. We build the runner image and
run `act` with `--container-architecture linux/amd64` to match. On Apple
Silicon, Docker Desktop emulates amd64 via Rosetta (≈1.5–2× slowdown).

Why not native arm64 on Apple Silicon? Ubuntu's `archive.ubuntu.com` only
ships **amd64** packages — arm64 lives on `ports.ubuntu.com` via a different
path. Native-arm64 runs 404 on every `apt-get install` because the workflow
sources.list points at `archive.ubuntu.com`. Forcing amd64 is the simplest
fix that also keeps us workflow-faithful (real CI is amd64 too).

## Maintenance

- **If catthehacker bumps `ubuntu:full-24.04`**, rebuild: `docker rmi geobrix-ci-runner:local && bash scripts/ci-local/run-act.sh -l`.
- **If proxy URLs change**, update the host env vars (`PIP_INDEX_URL` / `MAVEN_MIRROR_URL` / `NPM_REGISTRY_URL`) and rebuild the runner image — `docker rmi geobrix-ci-runner:local && bash scripts/ci-local/run-act.sh -l`.
- **If a new composite action also needs OIDC and act-stubbing**, add a sibling stub directory and extend `run-act.sh` with another `--container-options` mount.

## See also

- [act docs](https://nektosact.com/) — full `act` command reference
- `gbx:ci:act` / `bash scripts/ci-local/run-act.sh` — entry points for local GHA dry-runs
