# scripts/security/

Tooling that implements the Databricks Labs repository lockdown policy for
GeoBrix — supply-chain hardening around third-party GitHub Actions and Maven
dependencies. Two pinning regimes:

1. **GitHub Actions** — every third-party Action must be pinned to a full
   commit SHA taken from a release published before the
   `2026-03-10T00:00:00Z` cutoff. Tag names are preserved as inline
   comments for human-readable cross-reference; the comment is **not**
   authoritative — reviewers verify the SHA against the referenced
   release.

2. **Maven dependencies** — every dependency, transitive dependency,
   plugin, and plugin dependency must be signed by a PGP key whose
   fingerprint appears in `.maven-keys.list` at the repo root. Strict
   verification is implemented by `pgpverify-maven-plugin` under the
   `verify-pgp` Maven profile (see `pom.xml`).

   **Current state (Beta 0.3.0):** the profile is opt-in (`-Pverify-pgp`)
   and the keysmap is empty. The dedicated `Verify Maven dependency PGP
   signatures` workflow runs on `pom.xml` / `.maven-keys.list` changes,
   but it does NOT yet gate the test/build jobs.

   **Path to gating every build (deliberate follow-up, not this PR):**
   1. Run `maven-pgp-bootstrap` and add `noSig` sentinels for the
      ~20 known-legacy unsigned Maven Central artifacts
      (junit 3.8.1, dom4j 1.1, classworlds 1.1-alpha-2, etc.) the
      bootstrap surfaces. These pre-date broad PGP signing.
   2. Re-run the bootstrap, take the resulting fingerprints, and
      cross-check each against the project's published trust anchor
      (Apache KEYS file, GitHub release page, etc.). Trust-anchor URLs
      for direct deps are listed at the top of `.maven-keys.list`.
   3. Commit the reviewed keysmap.
   4. Flip the `verify-pgp` profile to `<activeByDefault>true</activeByDefault>`
      in `pom.xml`. Every subsequent build (including `scala_build`,
      `python_build`, the per-package shards, etc.) will pgp-verify its
      Maven closure before any test or compile step runs — satisfying
      "verify before use".
   5. Add the workflow to required status checks; the dedicated
      verify-maven-pgp.yml becomes redundant once every build gates,
      and can be deleted or kept as a fast-feedback signal.

## Scripts

| Script | Requires | Purpose |
|---|---|---|
| `list-external-actions` | `yq` (Mike Farah) | Emit the set of external actions referenced by any workflow or composite action under `.github/`, one per line. |
| `resolve-action-ref` | `gh`, `jq` | For each `action[@ref]`, resolve the most recent pre-cutoff release tag to the commit SHA it points at. Marks already-pinned entries with `✓` and drift with `⚠`. |
| `pin-gh-actions` | `git` | Consume `resolve-action-ref` output, rewrite every `uses:` line under `.github/` to the new SHA form (skipping `databricks*`-owned actions), and stage the result with `git add`. Prints the staged diff for review — **does not commit**. |
| `maven-pgp-bootstrap` | `mvn`, `awk` | Run `pgpverify-maven-plugin` with relaxed settings, capture every PGP fingerprint Maven Central serves for the resolved closure, and emit draft `.maven-keys.list` entries on stdout. The output is a draft — every fingerprint must be cross-checked against the project's published signing key before being committed. |
| `maven-pgp-verify` | `mvn` | Run strict verification (`mvn -Pverify-pgp verify`). Exits non-zero if any artifact is unsigned, weakly signed, or signed by a key not in `.maven-keys.list`. |

## Typical flow — GitHub Actions

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Preview what would change
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref

# 2. Apply (stages under .github/)
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref \
  | ./scripts/security/pin-gh-actions

# 3. Review, then commit
git diff --cached -- .github
git commit -m "Re-pin GitHub Actions to commits from releases prior to 2026-03-10"
```

## Typical flow — Maven PGP keysmap

Run inside the `geobrix-dev` container so Maven hits the `db-maven-proxy`
mirror (the proxy must pass `.asc` files through unmodified — confirm
once before relying on this).

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Generate a draft keysmap from the current resolved closure.
./scripts/security/maven-pgp-bootstrap > /tmp/draft.list

# 2. Cross-check every fingerprint in /tmp/draft.list against the
#    project's published signing key. Trust-anchor URLs for the direct
#    deps in pom.xml are listed at the top of .maven-keys.list. Do NOT
#    skip this step — the entire trust model rests on it.

# 3. Replace the TODO block in .maven-keys.list with the reviewed
#    entries.

# 4. Confirm strict verification passes.
./scripts/security/maven-pgp-verify

# 5. Commit. Once the workflow is green on main, flip the verify-pgp
#    profile in pom.xml to <activeByDefault>true</activeByDefault> and
#    add the "Verify Maven dependency PGP signatures" check to branch
#    protection's required status checks.
git diff -- pom.xml .maven-keys.list
git commit -m "Populate Maven PGP keysmap from reviewed signatures"
```

## Notes

- **`databricks*` / `databrickslabs*` actions are skipped.** They are
  considered first-party by the policy and do not require pinning; they
  remain on tag references.
- **Mono-repo tag prefixes.** `resolve-action-ref` handles actions under a
  mono-repo path (e.g. `databrickslabs/sandbox/acceptance` → tags like
  `acceptance/v0.4.4`). Review the `⚠` output before applying — the doc
  flags this as a known glitch.
- **`pin-gh-actions` does not switch branches.** Unlike the reference
  implementation at `databrickslabs/blueprint`, this script assumes the
  caller has already checked out the target branch.
- **Comment is informational only.** A reviewer verifying this PR must
  re-run `resolve-action-ref` (or an equivalent `gh api` lookup) to
  confirm every SHA corresponds to the claimed tag.

## Refresh cadence

**GitHub Actions:** the cutoff date is a constant inside
`resolve-action-ref` and `pin-gh-actions`. It will only change when the
policy is updated by the Databricks Labs team, at which point both
scripts should be updated in lockstep.

**Maven keysmap:** re-run `maven-pgp-bootstrap` whenever a Dependabot PR
bumps a Maven dep (or any direct dep is added/removed in `pom.xml`). The
"Verify Maven dependency PGP signatures" workflow runs automatically on
PRs touching `pom.xml` or `.maven-keys.list`, so drift surfaces as a
failing required check.

## Python lockfile (`python/geobrix/requirements-ci.txt`)

CI's Python dependency closure is locked with sha256 hashes via
`uv pip compile --generate-hashes`. The lockfile is consumed by both
`scala_build/action.yml` and `python_build/action.yml` as
`pip install --require-hashes -r python/geobrix/requirements-ci.txt`,
so pip refuses to install any dep whose hash doesn't match what was
recorded at lock time. This protects against a compromised mirror
substituting a same-version-but-different-bytes wheel.

GDAL is the one exception — its Python wheel must match the system's
apt-installed native version, which is dynamic per CI runner. It's
installed separately with `pip install gdal[numpy]==<detected>`.

### Regenerating the lockfile

Update `python/geobrix/requirements-ci.in` (the human-edited source),
then regenerate from inside the dev container:

```sh
docker exec -it geobrix-dev bash -lc \
  'cd /root/geobrix/python/geobrix && \
   uv pip compile --generate-hashes --python-version 3.12 \
       --output-file requirements-ci.txt requirements-ci.in'
```

> **Mirror caveat (confirmed 2026-09-11).** `uv` does not read `pip.conf`; the dev
> container now sets `UV_INDEX_URL` alongside `PIP_INDEX_URL` so both use the corp
> proxy (takes effect on image rebuild). BUT the dev proxy
> (`pypi-proxy.dev.databricks.com`) is a **rolling-recent mirror that prunes old
> releases** — it cannot reproduce runtime-matched locks. Example: the light-CI locks
> (`requirements-light-env{5,6}-ci.txt`) pin `botocore==1.40.70` to match the Serverless
> base image, but the dev proxy only serves `botocore>=1.42.91`, so an in-container
> recompile fails to resolve / would shift the pin off the runtime. CI installs those
> locks fine because it uses the **full-retention JFrog mirror (`db-pypi`)** via
> `jfrog-pip-bootstrap`/`jfrog-auth`. **Recompile runtime-matched locks against the
> JFrog mirror (as CI does), not the dev proxy and not public PyPI.** Wiring the dev
> container to JFrog (with credentials) is the only way to recompile those in-container.

Re-run whenever:
- A pin in `requirements-ci.in` changes (DBR version bump, security
  patch, new dev tool).
- A new transitive dep enters the closure.
- A Dependabot PR bumps a Python dep.

### Gitleaks pre-commit false positives

The Databricks corp pre-commit hook (gitleaks) treats any hex string
starting with `eaaa` (case-insensitive) as a potential Square access
token. sha256 hashes occasionally start with those bytes — currently
one entry in `requirements-dev-container.txt` (parso 0.8.7). The line
carries an inline `# gitleaks:allow` comment.

**After regenerating a lockfile**, scan it for any new collisions:

```sh
~/.databricks/githooks/gitleaks detect --source <lockfile> \
    --config ~/.databricks/githooks/gitleaks.toml --no-git
```

For every flagged line, append ` # gitleaks:allow — <reason>` to that
exact line. The `# via <package>` line on the next line cannot carry the
comment; it must be on the `--hash=` line itself.

## Python install paths covered

All non-customer-facing Python installs in the repo are hash-pinned:

| Trust boundary | Source | Lockfile |
|---|---|---|
| CI (scala_build, python_build) | `python/geobrix/requirements-ci.in` | `python/geobrix/requirements-ci.txt` |
| Dev container (geobrix-dev) | `python/geobrix/requirements-dev-container.in` | `python/geobrix/requirements-dev-container.txt` |
| Notebook test harness (gbx:test:notebooks) | `notebooks/tests/requirements.in` | `notebooks/tests/requirements.txt` |

Intentionally NOT hash-pinned (per maintainer policy):
- `%pip install` cells inside `notebooks/examples/**/*.ipynb` — customer-facing content.
- Code examples in `docs/docs/**/*.mdx` — illustrative for customers.
- The published wheel's loose `pyspark>=4.0.0` in `python/geobrix/pyproject.toml` — downstream consumers need flexibility.

## Base-pinned runtime packages and CVE triage

### Policy

geobrix pins runtime-sensitive core packages (`urllib3`, `pandas`, `numpy`, `idna`) to
the preinstalled version of each DBR/Serverless base so a cluster `%pip install
geobrix[light_dbrNN]` stays silent (no "a core Python package changed" notice) and the
installed versions remain base-compatible.  geobrix **never** ships a version newer than
the base.

Per-regime base versions verified 2026-09-03:

| Regime | urllib3 base | pyproject cap |
|---|---|---|
| DBR 17.3 / DBR 18 / Serverless env 5 | 2.3.0 | `urllib3<2.4` |
| DBR 19 / Serverless env 6 | 2.5.0 | `urllib3<2.6` |

`pandas` and `numpy` are capped similarly (`<2.3`/`<2.2` for pb5 regimes, `<2.4`/`<2.4`
for pb6); `idna` is capped at `<3.8` (pb5) and `<3.12` (pb6).

### Consequence for Dependabot triage

CVEs in a base-pinned runtime package are **inherited from the DBR base image** — geobrix
did not introduce the vulnerable version and cannot fix it by bumping the wheel pin (a
bump past the base triggers the "core package changed" cluster notice and breaks
base-version fidelity).  These CVEs are resolved when Databricks ships a patched DBR base.

**Specific cases (as of 2026-09-27):**

- **urllib3 — 16 HIGH alerts** across the four light-regime CI lockfiles (env5/env6
  canonical + _all variants).  Fix requires 2.6.0+; both pinned bases (2.3.0 and 2.5.0)
  are below the fix.  Every DBR base itself ships a vulnerable urllib3.  In geobrix the
  affected code path is the `[stac]` / `[earthdata]` HTTP optional extras (requests →
  urllib3) against attacker-controlled HTTP responses; the base wheel with no extras
  makes no outbound HTTP calls.  A user who must patch can override the cap at their own
  risk (`pip install geobrix[light_env6] "urllib3>=2.6"`) accepting the changed-package
  cluster notice.

- **pyarrow — 2 HIGH alerts** in the CI lockfiles (env5: 19.0.1, env6: 21.0.0 — both
  vulnerable; fix: 23.0.1).  `pyproject.toml` sets no upper bound on pyarrow, so geobrix
  does not prevent the fix.  The CI lockfiles are pinned to the DBR base-image version
  for test fidelity; upgrading them past the base triggers the "core package changed"
  cluster notice on DBR.  Patching is a CI-health action, not a 0.5.x release gate.

- **jackson-databind — 2 HIGH alerts** in `pom.xml` (scope `provided`).  The
  `jar-with-dependencies` assembly excludes provided-scope artifacts; Spark/DBR ships its
  own `jackson-databind` at runtime.  geobrix's JAR does not bundle it.

### Standing triage stance

Base-pinned-runtime CVEs and dev/docs/CI alerts (docs-npm, dev-container, notebook test
harness) are **tracked and deferred to the DBR base image**, not re-assessed per geobrix
release.

As of **2026-09-27**: 0 hard blockers for the 0.5.2 release.  274 of 353 open Dependabot
alerts (78%) are in three pure-noise manifests (`apps/genie_map/pnpm-lock.yaml`,
`requirements-dev-container.txt`, `docs/package-lock.json`) — none shipped in the wheel
or JAR.
