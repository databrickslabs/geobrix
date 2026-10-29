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
| `maven-pgp-bootstrap` | `./mvnw` (or `GBX_MVN=mvn` in geobrix-dev), `awk` | Run `pgpverify-maven-plugin` with relaxed settings, capture every PGP fingerprint Maven Central serves for the resolved closure, and emit draft `.maven-keys.list` entries on stdout. The output is a draft — every fingerprint must be cross-checked against the project's published signing key before being committed. |
| `maven-pgp-verify` | `./mvnw` (or `GBX_MVN=mvn` in geobrix-dev) | Run strict verification (`./mvnw -Pverify-pgp verify`). Exits non-zero if any artifact is unsigned, weakly signed, or signed by a key not in `.maven-keys.list`. |

## Maven itself (the wrapper)

CI runs every Maven call through `./mvnw` (Maven Wrapper 3.3.4, `only-script` type), so CI no
longer uses whatever Maven the runner image ships. `.mvn/wrapper/maven-wrapper.properties` pins
Maven by version, URL and `distributionSha256Sum`; the wrapper refuses a distribution whose
SHA-256 differs (`Failed to validate Maven distribution SHA-256`). It is the same discipline as
the other pinning regimes here:

- **No unpinned binary in the repo.** The `only-script` type checks in only the reviewable
  `mvnw` shell script, not a `maven-wrapper.jar` (a jar would need its own `wrapperSha256Sum`).
- **Hash-pinned download, like the Python locks and the Dockerfile's `MAVEN_SHA512`.** The
  SHA-256 was computed from the zip and checked against the SHA-512 Apache publishes, on two
  Apache sources (`archive.apache.org` and `dlcdn.apache.org`, byte-identical). Apache does not
  publish a SHA-256, so the bump procedure is in the properties file's header.
- **Through JFrog on the hardened runner.** `.github/actions/maven-wrapper` sets
  `MVNW_REPOURL` to the `db-maven` mirror and passes the OIDC token from `jfrog-auth`, in that
  one step only; the wrapper's pinned hash is checked on the bytes the mirror returns.
- **The PGP gate is unchanged and runs on the pinned Maven.** Maven itself is not a project
  dependency, so `.maven-keys.list` does not cover it; the SHA-256 pin does.
- **Inside geobrix-dev** the wrapper cannot download (Maven Central is blocked there), so the
  image installs the same version with a pinned SHA-512 and
  `scripts/ci/test_maven_wrapper.py` fails if the two ever differ. Use `GBX_MVN=mvn` for the
  scripts above in the container.

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
once before relying on this). There the scripts take `GBX_MVN=mvn` (the image's
Maven is the version the wrapper pins).

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Generate a draft keysmap from the current resolved closure.
GBX_MVN=mvn ./scripts/security/maven-pgp-bootstrap > /tmp/draft.list

# 2. Cross-check every fingerprint in /tmp/draft.list against the
#    project's published signing key. Trust-anchor URLs for the direct
#    deps in pom.xml are listed at the top of .maven-keys.list. Do NOT
#    skip this step — the entire trust model rests on it.

# 3. Replace the TODO block in .maven-keys.list with the reviewed
#    entries.

# 4. Confirm strict verification passes.
GBX_MVN=mvn ./scripts/security/maven-pgp-verify

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
> is a **rolling-recent mirror that prunes old
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
| Dev container (geobrix-dev) | `python/geobrix/requirements-dev-container.in` | `python/geobrix/requirements-dev-container.txt` (x86_64) and `requirements-dev-container-aarch64.txt` (arm64): same versions, checked by `scripts/ci/check_dev_container_locks.py`. The build passes a private package index as a BuildKit secret. |
| Notebook test harness (gbx:test:notebooks) | `examples/tests/requirements.in` | `examples/tests/requirements.txt` |

Intentionally NOT hash-pinned (per maintainer policy):
- `%pip install` cells inside `examples/notebooks/**/*.ipynb` — customer-facing content.
- Code examples in `docs/docs/**/*.mdx` — illustrative for customers.
- The published wheel's loose `pyspark>=4.0.0` in `python/geobrix/pyproject.toml` — downstream consumers need flexibility.

## Base-pinned runtime packages and CVE triage

### Policy

geobrix pins runtime-sensitive core packages (`urllib3`, `pandas`, `numpy`) to
the preinstalled version of each DBR/Serverless base so a cluster `%pip install
geobrix[light_dbrNN]` stays silent (no "a core Python package changed" notice) and the
installed versions remain base-compatible.  geobrix does not ship a newer version of these
than the base.  `idna` is the exception, and is **not** capped: see the httpx2 / httpcore2
case below.

Per-regime base versions verified 2026-09-03:

| Regime | urllib3 base | pyproject cap |
|---|---|---|
| DBR 17.3 / DBR 18 / Serverless env 5 | 2.3.0 | `urllib3<2.4` |
| DBR 19 / Serverless env 6 | 2.5.0 | `urllib3<2.6` |

`pandas` and `numpy` are capped similarly (`<2.3`/`<2.2` for pb5 regimes, `<2.4`/`<2.4`
for pb6).

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
  cluster notice on DBR.  Patching is a CI-health action, not a 0.6.0 release gate.

- **httpx2 / httpcore2 — resolved by lifting the idna cap (2026-10-28).**  rio-tiler 9.2 and
  later (9.4.12 is the newest admitted by the `rio-tiler>=9.0,<10` cap) fetch remote rasters
  through `httpx2`.  The patched `httpx2` (`>=2.11` for the MED multipart/header CVEs, `>=2.12`
  for HIGH CVE-2026-84382 decompression-amplification, plus the paired `httpcore2>=2.10`
  for HIGH CVE-2026-84381) **requires `idna>=3.18`**.  The extras used to cap idna at the
  runtime's version (`<3.8` env5 / `<3.12` env6), so pip backtracked to `httpx2` 2.3.0 for every
  user of a light extra, not only in the CI locks, and the cap held no one at a safe base either
  (the base idna 3.7 / 3.11 is itself below the 3.15 fix).  The cap was cosmetic (it only
  silenced Databricks' "a core package changed: idna" notice), so it is gone from
  `pyproject.toml` and the two light CI `.in` files: the four light locks now carry `httpx2` /
  `httpcore2` 2.16.0 and idna 3.20, and a light-extra install upgrades the runtime's idna and may
  show that one-time notice.  idna 3.x keeps the API `requests` (2.32.x) and `urllib3` use:
  their IDN host encoding gives the same result on 3.7, 3.11 and 3.20, and every runtime's
  preinstalled package set resolves with idna 3.20.  The patched `httpx2` also needs `anyio>=4.10`
  and `httpcore2` needs `h11>=0.16`, so the install upgrades `anyio` on env5 / DBR 17.3 / DBR 18
  (4.6.2 / 4.7.0) and `h11` on DBR 17.3 (0.14.0); the other preinstalled packages do not move
  (checked with `uv pip compile` against each runtime's package list).  (The earlier hold was confirmed with
  `uv pip compile` on 2026-09-28: `httpx2>=2.11` failed with *"httpx2>=2.11.0 depends on
  idna>=3.18 … and geobrix[light-env6-all] depends on idna<3.12"*.)

- **jackson-databind — 2 HIGH alerts** in `pom.xml` (scope `provided`).  The
  `jar-with-dependencies` assembly excludes provided-scope artifacts; Spark/DBR ships its
  own `jackson-databind` at runtime.  geobrix's JAR does not bundle it.

### Standing triage stance

Base-pinned-runtime CVEs and dev/docs/CI alerts (docs-npm, dev-container, notebook test
harness) are **tracked and deferred to the DBR base image**, not re-assessed per geobrix
release.

As of **2026-09-27**: 0 hard blockers for the 0.5.2 release.  274 of 353 open Dependabot
alerts (78%) are in three pure-noise manifests (`examples/apps/genie_map/pnpm-lock.yaml`,
`requirements-dev-container.txt`, `docs/package-lock.json`) — none shipped in the wheel
or JAR.

**2026-09-28 light-CI recompile.**  The two explicit light-CI locks
(`requirements-light-env5-ci.txt` / `-env6-ci.txt`) were recompiled to clear the 9
genuinely-clearable stale HIGH/MED alerts by bumping their non-base-pinned pins:
`wheel` 0.45.1→0.46.2 (HIGH), `setuptools` 80.9.0→83.0.0 (MED), `pytest` 8.4.2→9.0.3
(MED, CVE-2025-71176), and `requests` →2.33.0 (MED; 2.33.0's `urllib3<3,>=1.26` /
`idna<4,>=2.5` deps stay inside the base caps so the pinned urllib3/idna are undisturbed).
The two `_all` umbrella locks were **not** recompiled: their only outstanding alerts are the
base-pinned `urllib3`/`idna` and the idna-blocked `httpx2`/`httpcore2` cases above (0
clearable), so a recompile would clear nothing and only float unrelated transitives.  The
remaining light-lock alerts are all base-pinned/idna-blocked and deferred per the cases above.

**2026-10-28 dependency refresh.**  Every pin with an open alert was moved past its patched
release unless a runtime fixes its version:

- *CI and dev locks (pip).*  The four light CI locks, `requirements-ci`, `requirements-build`,
  both dev-container locks and `examples/tests/requirements.txt` were recompiled.  Floors in the
  `.in` files keep transitive packages at their patched releases (`pillow>=13`, `certifi`,
  `aiohttp`, `multidict`, plus the Jupyter-stack packages in the dev container); `setuptools`,
  `wheel` and `black` moved to patched releases.  Pillow is not a dependency of any shipped extra,
  so the floor is CI-only and the runtime's own pillow (11.1.0 or 12.0.0) is untouched.
- *Still held on purpose.*  `urllib3` and `pyarrow` in the light locks (cases above); `pip` and
  `pyarrow` in the dev container (the DBR 17.3 install toolchain and
  Arrow baseline the tests run on); `torch` in `models_gpu_env5` (pinned to the Serverless GPU base,
  CUDA 12).
- *Docs and the Genie Map app (npm / pnpm).*  Docusaurus moved to 3.10.2, and `overrides` lift
  `serialize-javascript`, `tinypool` and `uuid` (docs) and about twenty transitive packages
  (Genie Map, in `pnpm.overrides`) to patched releases within their major lines.  What remains
  has no patched release, is not yet on the package mirror, or needs a major bump of a pinned
  consumer (kepler.gl, the AI tool stack, `@loaders.gl`).

**2026-10-28 idna cap lifted.**  `idna` is no longer capped in any light extra, so the four light
locks carry `httpx2` / `httpcore2` 2.16.0 and idna 3.20, and the httpx2 / httpcore2 / idna alerts
on the light locks are cleared (see the httpx2 / httpcore2 case above).
