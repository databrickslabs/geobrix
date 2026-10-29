#!/bin/bash
# Run GitHub Actions workflows locally via `act`, against a Databricks-aware
# runner image. Real .github/ is NEVER modified — instead we mirror the project
# root into .cache/act-workspace/ with the .github/ tree freshly copied (and
# jfrog-auth stubbed) and every other top-level entry symlinked back to the
# real project. `act` parses YAML on the host, so the overlay HAS to live on
# the host filesystem — putting it in a sibling workspace dir keeps the real
# tree pristine.
#
# Invocation: bash scripts/ci-local/run-act.sh [act-args...]
# Or via: gbx:ci:act
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && cd .. && pwd)"
RUNNER_IMAGE="geobrix-ci-runner:local"
STUB_FILE="$SCRIPT_DIR/jfrog-auth-stub/action.yml"
WORKSPACE="$PROJECT_ROOT/.cache/act-workspace"

# --- Pre-flight ---
if ! command -v act >/dev/null 2>&1; then
    echo "❌ act is not installed."
    echo "   Install:  brew install act"
    echo "   See also: https://github.com/nektos/act"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Start Docker Desktop and retry."
    exit 1
fi

if [ ! -f "$STUB_FILE" ]; then
    echo "❌ jfrog-auth stub missing at $STUB_FILE"
    exit 1
fi

# Clean up leftover act containers from previous (likely failed) runs.
# These hold the act-toolcache volume so any image rebuild that the user
# expected to refresh tool-cache content gets masked by stale volume content.
LEFTOVER=$(docker ps -aq --filter name=^act-)
if [ -n "$LEFTOVER" ]; then
    echo "🧹 Removing leftover act containers from previous runs..."
    docker rm -f $LEFTOVER >/dev/null 2>&1 || true
fi

# --- Build runner image if missing ---
# Build for linux/amd64 so the image matches the GitHub-hosted runner arch
# (real CI runners are amd64). On Apple Silicon, Docker Desktop emulates
# amd64 via Rosetta — ~1.5-2x slowdown but workflow-faithful: Ubuntu apt
# mirrors only ship arm64 via ports.ubuntu.com (a different path), and
# building native arm64 here would 404 on every apt-get install.
if ! docker image inspect "$RUNNER_IMAGE" >/dev/null 2>&1; then
    echo "🔨 Building $RUNNER_IMAGE for linux/amd64 (one-time, ~2 min)..."
    # Forward host registry proxy URLs (if set) as build args. Export them
    # to point at a private mirror if your network requires it; otherwise
    # leave them unset and the image uses public registries.
    BUILD_ARGS=()
    [ -n "${PIP_INDEX_URL:-}" ]    && BUILD_ARGS+=("--build-arg" "PIP_INDEX_URL=${PIP_INDEX_URL}")
    [ -n "${MAVEN_MIRROR_URL:-}" ] && BUILD_ARGS+=("--build-arg" "MAVEN_MIRROR_URL=${MAVEN_MIRROR_URL}")
    [ -n "${NPM_REGISTRY_URL:-}" ] && BUILD_ARGS+=("--build-arg" "NPM_REGISTRY_URL=${NPM_REGISTRY_URL}")
    # ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} expands to nothing when the array
    # is empty — needed for the public-contributor path (no proxy URLs set)
    # under `set -u` on bash 3.2 (macOS default), which otherwise errors with
    # "BUILD_ARGS[@]: unbound variable" before docker even starts.
    docker build --platform linux/amd64 -t "$RUNNER_IMAGE" \
        ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} \
        -f "$SCRIPT_DIR/Dockerfile.gha-runner" \
        "$SCRIPT_DIR/"
    echo "✅ $RUNNER_IMAGE built."
fi

# --- Prepare workspace mirror ---
# `act --bind` only mounts the workspace dir, so symlinks pointing outside
# wouldn't resolve inside the container (e.g. setup-java's `pom.xml` glob
# would silently miss the file). Solution: rsync with hardlinks where the
# filesystem allows, plain copies otherwise. Heavy/derived/secret dirs are
# excluded to keep the sync fast (a few seconds for a clean repo).
echo "📁 Preparing workspace mirror at $WORKSPACE..."
rm -rf "$WORKSPACE"
mkdir -p "$WORKSPACE"

# Hardlink-rsync everything from the project EXCEPT:
#  - .cache/         (mirror lives here; recursion would explode)
#  - .github/        (handled separately to apply the stub overlay)
#  - target/         (Maven build output, GBs)
#  - python/geobrix/.venv/  (lint venv)
#  - scripts/docker/m2/     (Maven cache)
#  - sample-data/Volumes/   (sample-data downloads; the TRACKED files under it are
#                            mirrored separately below)
#  - docs/build/, docs/node_modules/  (docusaurus build/dep dirs)
#  - node_modules/, staging/, test-logs/, coverage-reports/
#  - spark-warehouse/, metastore_db/, derby.log  (gitignored local Spark output:
#    a test run in progress creates and deletes files there, and a file that
#    vanishes mid-copy makes rsync exit 23 and abort the whole act run)
# .git/ IS included — scripts called from workflows (e.g.
# scripts/security/maven-pgp-verify) use `git rev-parse --show-toplevel` to
# resolve the project root, which fails without a real .git tree.
# --link-dest tells rsync to hardlink files identical to PROJECT_ROOT.
rsync -a \
    --link-dest="$PROJECT_ROOT" \
    --exclude='/.cache/' \
    --exclude='/.github/' \
    --exclude='/target/' \
    --exclude='/python/geobrix/.venv/' \
    --exclude='__pycache__/' \
    --exclude='/scripts/docker/m2/' \
    --exclude='/sample-data/Volumes/' \
    --exclude='/node_modules/' \
    --exclude='/docs/build/' \
    --exclude='/docs/node_modules/' \
    --exclude='/staging/' \
    --exclude='/test-logs/' \
    --exclude='/coverage-reports/' \
    --exclude='spark-warehouse/' \
    --exclude='/metastore_db/' \
    --exclude='/derby.log' \
    "$PROJECT_ROOT/" "$WORKSPACE/"

# sample-data/Volumes/ is excluded above (untracked bulk downloads), but a few files
# under it are TRACKED fixtures that real CI checks out, e.g. proj-grids/synthetic.gsb
# (the pyvx grid-shift and pyrx PROJ end-to-end tests). Mirror exactly the tracked
# files so those tests do not fail for an act-only reason. A tracked file deleted in
# the working tree is skipped: rsync would otherwise exit 23 and abort the run.
(
    cd "$PROJECT_ROOT"
    git ls-files -z --cached -- sample-data/Volumes | while IFS= read -r -d '' tracked; do
        if [ -e "$tracked" ]; then printf '%s\0' "$tracked"; fi
    done
) | rsync -a \
    --link-dest="$PROJECT_ROOT" \
    --from0 --files-from=- \
    "$PROJECT_ROOT/" "$WORKSPACE/"

# Real CI runs every Maven call as ./mvnw, which downloads the pinned Maven through JFrog. Neither
# is available in the local runner image, which bakes the same Maven version (Dockerfile.gha-runner)
# plus the proxy settings. Give the mirror an ./mvnw that runs it. rsync hardlinked the real
# mvnw into the mirror, so unlink it first: writing through the link would replace the real file.
rm -f "$WORKSPACE/mvnw"
printf '#!/bin/sh\nexec mvn "$@"\n' > "$WORKSPACE/mvnw"
chmod +x "$WORKSPACE/mvnw"

# Now drop .github/ in fresh, with stub overlays applied.
cp -R "$PROJECT_ROOT/.github" "$WORKSPACE/.github"
cp "$STUB_FILE" "$WORKSPACE/.github/actions/jfrog-auth/action.yml"
# upload_artifacts stub: act's bundled artifact server doesn't understand
# upload-artifact@v4's `mime_type` protobuf field. Stub turns the upload
# step into a no-op locally so the job stays green; real CI is unaffected.
UPLOAD_STUB="$SCRIPT_DIR/upload-artifacts-stub/action.yml"
if [ -f "$UPLOAD_STUB" ] && [ -d "$WORKSPACE/.github/actions/upload_artifacts" ]; then
    cp "$UPLOAD_STUB" "$WORKSPACE/.github/actions/upload_artifacts/action.yml"
fi

# Workaround for nektos/act#2206-class issue: act doesn't propagate the parent
# job's matrix context into composite actions' `run:` blocks. So
# `pip install numpy==${{ matrix.numpy }}` inside scala_build/action.yml
# substitutes to `numpy==` (empty) and pip errors out. Real GHA expands these
# correctly. Workaround: rewrite the matrix expressions in the MIRROR copy of
# the composite actions to literal values. Keep these in sync with the matrix
# values declared in .github/workflows/*.yml — `gbx:versions:audit --package
# numpy` (etc.) shows what's currently pinned.
echo "🔧 Rewriting matrix refs in mirror's composite actions (act#2206 workaround)..."
for af in "$WORKSPACE"/.github/actions/scala_build/action.yml \
          "$WORKSPACE"/.github/actions/python_build/action.yml; do
    [ -f "$af" ] || continue
    # macOS sed needs an explicit backup suffix; use empty to skip backup file.
    sed -i.bak \
        -e 's|\${{ matrix\.python }}|3.12.3|g' \
        -e 's|\${{ matrix\.numpy }}|2.1.3|g' \
        -e 's|\${{ matrix\.spark }}|4.0.0|g' \
        -e 's|\${{ matrix\.gdal }}|3.11.4|g' \
        -e 's|\${{ matrix\.pytest }}|8.4.2|g' \
        "$af"
    rm -f "$af.bak"
done

# DELIBERATE LOCAL-ACT VARIATION (experimental):
# Workflow does `pip install --no-build-isolation --no-binary :all: gdal[numpy]==3.11.4`.
# With --no-build-isolation, pip uses our env's setuptools (74.0.0 — strict
# validate-pyproject) which rejects GDAL 3.11.4's invalid dual-key
# `[project.license]`. Removing --no-build-isolation lets pip create an
# isolated build env with whatever setuptools its own bootstrap uses, which
# *might* end up being a lenient combo. --no-binary :all: stays — we still
# want the sdist path locally so we exercise the same code path real CI runs.
echo "🔧 Patching gdal install: drop --no-build-isolation in mirror (act-local variation)..."
for af in "$WORKSPACE"/.github/actions/scala_build/action.yml \
          "$WORKSPACE"/.github/actions/python_build/action.yml; do
    [ -f "$af" ] || continue
    sed -i.bak \
        -e 's|pip install --no-build-isolation --no-binary :all: gdal\[numpy\]|pip install --no-binary :all: gdal[numpy]|g' \
        "$af"
    if ! diff -q "$af.bak" "$af" >/dev/null 2>&1; then
        echo "   patched: $(basename "$(dirname "$af")")/$(basename "$af")"
    fi
    rm -f "$af.bak"
done

# Force scoverage runtime version on the mvn CLI. pom.xml has
# `<scoverageVersion>2.3.0</scoverageVersion>` in the plugin <configuration>
# but scoverage-maven-plugin's `pre-compile` mojo (invoked via `scoverage:test`)
# evidently ignores the plugin-level XML config and falls back to its
# hardcoded default of `2.5.2` (which doesn't exist on Maven Central for
# scala 2.13.12). The CLI -D property is authoritative.
echo "🔧 Patching mvn invocations: add -Dscoverage.scalacPluginVersion=2.3.0 in mirror..."
for af in "$WORKSPACE"/.github/actions/scala_build/action.yml; do
    [ -f "$af" ] || continue
    sed -i.bak \
        -e 's|\./mvnw -T 1C -C -q clean scoverage:test|./mvnw -T 1C -C -q -Dscoverage.scalacPluginVersion=2.3.0 clean scoverage:test|g' \
        -e 's|\./mvnw -C -q scoverage:report-only|./mvnw -C -q -Dscoverage.scalacPluginVersion=2.3.0 scoverage:report-only|g' \
        "$af"
    if ! diff -q "$af.bak" "$af" >/dev/null 2>&1; then
        echo "   patched: $(basename "$(dirname "$af")")/$(basename "$af")"
    fi
    rm -f "$af.bak"
done

echo "✅ Workspace ready (real .github/ untouched; stub + matrix-rewrite + gdal/scoverage patches applied to mirror only)."

# --- Run act from the mirror ---
# `--bind` mounts the cwd (= mirror) into /github/workspace as a real bind.
# `--pull=false` keeps act from re-pulling our local-only image.
# `--container-architecture linux/amd64` matches the runner image arch (which
# we forced amd64 above for apt parity with real CI). On Apple Silicon this
# emulates via Rosetta.
# Sample-data bind: tests reference `/Volumes/main/default/geobrix_samples/...`
# (the dev container has this via `start_docker_with_volumes.sh`). We mirror
# the same mount here so tests resolve sample paths identically. Read-only
# because act-driven runs shouldn't write back into your sample tree.
SAMPLE_DATA="$PROJECT_ROOT/sample-data/Volumes"
SAMPLE_MOUNT_OPTS=""
if [ -d "$SAMPLE_DATA" ]; then
    SAMPLE_MOUNT_OPTS="--mount type=bind,source=$SAMPLE_DATA,target=/Volumes,readonly"
fi

# `--artifact-server-path` makes `actions/upload-artifact` work locally — act
# spins up a built-in artifact server backed by this path. Without it, uploads
# fail with "Unable to get the ACTIONS_RUNTIME_TOKEN" (real CI provides it).
# Artifacts are scratch — wipe each run.
ARTIFACT_PATH="/tmp/act-artifacts"
rm -rf "$ARTIFACT_PATH" && mkdir -p "$ARTIFACT_PATH"

cd "$WORKSPACE"
exec act \
    --bind \
    --pull=false \
    --container-architecture linux/amd64 \
    --artifact-server-path "$ARTIFACT_PATH" \
    -P ubuntu-latest="$RUNNER_IMAGE" \
    -P ubuntu-24.04="$RUNNER_IMAGE" \
    -P ubuntu-22.04="$RUNNER_IMAGE" \
    -P larger="$RUNNER_IMAGE" \
    -P linux-ubuntu-latest="$RUNNER_IMAGE" \
    ${SAMPLE_MOUNT_OPTS:+--container-options "$SAMPLE_MOUNT_OPTS"} \
    "$@"
