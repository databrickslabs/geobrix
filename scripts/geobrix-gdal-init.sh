#!/bin/bash
#
# Databricks cluster init script — volume-staged GDAL install for GeoBrix.
# Same security spirit as geobrix-gdal-init-ppa.sh (fingerprint-pinned PPA +
# source-built GDAL bindings), but the slow build runs once in CI and ships
# its outputs as a single tarball attached to each GeoBrix release. Cluster
# start downloads zero bytes from the internet, verifies the bundle against
# its release-published sidecar, extracts to local disk, and installs.
#
# Trust anchors (defense in depth):
#   1. UC Volume ACL — only the release/CI process has write access to
#      VOL_DIR. This is the boundary that lets us trust the sidecar found
#      there. (Read access to the volume is broader; that's fine.)
#   2. <tarball>.sha256 sidecar (staged in VOL_DIR) — pins the byte hash of
#      the tarball. The init script reads it at runtime and refuses to
#      proceed on mismatch, so a tampered tarball fails closed before
#      extraction.
#   3. SHA256SUMS inside the tarball — per-file manifest verified
#      post-extract. Catches transport corruption + gives an auditable
#      per-file pin for forensics.
#   4. UBUNTUGIS_FPR in scripts/build-gdal-artifacts.sh — gates what enters
#      the tarball at CI build time. The GPG fingerprint check is upstream
#      of every hash above.
#
# Why sidecar (not hardcoded hash in this script):
#   The init script is itself a release artifact (attached to each GeoBrix
#   release alongside the tarball), so the script+tarball pairing is already
#   visible at the release level. The sidecar lets the operator hot-swap a
#   re-built bundle (security patch, GDAL bump) without re-cutting an init-
#   script release — they just stage the new tarball + new .sha256 in
#   VOL_DIR. Trust still binds to the UC Volume ACL.
#
# Architecture: x86_64 / amd64 only — Intel and AMD CPUs are interchangeable
# (`amd64` is just Debian's name for `x86_64`). The exclusion is ARM /
# aarch64 — AWS Graviton, Ampere, Apple Silicon, etc. — because the PPA
# ships only amd64 .debs. Pick a non-ARM instance type for this cluster.
#
# Distribution flow:
#   CI:        scripts/build-gdal-artifacts.sh → tarball + tarball.sha256
#   Release:   both attached to the GeoBrix GitHub release, alongside the
#              matching version of this script
#   Operator:  download both files, upload to VOL_DIR
#   Cluster:   this script discovers them, verifies, extracts, installs
#
# TROUBLESHOOTING
#
# 1) This script FAILS and the cluster never launches.
#    By default, this script writes its full stdout+stderr to a local
#    /tmp file and copies that file to VOL_DIR via an EXIT trap on
#    script exit. The persistent copy survives the failing cluster's
#    teardown. Path layout (per host, fixed filenames):
#        $VOL_DIR/_init_logs/$DB_CLUSTER_ID/$(hostname)/init.log
#        $VOL_DIR/_init_logs/$DB_CLUSTER_ID/$(hostname)/_NN_*.txt
#    On every cluster launch the driver wipes the entire cluster-level
#    log dir ($VOL_DIR/_init_logs/$DB_CLUSTER_ID/) — clean slate, no
#    stale per-host subdirs from prior launches accumulating. Workers
#    leave peer subdirs alone and only clear their own. Per-host
#    subdirectories isolate driver vs worker logs (and multiple workers
#    from each other) — no risk of two nodes racing on the same path.
#    To read everything from any working cluster:
#        %sh
#        find /Volumes/<your-vol-dir>/_init_logs/<failing-cluster-id> -type f
#        cat /Volumes/<your-vol-dir>/_init_logs/<failing-cluster-id>/<hostname>/init.log
#    No env-var setup needed — reuses the VOL_DIR already configured
#    for the platform tarball.
#
#    Why the /tmp → cp dance? UC Volumes are S3-backed, and S3 objects
#    don't support append. Incremental writes from a long-running `tee`
#    process buffer at the FUSE layer and never flush if the host
#    terminates before close(). A single bulk cp from /tmp is one
#    open/write-all/close cycle that FUSE flushes as a single S3 PUT.
#    Caveat: if Databricks SIGKILLs this script (init-script TIMEOUT,
#    distinct from the script's own non-zero exit), the trap doesn't
#    run and the cp doesn't happen — the local /tmp file dies with the
#    node. dpkg failures, sha mismatches, and set-e aborts ALL run the
#    trap normally and produce a persistent log.
#
#    Override the log location by setting WS_LOG_DIR (env var or hardcoded
#    near the top of this script) if you want logs in a Workspace files
#    path or a dedicated logs volume instead.
#
#    Backstops if VOL_DIR write is denied (the script will log a WARNING
#    line via the cluster Event Log indicating this):
#      - Databricks UI: cluster page → "Event log" tab → click the
#        "Init script failure" event for the last lines of stderr.
#      - From a surviving cluster in the same workspace, before the
#        failing driver's local files are gone:
#            sudo ls /databricks/init_scripts/
#            sudo cat /databricks/init_scripts/*_geobrix-gdal-init.sh.stderr.log
#            sudo cat /databricks/init_scripts/*_geobrix-gdal-init.sh.stdout.log
#    Common causes and what to do:
#      - "no *.tar.gz.sha256 sidecar found in $VOL_DIR"     → operator
#        forgot to upload the .sha256 sidecar alongside the tarball.
#      - sha256sum -c reports "WARNING: 1 computed checksum did NOT match"
#                                                          → the tarball
#        was truncated or replaced; re-download from the release page,
#        verify locally with `sha256sum -c <tarball>.sha256`, re-upload.
#      - "dpkg: error processing ..."                       → the bundle
#        is missing a runtime dep, or a bundled runtime lib conflicts
#        with what DBR's base image pre-installs. Rebuild the platform
#        tarball locally via scripts/build-gdal-artifacts.sh against a
#        fresh ubuntu:24.04 container, smoke-test, and ship a replacement
#        release. (Build-time-only packages — `*-dev`, autotools, etc. —
#        are filtered out of the bundle by the build script; if dpkg
#        complains about one of those, that filter has regressed.)
#
# 2) This script SUCCEEDS but GDAL functions fail at runtime.
#    Run in a %sh notebook cell on the launched cluster to find the gap:
#        dpkg -l 2>/dev/null | grep -E '^ii\s+(libgdal|libproj|libgeos|gdal-bin|gdal-data|python3-gdal)'
#        python -c "from osgeo import gdal; print(gdal.__version__)"
#        ldd /usr/lib/x86_64-linux-gnu/libgdal.so.37 | grep 'not found'
#        env | grep -i proj
#    Any "not found" line in ldd → a runtime shared lib the bundle didn't
#    ship. Compare the installed set against PACKAGES.txt inside the
#    tarball (extract locally, grep). Fix by adding the missing package
#    to scripts/build-gdal-artifacts.sh's EXTRA_SYSTEM_PKGS, rebuilding
#    the platform tarball, and recommitting.

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# Unity Catalog volume where the operator stages the release tarball + sidecar.
VOL_DIR="/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix"

# Persistent logging — survives the failing cluster's teardown.
#
# Defaults to a sibling subdirectory of VOL_DIR (cluster already has
# read access there; write usually works under the same grant). Override
# by setting WS_LOG_DIR as a cluster env var or hardcoding here:
#   WS_LOG_DIR=/Workspace/Users/you@example.com/logging/geobrix
#
# Why local /tmp + cp on exit (not tee directly to UC Volume): UC Volume
# is S3-backed. S3 objects don't support append — incremental tee writes
# buffer at the FUSE layer and never flush if the host dies first
# (observed: destination file gets touched but stays empty). A single
# bulk cp on exit is one open/write-all/close cycle that FUSE flushes
# as a single S3 PUT, which works reliably.
WS_LOG_DIR="${WS_LOG_DIR:-$VOL_DIR/_init_logs}"
CLUSTER_ID="${DB_CLUSTER_ID:-no-cluster-id}"
HOSTNAME_LBL="$(hostname)"
# Per-host paths everywhere — both local (avoids any /tmp collision in
# edge cases like sequential init runs on the same node) and persistent
# (driver + workers each write to a clearly-distinct path; no chance of
# two nodes racing on the same S3 object). Path layout:
#   $WS_LOG_DIR/<cluster_id>/<hostname>/init.log     (full log)
#   $WS_LOG_DIR/<cluster_id>/<hostname>/_NN_*.txt    (step breadcrumbs)
# Filenames are STABLE per host (no timestamp) so each cluster launch
# overwrites the previous launch's logs in place — the directory never
# accumulates a growing set of files.
LOCAL_LOG="/tmp/geobrix-init-${HOSTNAME_LBL}.log"
FINAL_LOG_DIR="$WS_LOG_DIR/$CLUSTER_ID/$HOSTNAME_LBL"
FINAL_LOG="$FINAL_LOG_DIR/init.log"

# Cluster launch == clean slate.
#
# On the driver only, wipe the entire cluster-level log dir before this
# run starts. That removes stale per-host subdirs left over from prior
# launches of the same cluster — important for autoscaling clusters
# where workers come and go and hostnames vary between launches; without
# the driver-side wipe, those orphaned subdirs would accumulate forever.
#
# Workers do NOT wipe the cluster dir (would race with peer workers and
# with the still-running driver during the initial startup window). On a
# worker, the host-level clear below catches any residue tied to that
# specific hostname.
#
# Detection: DB_IS_DRIVER is set to "TRUE" on the driver, "FALSE" or
# unset on workers — set by Databricks on every cluster-scoped init run.
if [ "${DB_IS_DRIVER:-FALSE}" = "TRUE" ]; then
    rm -rf "$WS_LOG_DIR/$CLUSTER_ID" 2>/dev/null || true
fi

mkdir -p "$FINAL_LOG_DIR" 2>/dev/null || true
# Clear this host's prior logs (covers worker autoscale-in scenarios
# where the cluster-level wipe above didn't run on this node).
( cd "$FINAL_LOG_DIR" 2>/dev/null && rm -f -- *.txt *.log ) 2>/dev/null || true
# Also clear any local /tmp leftover so tee starts fresh (without -a).
: > "$LOCAL_LOG" 2>/dev/null || true

echo "started at $(date -Iseconds) host=$HOSTNAME_LBL cluster=$CLUSTER_ID pid=$$" \
    > "$FINAL_LOG_DIR/_01_started.txt" 2>/dev/null || true

# Step breadcrumbs — small per-step files written inline so we can see
# how far the script got even if the cluster terminates abruptly and
# the EXIT trap is skipped (SIGKILL during cluster teardown, FUSE
# unmount, etc.). Each call writes < 200 B + an explicit `sync` so
# FUSE flushes the PUT to S3 before the next step runs. Without sync,
# steps near the end of the script (written in quick succession right
# before script exit) may sit in the FUSE buffer and never flush
# before the host terminates.
step() {
    echo "$(date -Iseconds) step=$1 ${2:-}" \
        > "$FINAL_LOG_DIR/_$1.txt" 2>/dev/null || true
    sync 2>/dev/null || true
}

# EXIT trap copies local /tmp log to VOL_DIR on script exit. Runs on
# normal exit, on `set -e` failures, and on most signals — but NOT on
# SIGKILL. Databricks SIGKILLs init scripts only on init-script TIMEOUT,
# not on script-level non-zero exits, so dpkg/sha failures et al. land
# in the persistent log normally.
cleanup_log() {
    # Absolute first action — confirms the trap actually fired, no
    # matter what happens after.
    : > "$FINAL_LOG_DIR/_99_trap_pinged.txt" 2>/dev/null || true

    EXIT_CODE=$?

    echo "trap entered exit=$EXIT_CODE at $(date -Iseconds)" \
        > "$FINAL_LOG_DIR/_99_trap_entered.txt" 2>/dev/null || true

    echo "--- Init script finished at $(date -Iseconds) (exit $EXIT_CODE) ---"
    # Let tee flush its last bytes to /tmp before the bulk cp.
    sleep 0.5

    CP_STATUS="cp not attempted"
    if cp "$LOCAL_LOG" "$FINAL_LOG" 2>&1; then
        CP_STATUS="cp succeeded; bytes=$(stat -c%s "$FINAL_LOG" 2>/dev/null || echo unknown)"
        echo "--- Log copied to: $FINAL_LOG ---"
    else
        CP_STATUS="cp FAILED (exit $?)"
        echo "--- WARNING: cp '$LOCAL_LOG' '$FINAL_LOG' failed ---" >&2
    fi

    echo "$CP_STATUS at $(date -Iseconds)" \
        > "$FINAL_LOG_DIR/_99_trap_done.txt" 2>/dev/null || true

    # Clean up the temp extract dir if it exists. Previously this had
    # its own `trap 'rm -rf $WORK_DIR' EXIT` which silently OVERWROTE
    # this cleanup_log trap and threw away the persistent log. Doing
    # it inline here keeps both jobs on one trap handler.
    if [ -n "${WORK_DIR:-}" ] && [ -d "${WORK_DIR:-}" ]; then
        rm -rf "$WORK_DIR" 2>/dev/null || true
    fi

    # Encourage FUSE to flush the local S3-backed cache to the underlying
    # object store before the host terminates. sync is best-effort on
    # FUSE but cheap; the sleep is what gives FUSE time to actually do
    # the PUT — Databricks may terminate the host within seconds of
    # init-script exit.
    sync 2>/dev/null || true
    sleep 3

    exit $EXIT_CODE
}
trap cleanup_log EXIT

# Mirror all stdout+stderr to the local /tmp file. The trap above is
# what persists this to VOL_DIR; tee here only handles the script→file
# pipe, and tee's original stdout is still the parent's stdout, so the
# Databricks Event Log capture continues to surface output in parallel.
exec > >(tee -a "$LOCAL_LOG") 2>&1

echo "--- Init script started at $(date -Iseconds) ---"
echo "Cluster ID: $CLUSTER_ID"
echo "Hostname:   $(hostname)"
echo "Local log:  $LOCAL_LOG"
echo "Final log:  $FINAL_LOG"

# ---- preflight -----------------------------------------------------------

# Refuse to run on ARM. The bundled .debs are amd64 (a.k.a. x86_64) only —
# proceeding on aarch64 would silently install nothing useful (dpkg would
# reject every package) and the failure mode would be confusing. Intel and
# AMD CPUs both report `amd64` here and are equally supported.
step 02_preflight
ARCH="$(dpkg --print-architecture)"
if [ "$ARCH" != "amd64" ]; then
    echo "Unsupported architecture: $ARCH" >&2
    echo "The GeoBrix GDAL bundle ships amd64 / x86_64 .debs only (Intel or AMD CPUs)." >&2
    echo "ARM-based instance types — AWS Graviton, Ampere, Apple Silicon — are not supported." >&2
    echo "Choose a non-ARM instance type for this cluster." >&2
    exit 1
fi

# Pre-empt apt-daily timers so we don't sit on /var/lib/dpkg/lock-frontend
# while unattended-upgrades runs its boot-time pass. The PPA script used
# DPkg::Lock::Timeout=-1, which made lock contention invisible — here we
# just take the lock out of contention entirely.
sudo systemctl stop --no-block \
    apt-daily.service apt-daily-upgrade.service unattended-upgrades 2>/dev/null || true

if [ ! -d "$VOL_DIR" ]; then
    echo "VOL_DIR not found: $VOL_DIR" >&2
    echo "Upload the release tarball + matching .sha256 sidecar to $VOL_DIR before running." >&2
    exit 1
fi

cd "$VOL_DIR"

# ---- discover bundle from sidecar ---------------------------------------
# The release ships geobrix-gdal-artifacts-vX.Y.Z-noble.tar.gz + .sha256;
# the operator uploads both, unchanged, to VOL_DIR. We glob for the .sha256
# sidecar (expect exactly one) and let `sha256sum -c` read the tarball name
# out of it. This decouples script-version from bundle-version: a security
# patch can re-stage a new bundle without an init-script change.

shopt -s nullglob
sidecars=(*.tar.gz.sha256)
case "${#sidecars[@]}" in
    0)
        echo "no *.tar.gz.sha256 sidecar found in $VOL_DIR" >&2
        echo "Stage the GeoBrix release tarball and its matching .sha256 file in this volume." >&2
        exit 1
        ;;
    1) SIDECAR="${sidecars[0]}" ;;
    *)
        echo "multiple *.tar.gz.sha256 sidecars in $VOL_DIR — expected exactly one active bundle." >&2
        echo "Remove the older sidecar(s) so this script knows which bundle to install:" >&2
        printf '  %s\n' "${sidecars[@]}" >&2
        exit 1
        ;;
esac

# The sidecar's standard `<hash>  <filename>` line names the tarball; pull
# it out so we have a handle for extraction and error messages below.
ARTIFACT_TARBALL="$(awk 'NR==1 {sub(/^\*/, "", $2); print $2}' "$SIDECAR")"
if [ -z "$ARTIFACT_TARBALL" ] || [ ! -f "$ARTIFACT_TARBALL" ]; then
    echo "sidecar $SIDECAR references tarball '$ARTIFACT_TARBALL' but it's not present in $VOL_DIR" >&2
    exit 1
fi

# ---- verify tarball ------------------------------------------------------
# Outer trust anchor: tarball must match the sidecar staged alongside it.
# Trust binds via UC Volume ACLs — the sidecar is only present here if the
# release/CI process put it there.
step 03_sidecar_resolved "$ARTIFACT_TARBALL"
echo "==> Verifying $ARTIFACT_TARBALL against $SIDECAR..."
sha256sum -c "$SIDECAR"
step 04_outer_sha256_ok

# ---- extract -------------------------------------------------------------
# Extract to local /tmp rather than working off the FUSE-mounted volume.
# VOL_DIR sequential reads are fine for the tarball; per-file random I/O
# during dpkg/pip runs faster against local disk.
WORK_DIR="$(mktemp -d -t geobrix-gdal-XXXXXX)"
# Do NOT install a new EXIT trap here — that would override the
# cleanup_log trap installed at the top of the script, throwing away
# the persistent-log behavior. cleanup_log() now rms WORK_DIR itself
# (see the rm line at the bottom of that function).
tar -xzf "$ARTIFACT_TARBALL" -C "$WORK_DIR" --strip-components=1
step 05_extracted

cd "$WORK_DIR"

# Inner trust anchor: per-file manifest. The outer hash already proved the
# tarball is what CI built; this confirms the extraction wasn't corrupted
# and gives a per-file pin that's useful for forensic comparison later.
if [ ! -f SHA256SUMS ]; then
    echo "SHA256SUMS missing inside tarball — bundle is malformed." >&2
    exit 1
fi
sha256sum -c SHA256SUMS
step 06_inner_sha256_ok

# ---- install .debs -------------------------------------------------------
# Install everything the build script staged in one dpkg invocation. dpkg
# satisfies intra-set deps regardless of file order via its two-pass
# unpack-then-configure flow.
#
# Security: the .debs were resolved through the UbuntuGIS PPA whose key
# fingerprint was verified at build time in scripts/build-gdal-artifacts.sh
# (UBUNTUGIS_FPR), and then SHA256-pinned via the bundle's inner
# SHA256SUMS manifest which we verified at step 06_inner_sha256_ok.
# No bytes installed here came from an unverified source.
#
# Deliberately NO `|| apt-get install -fy` fallback. The fallback would
# reach out to whatever apt sources the cluster has configured (default
# Ubuntu archives, possibly the UbuntuGIS PPA itself) and could silently
# install or remove packages — both of which defeat the SHA256-pinned
# trust model. If dpkg fails here, the right response is to fix the
# bundle in scripts/build-gdal-artifacts.sh and re-release, not paper
# over the failure at install time.
step 07_about_to_dpkg
sudo dpkg -i debs/*.deb
step 08_dpkg_done

# ---- install Python bindings --------------------------------------------
# Security: every pip install on this cluster reads ONLY from the bundle's
# wheels/ directory (--find-links + --no-index forbids any PyPI lookup,
# --no-cache-dir forbids any stale wheel reuse). The wheels themselves
# were SHA256-pinned in the bundle's inner SHA256SUMS manifest which we
# verified at step 06_inner_sha256_ok. End-to-end the bytes installed
# trace back to the PR that committed the platform tarball into
# resources/static/ — satisfies the project's hash-pinned Python policy
# (see docs/docs/security.mdx "Hash-pinned Python dependencies").
#
# Bootstrap the pip toolchain from staged wheels first. Specific versions
# are determined by the wheel filenames in the bundle (pinned at build
# time by PIP_VERSION / SETUPTOOLS_VERSION / etc. in build-gdal-artifacts.sh).
pip install --upgrade --no-index --no-cache-dir --find-links=wheels/ \
    pip setuptools wheel cython numpy
step 09_pip_toolchain_done

# CI built this GDAL wheel with --no-binary :all: against the libgdal-dev
# headers from the fingerprint-pinned PPA, so the bytes verified above are
# exactly the bindings we want. --no-deps because numpy is already installed
# (pip would otherwise re-resolve numpy from PyPI via the [numpy] extra,
# which --no-index already blocks but --no-deps makes redundant-safe).
pip install --force-reinstall --no-index --no-deps --no-cache-dir --find-links=wheels/ GDAL
step 10_gdal_wheel_done

# Note: the GeoBrix Python wheel (geobrix-<version>-py3-none-any.whl) is
# NOT installed here. It's attached separately as a cluster Library via
# the Databricks UI (Cluster → Libraries → Install new → Upload Python
# Whl). Keeping it out of this script lets the Python wheel be versioned
# and bumped independently of the GDAL platform tarball — bump GeoBrix
# without touching the init script.

# ---- native + JAR --------------------------------------------------------
cp libgdalalljni.so /usr/lib/libgdalalljni.so
step 11_jni_copied

# The GeoBrix JAR can come from one of two places:
#   1. Bundled inside the tarball (release path) — package-geobrix-artifacts.yml
#      grafts it in, so the cluster gets it via SHA256SUMS-verified extraction.
#   2. Staged alongside the tarball in VOL_DIR (operator path) — useful for
#      smoke-testing a fresh platform tarball before it's baked into a
#      release, and for hot-swapping a JAR without rebuilding the tarball.
#      Trust here devolves to the same UC Volume write ACL that protects the
#      tarball + sidecar — a write to VOL_DIR is already privileged.
# If neither has a JAR, log clearly and proceed — the GDAL stack still
# installs; GeoBrix functions just won't be registered until a JAR is
# supplied. Exiting non-zero here would force every smoke test to ship a
# JAR even when only the platform layer is under test.
if compgen -G "geobrix-*-jar-with-dependencies.jar" > /dev/null; then
    echo "==> Installing GeoBrix JAR from bundle."
    cp geobrix-*-jar-with-dependencies.jar /databricks/jars/
elif compgen -G "$VOL_DIR/geobrix-*-jar-with-dependencies.jar" > /dev/null; then
    echo "==> No JAR in bundle; using JAR from VOL_DIR."
    cp "$VOL_DIR"/geobrix-*-jar-with-dependencies.jar /databricks/jars/
else
    echo "==> WARNING: no GeoBrix JAR in bundle or VOL_DIR; GDAL stack installed but GeoBrix functions will not be available until a JAR is staged." >&2
fi
step 12_jar_done

# ---- log installed PPA-sourced versions ---------------------------------
# Goes to the init-script stdout log so you can later confirm a given
# cluster is on the libproj/libgeos/etc. set captured in this bundle.
# Compare against PACKAGES.txt inside the tarball if you ever need to
# audit version drift after the fact.
echo "==> Installed PPA-sourced versions on cluster:"
dpkg-query -W -f='${Package} ${Version}\n' 2>/dev/null \
    | grep -E '^(libgdal|libproj|libgeos|proj-data|proj-bin|python3-gdal|gdal-bin|libspatialite|libnetcdf|libhdf[45])' \
    | LC_ALL=C sort \
    | sed 's/^/    /'
step 13_script_complete
