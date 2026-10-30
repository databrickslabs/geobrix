#!/usr/bin/env python3
"""
Build the GeoBrix JAR (mvn clean package -DskipTests) and upload *-jar-with-dependencies.jar
to GBX_ARTIFACT_VOLUME/<jar_filename>. The init-script dir (GBX_ARTIFACT_VOLUME) must hold
exactly one geobrix-*-jar-with-dependencies.jar: any stale other-version product JAR in that
dir is removed before the new one is uploaded (tests.jar and non-geobrix files are untouched).
Set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to build the JAR locally but skip the Databricks upload;
set GBX_BUNDLE_SKIP_JAR=1 to skip the JAR step ENTIRELY (no build, no upload). Loads config
from examples/tests/databricks_cluster_config.env. Overwrites if file already exists.

--from-wheel <env wheel> builds nothing (no Maven, no container, no lock): it takes the JAR bundled in the wheel
(databricks/labs/gbx/_jar/), checks that its version is the wheel's and that its sha256 is the one in the wheel's
RECORD, runs the packaging guard on it, and stages it with a <jar>.sha256 sidecar to
<volroot>/geobrix/<version>/jar/ (the bundle volroot; GBX_ARTIFACT_VOLUME is not used or touched).
GBX_BUNDLE_SKIP_JAR_UPLOAD=1 does the checks and stops before the upload.
"""
from __future__ import annotations

import base64
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import zipfile
import zlib
from pathlib import Path

from _artifact_volume import bundle_root, prune_stale, upload_verified, versioned_dir, wheel_version
from _cli import exit_if_help  # sibling module (script dir is on sys.path)

TESTS_DIR = Path(__file__).resolve().parent
# This checkout's dev container comes from the command that started this (gbx-data-stage-*.sh exports
# GBX_DEV_CONTAINER from scripts/docker/gbx-container.sh). There is no default: a worktree must never
# build in the main checkout's container, so a run without the name stops (see main()).
# Container side of the gbx test lock (scripts/docker/gbx-lock.sh, the repo as mounted in the container).
# The command that started this (gbx-data-stage-*.sh) exports GBX_LOCK_RUN with its lock overrides
# (work-lock file, timeout); a direct run falls back to the defaults.
LOCK_RUN = os.environ.get("GBX_LOCK_RUN", "").split() or [
    "/bin/bash",
    "/root/geobrix/scripts/docker/gbx-lock.sh",
    "run",
]
_env_file = TESTS_DIR / "databricks_cluster_config.env"
if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = v


def _packaging_ok(project_root: Path, jar_path: Path) -> bool:
    """Guard BEFORE any upload (and before the local-only early return): the JAR must not bundle classes the
    runtime provides (Scala, Spark or log4j), which would shadow the runtime's own from /databricks/jars.
    False after one line on stderr when it does, could not be read, or the guard failed to run."""
    check = subprocess.run(
        [sys.executable, str(project_root / "scripts" / "ci" / "check_jar_packaging.py"), str(jar_path)]
    )
    if check.returncode == 1:
        print(
            "Refusing to stage a JAR that bundles classes the runtime provides (Scala, Spark or log4j).",
            file=sys.stderr,
        )
        return False
    if check.returncode == 3:
        # Exit 3 is a JAR the guard could not read (missing, or not a zip file): not a bundling problem.
        print(
            f"Refusing to stage {jar_path}: it could not be checked (missing or not a readable "
            "zip file); rebuild it with mvn clean package.",
            file=sys.stderr,
        )
        return False
    if check.returncode != 0:
        # The guard itself failed (a usage error, a crash): say so, and do not blame the JAR.
        print(
            f"Refusing to stage {jar_path}: the packaging check failed to run (exit "
            f"{check.returncode}); its output is above.",
            file=sys.stderr,
        )
        return False
    return True


class WheelJarError(Exception):
    """The wheel's bundled JAR is missing, ambiguous, not the wheel's version, or not the bytes the wheel recorded."""


_JAR_IN_WHEEL = re.compile(r"databricks/labs/gbx/_jar/(geobrix-[^/]+-jar-with-dependencies\.jar)")


def _recorded_sha256(zf: zipfile.ZipFile, member: str) -> str:
    """The hex sha256 the wheel's RECORD lists for ``member`` (RECORD holds it as ``sha256=<urlsafe base64>``)."""
    import csv
    import io

    records = [n for n in zf.namelist() if re.fullmatch(r"[^/]+\.dist-info/RECORD", n)]
    if len(records) != 1:
        raise WheelJarError(f"the wheel holds {len(records)} RECORD files, expected 1")
    for row in csv.reader(io.StringIO(zf.read(records[0]).decode("utf-8"))):
        if row and row[0] == member:
            algo, _, digest = (row[1] if len(row) > 1 else "").partition("=")
            if algo != "sha256" or not digest:
                raise WheelJarError(f"RECORD lists no sha256 for {member}")
            return base64.urlsafe_b64decode(digest + "=" * (-len(digest) % 4)).hex()
    raise WheelJarError(f"RECORD does not list {member}")


def bundled_jar(wheel: Path, out_dir: Path):
    """Extract the JAR an env wheel bundles into ``out_dir``: ``(path, sha256 hex)``. WheelJarError unless the wheel
    holds exactly one, named for the wheel's own version, whose bytes have the sha256 its RECORD lists."""
    try:
        version = wheel_version(wheel.name)
        zf = zipfile.ZipFile(wheel)
    except (OSError, ValueError, zipfile.BadZipFile) as e:
        raise WheelJarError(f"{wheel} is not a readable geobrix wheel ({e})") from None
    with zf:
        jars = [n for n in zf.namelist() if _JAR_IN_WHEEL.fullmatch(n)]
        if len(jars) != 1:
            raise WheelJarError(f"the wheel bundles {len(jars)} JARs under databricks/labs/gbx/_jar/, expected 1")
        member, name = jars[0], _JAR_IN_WHEEL.fullmatch(jars[0]).group(1)
        if name != f"geobrix-{version}-jar-with-dependencies.jar":
            raise WheelJarError(f"the bundled JAR is {name}, the wheel is version {version}")
        want = _recorded_sha256(zf, member)
        out = out_dir / name  # the name matched the pattern above: no path component
        digest = hashlib.sha256()
        try:
            with zf.open(member) as src, open(out, "wb") as dst:
                for chunk in iter(lambda: src.read(1 << 20), b""):
                    digest.update(chunk)
                    dst.write(chunk)
        except (OSError, zipfile.BadZipFile, EOFError, zlib.error) as e:  # a bad CRC, a truncated member
            out.unlink(missing_ok=True)
            raise WheelJarError(f"the bundled JAR cannot be read from the wheel ({type(e).__name__}: {e})") from None
    if digest.hexdigest() != want:
        out.unlink()
        raise WheelJarError(f"the bundled JAR has sha256 {digest.hexdigest()[:12]}, RECORD lists {want[:12]}")
    return out, want


def _workspace_client():
    """``(WorkspaceClient, 0)`` from the profile or host/token in the environment, else ``(None, exit code)``."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
        return None, 2
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return None, 2
    return (WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)), 0


def _stage_from_wheel(wheel: Path, project_root: Path, skip_upload: bool) -> int:
    """--from-wheel: check the JAR bundled in an env wheel and stage it, with its sha256 sidecar, to
    ``<volroot>/geobrix/<version>/jar/``. No build, no container."""
    if not wheel.is_file():
        print(f"PRECONDITION MISSING: {wheel} is not a file; gbx:data:stage-jar --from-wheel not run", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="gbx-wheel-jar-") as tmp:
        try:
            jar, sha = bundled_jar(wheel, Path(tmp))
        except WheelJarError as e:
            print(f"Refusing to stage the JAR of {wheel.name}: {e}.", file=sys.stderr)
            return 1
        print(f"{jar.name}: sha256 {sha} (the wheel's RECORD lists the same)")
        if not _packaging_ok(project_root, jar):
            return 1
        if skip_upload:
            print("GBX_BUNDLE_SKIP_JAR_UPLOAD=1: JAR checked; skipping Databricks upload.")
            return 0
        w, rc = _workspace_client()
        if w is None:
            return rc
        folder = versioned_dir(bundle_root(), wheel_version(wheel.name), "jar")
        try:
            w.files.create_directory(folder)  # also creates its parents
        except Exception as e:
            print(f"Error: could not create {folder}: {e}", file=sys.stderr)
            return 1
        sidecar = Path(tmp) / (jar.name + ".sha256")
        sidecar.write_text(f"{sha}  {jar.name}\n")  # sha256sum's format
        # the JAR first: a sidecar on the Volume then means the JAR before it landed
        return upload_verified(w, jar, f"{folder}/{jar.name}") or upload_verified(w, sidecar, f"{folder}/{sidecar.name}")


def main() -> int:
    exit_if_help(__doc__)
    skip_jar_upload = os.environ.get("GBX_BUNDLE_SKIP_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    project_root = TESTS_DIR.parent.parent

    # --from-wheel is asked for by name, so it runs even when GBX_BUNDLE_SKIP_JAR=1 is in the environment
    # (that flag skips the JAR *build*, which this mode does not have).
    if "--from-wheel" in sys.argv:
        i = sys.argv.index("--from-wheel")
        value = sys.argv[i + 1] if i + 1 < len(sys.argv) else ""
        if not value or value.startswith("--"):
            print("--from-wheel needs the path of an env wheel", file=sys.stderr)
            return 2
        return _stage_from_wheel(Path(value), project_root, skip_jar_upload)

    # GBX_BUNDLE_SKIP_JAR: skip the JAR step ENTIRELY (no Maven build, no upload) —
    # for light-tier-only changes (pyrx/pyvx/pygx wheel) where the staged JAR is
    # unchanged. Distinct from GBX_BUNDLE_SKIP_JAR_UPLOAD, which still builds the JAR.
    if os.environ.get("GBX_BUNDLE_SKIP_JAR", "").strip().lower() in ("1", "true", "yes"):
        print("GBX_BUNDLE_SKIP_JAR=1: skipping JAR build + upload (wheel-only stage).")
        return 0

    # Auth/volume checks: only required when uploading
    if not skip_jar_upload:
        artifact_volume = (os.environ.get("GBX_ARTIFACT_VOLUME") or "").strip().rstrip("/")
        if not artifact_volume:
            print("Set GBX_ARTIFACT_VOLUME (e.g. /Volumes/catalog/schema/volume/artifacts)", file=sys.stderr)
            return 2

        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
        if not (host and token) and not profile:
            print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
            return 2

        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
            return 2

    container = os.environ.get("GBX_DEV_CONTAINER", "").strip()
    if not container:
        print(
            "GBX_DEV_CONTAINER is not set: run this through gbx:data:stage-jar (or gbx:data:stage-wheel), "
            "which export this checkout's dev container; there is no default.",
            file=sys.stderr,
        )
        return 2

    # Corp firewall blocks repo.maven.apache.org on the host; the dev container
    # has the db-maven-proxy mirror wired up via scripts/docker/m2/settings.xml.
    mvn_cmd = (
        'unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && '
        'export MAVEN_OPTS="-Xmx4G -XX:+UseG1GC" && '
        'cd /root/geobrix && mvn clean package -DskipTests'
    )
    print(f"Running in {container}: mvn clean package -DskipTests ...")
    # `clean` wipes target/, so this must never overlap a test run: the command
    # (gbx-data-stage-jar.sh / gbx-data-stage-wheel.sh) holds the container run lock, and the
    # Maven process holds the work lock via gbx-lock.sh, so a killed host script cannot free it.
    rc = subprocess.run(
        ["docker", "exec", container, *LOCK_RUN, "/bin/bash", "-c", mvn_cmd],
        cwd=project_root,
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"Maven build failed (is the {container} container running? try gbx:docker:start)", file=sys.stderr)
        return 1

    target = project_root / "target"
    jars = list(target.glob("*-jar-with-dependencies.jar"))
    if not jars:
        print("No *-jar-with-dependencies.jar found in target/", file=sys.stderr)
        return 1
    jar_path = jars[0]

    if not _packaging_ok(project_root, jar_path):
        return 1

    if skip_jar_upload:
        print("GBX_BUNDLE_SKIP_JAR_UPLOAD=1: JAR built locally; skipping Databricks upload.")
        return 0

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)
    try:
        w.files.create_directory(artifact_volume)
    except Exception:
        pass

    # Init-script dir: keep exactly one geobrix-*-jar-with-dependencies.jar.
    # Delete any other-version product JAR before uploading the new one.
    # Never touch geobrix-*-tests.jar, geobrix-gdal-init.sh, or platform tarballs.
    # A failure here is fatal: swallowing it would leave two JAR versions in the init dir.
    try:
        prune_stale(w, artifact_volume, "geobrix-", "-jar-with-dependencies.jar", jar_path.name)
    except Exception as e:
        print(f"Error: could not list/clean artifact volume {artifact_volume}: {e}", file=sys.stderr)
        return 1

    volume_path = f"{artifact_volume}/{jar_path.name}"
    print("Uploading product jar to %s (overwrite if exists)..." % volume_path)
    w.files.upload_from(
        file_path=volume_path,
        source_path=str(jar_path.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % volume_path)

    # Also stage the bench tests.jar (carries HeavyRunner / the bench Scala harness). The
    # bench launcher attaches it as a cluster library from the BUNDLE volroot -- a DIFFERENT
    # Volume than the artifact volume the init script reads the product jar from. The same
    # `mvn package` already built it (maven-jar-plugin bench-test-jar goal), so stage it here
    # too: one command keeps both jars current (no manual fs cp). Skippable for product-only.
    if os.environ.get("GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes"):
        print("GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1: skipping tests.jar upload.")
        return 0

    test_jars = [j for j in target.glob("*-tests.jar")]
    if not test_jars:
        print("WARNING: no *-tests.jar in target/; tests.jar NOT staged (heavy bench needs it).", file=sys.stderr)
        return 0
    test_jar = test_jars[0]

    # Match the launcher's resolution exactly: explicit override, else BUNDLE volroot.
    tests_jar_dest = (os.environ.get("GBX_BENCH_TESTS_JAR_VOLUME_PATH") or "").strip()
    if not tests_jar_dest:
        b_cat = (os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main").strip()
        b_sch = (os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default").strip()
        b_vol = (os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples").strip()
        b_root = f"/Volumes/{b_cat}/{b_sch}/{b_vol}"
        try:
            w.files.create_directory(b_root)
        except Exception:
            pass
        tests_jar_dest = f"{b_root}/{test_jar.name}"
    print("Uploading tests.jar to %s (overwrite if exists)..." % tests_jar_dest)
    w.files.upload_from(
        file_path=tests_jar_dest,
        source_path=str(test_jar.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % tests_jar_dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
