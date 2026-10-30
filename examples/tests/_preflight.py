"""Checks a bench launch can make before any compute is started.

A bench run on Serverless costs minutes of cold start before its first line runs, and a mistake in the
launch (a wheel that is not in the environment, a staged wheel that is not the one just built, SQL that
cannot run) shows only then. Each check returns the problems it found as a list of strings; the launcher
refuses to submit when any list is not empty.

Pure Python except ``staged_wheel_sha`` (one Files API download through the client it is given) and
``probe_problems`` (a local Spark session, built lazily).
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import re
import subprocess
import sys
from typing import List, Optional, Sequence

_ENV_WHEEL = re.compile(r"\+env(\d+)-")


def serverless_dependencies(wheel: str) -> List[str]:
    """The Serverless environment spec's dependencies: the wheel (it reaches the Python UDF workers only this way)
    and what the bench notebook itself imports."""
    return [wheel, "markdown"]


def check_env_spec(deps: Sequence[str], wheel: str, env_version) -> List[str]:
    """The environment spec must name the wheel, and the wheel must be the family of the environment version.

    A spec without the wheel left every geobrix UDF failing on the workers with
    ``No module named 'databricks.labs'`` (the notebook's own pip install reaches the driver only).
    """
    problems = []
    if wheel not in deps:
        problems.append(
            f"the Serverless environment spec does not name the wheel {wheel}: the Python UDF workers would "
            "not have geobrix (the notebook's pip install reaches the driver only)"
        )
    m = _ENV_WHEEL.search(os.path.basename(wheel))
    if m and m.group(1) != str(env_version):
        problems.append(
            f"the wheel is the +env{m.group(1)} family but the Serverless environment version is {env_version}: "
            f"the other family's wheel refuses to import (use --env-version {m.group(1)} or the +env{env_version} wheel)"
        )
    return problems


def local_wheel(root, wheel_name: str) -> Optional[pathlib.Path]:
    """``dist/env<N>/<wheel_name>`` under `root` when it exists (the local build of the staged wheel's family)."""
    m = _ENV_WHEEL.search(wheel_name)
    if not m:
        return None
    path = pathlib.Path(root) / "dist" / f"env{m.group(1)}" / wheel_name
    return path if path.is_file() else None


def file_sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def staged_wheel_sha(w, volume_path: str) -> str:
    """sha256 of the bytes on the Volume, streamed through the Files API."""
    h = hashlib.sha256()
    body = w.files.download(volume_path).contents
    for chunk in iter(lambda: body.read(1 << 20), b""):
        h.update(chunk)
    return h.hexdigest()


def check_wheel(local_path, staged_sha: str, volume_path: str) -> List[str]:
    """The staged wheel must be the build in ``dist/``: the same sha256."""
    if local_path is None:
        return [
            f"no local build of {os.path.basename(volume_path)} under dist/ to compare the staged wheel with: "
            "build it (gbx:build:wheel) and stage it (gbx:data:stage-wheel --env <N>)"
        ]
    local = file_sha256(local_path)
    if local != staged_sha:
        return [
            f"the staged wheel is not the local build: staged sha256 {staged_sha[:12]}, local {local[:12]} "
            f"({local_path}). Stage the build (gbx:data:stage-wheel --env <N>), or rebuild it"
        ]
    return []


def wheel_problems(w, wheel: str, root) -> List[str]:
    """Compare the wheel staged at `wheel` (a Volume path) with the local build."""
    local = local_wheel(root, os.path.basename(wheel))
    if local is None:
        return check_wheel(None, "", wheel)
    return check_wheel(local, staged_wheel_sha(w, wheel), wheel)


def local_corpus(root) -> Optional[str]:
    """A local bench corpus for the tile inputs of the probe run, or None (the tile forms are then skipped)."""
    env = os.environ.get("GBX_BENCH_LOCAL_CORPUS")
    candidates = [env] if env else []
    root = pathlib.Path(root)
    candidates += [str(root / "sample-data/Volumes/main/default/bench-corpus")]
    src_root = os.environ.get("GBX_SOURCE_ROOT")
    if src_root:
        candidates.append(str(pathlib.Path(src_root) / "sample-data/Volumes/main/default/bench-corpus"))
    # a linked worktree has an empty mount point: the main checkout's corpus is the one on disk
    try:
        common = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True, text=True, timeout=20,
        ).stdout.strip()  # fmt: skip
        if common:
            candidates.append(str(pathlib.Path(common).parent / "sample-data/Volumes/main/default/bench-corpus"))
    except (OSError, subprocess.SubprocessError):
        pass
    return next((c for c in candidates if c and (pathlib.Path(c) / "rows").is_dir()), None)


def probe_problems(mvp: str, scales: Sequence[int], root, *, rows: int = 8) -> List[str]:
    """The levels set's SQL on a local Spark session with a handful of rows, and its client-side input sizes."""
    from databricks.labs.gbx.bench import preflight

    problems = list(preflight.client_cap_problems(mvp, scales))
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-bench-preflight")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")  # the tile forms hold a few 4 MiB tiles per task; 1g ran out
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        failures, skipped = preflight.probe_failures(spark, mvp, local_corpus(root) or "/nonexistent", rows=rows)
    except Exception as exc:  # noqa: BLE001 - a probe that cannot run is a launch problem, not a crash
        failures, skipped = [f"the local probe run itself failed: {type(exc).__name__}: {str(exc)[:300]}"], []
    finally:
        try:
            spark.stop()
        except Exception:  # noqa: BLE001 - a JVM that died has nothing to stop
            pass
    problems += [f"probe SQL cannot run: {f}" for f in failures]
    if skipped:
        print(f"  preflight: {len(skipped)} form(s) not runnable locally (JAR, Unity Catalog or no local corpus)")
    return problems
