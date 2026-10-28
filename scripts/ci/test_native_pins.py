"""Tests for scripts/native/pins.py: the one pin file for the native stack and the audit that fails on drift.

Pure Python, no Docker and no network. The audit reads the repository (or a scratch copy of the files it
audits, via --root); the environment check reads the JSON that scripts/ci/venv_env_report.py collects.
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import REPO_ROOT

PINS = REPO_ROOT / "scripts" / "native" / "pins.py"
BOUND = ("rasterio", "pyogrio", "pyproj", "shapely", "netcdf4")


def pins(*args):
    return subprocess.run(
        [sys.executable, str(PINS), *args], capture_output=True, text=True
    )


def pin(key: str) -> str:
    res = pins("get", key)
    assert res.returncode == 0, res.stderr
    return res.stdout.strip()


def audited_copy(tmp_path: Path) -> Path:
    """The pin file, the toolchain file, the legacy list and every file the legacy list names."""
    root = tmp_path / "tree"
    paths = ["scripts/native/pins.conf", "scripts/native/legacy-consumers.txt"]
    paths.append("rust-toolchain.toml")
    for line in (
        (REPO_ROOT / "scripts/native/legacy-consumers.txt").read_text().splitlines()
    ):
        if line.strip() and not line.startswith("#"):
            paths.append(line.split("|")[0])
    for rel in sorted(set(paths)):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, root / rel)
    return root


def test_the_repository_passes_the_audit():
    res = pins("audit", "--check")
    assert res.returncode == 0, res.stdout + res.stderr
    shell = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts/commands/gbx-versions-audit.sh"),
            "--native",
            "--check",
        ],
        capture_output=True,
        text=True,
    )
    assert shell.returncode == 0, shell.stdout + shell.stderr


@pytest.mark.parametrize(
    "rel, change",
    [
        ("rust-toolchain.toml", (r'channel = "[^"]+"', 'channel = "0.0.1"')),
        (
            "scripts/docker/Dockerfile",
            (r"(?m)^ARG GDAL_VERSION=\S+", "ARG GDAL_VERSION=9.9.9"),
        ),
        ("scripts/native/pins.conf", (r"(?m)^(GDAL_SHA256=).*", r"\1<")),
    ],
    ids=["rust-channel", "legacy-gdal-arg", "checksum"],
)
def test_drift_is_reported(tmp_path, rel, change):
    root = audited_copy(tmp_path)
    assert (
        pins("audit", "--check", "--root", str(root)).returncode == 0
    )  # the copy matches first
    target = root / rel
    target.write_text(re.sub(change[0], change[1], target.read_text(), count=1))

    res = pins("audit", "--check", "--root", str(root))

    assert res.returncode == 1, res.stdout
    assert rel in res.stdout + res.stderr


def pinned_report() -> dict:
    """What venv_env_report.py collects, for an environment that is exactly what the pins say."""
    return {
        "python": pin("PYTHON_VERSION"),
        "packages": {n: pin(f"{n.upper()}_VERSION") for n in BOUND},
        "natives": {k: pin(f"{k.upper()}_VERSION") for k in ("gdal", "proj", "geos")},
    }


def drifted(report, field, name, value):
    if field == "python":
        report["python"] = value
    elif value is None:
        del report[field][name]
    else:
        report[field][name] = value
    return report


@pytest.mark.parametrize(
    "field, name, value",
    [
        (None, None, None),  # matches the pins
        ("python", None, "3.12.11"),
        ("natives", "gdal", "3.12.1"),
        ("natives", "geos", "3.13.1"),
        ("packages", "pyproj", "3.7.2"),
        ("packages", "shapely", None),  # not installed at all
    ],
    ids=["match", "python", "gdal", "geos", "package", "missing-package"],
)
def test_an_environment_is_checked_against_the_pins(tmp_path, field, name, value):
    report = pinned_report()
    if field:
        drifted(report, field, name, value)
    path = tmp_path / "env.json"
    path.write_text(json.dumps(report))

    res = pins("env-check", "--check", f"host={path}")

    if field is None:
        assert res.returncode == 0, res.stdout + res.stderr
    else:
        assert res.returncode == 1, res.stdout
        assert (name or "python") in res.stdout
