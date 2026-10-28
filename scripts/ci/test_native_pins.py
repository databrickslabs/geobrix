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
    """The pin files, the toolchain file, the legacy list, every file the legacy list names, and the crates' locks."""
    root = tmp_path / "tree"
    paths = ["scripts/native/pins.conf", "scripts/native/rust-pins.conf"]
    paths += ["scripts/native/legacy-consumers.txt", "rust-toolchain.toml"]
    paths += [str(p.relative_to(REPO_ROOT)) for p in REPO_ROOT.glob("rust/*/Cargo.lock")]
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
        # a half-done bump: the version moved, its URL, file name and checksum did not
        ("scripts/native/pins.conf", (r"(?m)^(GDAL_VERSION=).*", r"\g<1>9.9.9")),
        # a crate pin in pins.conf would rebuild GDAL on every bump
        (
            "scripts/native/pins.conf",
            (r"(?m)^(MATURIN_VERSION=.*)$", r"\1\nCRATE_GEOS_SYS_VERSION=2.0.10"),
        ),
        # a pin no Cargo.lock locks any more
        (
            "scripts/native/rust-pins.conf",
            (r"(?m)^(CRATE_GEOS_VERSION=.*)$", r"\1\nCRATE_GDAL_VERSION=0.19.0"),
        ),
    ],
    ids=[
        "rust-channel",
        "legacy-gdal-arg",
        "checksum",
        "version-without-url",
        "crate-pin-in-pins-conf",
        "stale-crate-pin",
    ],
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


CARGO_ROWS = {
    "pyo3-as-a-table": (
        {
            "crate/Cargo.toml": '[package]\nname = "x"\n\n[dependencies.pyo3]\nversion = "0.29.0"\n'
        },
        "crate/Cargo.toml",
    ),
    "inherited-but-never-pinned": (
        {
            "Cargo.toml": '[workspace]\nmembers = ["crate"]\n',
            "crate/Cargo.toml": "[dependencies]\npyo3.workspace = true\n",
        },
        "crate/Cargo.toml",
    ),
    "inherited-and-pinned": (
        {
            "Cargo.toml": '[workspace.dependencies]\npyo3 = { version = "={PYO3}", features = ["abi3"] }\n',
            "crate/Cargo.toml": "[dependencies]\npyo3.workspace = true\n",
        },
        None,
    ),
    "maturin-backend-before-requires": (
        {
            "py/pyproject.toml": '[build-system]\nbuild-backend = "maturin"\nrequires = ["maturin=={MATURIN}"]\n'
        },
        None,
    ),
    # a georust crate locked at another version than its CRATE_*_VERSION pin (a `cargo update` moved it)
    "lock-off-the-crate-pin": (
        {
            "crate/Cargo.lock": 'version = 4\n\n[[package]]\nname = "gdal-sys"\nversion = "0.11.0"\n'
        },
        "crate/Cargo.lock",
    ),
}


@pytest.mark.parametrize("files, drift", CARGO_ROWS.values(), ids=CARGO_ROWS.keys())
def test_crates_and_maturin_projects_are_audited_in_every_toml_form(
    tmp_path, files, drift
):
    root = audited_copy(tmp_path)
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        text = text.replace("{PYO3}", pin("PYO3_VERSION")).replace(
            "{MATURIN}", pin("MATURIN_VERSION")
        )
        (root / rel).write_text(text)

    res = pins("audit", "--check", "--root", str(root))

    if drift:
        assert res.returncode == 1 and drift in res.stdout, res.stdout
    else:
        assert res.returncode == 0, res.stdout


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


TAG_INPUTS = (
    "scripts/native/pins.conf",
    "scripts/native/build.Dockerfile",
    "scripts/native/build-requirements-aarch64.txt",
    "scripts/native/cargo-offline.toml",
    "rust-toolchain.toml",
)


@pytest.mark.parametrize(
    "change, moves",
    [
        ((r"(?m)^(# Native stack pins:)", r"\1 (reworded)"), False),
        ((r"(?m)^(GDAL_VERSION=.*)$", r"\n\n\1   "), False),  # blank lines, trailing space
        ((r"(?m)^GBX_NATIVE_REVISION=\d+", "GBX_NATIVE_REVISION=99"), True),
    ],
    ids=["comment", "blank-lines", "value"],
)
def test_only_a_pin_value_moves_the_image_tag_and_the_staged_pins(tmp_path, change, moves):
    # The image tag and the pins copied into the deps stage are the GDAL build's cache key: a comment edit must
    # rebuild nothing, a value edit must rebuild.
    root = tmp_path / "tag"
    for rel in TAG_INPUTS:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, root / rel)
    key = lambda: (  # noqa: E731
        pins("tag-hash", "aarch64", "--root", str(root)).stdout,
        pins("canonical", "--root", str(root)).stdout,
    )
    before = key()
    target = root / "scripts/native/pins.conf"
    target.write_text(re.sub(change[0], change[1], target.read_text(), count=1))
    after = key()

    assert re.fullmatch(r"[0-9a-f]{12}\n", before[0]) and "GDAL_VERSION=" in before[1]
    assert (after[0] != before[0], after[1] != before[1]) == (moves, moves)
    command = (REPO_ROOT / "scripts/commands/gbx-build-native.sh").read_text()
    assert "pins.py tag-hash" in command and "pins.py canonical" in command


def test_the_container_build_stays_offline_on_vendored_crates():
    # The build container compiles Rust from /vendor only. If either setting were lost, cargo would quietly
    # reach for a registry (or fail only on a machine without network), and the offline guarantee would be gone.
    config = REPO_ROOT / "scripts/native/cargo-offline.toml"
    lines = [row.split("#")[0].strip() for row in config.read_text().splitlines()]

    assert "[net]" in lines and "offline = true" in lines
    assert "[source.crates-io]" in lines
    assert 'replace-with = "vendored-sources"' in lines
    assert 'directory = "/vendor"' in lines
