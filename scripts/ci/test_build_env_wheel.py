"""gbx:build:wheel's pure rules (scripts/native/build_env_wheel.py, scripts/native/redact.py).

- requires-python: the env wheels' range must admit only Pythons the committed range admits (the payload is cp312-only).
- the version stamp: exactly one `__version__`, pre-releases kept, `+env<N>` appended.
- a vendored package's requirement on a core.txt package is checked by version, not name only.
- redaction: no index URL or host survives, with or without a scheme (fake hosts only).
"""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

NATIVE = Path(__file__).resolve().parents[1] / "native"
sys.path.insert(0, str(NATIVE))
_spec = importlib.util.spec_from_file_location(
    "build_env_wheel", NATIVE / "build_env_wheel.py"
)
bew = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bew)
import redact  # noqa: E402


@pytest.mark.parametrize(
    "env_range, core_range, ok",
    [
        (">=3.12,<3.13", ">=3.10, <3.13", True),
        (">=3.10, <3.13", ">=3.10, <3.13", True),
        (">=3.11,<3.13", ">=3.12,<3.13", False),
        (">=3.12", ">=3.10, <3.13", False),
        (">=3.14,<3.13", ">=3.10, <3.13", False),  # admits nothing
    ],
)
def test_requires_python_is_a_subset(env_range, core_range, ok):
    assert bew.requires_python_subset(env_range, core_range) is ok


@pytest.mark.parametrize(
    "written, env, stamped",
    [
        ("0.6.0", 6, "0.6.0+env6"),
        ("0.6.0rc1", 5, "0.6.0rc1+env5"),
        ("0.6.0.dev2", 6, "0.6.0.dev2+env6"),
    ],
)
def test_the_version_stamp_keeps_a_prerelease(written, env, stamped):
    text = f'"""doc"""\n__version__ = "{written}"\nX = 1\n'
    new, version = bew.stamp_version(text, env)
    assert (
        version == stamped
        and f'__version__ = "{stamped}"' in new
        and new.count("__version__") == 1
    )
    with pytest.raises(SystemExit):
        bew.stamp_version(text + f'__version__ = "{written}"\n', env)


@pytest.mark.parametrize(
    "vendored_req, core_req, base, ok",
    [
        ("protobuf (>=6.31.1,<7.0.0)", "protobuf>=6.31.1,<7", "6.33.5", True),
        (
            "protobuf (>=6.31.1,<7.0.0)",
            "protobuf>=6.31.1",
            None,
            False,
        ),  # core lets pip pass the cap
        (
            "protobuf (>=6.31.1,<7.0.0)",
            "protobuf>=5.26.1,<6",
            "5.29.4",
            False,
        ),  # base below the floor
        ("pydantic~=2.0", "pydantic>=2,<3", None, True),
        ("numpy>=2", "numpy>=2.1.3,<2.2", "2.1.3", True),
        ("click>=4.0", None, "8.1.8", True),  # rely-on-base: the runtime's version
        (
            "typing-extensions>=4.10.0",
            "typing_extensions",
            None,
            False,
        ),  # no floor to check
    ],
)
def test_a_vendored_requirement_is_checked_by_version(vendored_req, core_req, base, ok):
    run = lambda: bew.check_against_core(  # noqa: E731
        "pkg",
        Requirement(vendored_req),
        core_req and Requirement(core_req),
        base and Version(base),
    )
    if ok:
        run()
    else:
        with pytest.raises(SystemExit):
            run()


FAKE = [
    "Looking in indexes: https://user:secret@fake-index.example.com/api/pypi/simple",
    "WARNING: Retrying ... HTTPSConnectionPool(host='fake-index.example.com', port=443): Max retries",
    "Connection to fake-index.example.com timed out. (connect timeout=15)",
    "error: Failed to fetch: `fake-index.example.com/simple/numpy/`",
    "dns error: failed to lookup address information for fake-index.internal: nodename nor servname",
]


def test_no_index_host_survives_redaction():
    for line in FAKE:
        assert "fake-index" not in redact.redact(line), line
    for keep in (
        "geobrix-0.6.0+env6-py3-none-any.whl databricks.labs.gbx 4462389585 labs@databricks.com",
        "ModuleNotFoundError: No module named 'rasterio.io'",  # a module path, not a host
    ):
        assert redact.redact(keep) == keep
    piped = subprocess.run(
        [sys.executable, str(NATIVE / "redact.py")],
        input="\n".join(FAKE) + "\n",
        capture_output=True,
        text=True,
    )
    assert (
        piped.returncode == 0
        and "fake-index" not in piped.stdout
        and piped.stdout.count("\n") == len(FAKE)
    )


def _osgeo_trees(root, extra):
    """Both architectures' gdal-native python/ (osgeo_utils, with the left-out scripts) and a
    payload whose _vendor/<arch>/osgeo is in place; ``extra`` = (rel path, bytes) added to both."""
    natives, pkg = {}, root / "pkg"
    for a in bew.ARCHES:
        py = root / a / "python"
        (py / "osgeo_utils" / "samples").mkdir(parents=True)
        (py / "osgeo_utils" / "__init__.py").write_bytes(b"# SPDX-License-Identifier: MIT\n")
        for name in bew.OSGEO_UTILS_LEFT_OUT:
            (py / "osgeo_utils" / name).write_bytes(b"# GNU Library General Public License\n")
        vendor = pkg / "_vendor" / a / "osgeo"
        vendor.mkdir(parents=True)
        (vendor / "gdal.py").write_bytes(b"# SWIG-generated\n")
        (vendor / "_gdal.so").write_bytes(b"\x7fELF\x02\x01 libgdal-0a1b2c3d.so.39")
        if extra:
            rel, data = extra
            out = (pkg / "_vendor" / a / rel) if rel.startswith("osgeo/") else (py / rel)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
        natives[a] = root / a
    return natives, pkg


@pytest.mark.parametrize(
    "extra, refused",
    [
        (None, False),
        (("osgeo_utils/samples/x.py", b"# SPDX-License-Identifier: AGPL-3.0-only\n"), True),
        (("osgeo_utils/y.py", b"# Licensed under GPLv3.\n"), True),
        (("osgeo_utils/data/notice.txt", b"GNU General Public License v2\n"), True),
        (("osgeo/_ogr.so", b"\x7fELF\x02\x01 GPL-2.0-or-later"), True),
    ],
)
def test_no_gpl_family_file_ships_with_osgeo(tmp_path, capsys, extra, refused):
    natives, pkg = _osgeo_trees(tmp_path, extra)
    if not refused:
        bew.add_osgeo_utils(natives, pkg)
        shipped = {p.name for p in (pkg / "_vendor" / "common" / "osgeo_utils").iterdir()}
        assert shipped == {"__init__.py"}  # the left-out scripts are not copied on
        return
    with pytest.raises(SystemExit):
        bew.add_osgeo_utils(natives, pkg)
    assert "BUILD FAILED" in capsys.readouterr().out


def _kernel_tree(root, vendored, overrides):
    """A kernel crate rust/k ([lib] name _k) locking the crates `vendored` names, their vendored sources
    (with the licence files listed), override licence files, and a payload carrying rs/_k.abi3.so."""
    crate = root / "rust" / "k"
    crate.mkdir(parents=True)
    (crate / "Cargo.toml").write_text('[package]\nname = "k"\n\n[lib]\nname = "_k"\n')
    reg = "registry+https://github.com/rust-lang/crates.io-index"
    lock = ['version = 4\n\n[[package]]\nname = "k"\nversion = "0.1.0"\n']
    for name, files in vendored.items():
        lock.append(f'[[package]]\nname = "{name}"\nversion = "1.0.0"\nsource = "{reg}"\n')
        src = root / "vendor" / "k" / f"{name}-1.0.0"
        src.mkdir(parents=True)
        (src / "Cargo.toml").write_text(f'[package]\nname = "{name}"\nlicense = "MIT"\n')
        (src / "lib.rs").write_text("")
        for f in files:
            (src / f).write_text(f"{name} licence\n")
    (crate / "Cargo.lock").write_text("\n".join(lock))
    for name, files in overrides.items():
        (root / "overrides" / name).mkdir(parents=True)
        for f in files:
            (root / "overrides" / name / f).write_text(f"{name} upstream licence\n")
    nat = root / "pkg" / "_native"
    (nat / "aarch64" / "rs").mkdir(parents=True)
    (nat / "aarch64" / "rs" / "_k.abi3.so").write_bytes(b"\x7fELF")
    return nat


@pytest.mark.parametrize(
    "vendored, overrides, refused",
    [
        ({"a": ["LICENSE-MIT", "LICENSE-APACHE"], "b": ["COPYING"]}, {}, None),
        ({"a": ["LICENSE-MIT"], "b": ["README.md"]}, {}, "no licence file for b 1.0.0"),
        # a crate whose package ships no licence file (gdal-sys): its upstream text, kept in the repo
        ({"a": ["LICENSE-MIT"], "b": []}, {"b": ["LICENSE.txt"]}, None),
    ],
    ids=["shipped", "missing", "override"],
)
def test_a_rust_kernel_ships_its_crates_licence_notices(
    tmp_path, capsys, vendored, overrides, refused
):
    nat = _kernel_tree(tmp_path, vendored, overrides)
    run = lambda: bew.add_rust_licences(  # noqa: E731
        nat, tmp_path / "vendor", tmp_path, tmp_path / "overrides"
    )
    if refused:
        with pytest.raises(SystemExit):
            run()
        assert refused in capsys.readouterr().out
        return
    run()
    out = nat / "licenses" / "rust" / "k"
    shipped = {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}
    want = {f"{n}-1.0.0/{f}" for n, fs in {**vendored, **overrides}.items() for f in fs}
    assert shipped == want | {"INDEX.txt"}
    assert "b 1.0.0 (MIT): " in (out / "INDEX.txt").read_text()


@pytest.mark.parametrize(
    "lock",
    sorted((bew.REPO / "rust").glob("*/Cargo.lock")),
    ids=lambda p: p.parent.name,
)
def test_every_rust_crate_has_a_licence_file_for_each_crate_it_locks(tmp_path, lock):
    # Any crate under rust/ can ship as a kernel (the probe in test wheels): every crate its Cargo.lock
    # locks needs a licence file, or the wheel build stops.
    vendor = bew.RUST_VENDOR / lock.parent.name
    if not vendor.is_dir():
        pytest.skip(
            f"PRECONDITION MISSING: {vendor} (scripts/native/cargo-vendor.sh "
            f"{lock.parent.relative_to(bew.REPO)} {vendor.relative_to(bew.REPO)})"
        )
    assert bew.collect_rust_licences(lock.parent, vendor, tmp_path / "out") == []
