"""gbx:build:wheel's pure rules (scripts/native/build_env_wheel.py, scripts/native/redact.py).

- requires-python: the env wheels' range must admit only Pythons the committed range admits (the payload is cp312-only).
- the version stamp: exactly one `__version__`, pre-releases kept, `+env<N>` appended.
- a vendored package's requirement on a core.txt package is checked by version, not name only.
- redaction: no index URL or host survives, with or without a scheme (fake hosts only).
"""

import importlib.util
import os
import shutil
import subprocess
import sys
import zipfile
from email.parser import Parser
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
    # the Rust standard library is compiled into every kernel: its MIT / Apache-2.0 notice ships too
    std = bew.rust_std_version()
    want |= {
        f"rust-std-{std}/{f}"
        for f in ("LICENSE-MIT", "LICENSE-APACHE", "NOTICE.txt", "COPYRIGHT-library.html")
    }
    assert shipped == want | {"INDEX.txt"}
    assert "b 1.0.0 (MIT): " in (out / "INDEX.txt").read_text()
    assert f"rust-std {std} (MIT OR Apache-2.0): " in (out / "INDEX.txt").read_text()


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


def test_the_extracted_payload_keeps_executables_executable(tmp_path):
    # GBX_NATIVE_ROOT payloads (the dev container) run bin/projsync from the extracted tree, as pip's install does
    whl = tmp_path / "w.whl"
    with zipfile.ZipFile(whl, "w") as zf:
        for name, mode in (("bin/projsync", 0o755), ("lib/libx.so.1", 0o644)):
            info = zipfile.ZipInfo(f"{bew.PKG}/_native/x86_64/{name}")
            info.external_attr = (0o100000 | mode) << 16
            zf.writestr(info, b"\x7fELF")
    with zipfile.ZipFile(whl) as zf:
        bew.extract_payload(zf, tmp_path / "payload")
    native = tmp_path / "payload" / "_native" / "x86_64"
    assert os.access(native / "bin" / "projsync", os.X_OK)
    assert not os.access(native / "lib" / "libx.so.1", os.X_OK)


@pytest.mark.parametrize(
    "rel, pruned",
    [
        ("rasterio/_io.c", True),  # Cython-generated C and C++: build inputs only
        ("rasterio/_features.cpp", True),
        ("rasterio/_base.pyx", True),
        ("rasterio/_base.pxd", True),
        ("pyproj/proj.pxi", True),
        ("pyogrio/tests/conftest.py", True),  # a vendored package's own test suite and fixtures
        ("shapely/tests/geometry/test_point.py", True),
        ("rasterio/_io.cpython-312-aarch64-linux-gnu.so", False),
        ("rasterio/rio/main.py", False),
        ("shapely/testing.py", False),  # imported by users' tests: a module, not the suite
        ("pyogrio/_io.pyi", False),
        ("osgeo_utils/samples/validate_cloud_optimized_geotiff.py", False),
        ("netCDF4/__init__.py", False),
    ],
)
def test_the_vendored_trees_drop_what_no_runtime_imports(rel, pruned):
    assert bew.not_shipped(rel) is pruned


def test_gdals_own_libtiff_and_libgeotiff_are_in_the_licence_index(tmp_path):
    # GDAL is built with its internal libtiff and libgeotiff: the index says so, and names their licences
    lic = tmp_path / "licenses"
    for d in ("gdal", "libtiff", "libgeotiff"):
        (lic / d).mkdir(parents=True)
        (lic / d / "LICENSE").write_text(f"{d} licence\n")
    # the versions are MANIFEST.json's gdal_internal (what libgdal compiled in), which must name exactly both
    bew.add_gdal_internal_note(lic, {"libtiff": "4.7.2", "libgeotiff": "1.7.4"})
    note = (lic / "gdal" / "INTERNAL-COPIES.txt").read_text()
    assert "libtiff 4.7.2" in note and "libgeotiff 1.7.4" in note and "../libgeotiff/" in note
    for internal in (None, {"libtiff": "4.7.2"}, {"libtiff": "4.7.2", "libgeotiff": "1.7.4", "zlib": "1.3.2"}):
        with pytest.raises(SystemExit):  # a gdal-native from before the key, or a key that drifted
            bew.add_gdal_internal_note(lic, internal)
    (lic / "libgeotiff" / "LICENSE").unlink()  # the internal copy's notice must not go missing silently
    with pytest.raises(SystemExit):
        bew.add_gdal_internal_note(lic, {"libtiff": "4.7.2", "libgeotiff": "1.7.4"})


def test_the_rust_std_notice_is_the_pinned_toolchains():
    # where the pinned toolchain is installed on this host, the committed notice is its own file
    std = bew.rust_std_version()
    home = Path(os.environ.get("GBX_HOST_RUSTUP_HOME", Path.home() / ".cache/geobrix/rustup"))
    docs = sorted(home.glob(f"toolchains/{std}-*/share/doc/rust/COPYRIGHT-library.html"))
    if not docs:
        pytest.skip(f"PRECONDITION MISSING: the Rust {std} toolchain (scripts/native/install-host-rust.sh)")
    assert (bew.RUST_STD_NOTICE / "COPYRIGHT-library.html").read_bytes() == docs[0].read_bytes()


# --- the feature extras: each env's set, and their rules ----------------------------------------------------------

# What the env wheel vendors, by distribution name: the five bindings, the vendor-pure packages, osgeo's GDAL.
VENDORED = {
    "rasterio",
    "pyogrio",
    "pyproj",
    "shapely",
    "netcdf4",
    "rio-tiler",
    "morecantile",
    "mapbox-vector-tile",
    "geopandas",
    "contextily",
    "gdal",
}


def _env_copy(tmp_path, env):
    src = bew.PYDIR / "envs" / f"env{env}"
    dst = tmp_path / f"env{env}"
    dst.mkdir()
    for f in src.glob("*.txt"):
        (dst / f.name).write_text(f.read_text())
    return dst


@pytest.mark.parametrize("env", [5, 6])
def test_each_committed_extra_moves_nothing_and_names_no_vendored_package(env):
    envdir = bew.PYDIR / "envs" / f"env{env}"
    names = sorted(p.stem.removeprefix("extra-") for p in envdir.glob("extra-*.txt"))
    assert tuple(names) == bew.EXTRAS[env]
    assert ("zarr" in names) == (env == 6)  # Zarr targets DBR 19 / env 6 only
    pure = {
        bew.canonicalize_name(ln.split("==")[0])
        for ln in (envdir / "vendor-pure.txt").read_text().splitlines()
        if ln.strip() and not ln.startswith("#")
    }
    assert pure <= VENDORED
    bew.check_extras(env, VENDORED)  # passes on the committed files
    # each extra that names a preinstalled package holds it (the runtime's version is admitted), except a move up to a
    # security floor that the runtime's version lacks
    base = bew.pins_of(envdir / "base.txt")
    for name in names:
        for r in bew.reqs_of(envdir / f"extra-{name}.txt"):
            dep = bew.canonicalize_name(r.name)
            assert dep not in VENDORED, (name, dep)
            floor = bew.SECURITY_FLOORS.get(dep)
            if dep in base and not (floor and base[dep] < bew.Version(floor[0])):
                assert r.specifier.contains(base[dep]), (name, str(r), str(base[dep]))


@pytest.mark.parametrize("env, cap", [(5, "<2.33"), (6, None)])
def test_stac_pins_the_requests_security_floor(env, cap):
    # CVE-2024-47081 (requests < 2.32.4 leaks ~/.netrc credentials to another host); gbx.stac fetches catalog hrefs.
    # env5's frozen bases get a patch move (cap <2.33); env6's runtimes already have 2.32.5: no cap.
    (r,) = [
        r
        for r in bew.reqs_of(bew.PYDIR / "envs" / f"env{env}" / "extra-stac.txt")
        if r.name == "requests"
    ]
    assert not r.specifier.contains("2.32.3") and r.specifier.contains("2.32.5"), str(r)
    assert sorted(str(s) for s in r.specifier if s.operator.startswith("<")) == ([cap] if cap else [])
    assert bew.SECURITY_FLOORS["requests"][0] == "2.32.4"
    assert bew.FROZEN_BASE_CAPS == {5: {"requests": "<2.33"}}


@pytest.mark.parametrize("env", [5, 6])
def test_no_extra_caps_a_package_the_runtimes_preinstall(env):
    envdir = bew.PYDIR / "envs" / f"env{env}"
    base = bew.pins_of(envdir / "base.txt")
    for f in sorted(envdir.glob("extra-*.txt")):
        for r in bew.reqs_of(f):
            dep = bew.canonicalize_name(r.name)
            caps = [str(s) for s in r.specifier if s.operator in ("<", "<=", "==", "~=")]
            if dep in base:
                assert caps in ([], [bew.FROZEN_BASE_CAPS.get(env, {}).get(dep)]), (f.name, str(r))


@pytest.mark.parametrize(
    "env, file, line, refused",
    [
        (6, "extra-stac.txt", "requests>=2.34.2", "would move it"),  # the plain [stac]'s floor: past the fix
        (5, "extra-stac.txt", "requests>=2.32.3", "needs a floor of at least 2.32.4"),  # CVE-2024-47081
        (6, "extra-stac.txt", "requests>=2.32.5,<2.33", "so no cap"),  # an extra caps no base package but FROZEN_BASE_CAPS
        (5, "extra-stac.txt", "requests>=2.32.4,<2.34", "so no cap"),  # only the documented frozen-base cap
        (5, "extra-databricks.txt", "databricks-sdk>=0.49.0,<0.68", "so no cap"),
        (6, "extra-stac.txt", "tenacity>=9.1.2,<9.2", "so no cap"),
        (6, "extra-vizx.txt", "matplotlib>=3.10.6,<4", "so no cap"),
        (6, "extra-databricks.txt", "requests>=2.0", "needs a floor of at least 2.32.4"),  # any extra naming it
        (5, "extra-databricks.txt", "databricks-sdk>=0.67", "would move it"),  # above DBR 17.3's 0.49.0
        (6, "extra-zarr.txt", "shapely>=2", "which the wheel vendors"),
        (5, "extra-stac.txt", "GDAL==3.13.3", "which the wheel vendors"),  # osgeo's distribution
        (6, "extra-overture.txt", "overturemaps>=1.0.2,<2", "declares"),  # not one of EXTRAS[6]
        (5, "extra-zarr.txt", "zarr>=3.4.1,<3.5", "declares"),  # env6 only
    ],
)
def test_an_extra_that_moves_vendors_or_is_unexpected_is_refused(tmp_path, capsys, env, file, line, refused):
    envdir = _env_copy(tmp_path, env)
    with open(envdir / file, "a") as fh:
        fh.write(line + "\n")
    with pytest.raises(SystemExit):
        bew.check_extras(env, VENDORED, envdir)
    assert refused in capsys.readouterr().out


def test_a_missing_extra_is_refused(tmp_path, capsys):
    envdir = _env_copy(tmp_path, 6)
    (envdir / "extra-zarr.txt").unlink()
    with pytest.raises(SystemExit):
        bew.check_extras(6, VENDORED, envdir)
    assert "declares" in capsys.readouterr().out


def _metadata(env, version, extras):
    """A METADATA as setuptools writes it for the env wheel: core.txt unmarked, one marked block per extra,
    the tripwires."""
    envdir = bew.PYDIR / "envs" / f"env{env}"
    template = bew.tomllib.loads((bew.PYDIR / "envs" / "pyproject.env.toml").read_text())["project"]
    lines = [
        "Metadata-Version: 2.4",
        "Name: geobrix",
        f"Version: {version}",
        f"Requires-Python: {template['requires-python']}",
    ]
    lines += [f"Requires-Dist: {r}" for r in bew.reqs_of(envdir / "core.txt")]
    other = 11 - env
    for name in (*extras, *bew.TRIPWIRES[env]):
        lines.append(f"Provides-Extra: {name}")
        reqs = (
            bew.reqs_of(envdir / f"extra-{name}.txt")
            if name in extras
            else [Requirement(f"geobrix=={version.split('+')[0]}+env{other}")]
        )
        lines += [f'Requires-Dist: {r}; extra == "{name}"' for r in reqs]
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize("env", [5, 6])
def test_the_wheel_metadata_carries_exactly_the_envs_extras(tmp_path, capsys, env):
    version = f"0.6.0+env{env}"

    def check(extras):
        whl = tmp_path / f"w{len(extras)}.whl"
        with zipfile.ZipFile(whl, "w") as zf:
            zf.writestr(f"geobrix-{version}.dist-info/METADATA", _metadata(env, version, extras))
        with zipfile.ZipFile(whl) as zf:
            bew.verify_metadata(zf, env, version, extras)

    check(bew.EXTRAS[env])  # every extra, each as its file says: passes
    for dropped in bew.EXTRAS[env]:
        with pytest.raises(SystemExit):  # an extra missing from the wheel (and its file list) is caught
            check(tuple(e for e in bew.EXTRAS[env] if e != dropped))
        assert "declares" in capsys.readouterr().out


# --- --verify check 1 (scripts/native/base_held.py) ------------------------------------------------------------

_bh_spec = importlib.util.spec_from_file_location("base_held", NATIVE / "base_held.py")
base_held = importlib.util.module_from_spec(_bh_spec)
_bh_spec.loader.exec_module(base_held)


def _installed(env, **changes):
    """A version() lookup: every base.txt package at its base version, then `changes` (None = not installed)."""
    have = dict(base_held.pins(bew.PYDIR / "envs" / f"env{env}" / "base.txt"))
    have.update({k.replace("_", "-"): v for k, v in changes.items()})

    def version(name):
        if have.get(name) is None:
            raise base_held.md.PackageNotFoundError(name)
        return have[name]

    return version


@pytest.mark.parametrize(
    "env, changes, verdict",
    [
        # the core install of env5: [stac]'s requests and tenacity and [databricks]'s SDK are not needed by it
        (5, {"requests": None, "tenacity": None, "databricks_sdk": None},
         "ok; not needed by the core install: requests, tenacity, databricks-sdk"),
        (6, {}, "ok"),
        (6, {"numpy": None}, "missing: numpy (base 2.3.4)"),  # core.txt needs numpy: a missing one fails
        (6, {"click": "8.5.0"}, "moved: click==8.5.0 (base 8.2.1)"),
        (5, {"requests": "2.32.5"}, "moved: requests==2.32.5 (base 2.32.3)"),  # an installed extra-only one still counts
    ],
)  # fmt: skip
def test_verify_check_1_lists_only_what_just_an_extra_holds(env, changes, verdict):
    envdir = bew.PYDIR / "envs" / f"env{env}"
    assert base_held.judge(envdir, version=_installed(env, **changes)) == verdict


def test_verify_check_1_runs_base_held():
    text = (NATIVE / "check-env-wheel.sh").read_text()
    assert '"$V/bin/python" /check/base_held.py /env' in text


# --- the plain wheel (the committed pyproject): off Databricks; the 0.5.x runtime extras are tripwires ----------

def _plain():
    return bew.tomllib.loads((bew.PYDIR / "pyproject.toml").read_text())["project"]


def test_the_plain_wheels_old_runtime_extras_are_tripwires():
    # Each old name requires the env wheel that replaces it, so `geobrix[light_env6] @ <plain wheel>` fails at
    # resolution naming geobrix==<version>+env6 instead of installing PyPI's GDAL stack on a Databricks runtime.
    extras, version = _plain()["optional-dependencies"], bew.source_version()
    for env, names in bew.OLD_EXTRAS.items():
        for name in names:
            assert extras.get(name) == [f"geobrix=={version}+env{env}"], (name, extras.get(name))
    light = {n for n in extras if n.startswith("light")}
    assert light == {n for names in bew.OLD_EXTRAS.values() for n in names}  # no light-base, no other light name
    assert bew.TRIPWIRES == {6: bew.OLD_EXTRAS[5], 5: bew.OLD_EXTRAS[6]}


def test_the_plain_wheels_base_is_the_light_tier_with_floors_only():
    # `pip install geobrix` gives a working light tier off Databricks: floors only, no runtime hold or cap.
    deps = [Requirement(d) for d in _plain()["dependencies"]]
    names = {bew.canonicalize_name(r.name) for r in deps}
    assert {"rasterio", "pyogrio", "pyproj", "shapely", "netcdf4", "rio-tiler", "typing-extensions"} <= names
    assert "pyspark" not in names  # every runtime brings its own
    for r in deps:
        assert r.marker is None and all(s.operator == ">=" for s in r.specifier), str(r)
    te = next(r for r in deps if bew.canonicalize_name(r.name) == "typing-extensions")
    assert str(te.specifier) == ">=4.13"  # rio-tiler 9's PEP 728 TypedDict(extra_items=...)


# --- the env wheels' requirements against the runtimes' preinstalled lists (envs/runtimes/) ------------------------

_im_spec = importlib.util.spec_from_file_location("install_moves", NATIVE / "install_moves.py")
install_moves = importlib.util.module_from_spec(_im_spec)
_im_spec.loader.exec_module(install_moves)


@pytest.mark.parametrize("env", [5, 6])
def test_base_txt_is_the_oldest_preinstalled_version_of_every_package_an_extra_names(env):
    envdir = bew.PYDIR / "envs" / f"env{env}"
    lists = [install_moves.pins(bew.PYDIR / "envs" / "runtimes" / f"{r}.txt") for r in install_moves.RUNTIMES[env]]
    base = bew.pins_of(envdir / "base.txt")
    for dep, v in base.items():
        shipped = [Version(rt[dep]) for rt in lists if dep in rt]
        assert shipped and v == min(shipped), (dep, str(v), sorted(map(str, shipped)))
    for f in sorted(envdir.glob("extra-*.txt")):
        for r in bew.reqs_of(f):
            dep = bew.canonicalize_name(r.name)
            if any(dep in rt for rt in lists):
                assert dep in base, (f.name, dep)  # so its floor-only / frozen-cap rule applies


MASKED = "click: 8.2.1 under the runtime's constraints, 8.5.0 with them as preferences only (a move the constraints mask)"


@pytest.mark.parametrize(
    "env, install, resolved, unconstrained, failure",
    [
        (6, "core", {"idna": "3.20"}, None, None),  # a documented move
        (6, "core", {"click": "8.5.0"}, None, "click 8.2.1 -> 8.5.0 is not a documented move"),
        (5, "core", {"requests": "2.32.5"}, None, "requests 2.32.3 -> 2.32.5 is not a documented move"),  # [stac]'s only
        (5, "full", {"requests": "2.32.5"}, None, None),
        (6, "full", {"rasterio": "1.5.2"}, None, "pulls PyPI's rasterio 1.5.2, which the wheel vendors"),
        (6, "full", {"geopandas": "1.2.0"}, None, "pulls PyPI's geopandas 1.2.0, which the wheel vendors"),
        # the constraints held click by backtracking another package; pip on the runtime would move it
        (6, "full", {"click": "8.2.1", "x": "1.0"}, {"click": "8.5.0", "x": "1.0"}, MASKED),
        (6, "full", {"click": "8.2.1"}, {"click": "8.2.1"}, None),
    ],
)  # fmt: skip
def test_an_install_may_move_only_a_documented_package_and_pull_nothing_vendored(
    env, install, resolved, unconstrained, failure
):
    runtime = install_moves.RUNTIMES[env][0]
    _moves, failures = install_moves.judge(env, install, runtime, resolved, unconstrained)
    assert failures == ([failure] if failure else [])


# What uv prints when it cannot reach the index (not when it cannot solve): anchored phrases, so a resolution error
# that names databricks-connect is not taken for a network failure.
INDEX_DOWN = ("failed to fetch", "dns error", "error sending request", "tcp connect error", "connection refused",
              "failed to connect", "operation timed out", "request failed after")  # fmt: skip


def _precondition(why):
    """In CI (the scripts/ci step installs uv and points it at the job's index) a missing precondition fails the row;
    on a workstation it skips."""
    (pytest.fail if os.environ.get("CI") else pytest.skip)(f"PRECONDITION MISSING: {why}; this row not run")


@pytest.mark.parametrize("arch", install_moves.ARCHES)
@pytest.mark.parametrize("install", ["core", "full"])
@pytest.mark.parametrize("runtime", [r for rs in install_moves.RUNTIMES.values() for r in rs])
def test_each_runtime_keeps_its_packages_but_the_documented_moves(runtime, install, arch):
    # uv resolves the install with the runtime's preinstalled list as constraints, and again with it as preferences
    # only (the two must agree: a difference is a move the constraints masked); the same 28 cases as
    # gbx:versions:audit --install-moves.
    if shutil.which("uv") is None:
        _precondition("uv")
    env = next(e for e, rs in install_moves.RUNTIMES.items() if runtime in rs)
    resolved, err, unconstrained = install_moves.resolve(env, install, runtime, arch)
    if resolved is None and any(m in err.lower() for m in INDEX_DOWN):
        _precondition(f"the package index ({err[:80]})")
    assert resolved is not None, err
    _moves, failures = install_moves.judge(env, install, runtime, resolved, unconstrained)
    assert failures == []


# --- the soft size budget (envs/size-budget.json) ---------------------------------------------------------------

BUDGET = {"budget_mb": 100, "justified": [{"env": 6, "ceiling_mb": 110, "reason": "a reason", "date": "2026-10-29"}]}


@pytest.mark.parametrize(
    "size, env, refused",
    [
        (90.1, 6, None),
        (104.0, 5, "with no recorded justification"),  # over budget, nothing recorded for env5
        (104.0, 6, None),  # recorded: passes and says why
        (111.0, 6, "past its justified ceiling of 110 MB"),  # grew past the recorded ceiling
    ],
)
def test_the_size_budget_is_soft_under_a_recorded_ceiling(capsys, size, env, refused):
    run = lambda: bew.size_gate(size, env, BUDGET, last=90.0)  # noqa: E731
    if refused:
        with pytest.raises(SystemExit):
            run()
        assert refused in capsys.readouterr().out
        return
    run()
    out = capsys.readouterr().out
    assert f"size {size:.1f} MB ({size - 90.0:+.1f} MB against the last build)" in out
    assert ("a reason" in out) == (size > 100)


def test_the_committed_size_budget_records_justifications():
    budget = bew.json.loads(bew.SIZE_BUDGET.read_text())
    assert budget["budget_mb"] == 100
    for j in budget["justified"]:
        assert {"env", "ceiling_mb", "reason", "date"} <= set(j) and j["ceiling_mb"] > budget["budget_mb"]


# --- small rules of the build: the security floor, the architectures' gdal_internal, the licence note -------------

@pytest.mark.parametrize(
    "line, refused",
    [
        ("requests>=2.32.4", False),
        ("requests==2.32.5", False),  # a pin above the fix is a floor too
        ("requests~=2.32.5", False),
        ("requests>=2.32.4,<2.33", False),
        ("requests>=2.32.3", True),
        ("requests<3", True),  # no lower bound at all
        ("requests>2.32.3", True),  # admits 2.32.3.1
    ],
)
def test_the_security_floor_refuses_a_specifier_that_admits_an_older_release(line, refused):
    assert bew.admits_below(Requirement(line).specifier, "2.32.4") is refused


def test_the_architectures_must_agree_on_gdals_internal_copies(tmp_path, capsys):
    natives = {}
    for a, tiff in zip(bew.ARCHES, ("4.7.2", "4.7.1")):
        natives[a] = tmp_path / a
        natives[a].mkdir()
        manifest = {"versions": {"gdal": "3.13.3", "proj": "9.9.0", "geos": "3.14.1"}, "native_revision": 1,
                    "gdal_internal": {"libtiff": tiff, "libgeotiff": "1.7.4"}}  # fmt: skip
        (natives[a] / "MANIFEST.json").write_text(bew.json.dumps(manifest))
    (tmp_path / "tree" / "src" / bew.PKG).mkdir(parents=True)
    with pytest.raises(SystemExit):
        bew.write_build_info(tmp_path / "tree", 6, "0.6.0+env6", natives, False, 0)
    assert "disagree on gdal_internal" in capsys.readouterr().out


def _payload_zip(path, note, licences):
    """A wheel holding one architecture's minimal payload up to the gdal_internal licence check (the other
    architecture's MANIFEST.json too); `note` is INTERNAL-COPIES.txt's text, `licences` the licence dirs present."""
    pkg = bew.PKG
    with zipfile.ZipFile(path, "w") as zf:
        for a in bew.ARCHES:
            manifest = {"sonames": {}, "jni": "libgdalalljni.so", "versions": {},
                        "gdal_internal": {"libtiff": "4.7.2", "libgeotiff": "1.7.4"}}  # fmt: skip
            zf.writestr(f"{pkg}/_native/{a}/MANIFEST.json", bew.json.dumps(manifest))
            zf.writestr(f"{pkg}/_native/{a}/lib/libgdalalljni.so", b"")
            for binding in ("rasterio", "osgeo"):
                zf.writestr(f"{pkg}/_vendor/{a}/{binding}/__init__.py", b"")
            for plugin in bew.FILTER_PLUGINS:
                zf.writestr(f"{pkg}/_native/{a}/hdf5-plugins/{plugin}", b"")
            info = zipfile.ZipInfo(f"{pkg}/_native/{a}/bin/projsync")
            info.external_attr = 0o100755 << 16
            zf.writestr(info, b"")
        for d in licences:
            zf.writestr(f"{pkg}/_native/licenses/{d}/LICENSE", b"licence")
        zf.writestr(f"{pkg}/_native/licenses/gdal/INTERNAL-COPIES.txt", note)


@pytest.mark.parametrize(
    "note, licences, refused",
    [
        ("  libtiff 4.7.2: ../libtiff/\n  libgeotiff 1.7.4: ../libgeotiff/\n", ("libtiff", "libgeotiff"), None),
        ("  libtiff 4.7.2: ../libtiff/\n", ("libtiff", "libgeotiff"), "libgeotiff 1.7.4 is not in licenses/gdal"),
        ("  libtiff 4.7.2: ../libtiff/\n  libgeotiff 1.7.4: ../libgeotiff/\n", ("libtiff",), "libgeotiff (1.7.4) has no licence"),
    ],
)  # fmt: skip
def test_the_wheel_names_and_licenses_gdals_internal_copies(tmp_path, capsys, note, licences, refused):
    whl = tmp_path / "w.whl"
    _payload_zip(whl, note, licences)
    with zipfile.ZipFile(whl) as zf, pytest.raises(SystemExit):
        bew.verify_payload(zf, "0.6.0+env6", 0)  # the minimal wheel stops at a later check when this one passes
    out = capsys.readouterr().out
    if refused:
        assert refused in out
    else:
        assert "GDAL's internal" not in out and "BUILD FAILED" in out


def test_contextilys_requirements_come_from_vizx(tmp_path, capsys):
    # contextily is vendored and usable with [vizx] only: geopy, mercantile and xyzservices are extra-vizx.txt's
    envdir = _env_copy(tmp_path, 6)
    meta = Parser().parsestr(
        "Name: contextily\nVersion: 1.6.2\n"
        + "".join(f"Requires-Dist: {d}\n" for d in ("geopy", "matplotlib", "mercantile", "pillow", "rasterio",
                                                      "requests", "joblib", "xyzservices"))
    )  # fmt: skip
    vendored = {"contextily": (Version("1.6.2"), meta), "rasterio": (Version("1.5.2"), Parser().parsestr("Name: r\n"))}
    bew.check_requirements(6, vendored, envdir)
    vizx = envdir / "extra-vizx.txt"
    vizx.write_text("".join(ln + "\n" for ln in vizx.read_text().splitlines() if not ln.startswith("geopy")))
    with pytest.raises(SystemExit):
        bew.check_requirements(6, vendored, envdir)
    assert "vendored contextily requires geopy" in capsys.readouterr().out


def test_the_vizx_check_unholds_only_a_security_move():
    # check 9 installs [vizx] in a fresh venv: env5's [vizx] moves requests to its CVE-2024-47081 floor, so it cannot
    # be held at base there; everything else is
    assert bew.verify_unheld(bew.PYDIR / "envs" / "env5") == {"requests"}
    assert bew.verify_unheld(bew.PYDIR / "envs" / "env6") == set()
