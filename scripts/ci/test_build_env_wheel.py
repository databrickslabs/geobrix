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
