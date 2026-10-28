"""gbx:build:wheel's pure rules (scripts/native/build_env_wheel.py, scripts/native/redact.py).

- requires-python: the env wheels' range must admit only Pythons the committed range admits (NP-10).
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
    keep = "geobrix-0.6.0+env6-py3-none-any.whl databricks.labs.gbx 4462389585 labs@databricks.com"
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
