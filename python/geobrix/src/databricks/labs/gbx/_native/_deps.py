"""The env wheel's pinned, hashed dependency lists (``_native/deps/``): their names, their line format and the target
platform they were resolved for.

Standard library only: ``gbx:build:wheel`` (scripts/native/build_env_wheel.py) loads this file by path to write the
lists, and ``python -m databricks.labs.gbx.stage deps`` and ``gbx.uc.install(deps="volume")`` read them through it, so
the writer and the readers share one format.

The lists, per environment ``<N>`` and architecture ``<arch>`` (``x86_64``, ``aarch64``):

    env<N>-<arch>.txt          what the env wheel's own requirements (core.txt) resolve to, base.txt left out
    env<N>-<arch>-<extra>.txt  what an extra adds to that list (the core pins held), base.txt left out
    env<N>-<arch>-uc.txt       the Unity Catalog ENVIRONMENT pins (the manifest's uc_dependencies), env 6 only
    env<N>-base.txt            base.txt: what the target runtimes preinstall (``name==version``, no hash)

A list line is ``<name>==<version> --hash=sha256:<hex>``: one wheel, the one the build resolved for that platform.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

ARCHES = ("x86_64", "aarch64")
PYTHON_VERSION = "3.12"
IMPLEMENTATION = "cp"
# The platform tags a wheel may carry, in pip's order of preference: the build resolves with these and the staging
# helper downloads with the same, so both pick the same file. pip does not widen a manylinux_2_28 tag to the older
# glibc versions on its own, so each is listed (obstore's aarch64 wheel is manylinux_2_24 only). The ceiling is the
# payload's own floor, manylinux_2_28 (AlmaLinux 8, where `gbx:build:wheel --verify` installs offline).
PLATFORM_TAGS = tuple(f"manylinux_2_{v}_{{arch}}" for v in range(28, 16, -1)) + (
    "manylinux2014_{arch}",
)
DIR = "deps"
UC = "uc"

_LINE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+) --hash=sha256:([0-9a-f]{64})"
)
_PIN = re.compile(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")


def canon(name: str) -> str:
    """The PEP 503 normalized name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def platforms(arch: str) -> List[str]:
    """The ``--platform`` values for ``arch``."""
    if arch not in ARCHES:
        raise ValueError(f"arch must be one of {ARCHES} (got {arch!r})")
    return [t.format(arch=arch) for t in PLATFORM_TAGS]


def pip_target_args(arch: str) -> List[str]:
    """The pip options that select wheels for the target: binary only, its platforms, CPython 3.12."""
    out = ["--only-binary=:all:"]
    for p in platforms(arch):
        out += ["--platform", p]
    return out + [
        "--python-version",
        PYTHON_VERSION,
        "--implementation",
        IMPLEMENTATION,
    ]


def list_name(env: int, arch: str, part: Optional[str] = None) -> str:
    """The file name of a list: the core list, an extra's list, or (``part="uc"``) the UC list."""
    platforms(arch)  # the arch check
    return f"env{env}-{arch}" + (f"-{part}" if part else "") + ".txt"


def base_name(env: int) -> str:
    return f"env{env}-base.txt"


def format_list(header: str, pins: Dict[str, Tuple[str, str]]) -> str:
    """A list file: ``# `` header lines, then one ``name==version --hash=sha256:<hex>`` line per pin, sorted by name."""
    lines = [f"# {h}" if h else "#" for h in header.splitlines()]
    lines += [f"{n}=={v} --hash=sha256:{sha}" for n, (v, sha) in sorted(pins.items())]
    return "\n".join(lines) + "\n"


def parse_list(text: str, where: str = "the list") -> Dict[str, Tuple[str, str]]:
    """canonical name -> (version, sha256) of a list file; ValueError on a line that is not a hashed pin, or a name
    listed twice."""
    out: Dict[str, Tuple[str, str]] = {}
    for i, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE.fullmatch(line)
        if m is None:
            raise ValueError(
                f"{where} line {i} is not '<name>==<version> --hash=sha256:<hex>': {line!r}"
            )
        name = canon(m.group(1))
        if name in out:
            raise ValueError(f"{where} lists {name} twice")
        out[name] = (m.group(2), m.group(3))
    return out


def parse_pins(text: str, where: str = "the list") -> Dict[str, str]:
    """canonical name -> version of a ``name==version`` file (base.txt)."""
    out: Dict[str, str] = {}
    for i, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        m = _PIN.fullmatch(line)
        if m is None:
            raise ValueError(f"{where} line {i} is not '<name>==<version>': {line!r}")
        out[canon(m.group(1))] = m.group(2)
    return out


def wheel_name_version(filename: str) -> Tuple[str, str]:
    """(canonical name, version) of a wheel file name (``<name>-<version>(-<build>)?-<py>-<abi>-<plat>.whl``)."""
    base = filename.rsplit("/", 1)[-1]
    parts = base[: -len(".whl")].split("-") if base.endswith(".whl") else []
    if len(parts) not in (5, 6):
        raise ValueError(f"{base} is not a wheel file name")
    return canon(parts[0]), parts[1]
