#!/usr/bin/env python3
"""Fail when the two dev-container Python locks drift apart.

The geobrix-dev image installs a hash-pinned lock chosen by its own architecture:

- python/geobrix/requirements-dev-container.txt          (x86_64)
- python/geobrix/requirements-dev-container-aarch64.txt  (arm64)

Both are compiled from python/geobrix/requirements-dev-container.in, and an arm64 image must
behave like an amd64 one, so the two have to pin the same versions. Checked:

- each lock's `uv pip compile` header names its own `--python-platform`;
- every pin carries at least one sha256 hash;
- a package in both locks has the same version in both;
- every strict `name==version` pin in the .in file is pinned to that version in both locks (so a
  hollow or stale lock cannot pass by having no packages in common).

A package in only one lock is allowed (wheels exist for one platform only) and is listed, not
failed. Exit codes: 0 consistent, 1 drift, 2 usage error (a file is missing).

Usage: python3 scripts/ci/check_dev_container_locks.py [--x86 FILE] [--aarch64 FILE] [--in FILE]
"""

import argparse
import re
import sys
from pathlib import Path

GEOBRIX_DIR = Path(__file__).resolve().parents[2] / "python" / "geobrix"
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9_.-]*)==([^\s\\;]+)")
HASH = re.compile(r"^\s+--hash=sha256:[0-9a-f]{64}")
PLATFORM = re.compile(r"--python-platform\s+(\S+)")


def canonical(name: str) -> str:
    """PEP 503 name normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_lock(path: Path) -> tuple[dict[str, str], list[str], str | None]:
    """Return ({package: version}, [packages without a hash], the header's --python-platform)."""
    pins: dict[str, str] = {}
    unhashed: list[str] = []
    platform = None
    current = None
    hashes = 0
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            m = PLATFORM.search(line)
            if m and platform is None:
                platform = m.group(1)
            continue
        m = PIN.match(line)
        if m:
            if current is not None and hashes == 0:
                unhashed.append(current)
            current, hashes = canonical(m.group(1)), 0
            pins[current] = m.group(2)
        elif HASH.match(line):
            hashes += 1
    if current is not None and hashes == 0:
        unhashed.append(current)
    return pins, unhashed, platform


def parse_in(path: Path) -> dict[str, str]:
    """The strict `name==version` pins of a requirements .in file."""
    pins = {}
    for line in path.read_text().splitlines():
        m = PIN.match(line.split("#", 1)[0].strip())
        if m:
            pins[canonical(m.group(1))] = m.group(2)
    return pins


def check(x86: Path, aarch64: Path, in_file: Path) -> list[str]:
    """Return the problems found (empty when the locks are consistent)."""
    problems = []
    locks = {}
    for label, path, want in (
        ("x86_64", x86, "x86_64-unknown-linux-gnu"),
        ("aarch64", aarch64, "aarch64-unknown-linux-gnu"),
    ):
        pins, unhashed, platform = parse_lock(path)
        locks[label] = pins
        if platform != want:
            problems.append(
                f"{path.name}: header says --python-platform {platform}, expected {want}"
            )
        if unhashed:
            problems.append(
                f"{path.name}: {len(unhashed)} pin(s) without a sha256 hash, first {unhashed[0]}"
            )
        if not pins:
            problems.append(f"{path.name}: no pins found")
    for name in sorted(set(locks["x86_64"]) & set(locks["aarch64"])):
        a, b = locks["x86_64"][name], locks["aarch64"][name]
        if a != b:
            problems.append(f"{name}: x86_64 pins {a}, aarch64 pins {b}")
    for name, version in sorted(parse_in(in_file).items()):
        for label, pins in locks.items():
            if pins.get(name) != version:
                problems.append(
                    f"{in_file.name} pins {name}=={version}, {label} lock has {pins.get(name)}"
                )
    return problems


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument(
        "--x86", type=Path, default=GEOBRIX_DIR / "requirements-dev-container.txt"
    )
    ap.add_argument(
        "--aarch64",
        type=Path,
        default=GEOBRIX_DIR / "requirements-dev-container-aarch64.txt",
    )
    ap.add_argument(
        "--in",
        dest="in_file",
        type=Path,
        default=GEOBRIX_DIR / "requirements-dev-container.in",
    )
    args = ap.parse_args(argv)
    for path in (args.x86, args.aarch64, args.in_file):
        if not path.is_file():
            print(f"missing file: {path}", file=sys.stderr)
            return 2
    problems = check(args.x86, args.aarch64, args.in_file)
    x86_pins, aarch64_pins = parse_lock(args.x86)[0], parse_lock(args.aarch64)[0]
    for label, mine, theirs in (
        ("x86_64", x86_pins, aarch64_pins),
        ("aarch64", aarch64_pins, x86_pins),
    ):
        names = sorted(set(mine) - set(theirs))
        if names:
            print(f"note: only in the {label} lock: {', '.join(names)}")
    if problems:
        print(
            f"FAILED: the dev-container locks disagree ({len(problems)}):",
            file=sys.stderr,
        )
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        print(
            "Recompile both (x86_64 first, then aarch64 with -c on it); see the header of "
            "python/geobrix/requirements-dev-container.in.",
            file=sys.stderr,
        )
        return 1
    print(
        "OK: the x86_64 and aarch64 dev-container locks pin the same versions "
        f"({len(x86_pins)} packages in x86_64)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
