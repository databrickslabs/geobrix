#!/usr/bin/env python3
"""Check 1 of `gbx:build:wheel --verify` (check-env-wheel.sh): did the offline install hold the runtime's base?

    python base_held.py <envdir>

Run by the fresh venv's python, so importlib.metadata sees what the install put there. <envdir> is
python/geobrix/envs/env<N> (base.txt, core.txt, extra-*.txt). Prints one line:
- `ok`, plus `; not needed by the core install: a, b` for base.txt packages that only an extra names (the core
  install does not install them, so they cannot have moved);
- `moved: <name>==<installed> (base <v>); ...` for a base package installed at another version;
- `missing: <name> (base <v>); ...` for a base package that is not installed although core.txt needs it (or no
  extra names it): the core install should have installed it.
Standard library only (the toolchain image's venv has nothing else).
"""

import importlib.metadata as md
import re
import sys
from pathlib import Path

NAME = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def canon(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def names(path):
    """The requirement names of a requirements file (comments and blank lines skipped)."""
    out = set()
    for line in Path(path).read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        m = NAME.match(line)
        if m:
            out.add(canon(m.group(1)))
    return out


def pins(path):
    """name -> version of the `name==version` lines of base.txt."""
    out = {}
    for line in Path(path).read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if "==" in line:
            n, v = line.split("==", 1)
            out[canon(n)] = v.strip()
    return out


def judge(envdir, version=None):
    """The one-line verdict (see the module docstring). `version(name)` raises PackageNotFoundError when absent."""
    version = version or md.version
    envdir = Path(envdir)
    core = names(envdir / "core.txt")
    extras = set().union(*(names(p) for p in sorted(envdir.glob("extra-*.txt"))))
    only_extras = extras - core
    moved, missing, idle = [], [], []
    for name, want in pins(envdir / "base.txt").items():
        try:
            have = version(name)
        except md.PackageNotFoundError:
            (idle if name in only_extras else missing).append(f"{name} (base {want})")
            continue
        if have != want:
            moved.append(f"{name}=={have} (base {want})")
    if moved or missing:
        return "; ".join(
            ([f"moved: {'; '.join(moved)}"] if moved else [])
            + ([f"missing: {'; '.join(missing)}"] if missing else [])
        )
    return "ok" + (
        f"; not needed by the core install: {', '.join(n.split(' ')[0] for n in idle)}"
        if idle
        else ""
    )


if __name__ == "__main__":
    print(judge(sys.argv[1]))
