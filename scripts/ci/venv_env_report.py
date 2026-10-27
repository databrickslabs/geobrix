#!/usr/bin/env python3
"""Compare the host venv with the geobrix-dev container, for gbx:venv:check and gbx:venv:sync.

The host venv used for tests that need no Docker must match the container the same tests run in:
the same Python patch version, the same version of every package both have, and the same native
stack (GDAL, PROJ and GEOS as rasterio, pyproj and shapely report them). A test that passes on a
drifted host proves nothing about the container.

Stdlib only, so the same file runs under the container's python3 and the venv's interpreter:

    python - collect < venv_env_report.py          # JSON of this interpreter's environment
    python venv_env_report.py compare HOST.json CONTAINER.json [--extras FILE]
    python venv_env_report.py match-requirements --lock LOCK.txt --container CONTAINER.json
    python venv_env_report.py pin-drift HOST.json CONTAINER.json

``compare`` prints a table of the Python version, the natives and every drifting package, and exits
1 on any drift. A package only one side has is not drift (it cannot be compared); the count is
printed. Packages named in the ``--extras`` file (the Spark Connect client packages that
``gbx:venv:sync --connect`` installs and the container does not carry yet) are listed as declared
extras when they are host-only.
"""

import argparse
import importlib
import importlib.metadata as metadata
import json
import platform
import re
import sys

# (key, module, attribute): the native library versions each wheel bundles or links.
NATIVES = (
    ("gdal", "rasterio", "__gdal_version__"),
    ("proj", "pyproj", "proj_version_str"),
    ("geos", "shapely", "geos_version_string"),
)
# Installer tooling differs by how an environment was seeded, not by what the tests import.
IGNORED = frozenset({"pip", "setuptools", "wheel"})
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9_.-]*)==([^\s\\;]+)")


def canonical(name: str) -> str:
    """PEP 503 name normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def collect() -> dict:
    """The Python version, every installed distribution and the native versions of this interpreter."""
    packages = {}
    for dist in metadata.distributions():
        name = dist.metadata["Name"]
        if name:
            packages.setdefault(canonical(name), dist.version)
    natives = {}
    for key, module, attr in NATIVES:
        try:
            natives[key] = str(getattr(importlib.import_module(module), attr))
        except Exception:  # absent or unimportable: nothing to compare
            natives[key] = None
    return {
        "python": platform.python_version(),
        "packages": packages,
        "natives": natives,
    }


def compare(host: dict, container: dict, extras=()) -> tuple[list, list]:
    """Return (rows, notes). A row is (item, host, container, status); status is OK or DRIFT."""
    rows = [
        (
            "python",
            host["python"],
            container["python"],
            "OK" if host["python"] == container["python"] else "DRIFT",
        )
    ]
    for key in sorted(host["natives"]):
        h, c = host["natives"][key], container["natives"].get(key)
        status = "OK" if h is None or c is None or h == c else "DRIFT"
        rows.append((key, h or "-", c or "-", status))
    hp, cp = host["packages"], container["packages"]
    shared = sorted((set(hp) & set(cp)) - IGNORED)
    drift = [n for n in shared if hp[n] != cp[n]]
    rows += [(n, hp[n], cp[n], "DRIFT") for n in drift]
    declared = {canonical(e) for e in extras}
    host_only = sorted((set(hp) - set(cp)) - IGNORED)
    notes = [f"{len(shared)} packages in both, {len(drift)} differ"]
    names = sorted(n for n in host_only if n in declared)
    if names:
        notes.append(f"declared connect-tier extras (host only): {', '.join(names)}")
    others = [n for n in host_only if n not in declared]
    notes.append(
        f"{len(others)} packages only on the host, "
        f"{len((set(cp) - set(hp)) - IGNORED)} only in the container (not compared)"
    )
    return rows, notes


def render(rows: list, notes: list) -> str:
    header = ("item", "host", "container", "status")
    table = [header, *rows]
    widths = [max(len(str(r[i])) for r in table) for i in range(4)]
    lines = ["  ".join(str(r[i]).ljust(widths[i]) for i in range(4)) for r in table]
    return "\n".join([*lines, "", *notes])


def match_requirements(lock_text: str, container: dict) -> list:
    """The lock's packages, each at the container's version when the container has the package.

    The CI lock is hash-pinned for CI's platform; the host needs the container's versions on its
    own platform, so the result is plain ``name==version`` lines (environment markers kept).
    """
    pins = container["packages"]
    out = []
    for line in lock_text.splitlines():
        m = PIN.match(line)
        if not m:
            continue
        name, version = m.group(1), m.group(2)
        marker = ""
        if ";" in line:
            marker = " ;" + line.split(";", 1)[1].rstrip(" \\")
        out.append(f"{name}=={pins.get(canonical(name), version)}{marker}")
    return out


def pin_drift(host: dict, container: dict) -> list:
    """``name==version`` at the container's version, for each package both have at different ones.

    Matching the lock's packages can pull in packages the lock does not name (the container's
    older ipywidgets wants notebook, which pulls in jupyterlab ...), and those arrive at the newest
    release. The container is a consistent environment, so pinning them to its versions is too.
    """
    hp, cp = host["packages"], container["packages"]
    return [
        f"{n}=={cp[n]}" for n in sorted((set(hp) & set(cp)) - IGNORED) if hp[n] != cp[n]
    ]


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("collect")
    cmp_ = sub.add_parser("compare")
    cmp_.add_argument("host")
    cmp_.add_argument("container")
    cmp_.add_argument("--extras")
    pd = sub.add_parser("pin-drift")
    pd.add_argument("host")
    pd.add_argument("container")
    mr = sub.add_parser("match-requirements")
    mr.add_argument("--lock", required=True)
    mr.add_argument("--container", required=True)
    args = ap.parse_args(argv)

    if args.cmd == "collect":
        print(json.dumps(collect(), sort_keys=True))
        return 0
    if args.cmd == "compare":
        with open(args.host) as fh:
            host = json.load(fh)
        with open(args.container) as fh:
            container = json.load(fh)
        extras = []
        if args.extras:
            with open(args.extras) as fh:
                extras = [x.strip() for x in fh if x.strip()]
        rows, notes = compare(host, container, extras)
        print(
            render(
                [
                    r
                    for r in rows
                    if r[3] == "DRIFT" or r[0] in ("python", "gdal", "proj", "geos")
                ],
                notes,
            )
        )
        return 1 if any(r[3] == "DRIFT" for r in rows) else 0
    if args.cmd == "pin-drift":
        with open(args.host) as fh:
            host = json.load(fh)
        with open(args.container) as fh:
            container = json.load(fh)
        print("\n".join(pin_drift(host, container)))
        return 0
    with open(args.container) as fh:
        container = json.load(fh)
    with open(args.lock) as fh:
        print("\n".join(match_requirements(fh.read(), container)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
