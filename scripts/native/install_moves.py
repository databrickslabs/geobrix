#!/usr/bin/env python3
"""What installing an env wheel moves on each runtime it targets (gbx:versions:audit --install-moves).

    python3 install_moves.py [--env 5|6] [--arch x86_64|aarch64|both] [--runtime <name>] [--json <file>]

A runtime's preinstalled packages are python/geobrix/envs/runtimes/<runtime>.txt. For every runtime of the env's
family and two installs, the core install (envs/env<N>/core.txt) and the full one (core.txt and every
extra-<name>.txt), uv resolves the requirements for CPython 3.12 manylinux_2_28 <arch>, with
the runtime's list as CONSTRAINTS, except the packages ALLOWED_MOVES lets move there (their preinstalled version
stays a preference). So a resolution that fails would have moved another preinstalled package. What the install adds
must have a binary wheel for the platform (a preinstalled package needs none). Then:
- every resolved package whose version differs from the runtime's is a move, and must be an ALLOWED_MOVES entry;
- no resolved package may be one the wheel vendors (pip would install PyPI's copy beside the vendored build: a
  second GDAL, PROJ or GEOS on disk, and in a process that skips the activation, in memory).
Prints one line per (runtime, arch, install) and `INSTALL_MOVES env=<N> ... failures=<n>`; exits 1 on a failure.
Index output is redacted (scripts/native/redact.py). Stdlib only (the host's python3, 3.10 or newer) plus uv.
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from redact import redact  # noqa: E402

REPO = HERE.parent.parent
ENVS = REPO / "python" / "geobrix" / "envs"
ARCHES = ("x86_64", "aarch64")
RUNTIMES = {
    5: ("dbr17_3", "dbr18", "env5"),
    6: ("dbr19", "dbr19ml", "env6", "env6ml"),
}
# What the env wheels vendor, by distribution name: the five bindings and GDAL's osgeo; vendor-pure.txt adds its own.
BINDINGS = ("rasterio", "pyogrio", "pyproj", "shapely", "netcdf4", "gdal")
# The documented moves of a preinstalled package: name -> (envs, the installs it may move in, why). Each reason is
# also the pin comment of the requirement that causes it (core.txt or extra-<name>.txt).
ALLOWED_MOVES = {
    "idna": (
        (5, 6),
        ("core", "full"),
        "core.txt httpx2>=2.12 (rio-tiler's HTTP client without the two HIGH advisories) needs idna>=3.18; the "
        "runtimes ship 3.7 / 3.11",
    ),
    "anyio": (
        (5,),
        ("core", "full"),
        "core.txt httpx2>=2.12: its httpcore2 needs a newer anyio than the env5 runtimes' 4.6.2 / 4.7.0",
    ),
    "h11": (
        (5,),
        ("core", "full"),
        "core.txt httpx2>=2.12: its httpcore2 needs h11>=0.16 (CVE-2025-43859); DBR 17.3 ships 0.14.0",
    ),
    "httpcore": (
        (5,),
        ("core", "full"),
        "core.txt httpcore>=1.0.9: DBR 17.3's httpcore 1.0.2 requires h11<0.15, which the h11 move would break",
    ),
    "typing-extensions": (
        (5,),
        ("core", "full"),
        "core.txt typing_extensions>=4.13: rio-tiler 9's TypedDict(extra_items=...) (PEP 728); the env5 runtimes "
        "ship 4.12.2",
    ),
    "lazy-loader": (
        (6,),
        ("core", "full"),
        "core.txt lazy_loader>=0.5: scikit-image imports it, and on Serverless pip installs it into the "
        "notebook-scoped environment only above the runtime's version; the ML runtimes ship 0.4",
    ),
    "requests": (
        (5,),
        ("full",),
        "extra-stac.txt and extra-vizx.txt requests>=2.32.4,<2.33: CVE-2024-47081 (a URL can make requests send "
        "~/.netrc credentials for one host to another); the env5 runtimes ship 2.32.3",
    ),
}
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==(\S+)")


def canon(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def pins(path):
    """name -> version of the `name==version` lines of a requirements file (comments skipped)."""
    out = {}
    for line in Path(path).read_text().splitlines():
        m = PIN.match(line.split("#", 1)[0].strip())
        if m:
            out[canon(m.group(1))] = m.group(2)
    return out


def vendored(env):
    out = set(BINDINGS)
    for line in (ENVS / f"env{env}" / "vendor-pure.txt").read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(canon(line.split("==")[0]))
    return out


def requirement_files(env, install):
    envdir = ENVS / f"env{env}"
    files = [envdir / "core.txt"]
    if install == "full":
        files += sorted(envdir.glob("extra-*.txt"))
    return files


def allowed(env, install, runtime_pins):
    """The ALLOWED_MOVES names that apply to this env and install and that the runtime preinstalls."""
    return {
        n
        for n, (envs, installs, _why) in ALLOWED_MOVES.items()
        if env in envs and install in installs and n in runtime_pins
    }


def resolve(env, install, runtime, arch, uv="uv"):
    """(resolved pins, None) or (None, the redacted uv error)."""
    base = pins(ENVS / "runtimes" / f"{runtime}.txt")
    free = allowed(env, install, base)
    with tempfile.TemporaryDirectory(prefix="gbx-moves-") as tmp:
        tmp = Path(tmp)
        reqs = tmp / "requirements.in"
        reqs.write_text("".join(f"-r {f}\n" for f in requirement_files(env, install)))
        (tmp / "constraints.txt").write_text(
            "".join(f"{n}=={v}\n" for n, v in sorted(base.items()) if n not in free)
        )
        out = tmp / "resolved.txt"  # an existing output file is uv's preference: the runtime's versions
        out.write_text("".join(f"{n}=={v}\n" for n, v in sorted(base.items())))
        target = ["--python-version", "3.12", "--python-platform", f"{arch}-manylinux_2_28"]
        res = subprocess.run(
            [uv, "pip", "compile", "-q", "--no-header", "--no-annotate", *target,
             "-c", str(tmp / "constraints.txt"), "-o", str(out), str(reqs)],
            capture_output=True, text=True,
        )  # fmt: skip
        if res.returncode != 0:
            return None, redact((res.stderr or res.stdout).strip())[-1500:]
        resolved = pins(out)
        # What the install adds must come as a wheel for the platform (the preinstalled packages are there already,
        # some built by the runtime itself, so they need none).
        new = {n: v for n, v in resolved.items() if base.get(n) != v}
        (tmp / "new.txt").write_text("".join(f"{n}=={v}\n" for n, v in sorted(new.items())))
        res = subprocess.run(
            [uv, "pip", "compile", "-q", "--no-header", "--no-annotate", "--no-deps", "--only-binary", ":all:",
             *target, "-o", str(tmp / "new-wheels.txt"), str(tmp / "new.txt")],
            capture_output=True, text=True,
        )  # fmt: skip
        if res.returncode != 0:
            return None, "no wheel for what it adds: " + redact((res.stderr or res.stdout).strip())[-1200:]
        return resolved, None


def judge(env, install, runtime, resolved):
    """(moves, failures): moves = [(name, runtime version, resolved version)], failures = [what is wrong]."""
    base = pins(ENVS / "runtimes" / f"{runtime}.txt")
    free = allowed(env, install, base)
    moves = sorted((n, base[n], v) for n, v in resolved.items() if n in base and base[n] != v)
    failures = [f"{n} {b} -> {v} is not a documented move" for n, b, v in moves if n not in free]
    pulled = sorted(set(resolved) & vendored(env))
    failures += [f"pulls PyPI's {n} {resolved[n]}, which the wheel vendors" for n in pulled]
    return moves, failures


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--env", type=int, choices=(5, 6), action="append")
    p.add_argument("--arch", choices=(*ARCHES, "both"), default="both")
    p.add_argument("--runtime", action="append")
    p.add_argument("--json", type=Path)
    args = p.parse_args()
    uv = shutil.which("uv")
    if uv is None:
        print("PRECONDITION MISSING: uv is not installed; the install-moves check not run")
        return 1
    arches = ARCHES if args.arch == "both" else (args.arch,)
    rows, failures = [], 0
    for env in args.env or (5, 6):
        for runtime in RUNTIMES[env]:
            if args.runtime and runtime not in args.runtime:
                continue
            for arch in arches:
                for install in ("core", "full"):
                    resolved, err = resolve(env, install, runtime, arch, uv)
                    if resolved is None:
                        moves, bad = [], [f"no resolution holds the runtime's other packages: {err}"]
                    else:
                        moves, bad = judge(env, install, runtime, resolved)
                    added = sorted(set(resolved or {}) - set(pins(ENVS / "runtimes" / f"{runtime}.txt")))
                    failures += len(bad)
                    rows.append(
                        {"env": env, "runtime": runtime, "arch": arch, "install": install,
                         "moves": [f"{n} {b} -> {v}" for n, b, v in moves], "added": len(added),
                         "failures": bad}
                    )  # fmt: skip
                    moved = ", ".join(f"{n} {b} -> {v}" for n, b, v in moves) or "nothing"
                    status = "ok" if not bad else "FAILED"
                    print(
                        f"{status}: env{env} {runtime} {arch} {install}: moves {moved}; installs {len(added)} new",
                        flush=True,
                    )
                    for b in bad:
                        print(f"    {b}", flush=True)
    if args.json:
        args.json.write_text(json.dumps(rows, indent=1) + "\n")
    print(f"INSTALL_MOVES envs={','.join(map(str, args.env or (5, 6)))} rows={len(rows)} failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
