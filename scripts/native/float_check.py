#!/usr/bin/env python3
"""What `pip install <env wheel>[<extra>]` would move on each runtime it targets (gbx:build:wheel --float-check).

    python3 float_check.py --env 5|6 --arch x86_64|aarch64 [--wheel <path>] [--runtime <name>] [--out <dir>]

install_moves.py asks uv what the env's requirement files resolve to. This asks pip, the installer a runtime runs,
about the built wheel itself. Per runtime of the env's family (python/geobrix/envs/runtimes/<runtime>.txt, the
runtime's full `pip freeze`), in a fresh CPython 3.12 venv of the architecture's toolchain image (manylinux_2_28), the
runtime's packages are made installed (a dist-info with its name and version each, pip kept real), and then
`pip install --dry-run --report` runs, with no constraint, against the index the host's pip is configured with (its
newest releases), once for the core install, once per extra (envs/env<N>/extra-<name>.txt) and once with every extra.
pip keeps an installed version that satisfies every requirement and replaces one that does not, so each package of
the report's install list is one of:
- a MOVE: a package the runtime preinstalls, at another version (an upgrade or a downgrade), with the requirement
  chain from geobrix that asks for it. A move ALLOWED_MOVES of install_moves.py documents for the env and install is
  reported and passes; any other fails;
- NEW: a package the runtime lacks, with its version;
(everything else the install needs is a no-op: the runtime's version satisfies it, and pip's report does not list it).
Prints `FLOAT_ROW ...` per (runtime, install), each move and new package under it, and
`FLOAT_CHECK env=<N> arch=<arch> wheel=<name> rows=<n> moves=<n> undocumented=<n>`; exits 1 on an undocumented move or
a failed resolution. Writes <out>/float-<arch>.json. The index URL reaches the container in PIP_INDEX_URL and is never
printed (scripts/native/redact.py). The host side needs Docker and the toolchain image; stdlib only.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from install_moves import ALLOWED_MOVES, RUNTIMES, canon, pins  # noqa: E402
from redact import redact  # noqa: E402

REPO = HERE.parent.parent
ENVS = REPO / "python" / "geobrix" / "envs"
PLATFORMS = {"x86_64": "linux/amd64", "aarch64": "linux/arm64"}
PY = "/opt/python/cp312-cp312/bin/python"
# Never made installed: the venv's own pip (a second pip dist-info would shadow it).
REAL = {"pip"}
_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[([^\]]*)\])?")


def extras_of(env):
    """The env's extras: one per envs/env<N>/extra-<name>.txt (the tripwire names are not installs)."""
    return sorted(p.stem.removeprefix("extra-") for p in (ENVS / f"env{env}").glob("extra-*.txt"))


def installs(env):
    """(label, extras) per install: the core one, each extra alone, every extra together."""
    xs = extras_of(env)
    return [("core", ())] + [(x, (x,)) for x in xs] + [("full", tuple(xs))]


def allowed_for(env, label):
    """The ALLOWED_MOVES names for this env and install (an extra's install is install_moves' 'full')."""
    kind = "core" if label == "core" else "full"
    return {n for n, (envs, kinds, _why) in ALLOWED_MOVES.items() if env in envs and kind in kinds}


# --- the parts that read a pip report (pure: scripts/ci/test_float_check.py runs them) -------------------------------


def _marker_ok(marker, extras):
    """Whether a requirement's marker holds for a Linux CPython 3.12 install with ``extras`` requested."""
    if not marker:
        return True
    try:
        from packaging.markers import Marker
    except ImportError:  # the venv's pip carries one
        from pip._vendor.packaging.markers import Marker
    env = {"python_version": "3.12", "python_full_version": "3.12.3", "sys_platform": "linux",
           "platform_system": "Linux", "os_name": "posix", "implementation_name": "cpython",
           "platform_python_implementation": "CPython"}  # fmt: skip
    m = Marker(marker)
    return any(m.evaluate({**env, "extra": x}) for x in (extras or ("",)))


def _edges(requires_dist, extras):
    """(child name, child extras) for every requirement that applies with ``extras`` requested."""
    out = []
    for r in requires_dist or ():
        spec, _, marker = r.partition(";")
        m = _NAME.match(spec)
        if m and _marker_ok(marker.strip(), extras):
            child_x = tuple(sorted(x.strip() for x in (m.group(2) or "").split(",") if x.strip()))
            out.append((canon(m.group(1)), child_x))
    return out


def chains(report, root_extras):
    """name -> the requirement chain from the requested wheel to it (`geobrix[x] -> a -> b`), from the report's
    metadata. A package reached through an installed (unreported) package has no metadata here, so its chain stops
    at the last reported one."""
    items = {canon(i["metadata"]["name"]): i for i in report.get("install", [])}
    roots = [n for n, i in items.items() if i.get("requested")]
    seen, queue = {}, []
    for r in roots:
        label = r + (f"[{','.join(root_extras)}]" if root_extras else "")
        seen[r] = [label]
        queue.append((r, tuple(root_extras)))
    while queue:
        name, xs = queue.pop(0)
        meta = items.get(name, {}).get("metadata", {})
        for child, cx in _edges(meta.get("requires_dist"), xs):
            if child not in seen:
                seen[child] = seen[name] + [child]
                queue.append((child, cx))
    return {n: " -> ".join(c) for n, c in seen.items()}


def classify(report, runtime_pins, root_extras=()):
    """(moves, new): moves = [(name, runtime version, report version, chain)], new = [(name, version, chain)] for the
    report's install list, the requested wheel left out."""
    path = chains(report, root_extras)
    moves, new = [], []
    for i in report.get("install", []):
        if i.get("requested"):
            continue
        n, v = canon(i["metadata"]["name"]), i["metadata"]["version"]
        if n in runtime_pins:
            if runtime_pins[n] != v:
                moves.append((n, runtime_pins[n], v, path.get(n, n)))
        else:
            new.append((n, v, path.get(n, n)))
    return sorted(moves), sorted(new)


def make_installed(site, runtime_pins):
    """A dist-info (METADATA, INSTALLER, an empty RECORD) per runtime package in ``site``: pip then holds it installed
    at that version. Returns how many were written."""
    n = 0
    for name, version in sorted(runtime_pins.items()):
        if name in REAL:
            continue
        d = Path(site) / f"{name.replace('-', '_')}-{version}.dist-info"
        d.mkdir(parents=True, exist_ok=True)
        (d / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        (d / "INSTALLER").write_text("gbx-float-check\n")
        (d / "RECORD").write_text("")
        n += 1
    return n


# --- inside the toolchain image ----------------------------------------------------------------------------------


def inner(args):
    rows = []
    wheel = Path(args.wheel)
    for runtime in args.runtime:
        rt = pins(Path(args.runtimes) / f"{runtime}.txt")
        with tempfile.TemporaryDirectory(prefix=f"gbx-float-{runtime}-") as tmp:
            venv = Path(tmp) / "venv"
            subprocess.run([PY, "-m", "venv", str(venv)], check=True)
            py = str(venv / "bin" / "python")
            site = subprocess.run([py, "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
                                  capture_output=True, text=True, check=True).stdout.strip()  # fmt: skip
            made = make_installed(site, rt)
            for label, xs in installs(args.env):
                report = Path(tmp) / f"{label}.json"
                target = str(wheel) + (f"[{','.join(xs)}]" if xs else "")
                res = subprocess.run(
                    [py, "-m", "pip", "install", "--disable-pip-version-check", "--quiet", "--dry-run",
                     "--report", str(report), target],
                    capture_output=True, text=True,
                )  # fmt: skip
                row = {"runtime": runtime, "install": label, "installed_before": made}
                if res.returncode != 0:
                    row["error"] = redact((res.stderr or res.stdout).strip())[-1500:]
                    rows.append(row)
                    continue
                moves, new = classify(json.loads(report.read_text()), rt, xs)
                ok = allowed_for(args.env, label)
                row["moves"] = [{"name": n, "runtime": b, "to": v, "chain": c, "documented": n in ok}
                                for n, b, v, c in moves]  # fmt: skip
                row["new"] = [{"name": n, "version": v, "chain": c} for n, v, c in new]
                rows.append(row)
    Path(args.json).write_text(json.dumps(rows, indent=1) + "\n")
    return 0


# --- on the host ---------------------------------------------------------------------------------------------------


def index_url():
    """The index the host's pip is configured with (PIP_INDEX_URL first); never printed."""
    if os.environ.get("PIP_INDEX_URL"):
        return os.environ["PIP_INDEX_URL"]
    for py in ("python3.12", "python3"):
        res = subprocess.run([py, "-m", "pip", "config", "get", "global.index-url"], capture_output=True, text=True)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    return ""


def report(env, arch, wheel, rows):
    moves = undocumented = 0
    for r in rows:
        if "error" in r:
            undocumented += 1
            print(f"FLOAT_ROW env={env} arch={arch} runtime={r['runtime']} install={r['install']} status=FAILED")
            print(f"    no resolution: {r['error']}")
            continue
        bad = [m for m in r["moves"] if not m["documented"]]
        moves += len(r["moves"])
        undocumented += len(bad)
        status = "FAILED" if bad else "ok"
        print(f"FLOAT_ROW env={env} arch={arch} runtime={r['runtime']} install={r['install']} status={status} "
              f"moves={len(r['moves'])} undocumented={len(bad)} new={len(r['new'])}")  # fmt: skip
        for m in r["moves"]:
            tag = "documented move" if m["documented"] else "UNDOCUMENTED MOVE"
            print(f"    {tag}: {m['name']} {m['runtime']} -> {m['to']} ({m['chain']})")
        for n in r["new"]:
            print(f"    new: {n['name']}=={n['version']} ({n['chain']})")
    print(f"FLOAT_CHECK env={env} arch={arch} wheel={wheel.name} rows={len(rows)} moves={moves} "
          f"undocumented={undocumented}")
    return 1 if undocumented else 0


def host(args):
    dist = sorted((REPO / "dist" / f"env{args.env}").glob("*.whl"))
    wheel = Path(args.wheel) if args.wheel else (dist[0] if dist else None)
    if wheel is None or not wheel.is_file():
        print(f"PRECONDITION MISSING: no env{args.env} wheel (dist/env{args.env}/ or --wheel); float check not run")
        return 1
    sys.path.insert(0, str(HERE))
    from pins import tag_hash

    image = f"geobrix-native-build:toolchain-{tag_hash(REPO, args.arch)}-{args.arch}"
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        print(f"PRECONDITION MISSING: {image} (gbx:build:native --arch {args.arch} --target toolchain); not run")
        return 1
    url = index_url()
    if not url:
        print("PRECONDITION MISSING: no package index (host pip global.index-url or PIP_INDEX_URL); not run")
        return 1
    out = Path(args.out or REPO / "build" / "env-wheel" / f"env{args.env}")
    out.mkdir(parents=True, exist_ok=True)
    runtimes = args.runtime or list(RUNTIMES[args.env])
    env = {**os.environ, "PIP_INDEX_URL": url}
    cmd = ["docker", "run", "--rm", "--platform", PLATFORMS[args.arch], "-e", "PIP_INDEX_URL",
           "-v", f"{wheel.resolve().parent}:/wheel:ro", "-v", f"{HERE}:/check:ro", "-v", f"{ENVS}:/envs:ro",
           "-v", f"{out.resolve()}:/out", image, PY, "/check/float_check.py", "--inner", "--env", str(args.env),
           "--arch", args.arch, "--wheel", f"/wheel/{wheel.name}", "--runtimes", "/envs/runtimes",
           "--json", f"/out/float-{args.arch}.json"]  # fmt: skip
    for r in runtimes:
        cmd += ["--runtime", r]
    print(f"== float check: env{args.env} {wheel.name} on {', '.join(runtimes)} ({args.arch})", flush=True)
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        print(redact((res.stderr or res.stdout).strip())[-3000:])
        print(f"FLOAT_CHECK env={args.env} arch={args.arch} wheel={wheel.name} rows=0 moves=0 undocumented=1")
        return 1
    rows = json.loads((out / f"float-{args.arch}.json").read_text())
    return report(args.env, args.arch, wheel, rows)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--env", type=int, choices=(5, 6), required=True)
    p.add_argument("--arch", choices=tuple(PLATFORMS), required=True)
    p.add_argument("--wheel")
    p.add_argument("--runtime", action="append")
    p.add_argument("--out")
    p.add_argument("--inner", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--runtimes", help=argparse.SUPPRESS)
    p.add_argument("--json", help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.inner:
        global ENVS
        ENVS = Path(args.runtimes).parent
        return inner(args)
    return host(args)


if __name__ == "__main__":
    sys.exit(main())
