#!/usr/bin/env python3
"""The bindings probe: the five Python bindings, built from sdist against gdal-native, run their own test suites.

Driver (stdlib only), run by test-bindings.sh inside the build container with --network none:

    p11_probe.py closure --root /t/gbx --arch ARCH
    p11_probe.py run --arch ARCH --native DIR --bindings DIR
    p11_probe.py recount --bindings DIR      the counts of an existing run, from its logs

and a pytest plugin (`-p p11_probe`) that logs one JSON line per test phase to $P11_LOG.

What `run` does, per binding (shapely, pyogrio, pyproj, rasterio, netcdf4):
  1. the binding's own suite against the vendored package (PYTHONPATH=_vendor/<arch>, GDAL_DATA, PROJ_DATA and
     HDF5_PLUGIN_PATH of the payload, a venv with the hashed test requirements and click 8.2.1);
  2. every failure again in the same environment (flaky) and, for rasterio, again with the newest click rasterio
     accepts: a failure that passes there is click-attributable, a `cli_failure` when it comes from the `rio` command line
     (a test_rio_*/test_cli_* file, or a traceback through rasterio/rio or click), else a `library_failure`;
  3. every remaining failure against the PyPI wheel, without PYTHONPATH: it still fails there -> environmental, it passes
     there -> regression.
and writes <bindings>/p11.json: {binding: {version, build_s, passed, failed, skipped, regressions, environmental, ...}}.
A pytest process that dies on a signal is a crash of that test: it is recorded, deselected and the run continues.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------- pytest plugin (loaded with -p p11_probe)


# Read once, when pytest loads the plugin: a test may replace os.environ for its duration (rasterio's test_session
# does), and a record written then would be lost.
_LOG = os.environ.get("P11_LOG")


def _emit(record):
    if _LOG:
        with open(_LOG, "a") as f:
            f.write(json.dumps(record) + "\n")


def pytest_runtest_logstart(nodeid, location):
    _emit({"start": nodeid})


def pytest_collectreport(report):
    if report.failed or report.skipped:
        rec = {"id": report.nodeid, "when": "collect", "outcome": report.outcome}
        if report.failed:
            rec["tb"] = (getattr(report, "longreprtext", "") or "")[-6000:]
        _emit(rec)


def pytest_runtest_logreport(report):
    rec = {"id": report.nodeid, "when": report.when, "outcome": report.outcome}
    if hasattr(report, "wasxfail"):
        rec["xfail"] = True
    if hasattr(
        report, "context"
    ):  # a SubtestReport (pytest 9): counted apart from the test it belongs to
        rec["subtest"] = True
    if report.outcome == "failed":
        text = getattr(report, "longreprtext", "") or ""
        rec["tb"] = text[-6000:]
    _emit(rec)


# ---------------------------------------------------------------- driver

T = Path("/t")
BINDINGS = ("shapely", "pyogrio", "pyproj", "rasterio", "netcdf4")
# the wheel / pin names of the bindings, and the sdist prefix of each in pins.conf
PIN = {
    "shapely": "SHAPELY",
    "pyogrio": "PYOGRIO",
    "pyproj": "PYPROJ",
    "rasterio": "RASTERIO",
    "netcdf4": "NETCDF4",
}
# NEEDED entries a manylinux_2_28 extension may keep (the glibc family, libstdc++, libgcc_s)
SYSTEM_NEEDED = {
    "libc.so.6",
    "libm.so.6",
    "libdl.so.2",
    "libpthread.so.0",
    "librt.so.1",
    "libutil.so.1",
    "libresolv.so.2",
    "libnsl.so.1",
    "libstdc++.so.6",
    "libgcc_s.so.1",
    "ld-linux-x86-64.so.2",
    "ld-linux-aarch64.so.1",
}
# library families that must come from the payload, never from the system, once the bindings are loaded
OWN_FAMILIES = (
    "gdal",
    "proj",
    "geos",
    "netcdf",
    "hdf5",
    "curl",
    "sqlite3",
    "tiff",
    "geotiff",
    "ssl",
    "crypto",
    "png",
    "jpeg",
    "webp",
    "zstd",
    "lz4",
    "blosc",
    "openjp2",
    "deflate",
    "Lerc",
    "aec",
)


# the payload's command-line tools (gdalinfo, ogrinfo, projinfo; run() sets it): rasterio's gdalbin tests call gdalinfo
TOOLS_BIN = None


def say(msg):
    print(msg, flush=True)


def fail(msg):
    print(f"TEST FAILED: {msg}", flush=True)
    sys.exit(1)


def pins():
    d = {}
    for line in Path("/verify/pins.conf").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            d[k] = v.strip('"')
    return d


# ---- closure of the vendored tree over NEEDED / RUNPATH


def readelf(path):
    return subprocess.run(
        ["readelf", "-d", str(path)], capture_output=True, text=True
    ).stdout


def is_elf(path):
    with open(path, "rb") as f:
        return f.read(4) == b"\x7fELF"


def closure(root, arch):
    root = Path(root).resolve()
    lib = root / "_native" / arch / "lib"
    have = {p.name for p in lib.iterdir()}
    files, bad = 0, []
    for p in sorted((root / "_vendor" / arch).rglob("*")):
        if p.is_symlink() or not p.is_file() or not is_elf(p):
            continue
        files += 1
        dyn = readelf(p)
        runpath = re.findall(r"\(RUNPATH\)\s+Library runpath: \[(.*)\]", dyn)
        if re.search(r"\(RPATH\)", dyn) or len(runpath) != 1:
            bad.append(f"{p.relative_to(root)}: RPATH or no RUNPATH ({runpath})")
            continue
        resolved = (p.parent / runpath[0].replace("$ORIGIN", ".")).resolve()
        if resolved != lib.resolve():
            bad.append(
                f"{p.relative_to(root)}: RUNPATH {runpath[0]} resolves to {resolved}, not {lib}"
            )
        for n in re.findall(r"\(NEEDED\)\s+Shared library: \[(.*)\]", dyn):
            if n not in have and n not in SYSTEM_NEEDED:
                bad.append(
                    f"{p.relative_to(root)}: NEEDED {n} is neither in the payload's lib/ nor a system library"
                )
    if bad:
        fail(
            "ELF closure of _vendor/%s: %d problems, first: %s"
            % (arch, len(bad), "; ".join(bad[:5]))
        )
    say(
        f"ok: ELF closure: {files} ELF files in _vendor/{arch}, each RUNPATH -> _native/{arch}/lib, each NEEDED in lib/ or the system set"
    )


# ---- running pytest


def env_for(mode, arch):
    env = {
        k: v
        for k, v in os.environ.items()
        if k
        not in ("PYTHONPATH", "GDAL_DATA", "PROJ_DATA", "PROJ_LIB", "LD_LIBRARY_PATH", "HDF5_PLUGIN_PATH")
    }
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    if TOOLS_BIN:
        env["PATH"] = f"{TOOLS_BIN}:{env['PATH']}"
    if mode == "vendored":
        env["PYTHONPATH"] = f"{T}/gbx/_vendor/{arch}:{T}/distinfo:{T}/plug"
        env["GDAL_DATA"] = f"{T}/gbx/_native/share/gdal"
        env["PROJ_DATA"] = f"{T}/gbx/_native/share/proj"
        # the payload's HDF5 filter plugins (zstd, blosc, bzip2, ...), as the env wheel's activation sets them
        env["HDF5_PLUGIN_PATH"] = f"{T}/gbx/_native/{arch}/hdf5-plugins"
    else:  # the PyPI wheels carry their own data
        env["PYTHONPATH"] = f"{T}/plug"
    return env


PASSING = ("passed", "xpassed")


def parse_log(path):
    """({id: [(phase, outcome, xfail, subtest), ...] in order}, {id: traceback of its failure}, [the test that started and never
    tore down: a crash]). A node id can repeat (pytest allows duplicates), so every report is kept.
    """
    status, tb, pending = {}, {}, None
    if not Path(path).exists():
        return status, tb, []
    for line in Path(path).read_text().splitlines():
        r = json.loads(line)
        if "start" in r:
            pending = r["start"]
            continue
        i = r["id"]
        status.setdefault(i, []).append(
            (r["when"], r["outcome"], bool(r.get("xfail")), bool(r.get("subtest")))
        )
        if r["when"] == "teardown" and i == pending:
            pending = None
        if r["outcome"] == "failed":
            tb[i] = r.get("tb", "")
    return status, tb, [pending] if pending else []


def outcome_of(reports):
    """One word per test: `failed` when any phase (or subtest) failed, else the last call phase of the test itself
    (passed, xpassed, xfailed, skipped); a test skipped in setup is skipped."""
    if any(o == "failed" for _, o, _, _ in reports):
        return "failed"
    calls = [(o, x) for when, o, x, sub in reports if when == "call" and not sub]
    if calls:
        o, xfail = calls[-1]
        if o == "passed":
            return "xpassed" if xfail else "passed"
        return "xfailed" if xfail else "skipped"
    if any(o == "skipped" for _, o, _, _ in reports):
        return "skipped"
    return "passed"


def read_log(path):
    """({id: outcome}, {id: traceback}, [the test that started and never tore down, a crash])."""
    status, tb, crashed = parse_log(path)
    return {i: outcome_of(r) for i, r in status.items()}, tb, crashed


COUNT_KEYS = (
    "passed",
    "failed",
    "errors",
    "skipped",
    "xfailed",
    "xpassed",
    "subtests_passed",
)


def tally(path):
    """The counts of pytest's own summary line, one per report that has one: a call is passed, failed, skipped,
    xfailed (skipped with a wasxfail) or xpassed (passed with one); a passed subtest is `subtests passed`; a failed
    setup, teardown or collection is an error; a skipped setup, teardown or collection is skipped.
    """
    t = dict.fromkeys(COUNT_KEYS, 0)
    for reports in parse_log(path)[0].values():
        for when, o, xfail, sub in reports:
            if when == "call":
                if o == "failed":
                    t["failed"] += 1
                elif o == "passed":
                    t[
                        "subtests_passed" if sub else "xpassed" if xfail else "passed"
                    ] += 1
                elif o == "skipped":
                    t["xfailed" if xfail else "skipped"] += 1
            elif o == "failed":
                t["errors"] += 1
            elif o == "skipped":
                t["skipped"] += 1
    return t


def summary_counts(out_path):
    """The counts on pytest's last summary line in its output (`5 failed, 1 error, 90 passed in 3.2s`), or None."""
    for line in reversed(Path(out_path).read_text().splitlines()):
        found = re.findall(
            r"(\d+) (subtests passed|passed|failed|errors?|skipped|xfailed|xpassed)\b",
            line,
        )
        if found and re.search(r" in [\d.]+s", line):
            t = dict.fromkeys(COUNT_KEYS, 0)
            for n, word in found:
                t[
                    (
                        "subtests_passed"
                        if word.startswith("subtests")
                        else "errors" if word.startswith("error") else word
                    )
                ] = int(n)
            return t
    return None


def run_pytest(label, mode, venv, cwd, args, arch, logdir, timeout):
    """Run pytest (resuming past a crashing test) -> ({id: outcome}, {id: traceback}, [crashed ids], seconds)."""
    log = logdir / f"{label}.jsonl"
    out = logdir / f"{label}.out"
    out.write_text("")
    crashed, t0 = [], time.time()
    for attempt in range(8):
        log.unlink(
            missing_ok=True
        )  # a resumed run repeats every test: the log is the last attempt's
        cmd = [
            str(venv / "bin" / "python"),
            "-m",
            "pytest",
            "-p",
            "p11_probe",
            "-p",
            "no:cacheprovider",
            "-q",
            "--continue-on-collection-errors",
            f"--rootdir={cwd}",
        ]
        cmd += [a for c in crashed for a in ("--deselect", c)] + list(args)
        env = env_for(mode, arch)
        env["P11_LOG"] = str(log)
        with open(out, "a") as f:
            f.write(f"\n$ {' '.join(cmd[:8])} ... (attempt {attempt + 1})\n")
            f.flush()
            try:
                rc = subprocess.run(
                    cmd,
                    cwd=cwd,
                    env=env,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                ).returncode
            except subprocess.TimeoutExpired:
                f.write(f"\nTIMEOUT after {timeout}s\n")
                rc = "timeout"
        outcome, tb, last = read_log(log)
        if rc == "timeout":
            fail(f"{label}: pytest timed out after {timeout}s (log {out})")
        if (
            isinstance(rc, int) and rc < 0
        ):  # killed by a signal: the last test that started and did not report crashed
            if not last or last[0] in crashed:
                fail(
                    f"{label}: pytest died on signal {-rc} and no test is to blame (log {out})"
                )
            crashed.append(last[0])
            say(f"  {label}: CRASH (signal {-rc}) in {last[0]}; deselected, continuing")
            continue
        if rc not in (0, 1, 5):
            tail = out.read_text()[-1500:]
            fail(f"{label}: pytest exited {rc} (usage or internal error), tail: {tail}")
        break
    else:
        fail(f"{label}: too many crashes")
    for c in crashed:
        outcome[c], tb[c] = (
            "failed",
            "CRASH: the pytest process died on a signal in this test",
        )
    return outcome, tb, crashed, time.time() - t0


def write_args(path, ids):
    Path(path).write_text("\n".join(ids) + "\n")
    return [f"@{path}"]


# ---- classification (pure)


def is_cli(test_id, traceback):
    """A test of the `rio` command line: by file name, or a traceback through rasterio/rio or click."""
    base = test_id.split("::", 1)[0].rsplit("/", 1)[-1]
    if re.match(r"test_(rio_|cli_)", base) or base == "test_options.py":
        return True
    return bool(re.search(r"rasterio/rio/|/click/|click\.testing", traceback or ""))


def classify(failed, flaky_pass, click_pass, baseline, tbs, is_rasterio):
    """failed: ids that failed the first run. flaky_pass / click_pass: ids that passed on a repeat / with the newest click.
    baseline: {id: outcome} of the PyPI wheel for the rest. -> dict of sorted id lists.
    """
    res = {
        "flaky": [],
        "cli_failures": [],
        "library_failures": [],
        "regressions": [],
        "environmental": [],
        "not_collected_in_baseline": [],
    }
    for i in sorted(failed):
        if i in flaky_pass:
            res["flaky"].append(i)
        elif is_rasterio and i in click_pass:
            res["cli_failures" if is_cli(i, tbs.get(i)) else "library_failures"].append(
                i
            )
        elif baseline.get(i) in PASSING:
            res["regressions"].append(i)
        elif i not in baseline:
            res["not_collected_in_baseline"].append(i)
            res["regressions"].append(i)
        else:
            res["environmental"].append(i)
    return res


# ---- the run


def py_json(venv, env, code):
    r = subprocess.run(
        [str(venv / "bin" / "python"), "-c", code],
        env=env,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"import smoke failed: {r.stderr[-1500:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


SMOKE = r"""
import json, re
import netCDF4, pyogrio, pyproj, rasterio, shapely
shapely.Point(1, 2).buffer(1)
v = {
    "bindings": {"rasterio": rasterio.__version__, "pyogrio": pyogrio.__version__, "pyproj": pyproj.__version__,
                 "shapely": shapely.__version__, "netcdf4": netCDF4.__version__},
    "gdal_rasterio": rasterio.__gdal_version__, "gdal_pyogrio": pyogrio.__gdal_version_string__,
    "proj_rasterio": rasterio.__proj_version__, "proj_pyproj": pyproj.proj_version_str,
    "geos_rasterio": rasterio.__geos_version__, "geos_shapely": shapely.geos_version_string,
    "netcdf": re.match(r"[\d.]+", netCDF4.__netcdf4libversion__).group(0), "hdf5": netCDF4.__hdf5libversion__,
    "files": {m.__name__: m.__file__ for m in (rasterio, pyogrio, pyproj, shapely, netCDF4)},
    "maps": sorted({l.split()[-1] for l in open("/proc/self/maps") if ".so" in l}),
}
print(json.dumps(v))
"""


def smoke(venv, arch):
    v = py_json(venv, env_for("vendored", arch), SMOKE)
    p = pins()
    want = {
        "gdal": p["GDAL_VERSION"],
        "proj": p["PROJ_VERSION"],
        "geos": p["GEOS_VERSION"],
        "netcdf": p["NETCDF_C_VERSION"],
        "hdf5": p["HDF5_VERSION"],
    }
    got = {
        "gdal": (v["gdal_rasterio"], v["gdal_pyogrio"]),
        "proj": (v["proj_rasterio"], v["proj_pyproj"]),
        "geos": (v["geos_rasterio"], v["geos_shapely"]),
        "netcdf": (v["netcdf"],),
        "hdf5": (v["hdf5"],),
    }
    for k, vals in got.items():
        for x in vals:
            if not x.startswith(want[k]):
                fail(f"{k} reported by a binding is {x}, pins.conf says {want[k]}")
    for name, f in v["files"].items():
        if not f.startswith(f"{T}/gbx/_vendor/{arch}/"):
            fail(f"{name} was imported from {f}, not from the vendored tree")
    own = f"{T}/gbx/_native/{arch}/lib/"
    for fam in ("gdal", "proj", "geos_c", "netcdf", "hdf5"):
        mapped = [
            m for m in v["maps"] if re.match(rf"lib{fam}[-.]", os.path.basename(m))
        ]
        if not mapped or any(not m.startswith(own) for m in mapped):
            fail(f"lib{fam} is mapped from {mapped}, expected only the payload's {own}")
    # OpenSSL has its own SONAME (.4); zlib, expat and lzma are the Python's own and accepted; anything else of the
    # families the payload carries that the system also provides is reported, not failed: the SONAMEs are hashed, so
    # a system copy cannot stand in for ours
    foreign = sorted(
        {
            os.path.basename(m)
            for m in v["maps"]
            if not m.startswith((own, f"{T}/gbx/_vendor/"))
            and any(os.path.basename(m).startswith(f"lib{fam}") for fam in OWN_FAMILIES)
        }
    )
    if foreign:
        say(
            "note: system libraries of the payload's families are also mapped (Python's own modules): "
            + ", ".join(foreign)
        )
    say(
        f"ok: stack versions equal the pins: GDAL {want['gdal']}, PROJ {want['proj']}, GEOS {want['geos']}, netCDF-C {want['netcdf']}, HDF5 {want['hdf5']}"
    )
    say(
        f"ok: all five import from _vendor/{arch}; libgdal, libproj, libgeos_c, libnetcdf and libhdf5 are mapped only from the payload"
    )
    return v, want


def baseline_versions():
    out = {}
    for w in sorted(Path("/wheelhouse-pypi").glob("*.whl")):
        name, ver = w.name.split("-")[:2]
        out[name.lower()] = ver
    return out


def build_seconds(bindings_dir):
    secs = {}
    f = Path(bindings_dir) / "build-seconds.txt"
    if f.exists():
        for line in f.read_text().splitlines():
            k, v = line.split()
            secs[k.lower()] = int(v)
    return secs


def dist_smoke(arch, bindings_dir, results):
    """Our rasterio as a distribution: installed beside the payload, imported without PYTHONPATH, with the CLI."""
    venv = T / "venv"
    wheel = next(Path(bindings_dir, "dist").glob("rasterio-*.whl"))
    site = next((venv / "lib").glob("python3.*/site-packages"))
    subprocess.run(
        [
            str(venv / "bin" / "python"),
            "-m",
            "pip",
            "install",
            "-q",
            "--no-index",
            "--no-deps",
            str(wheel),
        ],
        check=True,
    )
    link = site / "databricks" / "labs" / "gbx" / "_native"
    link.mkdir(parents=True, exist_ok=True)
    (link / arch).symlink_to(T / "gbx" / "_native" / arch)
    env = env_for("pypi", arch)
    env["PYTHONPATH"] = ""
    env["GDAL_DATA"], env["PROJ_DATA"] = (
        f"{T}/gbx/_native/share/gdal",
        f"{T}/gbx/_native/share/proj",
    )
    code = (
        "import json, importlib.metadata as m, rasterio\n"
        "from rasterio.rio.main import main_group\n"
        "from click.testing import CliRunner\n"
        "r = CliRunner().invoke(main_group, ['info', '%s/work/rasterio/tests/data/RGB.byte.tif'])\n"
        "print(json.dumps({'file': rasterio.__file__, 'version': rasterio.__version__, 'dist': m.version('rasterio'),"
        " 'requires': [x for x in m.requires('rasterio') if x.startswith('click')], 'rio_exit': r.exit_code, 'rio_out': r.output[:80]}))\n"
        % T
    )
    v = py_json(venv, env, code)
    ok = (
        v["file"].startswith(str(site))
        and v["version"] == v["dist"]
        and "+gbx" in v["dist"]
        and v["requires"] == ["click>=4.0"]
        and v["rio_exit"] == 0
    )
    if not ok:
        fail(
            f"the rasterio distribution wheel does not work installed beside the payload: {v}"
        )
    say(
        f"ok: the rasterio distribution wheel installs beside the payload, imports without PYTHONPATH ({v['dist']}), "
        f"requires {v['requires']}, and `rio info` runs under click {results['click_in_main']}"
    )
    return v


def run(arch, native, bindings_dir):
    global TOOLS_BIN
    TOOLS_BIN = str(Path(native) / "bin")
    logdir = Path(bindings_dir) / "test-results"
    logdir.mkdir(parents=True, exist_ok=True)
    timeout = int(os.environ.get("P11_TIMEOUT", "7200"))
    venv, venv_nc, venv_pypi = T / "venv", T / "venv-newclick", T / "venv-pypi"
    smoke_v, want = smoke(venv, arch)
    click_ver = subprocess.run(
        [str(venv / "bin" / "python"), "-c", "import click; print(click.__version__)"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    click_nc = subprocess.run(
        [
            str(venv_nc / "bin" / "python"),
            "-c",
            "import click; print(click.__version__)",
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()
    say(f"click: main environment {click_ver}, newclick environment {click_nc}")

    vendor_base = T / "gbx" / "_vendor" / str(arch)
    site_pypi = next((venv_pypi / "lib").glob("python3.*/site-packages"))
    suites = {
        "shapely": dict(args=["shapely/tests"], base=True),
        "pyogrio": dict(args=["pyogrio/tests"], base=True),
        "pyproj": dict(cwd=T / "work" / "pyproj", args=["test", "-m", "not network"]),
        "rasterio": dict(
            cwd=T / "work" / "rasterio", args=["tests", "-m", "not network"]
        ),
        "netcdf4": dict(cwd=T / "work" / "netcdf4", args=["test"]),
    }
    secs, base_v, p = build_seconds(bindings_dir), baseline_versions(), pins()
    result = {
        "arch": arch,
        "stack": {
            "pins": want,
            "reported": {
                k: v for k, v in smoke_v.items() if k not in ("maps", "files")
            },
        },
        "click_in_main": click_ver,
        "click_newest": click_nc,
    }
    for b in BINDINGS:
        s = suites[b]
        cwd_v = vendor_base if s.get("base") else s["cwd"]
        cwd_p = site_pypi if s.get("base") else s["cwd"]
        say(f"== {b}: its own suite against the vendored package ({cwd_v})")
        first, tbs, crashed, secs_run = run_pytest(
            b, "vendored", venv, cwd_v, s["args"], arch, logdir, timeout
        )
        c = tally(logdir / f"{b}.jsonl")
        c["failed"] += len(crashed)
        if (
            not crashed
        ):  # the oracle for the counting: pytest's own summary line says the same
            want = summary_counts(logdir / f"{b}.out")
            if want != c:
                fail(
                    f"{b}: the counts from the log {c} are not pytest's summary line {want}"
                )
        failed = sorted(i for i, o in first.items() if o == "failed")
        say(
            f"   {b}: passed {c['passed']}, failed {c['failed']}, errors {c['errors']}, skipped {c['skipped']}, "
            f"xfailed {c['xfailed']}, xpassed {c['xpassed']}, subtests passed {c['subtests_passed']} in {secs_run:.0f}s"
            + (f", crashed {crashed}" if crashed else "")
        )
        flaky_pass, click_pass, baseline = set(), set(), {}
        if failed:
            args = write_args(logdir / f"{b}.failed.txt", failed)
            again, _, _, _ = run_pytest(
                f"{b}-again", "vendored", venv, cwd_v, args, arch, logdir, timeout
            )
            flaky_pass = {i for i in failed if again.get(i) in PASSING}
            rest = [i for i in failed if i not in flaky_pass]
            if rest and b == "rasterio":
                say(f"   rasterio: {len(rest)} failures again with click {click_nc}")
                nc, _, _, _ = run_pytest(
                    "rasterio-newclick",
                    "vendored",
                    venv_nc,
                    cwd_v,
                    write_args(logdir / "rasterio.rest.txt", rest),
                    arch,
                    logdir,
                    timeout,
                )
                click_pass = {i for i in rest if nc.get(i) in PASSING}
            rest = [i for i in rest if i not in click_pass]
            if rest:
                say(
                    f"   {b}: {len(rest)} failures against the PyPI wheel {base_v.get(b)}"
                )
                baseline, _, _, _ = run_pytest(
                    f"{b}-pypi",
                    "pypi",
                    venv_pypi,
                    cwd_p,
                    write_args(logdir / f"{b}.pypi.txt", rest),
                    arch,
                    logdir,
                    timeout,
                )
        cl = classify(failed, flaky_pass, click_pass, baseline, tbs, b == "rasterio")
        ver = smoke_v["bindings"][b]
        entry = {
            "version": ver,
            "baseline_version": base_v.get(b),
            "build_s": secs.get(b),
            "test_s": round(secs_run),
            **c,
            "crashed": crashed,
            "regressions": cl["regressions"],
            "environmental": cl["environmental"],
            "flaky": cl["flaky"],
            "not_collected_in_baseline": cl["not_collected_in_baseline"],
        }
        if b == "rasterio":
            entry["click_8_2_1"] = {
                "click": click_ver,
                "newest_click": click_nc,
                "cli_failures": cl["cli_failures"],
                "library_failures": cl["library_failures"],
            }
        if (
            b == "netcdf4"
        ):  # the brief's runner, as a count (the ids above come from pytest, which upstream's CI uses)
            r = subprocess.run(
                [str(venv / "bin" / "python"), "run_all.py"],
                cwd=T / "work" / "netcdf4" / "test",
                env={**env_for("vendored", arch)},
                capture_output=True,
                text=True,
            )
            (logdir / "netcdf4-run_all.out").write_text(r.stdout + r.stderr)
            tail = (r.stdout + r.stderr).strip().splitlines()[-3:]
            entry["run_all"] = {"exit": r.returncode, "tail": tail}
        result[b] = entry
        say(
            f"   {b}: {len(cl['regressions'])} regressions, {len(cl['environmental'])} environmental, {len(cl['flaky'])} flaky"
            + (
                f", click 8.2.1: {len(cl['cli_failures'])} cli, {len(cl['library_failures'])} library"
                if b == "rasterio"
                else ""
            )
        )
        (Path(bindings_dir) / "p11.json").write_text(
            json.dumps(result, indent=1) + "\n"
        )
    result["rasterio"]["dist_wheel"] = dist_smoke(arch, bindings_dir, result)
    (Path(bindings_dir) / "p11.json").write_text(json.dumps(result, indent=1) + "\n")
    tot_reg = sum(len(result[b]["regressions"]) for b in BINDINGS)
    say(
        f"P11_DONE arch={arch} regressions={tot_reg} "
        + " ".join(
            f"{b}={result[b]['passed']}p/{result[b]['failed']}f/{result[b]['errors']}e/{result[b]['skipped']}s/"
            f"{result[b]['xfailed']}xf/{result[b]['xpassed']}xp"
            for b in BINDINGS
        )
    )


def recount(bindings_dir):
    """Counts of an existing probe run, from its logs (test-results/<binding>.jsonl), written back into p11.json. The
    classification is not redone: the ids that failed must be the ones it was made from, and the counts must be the
    ones pytest printed."""
    bdir = Path(bindings_dir)
    d = json.loads((bdir / "p11.json").read_text())
    for b in BINDINGS:
        log, e = bdir / "test-results" / f"{b}.jsonl", d[b]
        t = tally(log)
        t["failed"] += len(e["crashed"])
        failed = sorted(i for i, o in read_log(log)[0].items() if o == "failed")
        listed = bdir / "test-results" / f"{b}.failed.txt"
        if failed != (
            sorted(listed.read_text().split("\n")[:-1]) if listed.exists() else []
        ):
            fail(
                f"{b}: the ids that failed are not the ones the classification was made from"
            )
        if not e["crashed"] and summary_counts(bdir / "test-results" / f"{b}.out") != t:
            fail(
                f"{b}: counts {t} differ from pytest's summary line {summary_counts(bdir / 'test-results' / f'{b}.out')}"
            )
        old = (e["passed"], e["failed"], e["skipped"])
        keep = {k: v for k, v in e.items() if k not in ("passed", "failed", "skipped")}
        d[b] = {
            **{
                k: keep[k] for k in ("version", "baseline_version", "build_s", "test_s")
            },
            **t,
            **{
                k: v
                for k, v in keep.items()
                if k not in ("version", "baseline_version", "build_s", "test_s")
            },
        }
        say(f"{b}: passed/failed/skipped {old} -> {t}")
    (bdir / "p11.json").write_text(json.dumps(d, indent=1) + "\n")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("closure")
    c.add_argument("--root", required=True)
    c.add_argument("--arch", required=True)
    r = sub.add_parser("run")
    r.add_argument("--arch", required=True)
    r.add_argument("--native", required=True)
    r.add_argument("--bindings", required=True)
    n = sub.add_parser("recount")
    n.add_argument("--bindings", required=True)
    a = ap.parse_args()
    if a.cmd == "closure":
        closure(a.root, a.arch)
    elif a.cmd == "recount":
        recount(a.bindings)
    else:
        run(a.arch, a.native, a.bindings)


if __name__ == "__main__":
    main()
