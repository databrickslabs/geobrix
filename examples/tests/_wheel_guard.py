"""Wheel guard: a run on Databricks compute tests the wheel the launcher staged, not an older one of the same version.

A wheel installed as a cluster library keeps its name and version (``geobrix-0.6.0+env6``). A later
``%pip install <the newly staged wheel>`` of the same version is "already satisfied" and changes nothing, so a
run once tested 4-day-old code and gave a false verdict. Two parts close that hole:

1. every test, bench or probe install FORCES the staged wheel (``--force-reinstall --no-deps`` for geobrix
   itself, the dependencies unchanged): ``force_line`` / ``force_pip_lines`` below;
2. every run then GUARDS what it imports: ``guard_source`` makes the notebook cell that, after the install and the
   import, prints the imported package's build commit, dirty flag, module path and a digest of its source, and
   FAILS the run when they are not the staged wheel's. It checks the driver AND the Python workers (a UDF), since
   the workers import from the cluster library path, which the notebook's own install may not have changed.

The identity compared is the digest of the package's ``.py`` files (``_vendor`` and caches left out), the same
function over the staged wheel's zip and over the installed tree. The build commit (``GIT_SHA`` of the wheel's
``_build_info.py``) is the readable half of it; the launcher passes the commit it staged (``expect_sha``) when it knows it.

Stdlib only, and no import of geobrix at module level: the guard cell is this file's text plus one call, so it runs
whatever the installed geobrix is, even one that predates the guard.
"""

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

PKG_PREFIX = "databricks/labs/gbx/"
_SKIP_PARTS = ("_vendor", "__pycache__")


# ---------------------------------------------------------------------------------------------- identity


def _wanted(rel):
    parts = rel.split("/")
    return rel.endswith(".py") and not any(p in _SKIP_PARTS for p in parts)


def _digest(files):
    """sha256 over the sorted ``path NUL sha256(content)`` of ``files`` ({path: sha256})."""
    h = hashlib.sha256()
    for rel in sorted(files):
        h.update(("%s\0%s\n" % (rel, files[rel])).encode())
    return h.hexdigest()


def _build_fields(text):
    """(git_sha, dirty, version) read from the text of a ``_build_info.py`` (None for a missing field)."""
    sha = re.search(r'GIT_SHA\s*=\s*"([0-9a-f]*)"', text or "")
    dirty = re.search(r"GIT_DIRTY\s*=\s*(True|False)", text or "")
    version = re.search(r'VERSION\s*=\s*"([^"]*)"', text or "")
    return (
        sha.group(1) if sha else None,
        (dirty.group(1) == "True") if dirty else None,
        version.group(1) if version else None,
    )


def wheel_identity(path):
    """The identity of a wheel file: name, git_sha, dirty, version, files, digest.

    A wheel on a Volume is copied to local disk first (a zip needs random reads, which a FUSE Volume avoids)."""
    tmp = tempfile.mkdtemp(prefix="gbx-guard-")
    try:
        local = os.path.join(tmp, os.path.basename(path))
        shutil.copy(path, local)
        files, info = {}, ""
        with zipfile.ZipFile(local) as z:
            for name in z.namelist():
                if not name.startswith(PKG_PREFIX):
                    continue
                rel = name[len(PKG_PREFIX):]
                if rel == "_build_info.py":
                    info = z.read(name).decode("utf-8", "replace")
                if _wanted(rel):
                    files[rel] = hashlib.sha256(z.read(name)).hexdigest()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    sha, dirty, version = _build_fields(info)
    return {"name": os.path.basename(path), "git_sha": sha, "dirty": dirty, "version": version, "files": files,
            "digest": _digest(files)}


def installed_identity(with_files=True):
    """The identity of the geobrix this process imports: module, root, git_sha, dirty, version, files, digest, host, pid.

    Raises ImportError when geobrix cannot be imported."""
    import importlib
    import socket

    gbx = importlib.import_module("databricks.labs.gbx")

    root = os.path.dirname(os.path.abspath(gbx.__file__))
    files, info = {}, ""
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if d not in _SKIP_PARTS]
        for f in fn:
            full = os.path.join(dp, f)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            if rel == "_build_info.py":
                info = open(full, encoding="utf-8", errors="replace").read()
            if _wanted(rel):
                with open(full, "rb") as fh:
                    files[rel] = hashlib.sha256(fh.read()).hexdigest()
    sha, dirty, version = _build_fields(info)
    out = {"module": os.path.abspath(gbx.__file__), "root": root, "git_sha": sha, "dirty": dirty, "version": version,
           "digest": _digest(files), "host": socket.gethostname(), "pid": os.getpid()}
    if with_files:
        out["files"] = files
    return out


def worker_identity():
    """What a Python worker imports, as a JSON string (a UDF returns it). A worker that cannot import says so."""
    try:
        return json.dumps(installed_identity(with_files=False))
    except Exception as exc:  # noqa: BLE001 - the verdict is the report
        import socket

        return json.dumps({"error": "%s: %s" % (type(exc).__name__, str(exc)[:300]), "host": socket.gethostname()})


# ---------------------------------------------------------------------------------------------- the verdict


def _short(x, n=12):
    return (x or "none")[:n]


def _describe(label, ident):
    return "%s: module=%s version=%s git_sha=%s dirty=%s digest=%s" % (
        label, ident.get("module"), ident.get("version"), _short(ident.get("git_sha")), ident.get("dirty"),
        _short(ident.get("digest")))


def _differing(installed, expected):
    a, b = installed.get("files") or {}, expected.get("files") or {}
    diff = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
    return diff[:3], len(diff)


def fix_text(wheel):
    return (
        "Fix: another geobrix of the same version is on this compute (a cluster library, an init script or an earlier "
        "install) and wins over the staged wheel%s. Uninstall that library from the cluster (Compute > Libraries, or "
        "`databricks libraries uninstall`) and restart the cluster, or run on a fresh cluster or Serverless; then "
        "launch again. The notebook already installed the staged wheel with --force-reinstall --no-deps." %
        (" " + wheel if wheel else ""))


def resolve_expected(expected):
    """The expected identity: the staged wheel's (read at ``expected['wheel']``) with the launcher's own fields over it.

    ``expected`` keys: ``wheel`` (a path the compute can read), ``git_sha`` (the commit the launcher staged),
    ``digest`` (a source digest the launcher computed). Returns (identity or None, problems)."""
    expected = dict(expected or {})
    out, problems = {}, []
    wheel = expected.get("wheel")
    if wheel:
        try:
            out = wheel_identity(wheel)
        except Exception as exc:  # noqa: BLE001
            problems.append("cannot read the staged wheel %s to learn its identity (%s: %s)" % (
                wheel, type(exc).__name__, str(exc)[:200]))
    for k in ("git_sha", "digest"):
        if expected.get(k):
            if out.get(k) and out[k] != expected[k]:
                problems.append("the launcher staged %s %s but the wheel %s says %s" % (
                    k, _short(expected[k]), wheel, _short(out[k])))
            out[k] = expected[k]
    return (out or None), problems


def check(installed, expected, label="driver"):
    """The problems (empty when it matches) of one imported geobrix against the expected identity."""
    if expected is None:
        return []
    if installed.get("error"):
        return ["%s cannot import geobrix: %s" % (label, installed["error"])]
    problems = []
    if expected.get("git_sha") and installed.get("git_sha") != expected["git_sha"]:
        problems.append("%s imports build commit %s, the staged wheel is commit %s (%s)" % (
            label, _short(installed.get("git_sha")), _short(expected["git_sha"]), installed.get("module")))
    if expected.get("digest") and installed.get("digest") != expected["digest"]:
        msg = "%s imports source digest %s, the staged wheel's is %s (%s)" % (
            label, _short(installed.get("digest")), _short(expected["digest"]), installed.get("module"))
        if installed.get("files") and expected.get("files"):
            names, n = _differing(installed, expected)
            msg += "; %d file(s) differ, such as %s" % (n, ", ".join(names))
        problems.append(msg)
    if expected.get("dirty") is not None and installed.get("dirty") is not None and installed["dirty"] != expected["dirty"]:
        problems.append("%s imports a build with dirty=%s, the staged wheel has dirty=%s" % (
            label, installed["dirty"], expected["dirty"]))
    return problems


def collect_workers(spark, tasks=8):
    """The distinct identities (dicts) the Python workers import: a UDF over ``tasks`` partitions."""
    import pyspark.sql.functions as F

    @F.udf("string")
    def _who(_):
        return worker_identity()

    rows = spark.range(tasks).repartition(tasks).select(_who("id").alias("w")).distinct().collect()
    return [json.loads(r["w"]) for r in rows]


def run_guard(expected, spark=None, say=print, workers=None):
    """Print what is imported, fail (RuntimeError) when it is not the staged wheel.

    ``expected`` is as in ``resolve_expected``; none given means nothing is staged to compare with: the identity is
    printed and the run goes on (said so). ``spark`` makes the Python workers be checked too; ``workers`` is a
    function returning their identities (a test fakes it). Returns the verdict dict."""
    exp, problems = resolve_expected(expected)
    say("WHEEL GUARD expected: %s" % (
        "none staged: nothing to compare with" if exp is None else
        "wheel=%s git_sha=%s dirty=%s digest=%s" % ((expected or {}).get("wheel"), _short(exp.get("git_sha")),
                                                    exp.get("dirty"), _short(exp.get("digest")))))
    try:
        drv = installed_identity()
    except Exception as exc:  # noqa: BLE001
        drv = {"error": "%s: %s" % (type(exc).__name__, str(exc)[:300])}
    say(_describe("WHEEL GUARD driver", drv) if not drv.get("error") else "WHEEL GUARD driver: cannot import geobrix: %s" % drv["error"])
    problems += check(drv, exp, "driver")
    seen = []
    if workers is not None or spark is not None:
        try:
            seen = (workers or (lambda: collect_workers(spark)))()
        except Exception as exc:  # noqa: BLE001 - a worker check that cannot run is a failed guard
            problems.append("the worker check could not run (%s: %s)" % (type(exc).__name__, str(exc)[:300]))
        for w in seen:
            say(_describe("WHEEL GUARD worker %s" % (w.get("host") or "?"), w) if not w.get("error")
                else "WHEEL GUARD worker %s: cannot import geobrix: %s" % (w.get("host"), w["error"]))
            problems += check(w, exp, "worker %s" % (w.get("host") or "?"))
        if not seen and not problems:
            problems.append("the worker check returned no worker")
    verdict = {"ok": not problems, "problems": problems, "driver": {k: v for k, v in drv.items() if k != "files"},
               "workers": seen}
    if problems:
        msg = "WHEEL GUARD FAILED:\n  - " + "\n  - ".join(problems) + "\n" + fix_text((expected or {}).get("wheel"))
        say(msg)
        raise RuntimeError(msg)
    say("WHEEL GUARD ok: %s%s" % ("driver" if not seen else "driver and %d worker identit%s" % (
        len(seen), "y" if len(seen) == 1 else "ies"), "" if exp is not None else " (reported, not compared)"))
    return verdict


# ---------------------------------------------------------------------------------------------- the launcher side


def guard_source(wheel=None, git_sha=None, digest=None, workers=True):
    """The text of the notebook cell that runs the guard: this file, then the call with what the launcher staged.

    ``wheel``: the Volume path of the staged wheel; ``git_sha``: the commit the launcher staged, when it knows it;
    ``workers=False`` skips the worker check (a notebook with no ``spark``)."""
    expected = {k: v for k, v in (("wheel", wheel), ("git_sha", git_sha), ("digest", digest)) if v}
    body = Path(__file__).with_suffix(".py").read_text(encoding="utf-8")
    return (
        "# Wheel guard: the run tests the staged wheel (see _wheel_guard.py); it fails here when the imported geobrix is another.\n"
        + body
        + "\nWHEEL_GUARD = run_guard(%r, spark=%s)\n" % (expected, "spark" if workers else "None")
    )


FORCE_FLAGS = "--force-reinstall --no-deps"
_PIP_FILE = re.compile(r"@\s*file://(/[^\"'\s]+\.whl)")


def force_line(wheel, quiet=False):
    """The ``%pip`` line that puts the staged wheel in place whatever same-version geobrix is installed (geobrix only)."""
    return '%%pip install %s%s "geobrix @ file://%s"' % ("--quiet " if quiet else "", FORCE_FLAGS, wheel)


def force_pip_lines(lines):
    """``lines`` (the lines of a notebook's code cells, each with its newline) with a forced geobrix install before every
    ``%pip install ... geobrix ... @ file://<wheel>`` line that is not forced already. Returns (lines, count)."""
    out, count = [], 0
    for line in lines:
        s = line.lstrip()
        m = _PIP_FILE.search(line)
        if s.startswith("%pip install") and m and "geobrix" in line.split("@")[0] and "--force-reinstall" not in line \
                and not (out and out[-1] == force_line(m.group(1), quiet="--quiet" in line) + "\n"):
            out.append(force_line(m.group(1), quiet="--quiet" in line) + "\n")
            count += 1
        out.append(line)
    return out, count


def pip_wheel_of(lines):
    """The wheel (a path) the first ``%pip install ... geobrix @ file://...whl`` line installs, or None."""
    for line in lines:
        m = _PIP_FILE.search(line)
        if line.lstrip().startswith("%pip") and m and "geobrix" in line.split("@")[0]:
            return m.group(1)
    return None

