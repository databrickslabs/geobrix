"""Build probe for the UC functions: which geobrix build does a UC Python function import?

A UC Python function names its wheel in ``ENVIRONMENT (dependencies = '["geobrix @ file:///Volumes/.../geobrix-<v>+env6-...whl"]')``.
The wheel keeps one name and one version across builds. If Unity Catalog serves a cached environment for a spec it has
seen (the same string), a wheel staged again over the same path would run the older build, and every UC result would
test code that is not the staged wheel. This is the hole closed for cluster runs by ``examples/tests/_wheel_guard.py``.

``assert_installed_build`` closes it for UC. It creates a function ``gbx_probe_build`` with the SAME ENVIRONMENT spec as
the installed functions, calls it once, and compares what the function imported (build commit, dirty flag, digest of the
package's ``.py`` files) with the identity of the wheel file now at the path. The digest is the one of ``_wheel_guard``:
sha256 over the sorted ``path NUL sha256(content)`` lines.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import zipfile
from typing import Dict, List

PROBE_NAME = "gbx_probe_build"
PKG_PREFIX = "databricks/labs/gbx/"
_SKIP_PARTS = ("_vendor", "__pycache__")


# ---- identity: the code below also runs inside the UC sandbox (``sandbox_source`` ships its text) -----------------


def _wanted(rel):
    return rel.endswith(".py") and not any(p in _SKIP_PARTS for p in rel.split("/"))


def _digest(files):
    h = hashlib.sha256()
    for rel in sorted(files):
        h.update(("%s\0%s\n" % (rel, files[rel])).encode())
    return h.hexdigest()


def _build_fields(text):
    sha = re.search(r'GIT_SHA\s*=\s*"([0-9a-f]*)"', text or "")
    dirty = re.search(r"GIT_DIRTY\s*=\s*(True|False)", text or "")
    return (sha.group(1) if sha else None), (
        (dirty.group(1) == "True") if dirty else None
    )


def _tree_files(root):
    files = {}
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if d not in _SKIP_PARTS]
        for f in fn:
            full = os.path.join(dp, f)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            if _wanted(rel):
                with open(full, "rb") as fh:
                    files[rel] = hashlib.sha256(fh.read()).hexdigest()
    return files


def imported_identity():
    """What this process imports as geobrix, as a JSON string: git_sha, dirty, digest, module. Errors are reported."""
    try:
        import importlib

        gbx = importlib.import_module("databricks.labs.gbx")
        root = os.path.dirname(os.path.abspath(gbx.__file__))
        info = ""
        if os.path.exists(os.path.join(root, "_build_info.py")):
            with open(
                os.path.join(root, "_build_info.py"), encoding="utf-8", errors="replace"
            ) as fh:
                info = fh.read()
        sha, dirty = _build_fields(info)
        return json.dumps(
            {
                "git_sha": sha,
                "dirty": dirty,
                "digest": _digest(_tree_files(root)),
                "module": os.path.abspath(gbx.__file__),
            }
        )
    except Exception as exc:  # noqa: BLE001 - the report is the verdict
        return json.dumps({"error": "%s: %s" % (type(exc).__name__, str(exc)[:300])})


def sandbox_source() -> str:
    """The body of the probe function: the identity code above, then ``handler``. It imports only the standard library."""
    parts = [
        "import hashlib\nimport json\nimport os\nimport re\n\n",
        "_SKIP_PARTS = %r\n\n\n" % (_SKIP_PARTS,),
    ]
    for fn in (_wanted, _digest, _build_fields, _tree_files, imported_identity):
        parts.append(inspect.getsource(fn) + "\n\n")
    parts.append("def handler():\n    return imported_identity()\n")
    return "".join(parts)


def wheel_identity(path: str) -> Dict[str, object]:
    """{git_sha, dirty, digest} of the wheel file at ``path``, by the rule ``imported_identity`` applies to the tree."""
    files, info = {}, ""
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if not name.startswith(PKG_PREFIX):
                continue
            rel = name[len(PKG_PREFIX) :]
            if rel == "_build_info.py":
                info = z.read(name).decode("utf-8", "replace")
            if _wanted(rel):
                files[rel] = hashlib.sha256(z.read(name)).hexdigest()
    sha, dirty = _build_fields(info)
    return {"git_sha": sha, "dirty": dirty, "digest": _digest(files)}


def problems(got: dict, want: dict, wheel: str = "the staged wheel") -> List[str]:
    """Why what the function imported is not ``want`` (the wheel's identity), or []."""
    if "error" in got:
        return ["the probe function could not import geobrix: %s" % got["error"]]
    out = []
    for key in ("git_sha", "dirty", "digest"):
        if got.get(key) != want.get(key):
            out.append(
                "%s: the function imports %r, %s has %r"
                % (key, got.get(key), wheel, want.get(key))
            )
    return out


# ---- the probe on a workspace ------------------------------------------------------------------------------------


def probe_ddl(
    catalog: str,
    schema: str,
    wheel: str,
    deps: Dict[str, str],
    version: str,
    shared: bool = True,
) -> str:
    """CREATE OR REPLACE for ``gbx_probe_build``, with the ENVIRONMENT spec ``uc.install`` gives its Python functions."""
    from databricks.labs.gbx.uc import ddl

    q = f"{ddl.quote_ident('catalog', catalog)}.{ddl.quote_ident('schema', schema)}"
    env = json.dumps([f"geobrix @ file://{wheel}"] + ddl._env_deps([], deps, shared))
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{PROBE_NAME}()\n"
        f"RETURNS STRING\nLANGUAGE PYTHON\nDETERMINISTIC\nCOMMENT {ddl.quote_str('geobrix ' + version + ': build probe')}\n"
        f"ENVIRONMENT (dependencies = {ddl.quote_str(env)}, environment_version = '6')\n"
        f"HANDLER 'handler'\nAS $$\n{sandbox_source()}$$"
    )


def assert_installed_build(
    spark, catalog: str, schema: str, volume_dir: str, say=print
) -> Dict[str, object]:
    """Create the probe over the install folder's wheel, call it once, and raise unless it imports that wheel's build.

    Returns the identity the function reported. Raises RuntimeError with a Fix when a UC function imports another build.
    """
    from databricks.labs.gbx import __version__, manifest, uc
    from databricks.labs.gbx.uc import ddl

    wheel = uc._single_wheel(volume_dir)
    want = wheel_identity(wheel)
    spark.sql(
        probe_ddl(
            catalog,
            schema,
            wheel,
            manifest.load().uc_dependencies,
            __version__.split("+", 1)[0],
        )
    )
    q = f"{ddl.quote_ident('catalog', catalog)}.{ddl.quote_ident('schema', schema)}"
    got = json.loads(spark.sql(f"SELECT {q}.{PROBE_NAME}()").collect()[0][0])
    say(
        "[UC build probe] function imports commit %s dirty=%s digest %s; %s has commit %s dirty=%s digest %s"
        % (
            (got.get("git_sha") or "none")[:12],
            got.get("dirty"),
            (got.get("digest") or "none")[:12],
            os.path.basename(wheel),
            (want["git_sha"] or "none")[:12],
            want["dirty"],
            str(want["digest"])[:12],
        )
    )
    bad = problems(got, want, os.path.basename(wheel))
    if bad:
        raise RuntimeError(
            "a UC function imports another geobrix build than the wheel in %s: %s. "
            "Fix: stage the wheel into a NEW folder (or under a new file name) and install from it; "
            "never stage a changed wheel over the same path."
            % (volume_dir, "; ".join(bad))
        )
    return got
