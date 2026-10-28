"""Fail fast when an env wheel runs on the other environment's runtime.

The env5 wheel serves DBR 17.3, DBR 18 and Serverless environment 5; the env6 wheel serves DBR 19,
Serverless environment 6, SQL warehouses and UC functions. Their pins differ (numpy, protobuf, ...),
so a mismatch is refused at import with an error naming the file to install. The signals, in
order: ``DATABRICKS_ENV_VERSION`` (Serverless), ``DATABRICKS_RUNTIME_VERSION`` (classic ``NN.M``,
19 and later is env6; Serverless ``client.N.M``), ``/databricks/DBR_VERSION`` (every surface,
including the UC sandbox, where it follows the function's declared environment), and the
protobuf major only when a runtime version exists but cannot be parsed. Without any Databricks
signal (a laptop, CI, a source checkout) the guard never raises.
"""

import os
import re
from typing import Mapping, Optional, Tuple


def _env_of(text: str) -> Optional[int]:
    t = (text or "").strip()
    m = re.match(r"^client\.(\d+)\.", t)
    if m:
        return int(m.group(1)) if m.group(1) in ("5", "6") else None
    m = re.match(r"^(\d+)\.\d+", t)
    return (6 if int(m.group(1)) >= 19 else 5) if m else None


def _protobuf_major() -> Optional[int]:
    try:
        from importlib.metadata import version

        return int(version("protobuf").split(".")[0])
    except Exception:  # noqa: BLE001 - absent or unparseable: no signal
        return None


def detect_runtime_env(
    environ: Mapping[str, str],
    dbr_version_text: Optional[str],
    protobuf_major: Optional[int],
) -> Optional[Tuple[int, str]]:
    """(env, the signal that said so), or None when nothing says this is Databricks."""
    v = environ.get("DATABRICKS_ENV_VERSION", "").strip()
    if v in ("5", "6"):
        return int(v), f"Serverless environment {v}"
    rt = environ.get("DATABRICKS_RUNTIME_VERSION", "").strip()
    if _env_of(rt):
        return _env_of(rt), f"runtime {rt}"
    if dbr_version_text is not None and _env_of(dbr_version_text):
        return _env_of(dbr_version_text), f"runtime {dbr_version_text.strip()}"
    if (rt or dbr_version_text is not None) and protobuf_major in (5, 6):
        return (
            protobuf_major,
            f"protobuf {protobuf_major} (unrecognised runtime version)",
        )
    return None


def check(
    build_env=None,
    version=None,
    environ=None,
    dbr_version_path="/databricks/DBR_VERSION",
) -> None:
    """Raise RuntimeError when this wheel's environment is not the runtime's; else return."""
    if build_env is None:
        try:
            from databricks.labs.gbx import _build_info
        except ImportError:
            return  # source checkout / editable install: no env wheel
        build_env, version = _build_info.ENV, _build_info.VERSION
    try:
        with open(dbr_version_path) as fh:
            dbr = fh.read()
    except OSError:
        dbr = None
    got = detect_runtime_env(
        os.environ if environ is None else environ, dbr, _protobuf_major()
    )
    if got is None or got[0] == build_env:
        return
    raise RuntimeError(
        f"geobrix: this is geobrix-{version}; the runtime is env{got[0]} ({got[1]}): "
        f"install geobrix-{version.split('+')[0]}+env{got[0]}-py3-none-any.whl"
    )
