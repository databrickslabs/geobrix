"""Public helper: register custom PROJ grid-shift directories.

Volume-hosted grid files are a *lightweight-tier* capability. The heavyweight tier
reads grid files directly from each worker's local filesystem and cannot read them
from a Unity Catalog Volume (the same reason ``rst_fromfile`` is lightweight-only),
so on the heavyweight tier grids must live on a cluster-local path.
"""

import importlib
import logging
import os
import warnings
from typing import List, Optional, Sequence, Union

from databricks.labs.gbx.core import proj_grids

_GRID_SUFFIXES = (".gsb", ".tif", ".gtx", ".gsa")  # NTv2 / PROJ geoid / NADCON-ish
# A grid dir under a Unity Catalog Volume is unreadable by heavyweight-tier workers
# (they open grid files directly, not through the credentialed UC connector).
_VOLUME_PREFIXES = ("/Volumes/", "dbfs:/Volumes/", "/dbfs/Volumes/")


def _warn_if_unusable(d: str) -> None:
    if not os.path.isdir(d):
        warnings.warn(
            f"register_proj_grids: '{d}' is not an existing directory; "
            f"grid-referencing transforms will silently lose accuracy until it exists.",
            UserWarning,
            stacklevel=3,
        )
        return
    try:
        has_grid = any(f.lower().endswith(_GRID_SUFFIXES) for f in os.listdir(d))
    except OSError:
        has_grid = (
            True  # unreadable now (e.g. Volume eventual-consistency); don't cry wolf
        )
    if not has_grid:
        warnings.warn(
            f"register_proj_grids: '{d}' contains no recognizable grid file "
            f"({', '.join(_GRID_SUFFIXES)}).",
            UserWarning,
            stacklevel=3,
        )


def _reregister_active_light_tiers(spark) -> None:
    """Re-run register() for whichever light tiers are already registered so live
    SQL bindings get fresh closures embedding the current grid dirs."""
    if spark is None:
        return
    for modname in (
        "databricks.labs.gbx.pyrx.functions",
        "databricks.labs.gbx.pyvx.functions",
        "databricks.labs.gbx.pygx.functions",
    ):
        try:
            mod = importlib.import_module(modname)
        except Exception:
            continue
        if getattr(mod, "_gbx_registered", False):  # set by register() — Task 4
            mod.register(spark)


def _warn_heavy_volume_grids(result) -> None:
    """Warn when the heavy tier is active but grids live on a Volume it cannot read.

    Heavyweight-tier workers open grid files directly on their local filesystem and
    cannot read a Unity Catalog Volume, so a Volume-hosted grid silently fails to load
    on executors (transforms return NULL). Surface it at registration time instead.
    """
    vol_dirs = [d for d in result if str(d).startswith(_VOLUME_PREFIXES)]
    if vol_dirs:
        warnings.warn(
            "register_proj_grids: the heavyweight tier reads grid files directly from "
            "each worker's local disk and cannot read them from a Unity Catalog Volume "
            f"({', '.join(vol_dirs)}) — grid-referencing transforms will return NULL on "
            "the heavyweight tier. Stage the grid files to a cluster-local path (e.g. via "
            "a cluster init script) and register that path instead; Volume-hosted grids "
            "are supported on the lightweight tier.",
            UserWarning,
            stacklevel=4,
        )


_LOG = logging.getLogger(__name__)
_heavy_refusal_logged = False


def _is_py4j_refusal(exc: BaseException) -> bool:
    """A cluster in standard (shared) access mode refuses py4j calls to classes it has not allowed. py4j has no Python
    class for it: the refusal is a ``Py4JError`` whose text names the JVM's ``py4j.security.Py4JSecurityException``
    ("... is not whitelisted")."""
    try:
        from py4j.protocol import Py4JError
    except ImportError:  # no py4j, so no py4j refusal
        return False
    text = str(exc)
    return isinstance(exc, Py4JError) and (
        "Py4JSecurityException" in text or "is not whitelisted" in text
    )


def _apply_heavy(spark, result) -> None:
    """Set the heavy-tier JVM grid registry when this session has a JVM with the GeoBrix JAR.

    Three cases are quiet, by design: no JVM (Spark Connect, Serverless, or no session: ``spark._jvm`` is absent); a
    JVM without the GeoBrix JAR (a light-only session: py4j resolves the class name to a ``JavaPackage``); and a
    cluster whose access mode refuses py4j calls to GeoBrix classes (one INFO line says so). In each, the light-tier
    registration stands. Any other failure raises: a swallowed one once left the JVM without the grids, silently.
    """
    global _heavy_refusal_logged
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        return
    try:
        reg = jvm.com.databricks.labs.gbx.operations.ProjGridRegistry
        if type(reg).__name__ == "JavaPackage":
            return
        # ProjGridRegistry.set takes a Scala Seq: py4j would hand a Python list over as a java.util.List, which
        # matches no `set`. Push the full de-duped set (an idempotent replace).
        reg.set(jvm.org.apache.spark.api.python.PythonUtils.toSeq(list(result)), True)
    except Exception as exc:
        if not _is_py4j_refusal(exc):
            raise
        if not _heavy_refusal_logged:
            _heavy_refusal_logged = True
            _LOG.info(
                "geobrix: heavy-tier PROJ grid registration is unavailable in this cluster's access mode (py4j "
                "refused the call); the light tier has the grid directories."
            )
        return
    # Heavy tier IS active: warn if any registered dir is a Volume executors can't read.
    _warn_heavy_volume_grids(result)


def projsync_path() -> Optional[str]:
    """PROJ's grid downloader, ``projsync``, as the GeoBrix env wheel installs it, or None (no env
    wheel, or not on Linux x86_64/aarch64).

    Run it to fetch grids from the PROJ CDN (cdn.proj.org, so it needs network access) into a local
    directory or a Volume, e.g. ``subprocess.run([projsync_path(), "--source-id", "uk_os",
    "--target-dir", "/Volumes/<catalog>/<schema>/proj-grids"], check=True)``, then register that
    directory with :func:`register_proj_grids`. ``--list-files`` lists what the CDN offers.
    """
    from databricks.labs.gbx import _native

    return _native.projsync_path()


def register_proj_grids(
    spark,
    dirs: Union[str, Sequence[str]],
    *,
    replace: bool = False,
) -> List[str]:
    """Register one or more dirs of PROJ grid-shift files so CRS transforms find them.

    Volume-hosted grids work on the lightweight tier (every worker reads them via the
    credentialed UC connector). On the heavyweight tier, register a cluster-local path
    instead — a Volume path warns because heavy workers cannot read it. See the
    session-start contract in the docs.
    """
    result = proj_grids.set_registered_dirs(dirs, replace=replace)
    # The driver's own PROJ context takes the union now, as every worker's does on its first UDF: a context built
    # before this registration keeps its search path, so the environment variable alone would not reach it.
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env(list(result))
    for d in (dirs if not isinstance(dirs, str) else [dirs]):
        _warn_if_unusable(d)
    _reregister_active_light_tiers(spark)
    _apply_heavy(spark, result)
    return result
