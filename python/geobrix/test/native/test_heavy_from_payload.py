"""The heavy tier runs the env wheel payload's GDAL (local probe P9).

- conf only: a fresh JVM whose first GDAL user has no ExpressionConfig (KernelGdal, a vector writer's load on a task)
  and only the session's ``spark.databricks.labs.gbx.native.dir`` loads that payload's GDAL (``ConfOnlyProbe``).
- guard: a JVM that already loaded another GDAL's JNI is refused the payload's (``DoubleLoadProbe``, exit 3).
- oracle: one Spark session on the staged JAR whose JVM sees the payload only through that conf. The light and then the
  heavy SQL functions give the oracle's answers (388 x 385, 3 Shapefile features), the JVM's GDAL is the pinned
  build, PROJ in the JVM transforms 27700 -> 4326 as pyproj does, and the JVM maps the payload's libgdal and no other.
  It needs the JAR built with the payload build's GDAL classes (pom.xml's gdal.java.version); else it skips.

Needs Linux, a payload (``GBX_NATIVE_ROOT``, e.g. ``build/env-wheel/env6/payload``) and the staged JAR. In the
dev container::

    GBX_NATIVE_ROOT=build/env-wheel/env6/payload bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/native/test_heavy_from_payload.py

GDAL's Java classes also load the first ``libgdalalljni.so`` on the JVM's library path, by name. The dev image's
system JNI (GDAL 3.11.4) is on that path, so these JVMs put the payload's ``lib/`` first (GDALManager refuses a
second GDAL otherwise). An image whose ``/usr/lib/libgdalalljni.so`` is a link to the payload's (DCS, init script),
or one with no system GDAL (Databricks runtimes), needs no such setting.
"""

import os
import re
import subprocess
import zipfile
from pathlib import Path

import pytest

from databricks.labs.gbx import _native

pytestmark = pytest.mark.integration

HERE = Path(__file__).resolve()
REPO = HERE.parents[4]
JARS = sorted(
    (REPO / "python" / "geobrix" / "lib").glob("geobrix-*-jar-with-dependencies.jar")
)
SYSTEM_JNI = "/usr/lib/libgdalalljni.so"
LIBGDAL = re.compile(r"/libgdal(-[0-9a-f]{8})?\.so(\.\d+)+$")
DEV_LIBRARY_PATH = "/usr/local/lib:/usr/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native"
CONSTANTS = "org/gdal/gdalconst/gdalconstConstants.class"


def _pin(key):
    for line in (REPO / "scripts" / "native" / "pins.conf").read_text().splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()


def _build_classes():
    ver = f"{_pin('GDAL_VERSION')}-gbx{_pin('GBX_NATIVE_REVISION')}"
    return REPO / "scripts" / "native" / "maven" / f"gdal-{ver}.jar"


@pytest.fixture(scope="module")
def native_dir():
    d = _native.native_dir() if os.environ.get("GBX_NATIVE_ROOT") else None
    if not d:
        pytest.skip(
            "PRECONDITION MISSING: a native payload for this machine (GBX_NATIVE_ROOT, Linux); P9 not run"
        )
    if not JARS:
        pytest.skip(
            "PRECONDITION MISSING: the staged JAR (gbx-test-parity.sh stages it); P9 not run"
        )
    return d


def _java(native_dir, main, *args, classes=None):
    """`main` in a fresh JVM on the staged JAR and pyspark's jars, with the payload's lib/ first on the library path
    and no GBX_NATIVE_ROOT in its environment."""
    import pyspark

    cp = f"{JARS[-1]}:{os.path.join(os.path.dirname(pyspark.__file__), 'jars', '*')}"
    env = {k: v for k, v in os.environ.items() if k != "GBX_NATIVE_ROOT"}
    lib = f"-Djava.library.path={os.path.join(native_dir, 'lib')}:{DEV_LIBRARY_PATH}"
    target = [main] if classes else [str(HERE.parent / f"{main}.java")]
    return subprocess.run(
        ["java", lib, "-cp", f"{classes}:{cp}" if classes else cp, *target, *args],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )


@pytest.fixture(scope="module")
def conf_only_probe(native_dir, tmp_path_factory):
    """ConfOnlyProbe compiled to a class directory (a task deserializes its lambda from the application class path)."""
    import pyspark

    out = tmp_path_factory.mktemp("conf_only")
    cp = f"{JARS[-1]}:{os.path.join(os.path.dirname(pyspark.__file__), 'jars', '*')}"
    res = subprocess.run(
        ["javac", "-cp", cp, "-d", str(out), str(HERE.parent / "ConfOnlyProbe.java")],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert res.returncode == 0, res.stdout + res.stderr
    return out


@pytest.mark.parametrize("first_user", ["kernel", "writer"])
def test_the_session_conf_alone_loads_the_payload(
    native_dir, conf_only_probe, first_user
):
    res = _java(
        native_dir, "ConfOnlyProbe", first_user, native_dir, classes=conf_only_probe
    )
    lines = res.stdout.splitlines()
    assert f"RESULT ok:{_pin('GDAL_VERSION')}" in lines, res.stdout + res.stderr[-3000:]
    mapped = [ln.split(" ", 1)[1] for ln in lines if ln.startswith("MAPPED ")]
    assert len(set(mapped)) == 1, mapped
    assert mapped[0].startswith(
        os.path.join(os.path.realpath(native_dir), "lib", "libgdal-")
    ), mapped


def test_a_second_gdal_in_the_jvm_is_refused(native_dir):
    if not os.path.exists(SYSTEM_JNI):
        pytest.skip(
            f"PRECONDITION MISSING: a system GDAL JNI at {SYSTEM_JNI}; the guard not run"
        )
    res = _java(native_dir, "DoubleLoadProbe", SYSTEM_JNI, native_dir)
    assert res.returncode == 3, res.stdout + res.stderr
    assert "a different GDAL is already loaded" in res.stderr, res.stderr


@pytest.fixture(scope="module")
def spark(native_dir):
    with zipfile.ZipFile(JARS[-1]) as jar, zipfile.ZipFile(_build_classes()) as build:
        if jar.read(CONSTANTS) != build.read(CONSTANTS):
            pytest.skip(
                "PRECONDITION MISSING: the staged JAR's GDAL classes are not the payload build's "
                "(pom.xml's gdal.java.version); the oracle not run"
            )
    from pyspark.sql import SparkSession

    if SparkSession.getActiveSession() is not None:
        pytest.skip(
            "A Spark session is already live; run this file alone (gbx-test-parity.sh --path ...)"
        )
    root = os.environ.pop(
        "GBX_NATIVE_ROOT"
    )  # the JVM learns the payload from the conf alone
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("gbx-heavy-from-payload")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.driver.extraJavaOptions",
                f"-Djava.library.path={os.path.join(native_dir, 'lib')}:{DEV_LIBRARY_PATH}",
            )
            .config("spark.jars", str(JARS[-1]))
            .config("spark.databricks.labs.gbx.native.dir", native_dir)
            .config(
                "spark.executorEnv.GBX_NATIVE_ROOT", root
            )  # the Python workers' payload
            .getOrCreate()
        )
    finally:
        os.environ["GBX_NATIVE_ROOT"] = root
    yield session
    session.stop()


def _jvm_maps(spark):
    pid = spark._jvm.java.lang.ProcessHandle.current().pid()
    with open(f"/proc/{pid}/maps") as fh:
        return {ln.split(None, 5)[-1].strip() for ln in fh if "/" in ln}


def test_light_and_heavy_give_the_oracle_answer(spark, native_dir, tmp_path):
    import pyproj

    from databricks.labs.gbx._native import _selfcheck

    light = _selfcheck.light_sql(spark)
    heavy = _selfcheck.heavy_sql(spark, str(tmp_path))

    assert light == {"ok": True, "width": 388, "height": 385}
    assert heavy["raster"] == light
    assert heavy["shapefile_count"] == 3
    jvm = spark._jvm
    config = jvm.com.databricks.labs.gbx.expressions.ExpressionConfig.apply(
        spark._jsparkSession
    )
    assert config.nativeDir().get() == native_dir
    assert jvm.com.databricks.labs.gbx.kernels.KernelGdal.probe() == (
        "ok:" + _pin("GDAL_VERSION")
    )
    # PROJ in the JVM: the payload's data, the same answer as pyproj
    osr = jvm.org.gdal.osr
    src, dst = osr.SpatialReference(), osr.SpatialReference()
    assert (src.ImportFromEPSG(27700), dst.ImportFromEPSG(4326)) == (0, 0)
    for s in (src, dst):
        s.SetAxisMappingStrategy(osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
    got = list(
        osr.CoordinateTransformation(src, dst).TransformPoint(530000.0, 180000.0)
    )[:2]
    want = pyproj.Transformer.from_crs(27700, 4326, always_xy=True).transform(
        530000.0, 180000.0
    )
    assert got == pytest.approx(list(want), abs=1e-7)
    libgdal = sorted(p for p in _jvm_maps(spark) if LIBGDAL.search(p))
    assert len(libgdal) == 1, libgdal
    assert libgdal[0].startswith(
        os.path.join(os.path.realpath(native_dir), "lib", "libgdal-")
    ), libgdal
