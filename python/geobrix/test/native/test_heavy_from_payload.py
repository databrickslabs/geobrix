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

import glob
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
# The dev image's own GDAL 3.11 JNI (scripts/docker/Dockerfile links SYSTEM_JNI to it). gbx:docker:start re-links
# SYSTEM_JNI into the env6 payload; this file stays, so the guard rows still have a second GDAL to refuse.
IMAGE_JNI = "/usr/local/lib/jni/libgdalalljni.so"
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


def _java(
    native_dir,
    main,
    *args,
    classes=None,
    payload_first=True,
    first_on_cp=None,
    library_path=None,
):
    """`main` in a fresh JVM on the staged JAR and pyspark's jars, with no GBX_NATIVE_ROOT in its environment and,
    unless `payload_first` is False (the JVM's default library path), the payload's lib/ first on the library path;
    `library_path` replaces that path. `first_on_cp` goes ahead of the JAR on the class path.
    """
    import pyspark

    cp = f"{JARS[-1]}:{os.path.join(os.path.dirname(pyspark.__file__), 'jars', '*')}"
    cp = ":".join(str(x) for x in (first_on_cp, classes, cp) if x)
    # as on an executor: no payload root and none of the data paths the Python activation set in this process
    drop = {"GBX_NATIVE_ROOT", "GDAL_DATA", "PROJ_DATA", "PROJ_LIB"}
    env = {k: v for k, v in os.environ.items() if k not in drop}
    lib = (
        [f"-Djava.library.path={library_path}"]
        if library_path
        else (
            [
                f"-Djava.library.path={os.path.join(native_dir, 'lib')}:{DEV_LIBRARY_PATH}"
            ]
            if payload_first
            else []
        )
    )
    target = [main] if classes else [str(HERE.parent / f"{main}.java")]
    return subprocess.run(
        ["java", *lib, "-cp", cp, *target, *args],
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


def _switched():
    """True when the staged JAR bundles the payload build's GDAL classes (pom.xml's gdal.java.version)."""
    with zipfile.ZipFile(JARS[-1]) as jar, zipfile.ZipFile(_build_classes()) as build:
        return jar.read(CONSTANTS) == build.read(CONSTANTS)


def _linked():
    """True where SYSTEM_JNI is a link into a payload (the dev container after gbx:docker:start, DCS, init)."""
    return os.path.exists(SYSTEM_JNI) and "/_native/" in os.path.realpath(SYSTEM_JNI)


def _system_gdal_or_skip():
    """A system JNI of another GDAL than the payload's: SYSTEM_JNI, or, where SYSTEM_JNI is linked into the payload,
    the image's own JNI it was linked away from (IMAGE_JNI). Skips when there is none.
    """
    jni = IMAGE_JNI if _linked() else SYSTEM_JNI
    if not os.path.exists(jni):
        pytest.skip(
            f"PRECONDITION MISSING: a system GDAL JNI at {jni}; this row not run"
        )
    if "/_native/" in os.path.realpath(jni):
        pytest.skip(
            f"PRECONDITION MISSING: {jni} is a payload's JNI, not a second GDAL; this row not run"
        )
    return jni


# kernel: KernelGdal on the driver; writer: a vector writer's load on a task; writer-no-session: the same with no
# session visible (as on an executor), so only the task's local property carries the conf; ogr: initOgr first.
@pytest.mark.parametrize("first_user", ["kernel", "writer", "writer-no-session", "ogr"])
def test_the_session_conf_alone_loads_the_payload(
    native_dir, conf_only_probe, first_user
):
    res = _java(
        native_dir, "ConfOnlyProbe", first_user, native_dir, classes=conf_only_probe
    )
    lines = res.stdout.splitlines()
    assert f"RESULT ok:{_pin('GDAL_VERSION')}" in lines, res.stdout + res.stderr[-3000:]
    if first_user == "writer-no-session":
        assert "SESSIONS_VISIBLE false" in lines, res.stdout
    if (
        first_user == "ogr"
    ):  # GDALManager loaded it, with the payload's data, before OGR registered
        assert f"GDAL_DATA {native_dir}/../share/gdal" in lines, res.stdout
    mapped = [ln.split(" ", 1)[1] for ln in lines if ln.startswith("MAPPED ")]
    assert len(set(mapped)) == 1, mapped
    assert mapped[0].startswith(
        os.path.join(os.path.realpath(native_dir), "lib", "libgdal-")
    ), mapped


def test_a_second_gdal_in_the_jvm_is_refused(native_dir):
    jni = _system_gdal_or_skip()  # a system JNI of another GDAL, loaded first
    res = _java(native_dir, "DoubleLoadProbe", jni, native_dir)
    assert res.returncode == 3, res.stdout + res.stderr
    assert "a different GDAL is already loaded" in res.stderr, res.stderr


def test_the_system_gdal_is_named_and_other_gdal_classes_are_refused(
    native_dir, conf_only_probe
):
    """No conf and no GBX_NATIVE_ROOT: the JVM says it runs the system GDAL; with the payload build's GDAL classes
    ahead of the JAR's, which that JNI cannot serve, GDALManager refuses them with a Fix. Where SYSTEM_JNI is linked
    into the payload (so the system GDAL is the payload's), the image's 3.11 JNI first on the JVM's library path is
    the other GDAL: GDAL's classes would load it by name, and GDALManager refuses it with a Fix before anything loads.
    """
    jni = _system_gdal_or_skip()
    if _linked():
        res = _java(
            native_dir,
            "ConfOnlyProbe",
            "kernel",
            "",
            classes=conf_only_probe,
            library_path=f"{os.path.dirname(jni)}:{DEV_LIBRARY_PATH}",
        )
        refused = (
            "RESULT error:" in res.stdout
            and f"a different GDAL is on this JVM's library path ({jni})" in res.stdout
            and "Fix:" in res.stdout
        )
        assert refused, res.stdout + res.stderr[-3000:]
        return
    res = _java(
        native_dir,
        "ConfOnlyProbe",
        "kernel",
        "",
        classes=conf_only_probe,
        payload_first=False,
        first_on_cp=_build_classes(),
    )
    named = "this JVM runs the system GDAL" in res.stderr
    refused = (
        "RESULT error:IllegalStateException:geobrix: GDAL's Java classes cannot use the GDAL loaded from"
        in res.stdout
        and "Fix:" in res.stdout
    )
    assert (named, refused) == (True, True), res.stdout + res.stderr[-3000:]


def test_a_pre_3_13_jni_as_the_only_gdal_is_refused_before_any_gdal_call(
    native_dir, conf_only_probe, tmp_path
):
    """The JAR's GDAL classes are the pinned build's, so an older JNI (the image's 3.11) is no runtime choice: as the
    JVM's only GDAL (the conf names a directory whose JNI is it, and nothing else is on the library path), the first
    GDAL use is refused with a Fix, before any GDAL call (the CRS parser's options overload never meets that JNI).
    """
    jni = _system_gdal_or_skip()
    lib = tmp_path / "_native" / "aarch64" / "lib"
    lib.mkdir(parents=True)
    (lib / "libgdalalljni.so").symlink_to(jni)
    res = _java(
        native_dir,
        "ConfOnlyProbe",
        "kernel",
        str(lib.parent),
        classes=conf_only_probe,
        library_path=str(lib),
    )
    refused = (
        "RESULT error:" in res.stdout
        and (
            "GDAL's Java classes cannot use the GDAL loaded from" in res.stdout
            or "a different GDAL is already loaded" in res.stdout
        )
        and "Fix:" in res.stdout
    )
    assert refused, res.stdout + res.stderr[-3000:]


def test_a_shared_object_that_maps_a_second_gdal_is_refused_on_the_system_gdal(
    native_dir, conf_only_probe
):
    """A JVM on the system GDAL whose sharedobjects conf names a library that maps another libgdal is refused. Where
    SYSTEM_JNI is linked into the payload, the system GDAL is the payload's and the other libgdal is the image's.
    """
    jni = _system_gdal_or_skip()
    if _linked():
        found = glob.glob("/usr/local/lib/libgdal.so.*") + glob.glob(
            "/usr/lib/*/libgdal.so.*"
        )
        other = sorted(
            {
                os.path.realpath(p)
                for p in found
                if "/_native/" not in os.path.realpath(p)
            }
        )
        if not other:
            pytest.skip(
                f"PRECONDITION MISSING: the libgdal of {jni}, outside the payload; this row not run"
            )
        res = _java(
            native_dir,
            "ConfOnlyProbe",
            "shared",
            "",
            other[0],
            classes=conf_only_probe,
            payload_first=False,
        )
        assert (
            "RESULT error:java.lang.IllegalStateException: geobrix: a different GDAL is already loaded"
            in res.stdout
        ), (res.stdout + res.stderr[-3000:])
        return
    if _switched():
        pytest.skip(
            "PRECONDITION MISSING: the JAR's GDAL classes are the payload build's, which the system JNI cannot serve; "
            "this row not run"
        )
    other = sorted(Path(native_dir, "lib").glob("libgdal-*.so.*"))
    res = _java(
        native_dir,
        "ConfOnlyProbe",
        "shared",
        "",
        str(other[0]),
        classes=conf_only_probe,
        payload_first=False,
    )
    assert (
        "RESULT error:java.lang.IllegalStateException: geobrix: a different GDAL is already loaded"
        in (res.stdout)
    ), (res.stdout + res.stderr[-3000:])


@pytest.fixture(scope="module")
def spark(native_dir):
    if not _switched():
        pytest.skip(
            "PRECONDITION MISSING: the staged JAR's GDAL classes are not the payload build's "
            "(pom.xml's gdal.java.version); the oracle not run"
        )
    from pyspark.sql import SparkSession

    if SparkSession.getActiveSession() is not None:
        pytest.skip(
            "PRECONDITION MISSING: a Spark session is already live; run this file alone "
            "(gbx-test-parity.sh --path ...); the oracle not run"
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


def test_a_shared_object_that_maps_a_second_gdal_is_refused_on_the_payload(
    native_dir, conf_only_probe
):
    """A JVM on the payload's GDAL whose sharedobjects conf names another libgdal is refused (also after the switch)."""
    found = glob.glob("/usr/local/lib/libgdal.so.*") + glob.glob(
        "/usr/lib/*/libgdal.so.*"
    )
    system = sorted({os.path.realpath(p) for p in found} - {""})
    system = [p for p in system if "/_native/" not in p]
    if not system:
        pytest.skip(
            "PRECONDITION MISSING: a libgdal outside the payload to name; this row not run"
        )
    res = _java(
        native_dir,
        "ConfOnlyProbe",
        "shared",
        native_dir,
        system[0],
        classes=conf_only_probe,
    )
    assert (
        "RESULT error:java.lang.IllegalStateException: geobrix: a different GDAL is already loaded"
        in res.stdout
    ), (res.stdout + res.stderr[-3000:])
