"""Cross-tier NetCDF raster parity: heavy ``netcdf_gdal`` vs light ``netcdf_gbx``.

This is the **correctness gate** for the heavy reader's heuristic grid filter
(``NetCDF_Batch``): on a shared gridded fixture, both tiers must

  1. enumerate the SAME variable set (the ``:var`` suffix of the ``source``
     column), and
  2. produce the SAME per-variable tile -- equal CRS (EPSG), equal geotransform,
     and cell values within tolerance.

Byte parity is NOT expected (light re-encodes via rasterio/GTiff; heavy via
GDAL), hence the value tolerance and the metadata being ignored.

HARNESS NOTE (why a dedicated session, not the shared ``spark`` fixture):
The light-tier ``spark`` fixture in ``ds/conftest.py`` is a plain ``local[2]``
session with NO geobrix JAR on its classpath, so the heavy Scala ``netcdf_gdal``
DataSource (auto-discovered via META-INF/services) is unresolvable there. This
module builds its own ``spark_with_jar`` session that puts the staged assembly
JAR on ``spark.jars`` -- exactly the pattern established by
``test_reader_parity.py``. The light Python DataSource is registered explicitly.

RUN (Docker, needs the freshly-built JAR containing ``netcdf_gdal`` in
``python/geobrix/lib/`` + the mounted test-resources fixtures)::

    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/ds/test_netcdf_cross_tier.py \\
        --with-integration --log netcdf-parity.log

MUST BE INVOKED TARGETED AT THIS FILE'S OWN PATH (not a whole-``ds/``-dir run).
PySpark reuses one JVM per process and ``getOrCreate()`` hands back whatever
SparkSession the JVM already owns. In a whole-dir run an earlier ``ds`` test has
already built a JAR-less session, so this module's ``spark_with_jar`` fixture
returns that JAR-less session and the heavy ``netcdf_gdal`` format is
unregistered -> the parity test SKIPS (does NOT gate). Run it on its own path so
this module owns the first ``getOrCreate()`` and the JAR is on the classpath.

SKIP vs FAIL contract (so a silent skip can never mask a real regression):
  * LEGITIMATE SKIP -- only when the heavy format is genuinely unregistered
    (``[DATA_SOURCE_NOT_FOUND]`` / ``ClassNotFoundException: netcdf_gdal...``),
    i.e. no JAR on the classpath. A skip here means the gate DID NOT RUN and
    needs a targeted Docker run with a freshly-built JAR.
  * FAILURE (never a skip) -- if the heavy format resolves/loads but returns
    zero rows on this known-non-empty gridded fixture (the filter dropped
    everything), or raises any error other than the unregistered signal.

A separate, always-run assertion (``test_light_enumerates_coral_grid_variables``)
guards the light reference variable set unconditionally, and the parity test
also pins the light tile's EPSG to its known value so a light-side CRS
regression is caught even when the heavy tier reports no EPSG.
"""

import logging
import os
from pathlib import Path

import numpy as np
import pytest
from rasterio.io import MemoryFile

# NOTE: the module-level marker is intentionally NOT ``integration``. The
# light-only reference gate (``test_light_enumerates_coral_grid_variables``)
# needs no JAR and runs in the default light suite; only the both-tiers parity
# test carries ``@pytest.mark.integration`` (it needs the heavy JAR + is opt-in
# via ``--with-integration``).

# Gridded fixtures mounted in the dev container (repo test resources). The coral
# granule is the canonical shared fixture: light data_vars == {bleaching_alert_area,
# mask}, which Task 2 reported the heavy filter also enumerated.
_HERE = Path(__file__).resolve()
_REPO = _HERE.parents[4]  # .../geobrix
_CORAL = (
    _REPO / "src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
)

# Light vs heavy never byte-equal; mirror bench/compare.py tolerances.
REL_TOL = 1e-3
ABS_TOL = 1e-3

# The coral CF grid carries ``crs.epsg_code`` -> light ``_crs_string``/``_epsg_int``
# yield EPSG:4326. Read from the light reader on the fixture, not guessed. This
# pins the light side so a CRS regression is caught even when heavy reports None.
CORAL_LIGHT_EPSG = 4326

# JAR lives in python/geobrix/lib/ (parents[2] == .../python/geobrix). NOTE:
# test_reader_parity.py uses parents[3] here, which resolves to python/lib and
# never matches -- so that precedent test always skips its JAR check. This one
# points at the real staged-JAR directory so the heavy tier actually runs.
_LIBDIR = (_HERE.parents[2] / "lib").resolve()
_JAR_CANDIDATES = sorted(_LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    """A SparkSession with the geobrix assembly JAR on the classpath.

    The JAR makes the heavy ``netcdf_gdal`` DataSource resolvable (auto-discovered
    via META-INF/services); the light ``netcdf_gbx`` DataSource is pure Python and
    is registered explicitly below.
    """
    if not _JAR_CANDIDATES:
        pytest.skip(f"no geobrix JAR in {_LIBDIR} (build/stage it first)")
    if not _CORAL.exists():
        pytest.skip(f"coral fixture not present at {_CORAL}")

    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    jar = str(_JAR_CANDIDATES[-1])
    session = (
        SparkSession.builder.master("local[2]")
        .appName("netcdf-cross-tier-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", jar)
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource

    session.dataSource.register(NetcdfGbxDataSource)
    yield session


def _var_of(source: str) -> str:
    """Trailing ``:var`` of a NETCDF:"path":var subdataset selector.

    The path itself may contain colons (``NETCDF:"/x":var``), so split on the
    LAST colon only -- identical to how both tiers form the selector.
    """
    return source.rsplit(":", 1)[-1]


def _by_var(rows):
    """Map variable name -> (epsg, geotransform tuple, band-1 array)."""
    out = {}
    for r in rows:
        v = _var_of(r["source"])
        with MemoryFile(bytes(r["tile"]["raster"])) as mf, mf.open() as ds:
            out[v] = (
                ds.crs.to_epsg() if ds.crs else None,
                tuple(ds.transform)[:6],
                ds.read(1),
            )
    return out


def _tile_values(rows):
    """Read each row's band-1 into a numpy array keyed by the ``source`` variable.

    Thin wrapper over ``_by_var`` that drops the CRS/geotransform metadata --
    the scaled-grid parity test only compares decoded physical cell values.
    Accepts an already-collected list of rows.
    """
    return {v: meta[2] for v, meta in _by_var(rows).items()}


def _write_scaled_grid(path):
    """A synthetic CF grid with packed int16 variables carrying
    ``scale_factor``/``add_offset`` and a ``_FillValue``.

    physical = raw * 0.01 + 250.0 for every non-fill cell. Both tiers must
    return that decoded physical value: light via xarray ``mask_and_scale``,
    heavy via the Task-2 ``applyScale`` (gdal_translate ``-unscale``) path.

    TWO data variables are written on purpose. GDAL's netCDF driver only exposes
    a ``SUBDATASETS`` metadata domain (which the heavy ``netcdf_gdal``
    enumeration reads to discover variables) when the file has >1 data variable;
    a single-variable file is opened as a plain single-band raster with NO
    subdatasets, so heavy would enumerate zero grids and the gate could not run.
    ``units`` (degrees_north/east) let GDAL derive the CF geotransform so the
    Task-1 grid filter keeps both variables.
    """
    from netCDF4 import Dataset

    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 4)
        ds.createDimension("lon", 5)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lat.standard_name = "latitude"
        lat.units = "degrees_north"
        lon = ds.createVariable("lon", "f8", ("lon",))
        lon.standard_name = "longitude"
        lon.units = "degrees_east"
        lat[:] = [50.0, 49.5, 49.0, 48.5]
        lon[:] = [10.0, 10.5, 11.0, 11.5, 12.0]
        v = ds.createVariable("t", "i2", ("lat", "lon"), fill_value=-32768)
        v.scale_factor = 0.01
        v.add_offset = 250.0
        v[:] = np.arange(20, dtype="i2").reshape(4, 5)
        m = ds.createVariable("m", "i2", ("lat", "lon"), fill_value=-32768)
        m.scale_factor = 0.01
        m.add_offset = 250.0
        m[:] = np.arange(20, 40, dtype="i2").reshape(4, 5)


@pytest.mark.integration
def test_netcdf_gdal_applies_scale_matches_light(spark_with_jar, tmp_path):
    """Scaled-grid value parity: heavy ``netcdf_gdal`` (Task-2 ``applyScale``)
    decodes CF ``scale_factor``/``add_offset`` to the SAME physical values light
    ``netcdf_gbx`` produces via xarray ``mask_and_scale``.

    This is the correctness gate for the heavy unscaling path. SKIP only when the
    heavy format is genuinely unregistered (no JAR on the classpath); any other
    error, or a zero-row load on this known-non-empty scaled fixture, is a
    FAILURE. If heavy values differ from light beyond tolerance the unscale path
    is wrong -- fix Task 2, do NOT loosen the tolerance here."""
    f = tmp_path / "scaled.nc"
    _write_scaled_grid(str(f))
    path = str(f)

    # Heavy first: probe capability by actually loading. Only an *unregistered
    # format* is a legitimate skip (mirrors test_netcdf_gdal_matches_light_raster).
    try:
        heavy_rows = (
            spark_with_jar.read.format("netcdf_gdal")
            .option("sizeInMB", "-1")
            .load(path)
            .collect()
        )
    except Exception as exc:  # noqa: BLE001 - classify: unregistered => skip
        msg = str(exc)
        unregistered = (
            "DATA_SOURCE_NOT_FOUND" in msg
            or "netcdf_gdal.DefaultSource" in msg
            or "Failed to find the data source" in msg
        )
        if unregistered:
            pytest.skip(
                "GATE DID NOT RUN: heavy 'netcdf_gdal' format is UNREGISTERED "
                "(no geobrix JAR on this session's classpath). Re-run TARGETED "
                f"at this file's path in Docker with a fresh JAR. Detail: {msg[:160]}"
            )
        raise
    assert len(heavy_rows) > 0, (
        "heavy 'netcdf_gdal' loaded but returned 0 rows on the scaled fixture "
        "(known to contain grid variables 't','m') -- enumerated nothing."
    )

    light_rows = spark_with_jar.read.format("netcdf_gbx").load(path).collect()

    light = _tile_values(light_rows)
    heavy = _tile_values(heavy_rows)
    assert sorted(light) == sorted(
        heavy
    ), f"variable-set mismatch: light={sorted(light)} heavy={sorted(heavy)}"
    assert light, "no grid variables enumerated (both tiers empty)"
    # Decoded physical values must agree per variable: raw*0.01+250 on both tiers.
    for v in sorted(light):
        np.testing.assert_allclose(
            light[v],
            heavy[v],
            rtol=1e-4,
            atol=1e-4,
            equal_nan=True,
            err_msg=(
                f"{v}: scaled-grid physical values differ beyond tolerance -- "
                "the heavy applyScale (unscale) path is wrong. Fix Task 2, do "
                "NOT loosen here."
            ),
        )


def test_light_enumerates_coral_grid_variables():
    """Always-run reference gate: light ``readable_variables`` on the coral
    fixture is exactly the two CF grid variables. This anchors the parity
    comparison's reference set even in environments where the heavy JAR cannot
    run, so the grid-filter contract never goes completely ungated."""
    if not _CORAL.exists():
        pytest.skip(f"coral fixture not present at {_CORAL}")
    from databricks.labs.gbx.ds import _netcdf

    with _netcdf.open_dataset(str(_CORAL), None) as ds:
        light_vars = sorted(_netcdf.readable_variables(ds, "raster"))
    assert light_vars == ["bleaching_alert_area", "mask"]


@pytest.mark.integration
def test_netcdf_gdal_matches_light_raster(spark_with_jar):
    """Heavy ``netcdf_gdal`` and light ``netcdf_gbx`` agree on the coral fixture:
    same enumerated variable set + per-variable EPSG, geotransform, and cell
    values within tolerance.

    SKIP only if the heavy format is genuinely unregistered (no JAR on the
    classpath). If it resolves but returns zero rows on this known-non-empty
    gridded fixture, that is a FAILURE (the filter dropped everything), not a
    skip -- exactly the regression this gate exists to catch."""
    path = str(_CORAL)

    # Heavy first: probe capability by actually loading. Only an *unregistered
    # format* is a legitimate skip; any other error, or a zero-row result on a
    # known-non-empty fixture, must fail so a real filter regression cannot hide.
    try:
        heavy_rows = (
            spark_with_jar.read.format("netcdf_gdal")
            .option("sizeInMB", "-1")
            .load(path)
            .collect()
        )
    except (
        Exception
    ) as exc:  # noqa: BLE001 - classify: unregistered => skip, else raise
        msg = str(exc)
        unregistered = (
            "DATA_SOURCE_NOT_FOUND" in msg
            or "netcdf_gdal.DefaultSource" in msg
            or "Failed to find the data source" in msg
        )
        if unregistered:
            pytest.skip(
                "GATE DID NOT RUN: heavy 'netcdf_gdal' format is UNREGISTERED "
                "(no geobrix JAR on this session's classpath -- e.g. a "
                "whole-ds/-dir run reusing a JAR-less getOrCreate() session). "
                "Re-run TARGETED at this file's path in Docker with a freshly "
                f"built JAR to exercise the gate. Detail: {msg[:160]}"
            )
        # Registered but broke some other way -> a real defect, surface it.
        raise
    # Registered and loaded, but empty on a known-non-empty gridded fixture: the
    # grid filter dropped every variable. That is the failure mode this gate
    # guards -- do NOT skip it.
    assert len(heavy_rows) > 0, (
        "heavy 'netcdf_gdal' loaded but returned 0 rows on the coral fixture "
        "(known to contain grid variables) -- the NetCDF_Batch grid filter "
        "enumerated nothing. This is a filter regression, not an environment skip."
    )

    light_rows = spark_with_jar.read.format("netcdf_gbx").load(path).collect()

    light_vars = sorted(_var_of(r["source"]) for r in light_rows)
    heavy_vars = sorted(_var_of(r["source"]) for r in heavy_rows)
    # The grid-filter correctness gate: over/under-inclusive heavy filter fails here.
    assert light_vars == heavy_vars, (
        f"grid-filter variable-set MISMATCH on coral fixture: "
        f"light={light_vars} heavy={heavy_vars}"
    )
    assert light_vars, "no grid variables enumerated (both tiers empty)"

    lm, hm = _by_var(light_rows), _by_var(heavy_rows)
    for v in light_vars:
        # CRS pedigree divergence, not a grid disagreement: GDAL's netCDF driver
        # reads a CF grid whose grid_mapping carries only an ``epsg_code``
        # attribute (no WKT authority) as an *unnamed* GEOGCS -- correct WGS84
        # ellipsoid but GetAuthorityCode()==None, so heavy emits no EPSG. Light
        # recovers EPSG:4326 from that CF attribute. Both describe the same
        # geographic grid; the geotransform + cell-value equality below proves
        # the pixels coincide. So: EPSGs must AGREE when both tiers report one;
        # a heavy None (GDAL couldn't map the CF authority) is tolerated.
        le, he = lm[v][0], hm[v][0]
        # Pin the light side to its known EPSG so a light-side CRS regression is
        # caught even when heavy reports None (the guarded compare below is
        # vacuous on coral, where GDAL yields None). Read, not guessed:
        # _crs_string on the coral fixture yields EPSG:4326 (see CORAL_LIGHT_EPSG).
        assert (
            le == CORAL_LIGHT_EPSG
        ), f"{v}: light EPSG regressed light={le} expected={CORAL_LIGHT_EPSG}"
        # And when heavy DOES report an EPSG, the two tiers must agree.
        if he is not None:
            assert le == he, f"{v}: EPSG differs light={le} heavy={he}"
        # Geotransform: sub-pixel agreement, not bit-identity. Light derives the
        # pixel size/origin from the file's float32 lon/lat coordinate arrays
        # (~0.0500031 deg); GDAL derives a cleaner ~0.0500000 deg. On this 0.05-deg
        # grid the two differ by <5e-6 deg -- ~1e-4 of a pixel. GEOTRANSFORM_ATOL
        # (5e-4 deg = 1% of a pixel) passes that float32-vs-GDAL noise while still
        # failing a real half/whole-pixel origin or resolution disagreement (~0.05).
        GEOTRANSFORM_ATOL = 5e-4
        np.testing.assert_allclose(
            lm[v][1],
            hm[v][1],
            rtol=1e-3,
            atol=GEOTRANSFORM_ATOL,
            err_msg=f"{v}: geotransform differs light={lm[v][1]} heavy={hm[v][1]}",
        )
        np.testing.assert_allclose(
            lm[v][2],
            hm[v][2],
            rtol=REL_TOL,
            atol=ABS_TOL,
            equal_nan=True,
            err_msg=f"{v}: cell values differ beyond tolerance",
        )
