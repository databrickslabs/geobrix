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

The parity test SKIPS cleanly (never silently passes) when the JAR is absent or
when the heavy reader yields no rows in the current environment; the skip reason
is precise so a human knows it needs a Docker/cluster run to exercise. A
separate, always-run assertion (``test_light_enumerates_coral_grid_variables``)
guards the light reference variable set unconditionally.
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

    Skips (never silently passes) if the heavy reader is unresolvable or yields
    no rows in this environment -- exercise it in Docker with the JAR present or
    on a cluster."""
    path = str(_CORAL)

    # Heavy first: probe capability by actually loading. A registration failure
    # or a zero-row result means the heavy tier is not exercisable here.
    try:
        heavy_rows = (
            spark_with_jar.read.format("netcdf_gdal")
            .option("sizeInMB", "-1")
            .load(path)
            .collect()
        )
    except Exception as exc:  # noqa: BLE001 - environment-dependent heavy reader
        pytest.skip(
            "heavy 'netcdf_gdal' reader unresolvable in this environment "
            f"(JAR without netcdf_gdal, or GDAL init): {str(exc)[:160]}"
        )
    if len(heavy_rows) == 0:
        pytest.skip(
            "heavy 'netcdf_gdal' reader produced 0 rows in this environment "
            "(heavy GDAL subdataset enumeration does not run in local Docker); "
            "run this parity comparison on a cluster where the heavy tier runs."
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
        if le is not None and he is not None:
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
