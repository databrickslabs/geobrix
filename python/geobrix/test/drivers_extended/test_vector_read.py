"""Vector drivers through GeoBrix's readers: each sample is written with GDAL, read back through ``vector_gbx`` with
the ``driverName`` option and through the named reader where there is one, and held to what plain pyogrio reads from
the same dataset (feature count, geometry WKB, attributes, CRS). A format that keeps the features as written is also
held to the features that were written.
"""

from test.drivers_extended._asserts import same_crs
from test.drivers_extended._vasserts import assert_rows_are, crs_text, geom_column
from test.drivers_extended._vector import (
    BY_DRIVER,
    FEATURES,
    VECTOR_CASES,
    build_mvt_folder,
    param,
    read_paths,
    reference,
    write_sample,
)

import pytest
import shapely

pytestmark = pytest.mark.filterwarnings("ignore")

_READS = [
    param(c, p, r)
    for c in VECTOR_CASES
    for p, r in zip(
        read_paths(c),
        dict.fromkeys(["vector_gbx", c.reader] if c.reader else ["vector_gbx"]),
    )
]


@pytest.fixture(scope="module")
def samples(gb, tmp_path_factory):
    """case id -> path, each sample in a directory of its own."""
    return {
        c.id: write_sample(gb, c, str(tmp_path_factory.mktemp(c.id)))
        for c in VECTOR_CASES
    }


@pytest.mark.parametrize("reader,case", _READS)
def test_the_reader_returns_the_features_as_pyogrio_reads_them(
    spark, samples, reader, case
):
    for layer in case.layers:
        r = spark.read.format(reader).option("driverName", case.driver)
        if layer:
            r = r.option("layerName", layer)
        rows = r.load(samples[case.id]).collect()
        assert_rows_are(
            rows, reference(samples[case.id], layer), f"{reader} {case.driver} {layer}"
        )


@pytest.mark.parametrize(
    "case", [c for c in VECTOR_CASES if c.build is None], ids=lambda c: c.id
)
def test_the_features_that_were_written_come_back(spark, samples, case):
    """Geometry and CRS of the features written (a format that keeps attributes also gets those): so the sample is
    a real round trip, not a read of an empty or altered file."""
    rows = (
        spark.read.format("vector_gbx")
        .option("driverName", case.driver)
        .load(samples[case.id])
        .collect()
    )
    gname = geom_column(list(rows[0].asDict()))
    got = {shapely.from_wkb(bytes(r[gname])).normalize().wkt for r in rows}
    want = {shapely.from_wkt(FEATURES[i][0]).normalize().wkt for i in case.kinds}
    assert got == want
    if case.keeps_crs:
        texts = {crs_text(r, gname) for r in rows}
        assert len(texts) == 1
        assert same_crs(texts.pop(), f"EPSG:{case.epsg}" if case.epsg else None)
    if case.fields and case.keeps_attrs:
        assert sorted(
            (int(r["id"]), r["name"], float(r["val"])) for r in rows
        ) == sorted(
            (FEATURES[i][1], FEATURES[i][2], FEATURES[i][3]) for i in case.kinds
        )


@pytest.mark.parametrize("case", [param(BY_DRIVER["MVT"], "folder")])
def test_an_mvt_tile_folder_reads_as_a_dataset(gb, spark, tmp_path, case):
    """The folder GDAL's MVT writer makes (z/x/y.pbf and metadata.json) opens as one dataset in GDAL."""
    folder = str(tmp_path / "tiles")
    build_mvt_folder(gb, case, folder)
    rows = (
        spark.read.format("vector_gbx")
        .option("driverName", "MVT")
        .load(folder)
        .collect()
    )
    assert rows, "no features read from the tile folder"


def test_a_csv_with_x_and_y_columns_reads_as_a_table_with_no_geometry(
    gb, spark, tmp_path
):
    """GeoBrix passes no open options, so GDAL reads the X and Y columns as plain attributes (no point): the rows are
    the table pyogrio reads, with a NULL geometry."""
    from test.drivers_extended._vector import VectorCase, write_generic

    case = VectorCase("CSV", "xy.csv", (0,), lco=("GEOMETRY=AS_XY",))
    path = str(tmp_path / case.name)
    write_generic(gb, case, path)
    ref = reference(path)
    rows = (
        spark.read.format("vector_gbx").option("driverName", "CSV").load(path).collect()
    )
    assert len(rows) == len(ref) == 1
    cols = [c for c in rows[0].asDict() if not c.startswith("geom_0")]
    assert cols == list(ref.columns) == ["X", "Y", "id", "name", "val"]
    assert [str(rows[0][c]) for c in cols] == [str(ref.iloc[0][c]) for c in cols]
    assert rows[0]["geom_0"] is None
