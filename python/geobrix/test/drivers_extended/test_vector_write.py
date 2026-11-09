"""Vector drivers through GeoBrix's writers: the features go out of a DataFrame through ``vector_gbx`` (with the
``driverName`` option) or the named writer, and are read back with plain pyogrio: the geometry, the attributes the
format keeps, and the CRS the format stores must be the ones that went in.
"""

import glob
import os
from test.drivers_extended._asserts import same_crs
from test.drivers_extended._vector import FEATURES, VECTOR_CASES, param, reference

import pytest
import shapely

pytestmark = pytest.mark.filterwarnings("ignore")

# each writer with the frame in one partition and in two (the distributed case)
_WRITERS = [param(c, f"write{n}", n) for c in VECTOR_CASES if c.write for n in (1, 2)]


def _outputs(out: str, case) -> list:
    """The datasets a writer made at ``out``: the file itself, ``out`` plus the format's extension, or the files or
    folders of that extension in it (a sharded writer makes several)."""
    if os.path.isfile(out):
        return [out]
    ext = os.path.splitext(case.name)[1]
    if os.path.exists(out + ext):
        return [out + ext]
    hits = sorted(glob.glob(os.path.join(out, "**", f"*{ext}"), recursive=True))
    assert hits, f"{case.driver}: nothing written at {out}"
    return hits


@pytest.mark.parametrize("partitions,case", _WRITERS)
def test_the_writer_makes_a_dataset_that_pyogrio_reads_back(
    spark, tmp_path, partitions, case
):
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    kinds = case.write_kinds or case.kinds
    cols = {"id": IntegerType(), "name": StringType(), "val": DoubleType()}
    fields = [StructField("geom_0", BinaryType())]
    fields += [StructField(c, cols[c]) for c in case.write_cols]
    fields += [StructField("geom_0_srid", StringType())]
    schema = StructType(fields)
    rows = []
    for i in kinds:
        wkt, idv, nm, val = FEATURES[i]
        attrs = {"id": idv, "name": nm, "val": val}
        rows.append(
            (
                bytearray(shapely.from_wkt(wkt).wkb),
                *[attrs[c] for c in case.write_cols],
                "4326",
            )
        )
    df = spark.createDataFrame(rows, schema).repartition(partitions)
    out = str(tmp_path / "out")
    w = df.write.format(case.write)
    if case.write == "vector_gbx":
        w = w.option("driverName", case.driver)
    for k, v in case.write_opts:
        w = w.option(k, v)
    w.mode("overwrite").save(out)

    import pandas as pd

    layer = case.write_opts[0][1] if case.write_opts else None
    ref = pd.concat(
        [reference(p, layer) for p in _outputs(out, case)], ignore_index=True
    )
    assert len(ref) == len(
        kinds
    ), f"{case.driver}: {len(ref)} features back, {len(kinds)} written"
    got = {g.normalize().wkt for g in ref.geometry}
    assert got == {shapely.from_wkt(FEATURES[i][0]).normalize().wkt for i in kinds}
    if case.keeps_attrs and case.write_cols:
        have = {tuple(r[c] for c in case.write_cols) for _, r in ref.iterrows()}
        want = {
            tuple(
                {"id": FEATURES[i][1], "name": FEATURES[i][2], "val": FEATURES[i][3]}[c]
                for c in case.write_cols
            )
            for i in kinds
        }
        assert {
            tuple(float(x) if isinstance(x, float) else x for x in t) for t in have
        } == want
    if case.keeps_crs and case.epsg:
        assert same_crs(ref.crs.to_wkt() if ref.crs else None, "EPSG:4326")


def test_a_column_named_like_a_gpx_built_in_field_is_refused_with_a_fix(
    spark, tmp_path
):
    """GPX creates ``name`` itself: the writer says so and what to do, not a bare FieldError."""
    from pyspark.sql.types import BinaryType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("geom_0", BinaryType()),
            StructField("name", StringType()),
            StructField("geom_0_srid", StringType()),
        ]
    )
    df = spark.createDataFrame(
        [(bytearray(shapely.from_wkt("POINT (10 50)").wkb), "w1", "4326")], schema
    )
    w = (
        df.write.format("vector_gbx")
        .option("driverName", "GPX")
        .option("layerName", "waypoints")
    )
    with pytest.raises(Exception) as raised:
        w.mode("overwrite").save(str(tmp_path / "out"))
    text = str(raised.value)
    assert (
        "Fix:" in text and "name" in text and "FieldError" not in text.split("Fix:")[0]
    )
