"""Raster drivers through GeoBrix's readers: each sample is written with GDAL, read back by the entry a user would call,
and held to what plain GDAL reads from the same file (band count, size, type, pixels, NoData, geotransform, CRS).

Per case:
- ``raster_gbx`` (or the named reader) on the path, the default virtual tile;
- ``rst_fromfile`` on the path, the Python column API (a virtual tile);
- ``gbx_rst_fromcontent`` on the file's bytes, for a driver whose bytes alone are a tile. The bytes carry only the
  one file, so the reference is GDAL's reading of that file copied alone.
"""

import json
import os
import shutil
from test.drivers_extended._asserts import assert_tile_is, tile_dict
from test.drivers_extended._raster import RASTER_CASES, param, reference, write_sample

import pytest

pytestmark = pytest.mark.filterwarnings("ignore")

# every case through its own reader, and through the generic one when it has a named reader
_READS = [
    param(c, f"reader:{r}", r)
    for c in RASTER_CASES
    for r in dict.fromkeys([c.reader, "raster_gbx"])
]
_FROMFILE = [param(c, "fromfile") for c in RASTER_CASES]
_FROMCONTENT = [param(c, "fromcontent") for c in RASTER_CASES]


@pytest.fixture(scope="module")
def samples(gb, tmp_path_factory):
    """case id -> (path, reference), each sample in a directory of its own."""
    out = {}
    for c in RASTER_CASES:
        d = tmp_path_factory.mktemp(c.id.replace("/", "_"))
        path = write_sample(gb, c, str(d))
        out[c.id] = (path, reference(gb, path))
    return out


@pytest.mark.parametrize("reader,case", _READS)
def test_the_reader_returns_the_file_as_gdal_reads_it(spark, samples, reader, case):
    path, ref = samples[case.id]
    rows = spark.read.format(reader).load(path).collect()
    assert len(rows) == 1, f"{reader}: {len(rows)} tiles"
    assert_tile_is(
        tile_dict(rows[0]["tile"]),
        ref,
        f"{reader} {case.driver}",
        fill_to_nan=reader == "netcdf_gbx",
    )


@pytest.mark.parametrize("case", _FROMFILE)
def test_rst_fromfile_on_the_path_returns_the_file_as_gdal_reads_it(
    spark, samples, case
):
    from databricks.labs.gbx.rasterx import functions as rx

    path, ref = samples[case.id]
    row = (
        spark.createDataFrame([(path,)], ["path"])
        .select(rx.rst_fromfile("path", case.driver).alias("tile"))
        .collect()[0]
    )
    assert row["tile"] is not None, f"rst_fromfile returned NULL for {case.driver}"
    assert_tile_is(tile_dict(row["tile"]), ref, f"rst_fromfile {case.driver}")


@pytest.mark.parametrize("case", _FROMCONTENT)
def test_rst_fromcontent_follows_the_tile_bytes_flag_of_gbx_drivers(
    gb, spark, samples, tmp_path, case
):
    """A driver whose ``tile_bytes`` is true opens from the bytes of its file and reads as GDAL does; one whose
    ``tile_bytes`` is false is refused (an error tile with a reason)."""
    from databricks.labs.gbx.drivers import _listing

    snap = {
        r["driver"]: r for r in json.loads(_listing.SNAPSHOT.read_text())["drivers"]
    }
    path, _ = samples[case.id]
    alone = os.path.join(str(tmp_path), os.path.basename(path))
    shutil.copy(path, alone)  # the one file, with no sidecar
    with open(path, "rb") as fh:
        content = fh.read()
    df = spark.createDataFrame([(bytearray(content),)], ["content"])
    row = df.selectExpr(
        f"gbx_rst_fromcontent(content, '{case.driver}') AS tile"
    ).collect()[0]
    tile = tile_dict(row["tile"])
    if not snap[case.driver]["tile_bytes"]:
        assert (
            tile["raster"] is None
        ), f"{case.driver}: tile_bytes is false but the bytes opened"
        assert tile["metadata"].get("last_error"), "a refusal names its reason"
        return
    assert (
        tile["raster"] is not None
    ), f"{case.driver}: tile_bytes is true but the bytes do not open: {tile['metadata'].get('last_error')}"
    assert_tile_is(tile, reference(gb, alone), f"rst_fromcontent {case.driver}")
