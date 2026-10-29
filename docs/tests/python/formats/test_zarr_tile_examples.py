"""Executes the Zarr tile examples and checks the pages that describe Zarr tiles (Docker only).

``docs/docs/api/tile-structure.mdx`` lists the metadata keys of a Zarr tile and
``docs/docs/api/virtual-tiles.mdx`` says how a Zarr tile is read. The example on the first page
is a function of ``zarr_tile_examples.py`` and runs here on the sample cube. The other tests
read real tiles from a plain store, a registered store and a band stack, and check that every
key the page lists is a key a tile has (and the reverse), and that each behaviour the second page
describes is the behaviour of the package.
"""

import json
import re
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import zarr_tile_examples as ex  # noqa: E402

xr = pytest.importorskip("xarray")
pytest.importorskip("zarr")

# runs after the tests that use the shared session
pytestmark = pytest.mark.delta_session

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile  # noqa: E402
from databricks.labs.gbx.zarr import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrStoreDrift,
    ZarrUnsupported,
    gbx_zarr_export,
    gbx_zarr_read,
    gbx_zarr_register,
    gbx_zarr_write,
)

DOCS = Path(__file__).resolve().parents[3] / "docs"
TILE_STRUCTURE = DOCS / "api" / "tile-structure.mdx"
VIRTUAL_TILES = DOCS / "api" / "virtual-tiles.mdx"
PAGES = [
    TILE_STRUCTURE,
    VIRTUAL_TILES,
    DOCS / "limitations.mdx",
    DOCS / "readers-writers.mdx",
]


def shown(capsys):
    return capsys.readouterr().out.strip()


def first_tile(spark, source, **kw):
    tiles = gbx_zarr_read(spark, source, variables=["t2m"], output="tiles", **kw)
    return tiles.orderBy("tile.window.row_off", "tile.window.col_off").first()["tile"]


def keys_the_page_lists():
    """The metadata keys in the first column of the 'Zarr tile metadata' table."""
    text = TILE_STRUCTURE.read_text()
    section = text.split("### Zarr tile metadata", 1)[1].split("\n---", 1)[0]
    keys = set()
    for row in re.findall(r"^\| ((?:`[^`]+`(?:, )?)+) \|", section, flags=re.M):
        keys |= set(re.findall(r"`([^`]+)`", row))
    return keys - {"Key"}


# --- the example -----------------------------------------------------------------------


def test_zarr_tile_metadata(spark, cube, capsys):
    ex.zarr_tile_metadata(spark, cube)
    assert shown(capsys) == ex.zarr_tile_metadata_output.strip()


# --- the keys of a Zarr tile -------------------------------------------------------------


def test_the_page_lists_exactly_the_keys_a_zarr_tile_has(
    spark, cube, copy_of_cube, tmp_path, new_table
):
    seen = set(first_tile(spark, cube, isel={"time": 0}).metadata)
    # a band stack adds its own keys and drops sliceDims (time is the stacked dimension)
    stack = first_tile(spark, cube, isel={"time": slice(0, 3)}, band_dim="time")
    seen |= set(stack.metadata)
    # a registered store adds the store's own directory
    table = new_table("tilekeys")
    gbx_zarr_register(spark, copy_of_cube, table)
    registered = first_tile(spark, table, isel={"time": 0})
    seen |= set(registered.metadata)
    listed = keys_the_page_lists()
    # sliceCoord_<dim> stands for one key per dimension that has coordinates
    listed = {"sliceCoord_time" if k == "sliceCoord_<dim>" else k for k in listed}
    assert (
        listed == seen
    ), f"only on the page: {listed - seen}; only on tiles: {seen - listed}"
    assert registered.metadata["zarr_base_root"] == copy_of_cube
    assert (
        registered.metadata["zarr_table"] == table
        and registered.metadata["zarr_version"]
    )


def test_a_plain_store_tile_has_no_refs_and_a_band_stack_has_no_slice_dims(spark, cube):
    tile = first_tile(spark, cube, isel={"time": 0})
    assert json.loads(tile.metadata["zarr_refs"]) == [[]]
    assert tile.metadata["zarr_table"] == "" and tile.metadata["zarr_version"] == ""
    assert "zarr_base_root" not in tile.metadata
    stack = first_tile(spark, cube, isel={"time": slice(0, 3)}, band_dim="time")
    assert "sliceDims" not in stack.metadata and not any(
        k.startswith("sliceCoord_") for k in stack.metadata
    )
    assert (
        stack.metadata["zarr_band_index"] == "0,1,2" and stack.metadata["count"] == "3"
    )


def test_slice_dims_are_listed_by_name_not_by_axis_order(spark, tmp_path):
    """A (time, level, latitude, longitude) array: ``level`` sorts before ``time``."""
    import numpy as np

    data = np.arange(2 * 3 * 4 * 4, dtype="float32").reshape(2, 3, 4, 4)
    ds = xr.Dataset(
        {"t": (("time", "level", "latitude", "longitude"), data)},
        coords={
            "time": np.array(["2023-01-01", "2023-01-02"], dtype="datetime64[ns]"),
            "level": [850, 500, 200],
            "latitude": [3.0, 2.0, 1.0, 0.0],
            "longitude": [0.0, 1.0, 2.0, 3.0],
        },
    )
    path = str(tmp_path / "levels.zarr")
    ds.to_zarr(path, zarr_format=3, consolidated=False)
    tiles = gbx_zarr_read(spark, path, variables=["t"], output="tiles").collect()
    dims = sorted(r["tile"].metadata["sliceDims"] for r in tiles)
    assert dims == sorted(f"level={k},time={j}" for j in range(2) for k in range(3))
    one = next(
        r["tile"] for r in tiles if r["tile"].metadata["sliceDims"] == "level=1,time=0"
    )
    assert one.metadata["sliceCoord_level"] == "500"
    assert one.metadata["sliceCoord_time"].startswith("2023-01-01T00:00:00")


def test_a_virtual_tile_is_virtual_and_a_materialized_one_is_a_geotiff(spark, cube):
    virtual = first_tile(spark, cube, isel={"time": 0})
    assert virtual.raster is None and virtual.path_mode is None and virtual.crs is None
    assert (
        virtual.path == virtual.metadata["sourcePath"] == virtual.metadata["zarr_root"]
    )
    held = first_tile(spark, cube, isel={"time": 0}, virtual_tiles=False)
    assert held.raster is not None and held.path_mode is None
    assert held.metadata["driver"] == held.metadata["format"] == "GTiff"
    assert {k for k in virtual.metadata if k.startswith("zarr_")} <= set(held.metadata)


# --- what the virtual tiles page says ------------------------------------------------------


def test_a_virtual_tile_cannot_use_direct_access(spark, copy_of_cube, new_table):
    table = new_table("tiledirect")
    gbx_zarr_register(spark, copy_of_cube, table)
    with pytest.raises(ZarrAccessUnavailable) as err:
        gbx_zarr_read(spark, table, variables=["t2m"], output="tiles", access="direct")
    assert "virtual_tiles=False" in str(err.value)


def test_a_store_at_a_cloud_url_cannot_be_opened_as_a_tile(spark, cube):
    from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes

    row = first_tile(spark, cube, isel={"time": 0}).asDict(recursive=True)
    row["metadata"]["zarr_root"] = "s3://bucket/era5.zarr"
    with pytest.raises(ZarrAccessUnavailable) as err:
        materialize_to_bytes(VirtualTile.from_row(row))
    assert "materialize the tiles, or keep the store on a Volume" in str(err.value)


def test_a_file_a_tile_names_that_has_gone_is_drift(spark, copy_of_cube, new_table):
    from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes

    table = new_table("tilegone")
    gbx_zarr_register(spark, copy_of_cube, table)
    row = first_tile(spark, table, isel={"time": 0}).asDict(recursive=True)
    shutil.rmtree(Path(copy_of_cube) / "t2m")
    with pytest.raises(ZarrStoreDrift):
        materialize_to_bytes(VirtualTile.from_row(row))


# --- the codec limit on the limitations page -------------------------------------------


@pytest.fixture
def outer_codec_store(copy_of_cube):
    """The sample cube with a bytes-to-bytes codec added after ``sharding_indexed`` on ``msl``."""
    path = Path(copy_of_cube) / "msl" / "zarr.json"
    doc = json.loads(path.read_text())
    doc["codecs"].append({"name": "gzip", "configuration": {"level": 1}})
    path.write_text(json.dumps(doc))
    return copy_of_cube


def test_an_array_with_an_outer_codec_is_left_out_and_raises_when_named(
    spark, outer_codec_store
):
    default = gbx_zarr_read(spark, outer_codec_store, sel={"time": "2023-01-01T00:00"})
    assert {r["variable"] for r in default.collect()} == {"t2m", "u10", "v10"}
    with pytest.raises(ZarrUnsupported) as err:
        gbx_zarr_read(spark, outer_codec_store, variables=["msl"])
    assert "sharding_indexed" in str(err.value)


def test_registering_a_store_with_an_outer_codec_leaves_the_array_unreadable(
    spark, outer_codec_store, new_table
):
    table = new_table("outer")
    gbx_zarr_register(spark, outer_codec_store, table)
    assert {
        r["variable"]
        for r in gbx_zarr_read(spark, table, sel={"time": "2023-01-01T00:00"}).collect()
    } == {
        "t2m",
        "u10",
        "v10",
    }
    with pytest.raises(ZarrUnsupported):
        gbx_zarr_read(spark, table, variables=["msl"])


# --- the format of an export (formats/zarr and api/zarr-functions) ------------------------


def test_a_copy_keeps_the_stores_format_and_a_written_store_is_v3_with_crc32c(
    spark, cube, new_table, tmp_path
):
    import zarr

    # a Zarr v2 store, registered in place and copied out, is still Zarr v2
    v2 = tmp_path / "v2.zarr"
    shutil.copytree(Path(cube).parent / "era5_like_v2.zarr", v2)
    registered = new_table("v2copy")
    gbx_zarr_register(spark, str(v2), registered)
    out2 = tmp_path / "v2-out"
    gbx_zarr_export(spark, registered, str(out2))
    assert (out2 / ".zgroup").is_file() and (out2 / "t2m" / ".zarray").is_file()
    assert not (out2 / "zarr.json").exists()
    assert zarr.open_group(str(out2), mode="r").metadata.zarr_format == 2

    # a store gbx_zarr_write made is Zarr v3, sharded, with crc32c in its inner codecs
    written = new_table("v3write")
    blocks = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    gbx_zarr_write(blocks, written, mode="create", root=str(tmp_path / "root"))
    out3 = tmp_path / "v3-out"
    gbx_zarr_export(spark, written, str(out3))
    doc = json.loads((out3 / "t2m" / "zarr.json").read_text())
    assert doc["zarr_format"] == 3
    assert [c["name"] for c in doc["codecs"]] == ["sharding_indexed"]
    inner = doc["codecs"][0]["configuration"]["codecs"]
    assert [c["name"] for c in inner] == ["bytes", "zstd", "crc32c"]


# --- the pages ----------------------------------------------------------------------------


@pytest.mark.structure
def test_every_snippet_the_tile_structure_page_shows_exists():
    text = TILE_STRUCTURE.read_text()
    shown_here = re.findall(
        r'<CodeFromTest code=\{zarrTileExamples\} functionName="([A-Za-z_0-9]+)".*?'
        r'outputConstant="([A-Za-z_0-9]+)"',
        text,
    )
    assert shown_here, "the page shows no code from the Zarr tile examples"
    for function, output in shown_here:
        assert hasattr(ex, function) and hasattr(ex, output)


@pytest.mark.structure
def test_the_pages_speak_to_users_and_link_to_each_other():
    pattern = re.compile(
        r"\bwave[ -]?\d+|\btask \d+|gate g\d|ruling|\bZT\d+|\bAP-\d+", re.I
    )
    for page in PAGES:
        leaks = pattern.findall(page.read_text())
        assert not leaks, f"{page.name} leaks internal vocabulary: {leaks}"
    assert "#zarr-tile-metadata" in VIRTUAL_TILES.read_text()
    assert "#zarr-tiles" in TILE_STRUCTURE.read_text()
    assert "## Zarr tiles" in VIRTUAL_TILES.read_text()
    assert "Delta-indexed collections" in (DOCS / "readers-writers.mdx").read_text()
