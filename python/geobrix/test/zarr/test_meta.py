import base64
import copy
import json
import struct

import numpy as np
import pytest

from databricks.labs.gbx.zarr._meta import (
    cf_decode,
    decode_coord,
    doc_bytes,
    inner_meta_doc,
    parse_v2,
    parse_v3,
    with_shape,
)
from databricks.labs.gbx.zarr.errors import ZarrUnsupported


def _doc(store_dir, name):
    return json.loads((store_dir / name / "zarr.json").read_text())


def _with_attrs(meta, attrs):
    return meta.__class__(**{**meta.__dict__, "attrs": attrs})


def test_parse_v3_sharded(era5_v3_sharded):
    m = parse_v3("t2m", _doc(era5_v3_sharded, "t2m"))
    assert (
        m.shape == (48, 73, 144)
        and m.chunks == (12, 37, 72)
        and m.shards == (24, 74, 144)
    )
    assert m.dims == ("time", "latitude", "longitude") and m.dtype == "<f4"
    assert m.shard.cps == (2, 2, 2) and m.shard.index_location == "end"
    assert m.grid_shape == (4, 2, 2) and m.itemsize == 4


def test_parse_v3_unsharded_and_float_fill(era5_v3_unsharded):
    m = parse_v3("t2m", _doc(era5_v3_unsharded, "t2m"))
    assert m.shards is None and m.shard is None and m.chunks == (12, 37, 72)
    # zarr.json stores float NaN as the string "NaN"; the model holds the float
    assert isinstance(m.fill_value, float) and np.isnan(m.fill_value)
    assert m.doc["fill_value"] == "NaN"


def test_parse_v2(era5_v2):
    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    zt = json.loads((era5_v2 / "t2m" / ".zattrs").read_text())
    m = parse_v2("t2m", za, zt)
    assert (
        m.zarr_format == 2
        and m.shards is None
        and m.dims == ("time", "latitude", "longitude")
    )
    assert m.key_encoding.chunk_key((0, 1, 1)) == "0.1.1"
    assert m.dtype == "<f4" and np.isnan(m.fill_value)
    assert "_ARRAY_DIMENSIONS" not in m.attrs and m.attrs["units"] == "K"
    assert m.doc[".zattrs"]["_ARRAY_DIMENSIONS"] == list(m.dims)


def test_with_shape_updates_doc(era5_v3_sharded):
    m = parse_v3("t2m", _doc(era5_v3_sharded, "t2m"))
    m2 = with_shape(m, (72, 73, 144))
    assert m2.shape == (72, 73, 144)
    assert json.loads(doc_bytes(m2)["zarr.json"])["shape"] == [72, 73, 144]
    assert m.doc["shape"] == [48, 73, 144], "the original meta is not mutated"


def test_v2_with_shape_and_doc_bytes(era5_v2):
    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    zt = json.loads((era5_v2 / "t2m" / ".zattrs").read_text())
    m2 = with_shape(parse_v2("t2m", za, zt), (72, 73, 144))
    docs = doc_bytes(m2)
    assert set(docs) == {".zarray", ".zattrs"}
    assert json.loads(docs[".zarray"])["shape"] == [72, 73, 144]
    assert json.loads(docs[".zattrs"])["_ARRAY_DIMENSIONS"] == list(m2.dims)


def test_inner_meta_doc_is_unsharded(era5_v3_sharded):
    m = parse_v3("t2m", _doc(era5_v3_sharded, "t2m"))
    inner = inner_meta_doc(m)
    assert inner["chunk_grid"]["configuration"]["chunk_shape"] == [12, 37, 72]
    assert inner["shape"] == [12, 37, 72]
    assert all(c["name"] != "sharding_indexed" for c in inner["codecs"])
    assert m.doc["chunk_grid"]["configuration"]["chunk_shape"] == [24, 74, 144]


def test_unsupported_dtype_raises():
    doc = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [2],
        "data_type": "string",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2]}},
        "chunk_key_encoding": {"name": "default"},
        "fill_value": "",
        "codecs": [],
    }
    with pytest.raises(ZarrUnsupported) as e:
        parse_v3("s", doc)
    assert "Fix:" in str(e.value)


def test_non_regular_chunk_grid_raises(era5_v3_unsharded):
    doc = _doc(era5_v3_unsharded, "t2m")
    doc["chunk_grid"] = {"name": "rectilinear", "configuration": {}}
    with pytest.raises(ZarrUnsupported) as e:
        parse_v3("t2m", doc)
    assert "Fix:" in str(e.value)


def test_cf_decode_scale_offset_fill(era5_v3_sharded):
    m = parse_v3("t2m", _doc(era5_v3_sharded, "t2m"))
    m = m.__class__(
        **{
            **m.__dict__,
            "attrs": {"scale_factor": 0.5, "add_offset": 10.0, "_FillValue": -1},
        }
    )
    out = cf_decode(np.array([0, 2, -1], dtype="<i2"), m)
    assert out[0] == 10.0 and out[1] == 11.0 and np.isnan(out[2])


def test_cf_decode_reads_xarray_base64_fill_attr(era5_v3_unsharded):
    """xarray writes a Zarr v3 float _FillValue attribute as base64 of a little-endian f8."""
    m = parse_v3("t2m", _doc(era5_v3_unsharded, "t2m"))
    b64 = base64.standard_b64encode(struct.pack("<d", -9999.0)).decode()
    out = cf_decode(
        np.array([1.0, -9999.0], dtype="<f4"), _with_attrs(m, {"_FillValue": b64})
    )
    assert out[0] == 1.0 and np.isnan(out[1])
    # the store's own t2m attribute (NaN) decodes without error
    assert np.isnan(cf_decode(np.array([np.nan], dtype="<f4"), m)[0])


def _int_meta(era5_v3_unsharded, attrs):
    doc = copy.deepcopy(_doc(era5_v3_unsharded, "t2m"))
    doc.update(data_type="int16", fill_value=0, attributes=attrs)
    return parse_v3("n", doc)


def test_cf_decode_v3_zarr_fill_value_is_not_a_mask(era5_v3_unsharded):
    """Zarr v3 fill_value is the value of unwritten chunks, not CF missing data (xarray default)."""
    m = _int_meta(era5_v3_unsharded, {})
    assert list(cf_decode(np.array([0, 5], dtype="<i2"), m)) == [0, 5]
    masked = cf_decode(np.array([0, 5], dtype="<i2"), _with_attrs(m, {"_FillValue": 0}))
    assert np.isnan(masked[0]) and masked[1] == 5


def test_cf_decode_v2_zarr_fill_value_masks(era5_v2):
    """Zarr v2 has no _FillValue attribute; xarray treats the array fill_value as the mask."""
    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    za.update(dtype="<i2", fill_value=-1)
    m = parse_v2("n", za, {"_ARRAY_DIMENSIONS": ["time", "latitude", "longitude"]})
    out = cf_decode(np.array([3, -1], dtype="<i2"), m)
    assert out[0] == 3 and np.isnan(out[1])


def _v2_array_store(tmp_path, *, fill_value, attrs):
    """A real Zarr v2 group with one float32 array ``a`` holding [1, -9999, nan, 3]."""
    import zarr

    root = tmp_path / "golden.zarr"
    group = zarr.open_group(str(root), mode="w", zarr_format=2)
    arr = group.create_array(
        "a",
        shape=(4,),
        chunks=(4,),
        dtype="float32",
        fill_value=fill_value,
        compressors=None,
        attributes={"_ARRAY_DIMENSIONS": ["x"], **attrs},
    )
    arr[:] = np.array([1.0, -9999.0, np.nan, 3.0], dtype="float32")
    return root


def _ours_and_xarrays(root):
    """cf_decode of the raw bytes vs what xarray.open_zarr returns for the same store."""
    import xarray as xr
    import zarr

    za = json.loads((root / "a" / ".zarray").read_text())
    zt = json.loads((root / "a" / ".zattrs").read_text())
    raw = zarr.open_array(str(root / "a"), mode="r", zarr_format=2)[:]
    ours = cf_decode(raw, parse_v2("a", za, zt))
    return ours, xr.open_zarr(str(root), consolidated=False)["a"].values


def test_v2_zarr_fill_value_overrides_the_fill_attribute_like_xarray(tmp_path):
    root = _v2_array_store(
        tmp_path, fill_value=float("nan"), attrs={"_FillValue": -9999}
    )
    ours, theirs = _ours_and_xarrays(root)
    np.testing.assert_array_equal(ours, theirs)
    assert ours[1] == -9999.0 and np.isnan(ours[2])  # [1, -9999, nan, 3]


def test_v2_fill_attribute_alone_masks_like_xarray(tmp_path):
    root = _v2_array_store(tmp_path, fill_value=None, attrs={"_FillValue": -9999})
    ours, theirs = _ours_and_xarrays(root)
    np.testing.assert_array_equal(ours, theirs)
    assert np.isnan(ours[1]) and ours[3] == 3.0


@pytest.mark.parametrize(
    "fill_value", [float("nan"), None], ids=["fill_nan", "fill_null"]
)
def test_v2_string_fill_attribute_is_left_raw_like_xarray(tmp_path, fill_value):
    """A v2 attribute is never base64: "-9999" must not reach the v3 decoder (struct.error)."""
    root = _v2_array_store(
        tmp_path, fill_value=fill_value, attrs={"_FillValue": "-9999"}
    )
    ours, theirs = _ours_and_xarrays(root)
    np.testing.assert_array_equal(ours, theirs)
    assert ours[1] == -9999.0


@pytest.mark.parametrize("bad", ["-9999", "YWJjZA=="], ids=["not_base64", "4_bytes"])
def test_v3_float_fill_attribute_that_is_not_a_float64_is_unsupported(
    era5_v3_unsharded, bad
):
    m = parse_v3("t2m", _doc(era5_v3_unsharded, "t2m"))
    with pytest.raises(ZarrUnsupported) as e:
        cf_decode(np.array([1.0], dtype="<f4"), _with_attrs(m, {"_FillValue": bad}))
    assert "Fix:" in str(e.value) and "t2m" in str(e.value)


def test_cf_decode_v2_bool_stays_bool(era5_v2):
    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    za.update(dtype="|b1", fill_value=False)
    m = parse_v2("flag", za, {})
    out = cf_decode(np.array([True, False]), m)
    assert out.dtype == np.dtype("bool") and list(out) == [True, False]


def test_decode_time_and_reject_noleap(era5_v3_sharded):
    t = parse_v3("time", _doc(era5_v3_sharded, "time"))
    vals = decode_coord(np.array([1078872, 1078873], dtype="<i8"), t)
    assert vals.dtype == np.dtype("datetime64[ns]")
    nl = t.__class__(**{**t.__dict__, "attrs": {**t.attrs, "calendar": "noleap"}})
    with pytest.raises(ZarrUnsupported) as e:
        decode_coord(np.array([0], dtype="<i8"), nl)
    assert "decode=False" in str(e.value)


@pytest.mark.parametrize(
    "units,values",
    [
        ("days since 1000-01-01", [0, 1]),  # before 1678: datetime64[ns] cannot hold it
        ("days since 2500-01-01", [0, 1]),  # after 2262
        ("days since 1900-01-01", [10**9]),  # overflows int64 nanoseconds
    ],
)
def test_decode_time_outside_the_ns_range_is_unsupported(
    era5_v3_sharded, units, values
):
    t = parse_v3("time", _doc(era5_v3_sharded, "time"))
    meta = _with_attrs(t, {"units": units, "calendar": "standard"})
    with pytest.raises(ZarrUnsupported) as e:
        decode_coord(np.array(values, dtype="<i8"), meta)
    assert "decode=False" in str(e.value)


def test_decode_coord_leaves_other_coordinates(era5_v3_sharded):
    lat = parse_v3("latitude", _doc(era5_v3_sharded, "latitude"))
    vals = np.array([90.0, 87.5])
    assert decode_coord(vals, lat) is vals


def test_sharding_must_be_the_only_outer_codec(era5_v3_sharded):
    """Zarr allows array->array codecs before and bytes->bytes codecs after an
    array->bytes codec such as sharding_indexed; the shard model has no place for them.
    """
    transpose = {"name": "transpose", "configuration": {"order": [2, 1, 0]}}
    for before, after in (
        ([transpose], []),
        ([], [{"name": "crc32c"}]),
        ([], [{"name": "gzip", "configuration": {"level": 1}}]),
        ([{"name": "crc32c"}], []),  # not even a valid order
    ):
        doc = _doc(era5_v3_sharded, "t2m")
        doc["codecs"] = [*before, *doc["codecs"], *after]
        with pytest.raises(ZarrUnsupported) as e:
            parse_v3("t2m", doc)
        assert "Fix:" in str(e.value) and "sharding_indexed" in str(e.value)


def test_parse_keeps_its_own_copy_of_the_document(era5_v3_sharded, era5_v2):
    doc = _doc(era5_v3_sharded, "t2m")
    m = parse_v3("t2m", doc)
    doc["shape"][0] = 999
    doc["attributes"]["units"] = "changed"
    doc["codecs"][0]["configuration"]["codecs"][0]["configuration"]["endian"] = "big"
    assert m.doc["shape"][0] == 48 and m.attrs["units"] == "K"
    assert m.shard.inner_codecs[0]["configuration"]["endian"] == "little"
    m.attrs["units"] = "edited"  # attrs and doc are independent copies too
    assert m.doc["attributes"]["units"] == "K"

    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    zt = json.loads((era5_v2 / "t2m" / ".zattrs").read_text())
    m2 = parse_v2("t2m", za, zt)
    za["shape"][0] = 999
    zt["units"] = "changed"
    assert m2.doc[".zarray"]["shape"][0] == 48 and m2.attrs["units"] == "K"
    assert m2.doc[".zattrs"]["units"] == "K"
