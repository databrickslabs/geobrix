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


# --- Task 9 carries: time-unit errors, time fill, use_zarr_fill_value_as_mask (Ruling Z13) ---


def _time_meta(era5_v3_sharded, **attrs):
    t = parse_v3("time", _doc(era5_v3_sharded, "time"))
    return _with_attrs(t, {"units": "hours since 1900-01-01", **attrs})


def test_decode_time_with_an_unknown_unit_is_unsupported_not_a_key_error(
    era5_v3_sharded,
):
    meta = _time_meta(era5_v3_sharded, units="furlongs since 2000-01-01")
    with pytest.raises(ZarrUnsupported) as e:
        decode_coord(np.array([0, 1], dtype="<i8"), meta)
    text = str(e.value)
    assert "furlongs" in text and "'time'" in text
    assert "outside" not in text and "Fix:" in text and "decode=False" in e.value.fix


def test_decode_time_with_a_malformed_reference_date_says_so(era5_v3_sharded):
    for units in ("hours since garbage", "days since 2000-13-45"):
        meta = _time_meta(era5_v3_sharded, units=units)
        with pytest.raises(ZarrUnsupported) as e:
            decode_coord(np.array([0, 1], dtype="<i8"), meta)
        text = str(e.value)
        assert "reference date" in text and "not a date" in text, units
        assert "outside" not in text and "Fix:" in text


def test_decode_time_range_errors_still_say_outside_the_range(era5_v3_sharded):
    for units in ("days since 1000-01-01", "days since 2500-01-01"):
        with pytest.raises(ZarrUnsupported, match="outside the range") as e:
            decode_coord(
                np.array([0, 1], dtype="<i8"),
                _time_meta(era5_v3_sharded, units=units),
            )
        assert "reference date" not in str(e.value)


def test_time_coordinate_fill_values_become_nat_like_xarray(era5_v3_sharded):
    nat = np.datetime64("NaT", "ns")
    # a float coordinate with NaN
    out = decode_coord(np.array([0.0, np.nan, 5.0]), _time_meta(era5_v3_sharded))
    assert np.isnat(out[1]) and out[2] == np.datetime64("1900-01-01T05", "ns")
    # an integer coordinate with a _FillValue attribute (Zarr v3)
    meta = _time_meta(era5_v3_sharded, _FillValue=-1)
    out = decode_coord(np.array([0, -1, 5], dtype="<i8"), meta)
    assert out[0] == np.datetime64("1900-01-01T00", "ns") and np.isnat(out[1])
    assert out.dtype == nat.dtype


def test_time_coordinate_zarr_v2_fill_value_masks_to_nat_unless_turned_off(era5_v2):
    za = json.loads((era5_v2 / "time" / ".zarray").read_text())
    za["fill_value"] = -1
    zt = json.loads((era5_v2 / "time" / ".zattrs").read_text())
    meta = parse_v2("time", za, zt)
    raw = np.array([0, -1, 5], dtype="<i8")
    assert np.isnat(decode_coord(raw, meta)[1])  # xarray's default for Zarr v2
    kept = decode_coord(raw, meta, use_zarr_fill_value_as_mask=False)
    assert kept[1] == np.datetime64("1899-12-31T23", "ns")  # -1 hour: a real time


def test_use_zarr_fill_value_as_mask_follows_xarray(era5_v3_unsharded, era5_v2):
    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    za.update(dtype="<i2", fill_value=0)
    v2 = parse_v2("n", za, {"_ARRAY_DIMENSIONS": ["time", "latitude", "longitude"]})
    values = np.array([0, 5], dtype="<i2")
    assert np.isnan(cf_decode(values, v2)[0])  # None: v2 masks on fill_value
    assert np.isnan(cf_decode(values, v2, use_zarr_fill_value_as_mask=True)[0])
    kept = cf_decode(values, v2, use_zarr_fill_value_as_mask=False)
    assert list(kept) == [0, 5] and kept.dtype == values.dtype

    v3 = _int_meta(era5_v3_unsharded, {})  # int16 with fill_value 0
    assert list(cf_decode(values, v3)) == [0, 5]  # None: v3 never masks on it
    assert np.isnan(cf_decode(values, v3, use_zarr_fill_value_as_mask=True)[0])
    assert list(cf_decode(values, v3, use_zarr_fill_value_as_mask=False)) == [0, 5]


def test_use_zarr_fill_value_as_mask_false_still_reads_the_v3_fill_attribute(
    era5_v3_unsharded,
):
    b64 = base64.standard_b64encode(struct.pack("<d", -9999.0)).decode()
    m = _with_attrs(
        parse_v3("t2m", _doc(era5_v3_unsharded, "t2m")), {"_FillValue": b64}
    )
    values = np.array([1.0, -9999.0], dtype="<f4")
    for flag in (None, False):
        assert np.isnan(cf_decode(values, m, use_zarr_fill_value_as_mask=flag)[1])


def test_use_zarr_fill_value_as_mask_must_be_a_bool_or_none(era5_v3_unsharded):
    m = _int_meta(era5_v3_unsharded, {})
    with pytest.raises(ValueError, match="use_zarr_fill_value_as_mask"):
        cf_decode(np.array([0], dtype="<i2"), m, use_zarr_fill_value_as_mask="yes")


# --- Task 9 fix round 1: exact integers, masks as masks ---------------------------------


def _packed_meta(era5_v3_unsharded, dtype, attrs, fill=0):
    doc = copy.deepcopy(_doc(era5_v3_unsharded, "t2m"))
    doc.update(data_type=dtype, fill_value=fill, attributes=attrs)
    return parse_v3("n", doc)


def test_missing_mask_compares_the_raw_values_with_the_fill_attributes(
    era5_v3_unsharded,
):
    from databricks.labs.gbx.zarr._meta import missing_mask

    m = _packed_meta(
        era5_v3_unsharded, "int16", {"_FillValue": -1, "missing_value": [-2, -3]}
    )
    raw = np.array([-1, 5, -2, -3, 0], dtype="<i2")
    assert list(missing_mask(raw, m)) == [True, False, True, True, False]
    plain = _packed_meta(era5_v3_unsharded, "int16", {})
    assert (
        missing_mask(raw, plain) is None
    )  # nothing to mask: v3 fill_value never masks
    assert list(missing_mask(raw, plain, use_zarr_fill_value_as_mask=True)) == [
        False,
        False,
        False,
        False,
        True,
    ]


def test_missing_mask_nan_fill_masks_nan_and_a_numeric_fill_leaves_nan_alone(
    era5_v3_unsharded,
):
    from databricks.labs.gbx.zarr._meta import missing_mask

    nan_fill = _packed_meta(era5_v3_unsharded, "float32", {"_FillValue": "NaN"}, "NaN")
    values = np.array([1.0, np.nan, -9999.0], dtype="<f4")
    assert list(missing_mask(values, nan_fill)) == [False, True, False]
    numeric = _packed_meta(era5_v3_unsharded, "float32", {"_FillValue": -9999.0}, "NaN")
    # a NaN that is data, with a different fill, is not missing
    assert list(missing_mask(values, numeric)) == [False, False, True]


def test_missing_mask_never_masks_a_bool_array(era5_v2):
    from databricks.labs.gbx.zarr._meta import missing_mask

    za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
    za.update(dtype="|b1", fill_value=False)
    m = parse_v2("flag", za, {})
    assert missing_mask(np.array([True, False]), m) is None


def test_cf_unpack_keeps_exact_integers_and_returns_the_mask_separately(
    era5_v3_unsharded,
):
    from databricks.labs.gbx.zarr._meta import cf_unpack

    big = 2**53 + 1
    m = _packed_meta(era5_v3_unsharded, "int64", {"_FillValue": -1})
    raw = np.array([big, -1, 7], dtype="<i8")
    values, mask = cf_unpack(raw, m)
    assert (
        values.dtype == np.dtype("int64") and values[0] == big
    )  # exact: not via float64
    assert list(mask) == [False, True, False]
    # packing is decoded to floating point (the mask is still from the raw values)
    packed = _packed_meta(
        era5_v3_unsharded,
        "int16",
        {"_FillValue": -1, "scale_factor": 0.5, "add_offset": 10.0},
    )
    values, mask = cf_unpack(np.array([2, -1], dtype="<i2"), packed)
    assert (
        values.dtype.kind == "f" and values[0] == 11.0 and list(mask) == [False, True]
    )
    # no fill, no packing: the values as stored, no mask
    values, mask = cf_unpack(raw, _packed_meta(era5_v3_unsharded, "int64", {}))
    assert values.dtype == np.dtype("int64") and mask is None


def test_integer_time_coordinates_with_a_fill_decode_exactly(tmp_path):
    """int64 nanoseconds beyond 2**53 must not pass through float64; the fill becomes NaT."""
    import zarr

    raw = np.array(
        [1672531200000000001, -9223372036854775808, 1685620800123456789], dtype="i8"
    )
    want = [1672531200000000001, None, 1685620800123456789]
    for fmt in (2, 3):
        path = tmp_path / f"t{fmt}.zarr"
        g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
        attrs = {"units": "nanoseconds since 1970-01-01", "calendar": "standard"}
        if fmt == 2:
            attrs["_ARRAY_DIMENSIONS"] = ["time"]
        else:
            attrs["_FillValue"] = int(raw[1])
        kw = {"dimension_names": ("time",)} if fmt == 3 else {}
        a = g.create_array(
            "time",
            shape=(3,),
            chunks=(3,),
            dtype="i8",
            fill_value=int(raw[1]),
            attributes=attrs,
            **kw,
        )
        a[:] = raw
        meta = (
            parse_v3("time", json.loads((path / "time" / "zarr.json").read_text()))
            if fmt == 3
            else parse_v2(
                "time",
                json.loads((path / "time" / ".zarray").read_text()),
                json.loads((path / "time" / ".zattrs").read_text()),
            )
        )
        ours = decode_coord(raw, meta)
        assert ours.dtype == np.dtype("datetime64[ns]")
        got = [None if np.isnat(x) else int(x.astype("i8")) for x in ours]
        assert got == want, (fmt, got)
        theirs = xr_time(path)
        np.testing.assert_array_equal(ours, theirs)


def xr_time(path):
    import xarray as xr
    from xarray.backends.zarr import ZarrStore

    return xr.open_dataset(
        ZarrStore.open_group(str(path), mode="r", consolidated=False)
    ).time.values


# --- Task 9 fix round 2: xarray's _Unsigned and dtype:bool coders, one reinterpretation ----


def _stored(tmp_path, fmt, values, dtype, attrs, name="v"):
    """A one-array store (dimension ``k``) and the array's ``ArrayMeta``."""
    import zarr

    path = (
        tmp_path / f"s{fmt}_{name}_{abs(hash(json.dumps(attrs, sort_keys=True)))}.zarr"
    )
    g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
    attributes, kw = dict(attrs), {}
    if fmt == 2:
        attributes["_ARRAY_DIMENSIONS"] = ["k"]
    else:
        kw["dimension_names"] = ("k",)
    arr = g.create_array(
        name,
        shape=(len(values),),
        chunks=(len(values),),
        dtype=dtype,
        fill_value=None if fmt == 2 else 0,
        attributes=attributes,
        **kw,
    )
    arr[:] = np.asarray(values, dtype=dtype)
    doc = path / name
    meta = (
        parse_v3(name, json.loads((doc / "zarr.json").read_text()))
        if fmt == 3
        else parse_v2(
            name,
            json.loads((doc / ".zarray").read_text()),
            json.loads((doc / ".zattrs").read_text()),
        )
    )
    return path, meta


def _xarray_values(path, name="v"):
    import xarray as xr
    from xarray.backends.zarr import ZarrStore

    store = ZarrStore.open_group(str(path), mode="r", consolidated=False)
    return xr.open_dataset(store)[name].values


@pytest.mark.parametrize("fmt", [2, 3])
def test_an_unsigned_attribute_reinterprets_signed_integers_like_xarray(tmp_path, fmt):
    from databricks.labs.gbx.zarr._meta import cf_reinterpret, cf_unpack

    raw = np.array([-1, -56, 0, 100], dtype="i1")
    path, meta = _stored(tmp_path, fmt, raw, "i1", {"_Unsigned": "true"})
    values, attrs = cf_reinterpret(raw, meta)
    assert values.dtype == np.dtype("u1") and list(values) == [255, 200, 0, 100]
    assert "_Unsigned" not in attrs  # consumed, as xarray moves it to the encoding
    out, mask = cf_unpack(raw, meta)
    assert out.dtype == np.dtype("u1") and mask is None
    np.testing.assert_array_equal(out, _xarray_values(path))


@pytest.mark.parametrize("fmt", [2, 3])
def test_an_unsigned_false_attribute_reinterprets_unsigned_integers(tmp_path, fmt):
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([65535, 5, 7], dtype="u2")
    path, meta = _stored(tmp_path, fmt, raw, "u2", {"_Unsigned": "false"})
    out, mask = cf_unpack(raw, meta)
    assert out.dtype == np.dtype("i2") and list(out) == [-1, 5, 7] and mask is None
    np.testing.assert_array_equal(out, _xarray_values(path))


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_bool_dtype_attribute_makes_booleans_like_xarray(tmp_path, fmt):
    from databricks.labs.gbx.zarr._meta import cf_unpack, missing_mask

    raw = np.array([0, 1, 1, 0], dtype="i1")
    path, meta = _stored(tmp_path, fmt, raw, "i1", {"dtype": "bool"})
    out, mask = cf_unpack(raw, meta)
    assert out.dtype == np.dtype("bool") and list(out) == [False, True, True, False]
    assert mask is None and missing_mask(raw, meta) is None  # a boolean is never masked
    np.testing.assert_array_equal(out, _xarray_values(path))


@pytest.mark.parametrize("fmt", [2, 3])
def test_an_unsigned_array_masks_its_fill_in_the_unsigned_values(tmp_path, fmt):
    """_FillValue -1 on int8 with _Unsigned is 255 once reinterpreted (xarray's rule)."""
    from databricks.labs.gbx.zarr._meta import cf_reinterpret, cf_unpack

    raw = np.array([-1, -56, 0, 100], dtype="i1")
    path, meta = _stored(
        tmp_path, fmt, raw, "i1", {"_Unsigned": "true", "_FillValue": -1}
    )
    values, attrs = cf_reinterpret(raw, meta)
    assert attrs["_FillValue"] == 255 and list(values) == [255, 200, 0, 100]
    out, mask = cf_unpack(raw, meta)
    assert list(mask) == [True, False, False, False] and out.dtype == np.dtype("u1")
    theirs = _xarray_values(path)  # float, NaN where missing
    np.testing.assert_array_equal(np.where(mask, np.nan, out), theirs)


def test_unsigned_is_ignored_where_xarray_ignores_it(tmp_path):
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([-1, 5], dtype="i1")
    for attrs in ({"_Unsigned": True}, {"_Unsigned": "TRUE"}, {"_Unsigned": "false"}):
        path, meta = _stored(
            tmp_path, 3, raw, "i1", attrs
        )  # not "true" on a signed int
        out, _ = cf_unpack(raw, meta)
        assert out.dtype == np.dtype("i1") and list(out) == [-1, 5], attrs
        np.testing.assert_array_equal(out, _xarray_values(path))
    flt = np.array([1.5, -2.0], dtype="f4")
    path, meta = _stored(
        tmp_path, 3, flt, "f4", {"_Unsigned": "true"}
    )  # not an integer
    out, _ = cf_unpack(flt, meta)
    np.testing.assert_array_equal(out, flt)


def test_an_unsigned_fill_that_does_not_fit_is_a_typed_refusal(tmp_path):
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([-1, 5], dtype="i1")
    _, meta = _stored(tmp_path, 3, raw, "i1", {"_Unsigned": "true", "_FillValue": 255})
    with pytest.raises(ZarrUnsupported) as e:
        cf_unpack(raw, meta)  # xarray raises a bare OverflowError for the same array
    assert "_FillValue" in str(e.value) and "Fix:" in str(e.value)
    with pytest.raises(ZarrUnsupported):
        cf_decode(raw, meta)


@pytest.mark.parametrize("fmt", [2, 3])
def test_an_unsigned_time_coordinate_decodes_from_the_unsigned_values(tmp_path, fmt):
    raw = np.array([-1, 10], dtype="i2")
    attrs = {
        "units": "hours since 2000-01-01",
        "calendar": "standard",
        "_Unsigned": "true",
    }
    path, meta = _stored(tmp_path, fmt, raw, "i2", attrs, name="time")
    ours = decode_coord(raw, meta)
    assert str(ours[0]) == "2007-06-23T15:00:00.000000000"  # 65535 hours, not -1
    assert str(ours[1]) == "2000-01-01T10:00:00.000000000"
    np.testing.assert_array_equal(ours, _xarray_values(path, "time"))
    # with a fill expressed in the stored (signed) type: that cell is NaT
    attrs["_FillValue"] = -1
    path, meta = _stored(tmp_path, fmt, raw, "i2", attrs, name="time")
    ours = decode_coord(raw, meta)
    assert np.isnat(ours[0]) and str(ours[1]) == "2000-01-01T10:00:00.000000000"
    np.testing.assert_array_equal(ours, _xarray_values(path, "time"))


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_packed_time_coordinate_with_a_fill_decodes_through_the_packing(
    tmp_path, fmt
):
    raw = np.array([2, -1], dtype="i2")
    attrs = {
        "units": "hours since 2000-01-01",
        "calendar": "standard",
        "scale_factor": 0.5,
        "_FillValue": -1,
    }
    path, meta = _stored(tmp_path, fmt, raw, "i2", attrs, name="time")
    ours = decode_coord(raw, meta)
    assert str(ours[0]) == "2000-01-01T01:00:00.000000000" and np.isnat(ours[1])
    np.testing.assert_array_equal(ours, _xarray_values(path, "time"))


@pytest.mark.parametrize("attr", ["_FillValue", "missing_value"])
@pytest.mark.parametrize("fill", ["NaN", "-9999"])
def test_a_string_fill_matches_nothing_exactly_as_it_does_in_xarray(
    tmp_path, attr, fill
):
    """xarray compares the data with the string, which is never equal: nothing is masked.
    The table's mask must not disagree with the blocks it sits beside."""
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([1.0, np.nan, -9999.0, 2.0], dtype="f4")
    path, meta = _stored(
        tmp_path, 2, raw, "f4", {attr: fill}
    )  # v2: the attribute stays raw
    out, mask = cf_unpack(raw, meta)
    assert mask is None or not mask.any()
    np.testing.assert_array_equal(out, _xarray_values(path))


# --- Task 9 follow-ups: rank mismatch, scalar fills, byte order, _Unsigned corners ----------


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_shape_and_chunks_of_different_rank_are_a_typed_refusal(
    era5_v2, era5_v3_unsharded, fmt
):
    if fmt == 3:
        doc = _doc(era5_v3_unsharded, "t2m")
        doc["chunk_grid"]["configuration"]["chunk_shape"] = [12, 37]  # 3-D shape
        parse = lambda: parse_v3("t2m", doc)  # noqa: E731
    else:
        za = json.loads((era5_v2 / "t2m" / ".zarray").read_text())
        za["chunks"] = [12, 37]
        zt = json.loads((era5_v2 / "t2m" / ".zattrs").read_text())
        parse = lambda: parse_v2("t2m", za, zt)  # noqa: E731
    with pytest.raises(ZarrUnsupported, match="differ in length") as e:
        parse()
    assert "'t2m'" in str(e.value) and "Fix:" in str(e.value)


def test_an_overflow_that_is_not_about_unsigned_is_not_blamed_on_unsigned(
    era5_v3_unsharded, monkeypatch
):
    """cf_decode words an OverflowError as an _Unsigned fill problem only when _Unsigned is
    applied; any other one reaches the caller as it is."""
    import xarray.conventions

    def boom(*a, **k):
        raise OverflowError("python int too large to convert to C long")

    monkeypatch.setattr(xarray.conventions, "decode_cf_variable", boom)
    plain = _packed_meta(era5_v3_unsharded, "int16", {"_FillValue": -1})
    with pytest.raises(OverflowError, match="too large"):
        cf_decode(np.array([1, -1], dtype="<i2"), plain)
    unsigned = _packed_meta(
        era5_v3_unsharded, "int16", {"_FillValue": -1, "_Unsigned": "true"}
    )
    with pytest.raises(ZarrUnsupported, match="_Unsigned") as e:
        cf_decode(np.array([1, -1], dtype="<i2"), unsigned)
    assert "Fix:" in str(e.value)


@pytest.mark.parametrize(
    "dtype, unsigned", [("i1", "true"), ("u1", "false")], ids=["i1-true", "u1-false"]
)
@pytest.mark.parametrize("fmt", [2, 3])
def test_an_array_valued_fill_beside_unsigned_is_a_typed_refusal(
    tmp_path, fmt, dtype, unsigned
):
    """CF wants one fill; xarray fails with a bare TypeError and a mask would silently
    cover nothing. Every reader refuses with a Fix instead."""
    from databricks.labs.gbx.zarr._meta import cf_unpack, missing_mask

    raw = np.array([1, 5, 7], dtype=dtype)
    _, meta = _stored(
        tmp_path, fmt, raw, dtype, {"_Unsigned": unsigned, "_FillValue": [1, 5]}
    )
    for read in (cf_decode, cf_unpack, missing_mask):
        with pytest.raises(ZarrUnsupported, match="scalar") as e:
            read(raw, meta)
        assert "_FillValue" in str(e.value) and "Fix:" in str(e.value)
    time_attrs = {
        "units": "hours since 2000-01-01",
        "_Unsigned": unsigned,
        "_FillValue": [1, 5],
    }
    _, tmeta = _stored(tmp_path, fmt, raw, dtype, time_attrs, name="time")
    with pytest.raises(ZarrUnsupported, match="scalar"):
        decode_coord(raw, tmeta)


def test_an_array_valued_fill_without_unsigned_still_masks_every_value(tmp_path):
    """Only the _Unsigned conversion needs a single fill; xarray masks a list otherwise."""
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([1, 5, 7], dtype="i1")
    _, meta = _stored(tmp_path, 3, raw, "i1", {"_FillValue": [1, 5]})
    _, mask = cf_unpack(raw, meta)
    assert list(mask) == [True, True, False]
    out = cf_decode(raw, meta)  # xarray's own decoding of the same attributes
    assert np.isnan(out[:2]).all() and out[2] == 7


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_big_endian_unsigned_time_with_a_fill_decodes_like_native(tmp_path, fmt):
    """The fill is converted in the order the values are in: a direct caller may hold
    big-endian values (an engine read is always native)."""
    attrs = {
        "units": "hours since 2000-01-01",
        "calendar": "standard",
        "_Unsigned": "true",
        "_FillValue": -2,
    }
    native = np.array([-2, 10, 20], dtype="<i2")
    _, meta = _stored(tmp_path, fmt, native, "i2", attrs, name="time")
    want = decode_coord(native, meta)
    assert np.isnat(want[0]) and str(want[1]) == "2000-01-01T10:00:00.000000000"
    got = decode_coord(native.astype(">i2"), meta)
    np.testing.assert_array_equal(got, want)


def test_a_big_endian_unsigned_array_is_masked_like_native(tmp_path):
    from databricks.labs.gbx.zarr._meta import cf_unpack, missing_mask

    native = np.array([-2, 10, 20], dtype="<i2")
    _, meta = _stored(
        tmp_path, 3, native, "i2", {"_Unsigned": "true", "_FillValue": -2}
    )
    big = native.astype(">i2")
    assert list(missing_mask(big, meta)) == [True, False, False]
    out, mask = cf_unpack(big, meta)
    assert out.dtype.isnative and list(out) == [65534, 10, 20]
    assert list(mask) == [True, False, False]
    np.testing.assert_array_equal(cf_decode(big, meta), cf_decode(native, meta))


@pytest.mark.parametrize("fmt", [2, 3])
def test_unsigned_false_masks_its_fill_in_the_signed_values_like_xarray(tmp_path, fmt):
    """uint16 [65535, 5, 7] with _Unsigned 'false' reads as int16 [-1, 5, 7]; a fill of -1
    is then the first cell."""
    from databricks.labs.gbx.zarr._meta import cf_reinterpret, cf_unpack

    raw = np.array([65535, 5, 7], dtype="u2")
    path, meta = _stored(
        tmp_path, fmt, raw, "u2", {"_Unsigned": "false", "_FillValue": -1}
    )
    values, attrs = cf_reinterpret(raw, meta)
    assert values.dtype == np.dtype("i2") and attrs["_FillValue"] == -1
    out, mask = cf_unpack(raw, meta)
    assert out.dtype == np.dtype("i2") and list(mask) == [True, False, False]
    theirs = _xarray_values(path)  # float, NaN where missing
    np.testing.assert_array_equal(np.where(mask, np.nan, out), theirs)
    assert np.isnan(theirs[0]) and list(theirs[1:]) == [5, 7]


@pytest.mark.parametrize("fmt", [2, 3])
@pytest.mark.parametrize("fill", [None, -1], ids=["no-fill", "fill"])
def test_unsigned_with_a_scale_factor_masks_the_reinterpreted_values(
    tmp_path, fmt, fill
):
    """_Unsigned and scale_factor together: the mask is found on the unsigned values and
    the numbers are the packed ones, decoded as xarray decodes them."""
    from databricks.labs.gbx.zarr._meta import cf_unpack

    raw = np.array([-1, 5, 100], dtype="i1")  # unsigned: 255, 5, 100
    attrs = {"_Unsigned": "true", "scale_factor": 0.5}
    if fill is not None:
        attrs["_FillValue"] = fill
    path, meta = _stored(tmp_path, fmt, raw, "i1", attrs)
    out, mask = cf_unpack(raw, meta)
    theirs = _xarray_values(path)
    assert out.dtype.kind == "f"
    if fill is None:
        assert mask is None
        np.testing.assert_array_equal(out, theirs)
        assert list(theirs) == [127.5, 2.5, 50.0]  # 255, 5 and 100 times 0.5
    else:
        assert list(mask) == [True, False, False]
        np.testing.assert_array_equal(np.where(mask, np.nan, out), theirs)
        assert np.isnan(theirs[0]) and list(theirs[1:]) == [2.5, 50.0]


@pytest.mark.parametrize("fmt", [2, 3])
@pytest.mark.parametrize(
    "fill, masked",
    [(0, [True, False, False, True]), (1, [False, True, True, False]), (-1, None)],
)
def test_a_bool_dtype_array_is_masked_before_the_cast_in_a_table_not_in_blocks(
    tmp_path, fmt, fill, masked
):
    """Ruling Z36: an integer array read as booleans (``dtype: "bool"``) whose fill is a
    stored value. The table finds the missing cells on the integers, before the cast, so
    they are NULL; blocks keep xarray's rule, which casts the NaN of a masked cell to True.
    """
    from databricks.labs.gbx.zarr._meta import cf_unpack, missing_mask

    raw = np.array([0, 1, 1, 0], dtype="i1")
    path, meta = _stored(
        tmp_path, fmt, raw, "i1", {"dtype": "bool", "_FillValue": fill}
    )
    out, mask = cf_unpack(raw, meta)
    assert out.dtype == np.dtype("bool") and list(out) == [False, True, True, False]
    if masked is None:  # a fill no cell holds masks nothing
        assert mask is None or not mask.any()
        assert not missing_mask(raw, meta).any()
    else:
        assert list(mask) == masked and list(missing_mask(raw, meta)) == masked
    blocks = cf_decode(raw, meta)
    np.testing.assert_array_equal(blocks, _xarray_values(path))
    assert blocks.dtype == np.dtype("bool")
    if fill != -1:  # the masked cells are True in blocks, whatever they stored
        assert blocks[raw == fill].all()
