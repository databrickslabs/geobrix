"""The native engine against zarr itself, across the codec chains it decodes.

Each case writes a small array (some chunks never written, so fills show) with zarr and
reads it back through the manifest store with the native engine, with ``auto`` and with
zarr's default pipeline; all must equal zarr reading the files directly.
"""

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
numcodecs = pytest.importorskip("numcodecs")

from zarr.codecs import (  # noqa: E402
    BloscCodec,
    BytesCodec,
    Crc32cCodec,
    GzipCodec,
    ShardingCodec,
    TransposeCodec,
    ZstdCodec,
)

from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._engine import (  # noqa: E402
    open_manifest_array,
    open_plain_array,
    read_box,
)
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrUnsupported  # noqa: E402

SHAPE, CHUNKS, SHARDS = (13, 21, 30), (4, 5, 7), (8, 10, 14)
BOXES = [
    (slice(None),) * 3,
    (slice(0, 5), slice(17, 21), slice(25, 30)),  # the edge chunks of every axis
    (slice(12, 13), slice(20, 21), slice(29, 30)),  # the last value
    (slice(3, 9), slice(4, 16), slice(6, 22)),  # across chunk and shard boundaries
    (slice(1, 2),),
]


def _data(dtype):
    dt = np.dtype(dtype)
    rng = np.random.default_rng(3)
    if dt.kind == "f":
        return (rng.normal(size=SHAPE) * 100).astype(dt)
    if dt.kind == "c":
        return (rng.normal(size=SHAPE) + 1j * rng.normal(size=SHAPE)).astype(dt)
    if dt.kind == "b":
        return rng.random(SHAPE) > 0.5
    return rng.integers(-1000, 1000, SHAPE).astype(dt)


def _write(array, full):
    # two blocks only: the chunks outside them are never written and read as the fill
    array[0:8, 0:10, 0:14] = full[0:8, 0:10, 0:14]
    array[8:13, 15:21, 21:30] = full[8:13, 15:21, 21:30]


def _v3(path, dtype, *, compressors, serializer=None, shards=None, fill=None):
    group = zarr.open_group(str(path), mode="w", zarr_format=3)
    args = dict(
        name="a", shape=SHAPE, chunks=CHUNKS, dtype=dtype, compressors=compressors
    )
    if serializer is not None:
        args["serializer"] = serializer
    if shards is not None:
        args["shards"] = shards
    if fill is not None:
        args["fill_value"] = fill
    _write(group.create_array(**args), _data(dtype))


def _v3_index_at_start(path, dtype):
    group = zarr.open_group(str(path), mode="w", zarr_format=3)
    sharding = ShardingCodec(
        chunk_shape=CHUNKS,
        codecs=[BytesCodec(), BloscCodec()],
        index_location="start",
    )
    array = group.create_array(
        name="a",
        shape=SHAPE,
        chunks=SHARDS,
        dtype=dtype,
        serializer=sharding,
        compressors=None,
    )
    _write(array, _data(dtype))


def _v2(path, dtype, *, compressor, filters=None, fill=0):
    group = zarr.open_group(str(path), mode="w", zarr_format=2)
    array = group.create_array(
        name="a",
        shape=SHAPE,
        chunks=CHUNKS,
        dtype=dtype,
        compressors=compressor,
        filters=filters,
        fill_value=fill,
        chunk_key_encoding={"name": "v2", "separator": "."},
    )
    _write(array, _data(dtype))


def _zstd_crc():  # what the GeoBrix writer emits
    return [ZstdCodec(level=3), Crc32cCodec()]


CASES = {
    "v3 bytes+zstd+crc32c": lambda p: _v3(
        p, "f4", compressors=_zstd_crc(), serializer=BytesCodec()
    ),
    "v3 sharded bytes+zstd+crc32c": lambda p: _v3(
        p, "f4", compressors=_zstd_crc(), serializer=BytesCodec(), shards=SHARDS
    ),
    "v3 gzip": lambda p: _v3(
        p, "u2", compressors=[GzipCodec(level=5)], serializer=BytesCodec()
    ),
    "v3 sharded gzip+crc32c": lambda p: _v3(
        p,
        "i4",
        compressors=[GzipCodec(), Crc32cCodec()],
        serializer=BytesCodec(),
        shards=SHARDS,
    ),
    "v3 blosc": lambda p: _v3(
        p,
        "f4",
        compressors=[BloscCodec(cname="lz4", shuffle="shuffle", typesize=4)],
        serializer=BytesCodec(),
    ),
    "v3 sharded blosc": lambda p: _v3(
        p,
        "i4",
        compressors=[BloscCodec(cname="zstd", shuffle="bitshuffle", typesize=4)],
        serializer=BytesCodec(),
        shards=SHARDS,
    ),
    "v3 big-endian": lambda p: _v3(
        p, "f8", compressors=[ZstdCodec()], serializer=BytesCodec(endian="big")
    ),
    "v3 sharded big-endian int": lambda p: _v3(
        p,
        "i2",
        compressors=[GzipCodec()],
        serializer=BytesCodec(endian="big"),
        shards=SHARDS,
        fill=-7,
    ),
    "v3 sharded, index at the start": lambda p: _v3_index_at_start(p, "f4"),
    "v3 NaN fill": lambda p: _v3(
        p,
        "f4",
        compressors=[ZstdCodec()],
        serializer=BytesCodec(),
        fill=float("nan"),
    ),
    "v3 sharded negative fill": lambda p: _v3(
        p,
        "f8",
        compressors=[ZstdCodec()],
        serializer=BytesCodec(),
        shards=SHARDS,
        fill=-9999.0,
    ),
    "v3 bool": lambda p: _v3(
        p, "bool", compressors=[ZstdCodec()], serializer=BytesCodec(), fill=True
    ),
    "v3 complex": lambda p: _v3(
        p, "complex64", compressors=[ZstdCodec()], serializer=BytesCodec()
    ),
    "v2 no compressor": lambda p: _v2(p, "<f4", compressor=None),
    "v2 blosc": lambda p: _v2(
        p, "<f8", compressor=numcodecs.Blosc(cname="lz4", shuffle=2)
    ),
    "v2 big-endian": lambda p: _v2(p, ">f8", compressor=numcodecs.Zstd()),
    "v2 big-endian int, fill": lambda p: _v2(
        p, ">i4", compressor=numcodecs.GZip(), fill=11
    ),
    "v2 Delta": lambda p: _v2(
        p, "<i4", compressor=numcodecs.Zstd(), filters=[numcodecs.Delta(dtype="<i4")]
    ),
    "v2 FixedScaleOffset": lambda p: _v2(
        p,
        "<f8",
        compressor=numcodecs.Zstd(),
        filters=[
            numcodecs.FixedScaleOffset(offset=0, scale=10, dtype="<f8", astype="<i4")
        ],
    ),
    "v2 Shuffle+Zlib": lambda p: _v2(
        p,
        "<i8",
        compressor=numcodecs.Zlib(),
        filters=[numcodecs.Shuffle(elementsize=8)],
    ),
    "v2 NaN fill": lambda p: _v2(
        p, "<f4", compressor=numcodecs.Zstd(), fill=float("nan")
    ),
    "v2 no fill": lambda p: _v2(p, "<f4", compressor=numcodecs.Zstd(), fill=None),
}


def _store(path):
    metas, rows, arrays = plain_store_rows(path)
    return ManifestStore(metas, rows, FuseRangeReader(str(path)), arrays)


@pytest.mark.parametrize("case", list(CASES))
def test_native_reads_what_zarr_reads(case, tmp_path):
    CASES[case](tmp_path / "s.zarr")
    path = tmp_path / "s.zarr"
    direct = zarr.open_array(str(path / "a"), mode="r")
    store = _store(path)
    for pipeline in ("native", "auto", "default"):
        arr = open_manifest_array(store, "a", pipeline=pipeline)
        for box in BOXES:
            got = read_box(arr, box)
            want = direct[box]
            assert got.shape == want.shape
            assert got.dtype.isnative, f"{pipeline}: {got.dtype}"
            np.testing.assert_array_equal(got, want)
    native = open_manifest_array(store, "a", pipeline="native")
    assert native.pipeline == "native" and native.dtype.isnative
    assert open_manifest_array(store, "a").pipeline == "native"


@pytest.mark.parametrize(
    "case", ["v3 big-endian", "v3 sharded big-endian int", "v2 big-endian"]
)
def test_every_engine_returns_native_byte_order(case, tmp_path):
    pytest.importorskip("tensorstore")
    CASES[case](tmp_path / "s.zarr")
    path = tmp_path / "s.zarr"
    want = zarr.open_array(str(path / "a"), mode="r")[:]
    engines = [
        ("zarr", "default"),
        ("zarr", "auto"),
        ("tensorstore", "auto"),
        ("auto", "auto"),
    ]
    if case != "v2 big-endian":  # zarrs cannot decode big-endian v2 chunks
        engines.append(("zarr", "zarrs"))
    for engine, pipeline in engines:
        arr = open_plain_array(str(path), "a", engine=engine, pipeline=pipeline)
        assert arr.dtype.isnative
        got = read_box(arr, (slice(None),) * 3)
        assert got.dtype.isnative
        np.testing.assert_array_equal(got, want)


def test_zarrs_is_not_used_for_big_endian_v2_chunks(tmp_path):
    """zarrs-python refuses a non-native byte order at read time; ``auto`` opens such an
    array with zarr's own pipeline and an explicit request says why it cannot."""
    CASES["v2 big-endian"](tmp_path / "s.zarr")
    path = str(tmp_path / "s.zarr")
    assert open_plain_array(path, "a", engine="zarr").pipeline == "default"
    with pytest.raises(ZarrUnsupported) as ei:
        open_plain_array(path, "a", engine="zarr", pipeline="zarrs")
    assert "big-endian" in str(ei.value) and "pipeline='default'" in ei.value.fix


def _unsharded_crc_store(tmp_path):
    path = tmp_path / "s.zarr"
    CASES["v3 bytes+zstd+crc32c"](path)
    return path, _store(path)


def test_native_refuses_crc32c_when_numcodecs_has_none_and_auto_falls_back(
    tmp_path, monkeypatch
):
    path, store = _unsharded_crc_store(tmp_path)
    monkeypatch.delattr(numcodecs, "CRC32C")
    with pytest.raises(ZarrUnsupported) as ei:
        open_manifest_array(store, "a", pipeline="native")
    assert "crc32c" in str(ei.value).lower() and "pipeline='default'" in ei.value.fix
    fallback = open_manifest_array(store, "a")
    assert (fallback.engine, fallback.pipeline) == ("manifest", "default")
    np.testing.assert_array_equal(
        read_box(fallback, (slice(None),) * 3),
        zarr.open_array(str(path / "a"), mode="r")[:],
    )


def test_native_refuses_transposed_arrays_and_auto_falls_back(tmp_path):
    path = tmp_path / "s.zarr"
    group = zarr.open_group(str(path), mode="w", zarr_format=3)
    array = group.create_array(
        name="a",
        shape=SHAPE,
        chunks=CHUNKS,
        dtype="f4",
        filters=[TransposeCodec(order=(2, 1, 0))],
        serializer=BytesCodec(),
        compressors=[ZstdCodec()],
    )
    _write(array, _data("f4"))
    store = _store(path)
    with pytest.raises(ZarrUnsupported) as ei:
        open_manifest_array(store, "a", pipeline="native")
    assert "pipeline='default'" in ei.value.fix
    fallback = open_manifest_array(store, "a")
    assert fallback.pipeline == "default"
    for box in BOXES:
        np.testing.assert_array_equal(
            read_box(fallback, box), zarr.open_array(str(path / "a"), mode="r")[box]
        )
