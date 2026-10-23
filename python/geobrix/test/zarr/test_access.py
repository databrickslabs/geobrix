"""``FuseRangeReader``: coalesced reads, drift, and the containment of every path it opens."""

import errno
import os
import subprocess
import sys
import time

import pytest

from databricks.labs.gbx.zarr._access import FuseRangeReader
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift

DATA = bytes(range(100))


@pytest.fixture
def packs(tmp_path):
    (tmp_path / "base").mkdir()
    (tmp_path / "over").mkdir()
    (tmp_path / "base" / "a.bin").write_bytes(DATA)
    (tmp_path / "over" / "b.bin").write_bytes(bytes(range(100, 200)))
    return tmp_path


@pytest.fixture
def vol(tmp_path):
    """A store root, a sibling whose name starts with the root's, a base root, and a
    directory that belongs to neither."""
    for name in ("store", "store2", "base", "outside"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "x.bin").write_bytes(DATA)
    (tmp_path / "outside" / "secret.bin").write_bytes(b"secret-bytes")
    return tmp_path


def test_reader_resolves_relative_files_and_external_origin(packs):
    reader = FuseRangeReader(str(packs / "over"), base_root=str(packs / "base"))
    got = reader.read(
        [
            Region("b.bin", 2, 3),
            Region("a.bin", 10, 4, "external"),
            Region(str(packs / "base" / "a.bin"), 0, 2),  # absolute, under base_root
        ]
    )
    assert got == [bytes([102, 103, 104]), bytes([10, 11, 12, 13]), bytes([0, 1])]
    # without a base_root an external file resolves under the root
    assert FuseRangeReader(str(packs / "base")).read(
        [Region("a.bin", 5, 2, "external")]
    ) == [bytes([5, 6])]


def test_reader_coalesces_adjacent_ranges_and_keeps_the_order(packs, monkeypatch):
    preads = []
    real = os.pread

    def counting(fd, n, offset):
        preads.append((offset, n))
        return real(fd, n, offset)

    monkeypatch.setattr(os, "pread", counting)
    reader = FuseRangeReader(str(packs / "base"))
    regions = [
        Region("a.bin", 30, 10),  # adjacent to the next one in file order
        Region("a.bin", 10, 20),
        Region("a.bin", 50, 5),  # a gap: its own read
        Region("a.bin", 35, 10),  # overlaps the first
        Region("a.bin", 7, 0),  # empty
    ]
    got = reader.read(regions)
    assert got == [
        bytes(range(30, 40)),
        bytes(range(10, 30)),
        bytes(range(50, 55)),
        bytes(range(35, 45)),
        b"",
    ]
    assert preads == [(10, 35), (50, 5)]


def test_reader_reports_a_missing_or_short_file_as_drift(packs):
    reader = FuseRangeReader(str(packs / "base"), attempts=1)
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("gone.bin", 0, 4)])
    assert "gone.bin" in str(ei.value) and "gbx_zarr_sync" in ei.value.fix
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("a.bin", 90, 20)])  # the file has 100 bytes
    assert "a.bin" in str(ei.value) and "Fix:" in str(ei.value)


@pytest.mark.parametrize("bad", [(-1, 4), (0, -4), (-8, -8)])
def test_a_negative_offset_or_length_is_drift(packs, bad):
    with pytest.raises(ZarrStoreDrift) as ei:
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", *bad)])
    assert "Fix:" in str(ei.value)


# --- containment ----------------------------------------------------------------


def _refused(reader, region):
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([region])
    return ei.value


def test_a_dotdot_escape_is_refused_and_one_that_stays_inside_is_not(vol):
    reader = FuseRangeReader(str(vol / "store"), attempts=1)
    err = _refused(reader, Region("../outside/secret.bin", 0, 4))
    assert "'../outside/secret.bin' resolves outside the store root" in str(err)
    assert str(vol / "store") in str(err)
    assert "gbx_zarr_sync" in err.fix and "base root" in err.fix
    assert reader.read([Region("sub/../x.bin", 0, 3)]) == [bytes([0, 1, 2])]


def test_an_absolute_path_is_allowed_only_under_a_root(vol):
    reader = FuseRangeReader(
        str(vol / "store"), base_root=str(vol / "base"), attempts=1
    )
    for ok in ("store", "base"):
        assert reader.read([Region(str(vol / ok / "x.bin"), 1, 2)]) == [bytes([1, 2])]
    err = _refused(reader, Region(str(vol / "outside" / "secret.bin"), 0, 4))
    assert "resolves outside the store root" in str(err)
    assert str(vol / "store") in str(err) and str(vol / "base") in str(err)
    # without a base_root only the root is allowed
    plain = FuseRangeReader(str(vol / "store"), attempts=1)
    _refused(plain, Region(str(vol / "base" / "x.bin"), 0, 4))


def test_a_sibling_whose_name_starts_with_the_root_is_outside_it(vol):
    reader = FuseRangeReader(str(vol / "store"), attempts=1)
    _refused(reader, Region(str(vol / "store2" / "x.bin"), 0, 4))
    _refused(reader, Region("../store2/x.bin", 0, 4))
    # the same holds for a base_root
    ext = FuseRangeReader(
        str(vol / "outside"), base_root=str(vol / "store"), attempts=1
    )
    _refused(ext, Region("../store2/x.bin", 0, 4, "external"))


def test_a_relative_file_is_confined_to_the_root_of_its_origin(vol):
    reader = FuseRangeReader(
        str(vol / "store"), base_root=str(vol / "base"), attempts=1
    )
    # under the root a '..' reaching the base root is still outside the root
    _refused(reader, Region("../base/x.bin", 0, 4))
    # an external file may not reach the overlay root either
    _refused(reader, Region("../store/x.bin", 0, 4, "external"))


def test_a_symlink_may_not_leave_the_root(vol):
    store = vol / "store"
    (store / "link_dir").symlink_to(vol / "outside", target_is_directory=True)
    (store / "link.bin").symlink_to(vol / "outside" / "secret.bin")
    (store / "inside.bin").symlink_to(store / "x.bin")
    reader = FuseRangeReader(str(store), attempts=1)
    _refused(reader, Region("link_dir/secret.bin", 0, 4))
    _refused(reader, Region("link.bin", 0, 4))
    _refused(reader, Region(str(store / "link.bin"), 0, 4))
    # a link that stays inside is fine
    assert reader.read([Region("inside.bin", 0, 2)]) == [bytes([0, 1])]


@pytest.mark.parametrize("file", ["", None, "a\0b", 7])
def test_an_unusable_file_value_is_drift(vol, file):
    err = _refused(FuseRangeReader(str(vol / "store")), Region(file, 0, 4))
    assert "Fix:" in str(err)


def test_the_real_path_of_a_file_is_looked_up_once(vol, monkeypatch):
    calls = []
    real = os.path.realpath
    monkeypatch.setattr(
        os.path, "realpath", lambda p, **k: calls.append(p) or real(p, **k)
    )
    reader = FuseRangeReader(str(vol / "store"))
    reader.read([Region("x.bin", 0, 4)])
    first = len(calls)
    assert first > 0
    for _ in range(3):
        reader.read([Region("x.bin", 4, 4), Region("x.bin", 20, 4)])
    assert len(calls) == first


# --- retries --------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        PermissionError(errno.EACCES, "denied"),
        IsADirectoryError(errno.EISDIR, "a directory"),
        NotADirectoryError(errno.ENOTDIR, "not a directory"),
    ],
)
def test_permanent_errors_are_not_retried(packs, monkeypatch, exc):
    opens, sleeps = [], []

    def refuse(*args, **kwargs):
        opens.append(1)
        raise exc

    monkeypatch.setattr(os, "open", refuse)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(type(exc)):
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", 0, 4)])
    assert opens == [1] and sleeps == []


def test_a_transient_error_is_retried(packs, monkeypatch):
    tries, sleeps = [], []
    real = os.open

    def flaky(*args, **kwargs):
        tries.append(1)
        if len(tries) < 3:
            raise OSError(errno.EIO, "Input/output error")
        return real(*args, **kwargs)

    monkeypatch.setattr(os, "open", flaky)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    got = FuseRangeReader(str(packs / "base")).read([Region("a.bin", 5, 2)])
    assert got == [bytes([5, 6])] and len(tries) == 3 and len(sleeps) == 2


# --- the light import -----------------------------------------------------------


def test_importing_and_using_the_reader_loads_no_raster_or_spark_stack():
    """The retry guard is lazy-free: a first read must not pull in the ds package
    (PySpark, rasterio, DataSource registration)."""
    code = (
        "import os, sys, tempfile\n"
        "from databricks.labs.gbx.zarr._access import FuseRangeReader\n"
        "from databricks.labs.gbx.zarr._shard import Region\n"
        "d = tempfile.mkdtemp()\n"
        "open(os.path.join(d, 'f'), 'wb').write(b'abcdef')\n"
        "assert FuseRangeReader(d).read([Region('f', 1, 3)]) == [b'bcd']\n"
        "bad = [m for m in ('rasterio', 'pyspark', 'databricks.labs.gbx.ds') "
        "if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    done = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
