"""LAZ point data with a dense run of 0xFF bytes is set aside before it is decoded.

The GPS-time decompressor of the native LAZ decoder (lazrs, the ``laz`` crate)
recurses without a depth limit, and 0xFF bytes keep it recursing: the stack
overflows, a SIGSEGV that no ``except`` can catch, and the whole Python worker
dies. Only point formats with GPS time (1, 3-10) are affected. The chunk table
check (see ``test_quarantine.py``) cannot see this: the damage is inside a chunk.

``lidar_gbx`` therefore scans the compressed points of the chunks a read needs
for 256-byte blocks that are at least half 0xFF, before any decoder exists, and
sets the file aside as unreadable. Every read that could crash runs in a CHILD
process here, so a regression fails the test (the child's signal) instead of
taking pytest down.
"""

import io
import json
import os
import struct
import subprocess
import sys

import numpy as np
import pytest

from databricks.labs.gbx.ds import lidar as L

VERDICT = (
    "compressed point data looks damaged (a run of 0xFF bytes); "
    "the decoder would crash on it"
)
_BLOCK = 256


# --- fixtures ------------------------------------------------------------------


def _make_laz(path, point_format=3, n=5000, version="1.2", extra_bytes=0):
    """A LAZ file of ``n`` points (50,000 per chunk). ``extra_bytes`` adds that
    many bytes to every point record (three-byte extra dimensions, all 255)."""
    import laspy

    header = laspy.LasHeader(point_format=point_format, version=version)
    header.offsets = np.array([500000.0, 4000000.0, 100.0])
    header.scales = np.array([0.01, 0.01, 0.01])
    names = [f"e{i}" for i in range(extra_bytes // 3)]
    for name in names:
        header.add_extra_dim(laspy.ExtraBytesParams(name=name, type="3u1"))
    las = laspy.LasData(header)
    rng = np.random.default_rng(3)
    las.x = rng.uniform(500000, 500100, n)
    las.y = rng.uniform(4000000, 4000100, n)
    las.z = rng.uniform(100, 110, n)
    for name in names:
        setattr(las, name, np.full((n, 3), 255, dtype=np.uint8))
    las.write(str(path))
    return str(path)


def _layout(path):
    """``(start of the point data, its LazVlr, [(points, bytes)] per chunk)``."""
    import laspy
    import lazrs

    with laspy.open(path) as r:
        vlr = lazrs.LazVlr(r.header.vlrs.get("LasZipVlr")[0].record_data)
        start = r.header.offset_to_point_data
    with open(path, "rb") as fh:
        fh.seek(start)
        return start, vlr, lazrs.read_chunk_table(fh, vlr)


def _overwrite(path, chunk, at, data):
    """Write ``data`` over the compressed points of chunk ``chunk``, ``at`` bytes
    after its first (uncompressed) point record."""
    start, vlr, entries = _layout(path)
    pos = start + 8 + sum(n for _, n in entries[:chunk]) + vlr.item_size() + at
    with open(path, "r+b") as fh:
        fh.seek(pos)
        fh.write(data)


def _body_of_first_chunk(path, body_fn):
    """The file with its first chunk's bytes replaced by ``body_fn(first point
    record, bytes of the chunk after it)`` and a chunk table to match."""
    import lazrs

    start, vlr, entries = _layout(path)
    item = vlr.item_size()
    with open(path, "rb") as fh:
        raw = fh.read()
    first = raw[start + 8 : start + 8 + item]
    rest = raw[start + 8 + item : start + 8 + entries[0][1]]
    body = first + body_fn(rest)
    table = io.BytesIO()
    lazrs.write_chunk_table(table, [(entries[0][0], len(body))] + entries[1:], vlr)
    out = (
        raw[:start] + struct.pack("<q", start + 8 + len(body)) + body + table.getvalue()
    )
    with open(path, "wb") as fh:
        fh.write(out)
    return path


def _minimal_repro(path, point_format=1, ff_bytes=1286, n=100):
    """The smallest file that kills the decoder: its first chunk is one
    uncompressed point record and ``ff_bytes`` bytes of 0xFF (1,663 bytes for
    format 1, 100 points, 1,286 bytes)."""
    _make_laz(path, point_format, n)
    return _body_of_first_chunk(path, lambda rest: b"\xff" * ff_bytes)


# --- the child -----------------------------------------------------------------

_CHILD = r"""
import json, sys, warnings
from databricks.labs.gbx.ds import _quarantine as Q, paths as P
from databricks.labs.gbx.ds.lidar import LidarGbxReader

path, scratch, plain = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
try:
    if plain:  # no bad-record hook: the plain reader
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            rows = sum(len(b) for b in LidarGbxReader({"path": path})._read_points(path))
        out = [rows, [str(w.message) for w in caught]]
    else:
        bad = Q.BadRecords(scratch)
        expected = P.path_schema("lidar_gbx", None, path)
        batches = P.iter_path_batches(path, 1_760_000_000_000_000, "lidar_gbx", None, expected, True, bad)
        rows = sum(len(b) for b in batches)
        bad.close()
        out = [rows, []]
except Exception as e:  # the decoder's own error: the process is alive
    out = [None, [f"{type(e).__name__}: {e}"]]
print(json.dumps(out))
"""


def _records(folder):
    import pyarrow.parquet as pq

    if not os.path.isdir(folder) or not os.listdir(folder):
        return []
    return pq.read_table(folder).to_pylist()


def _read_in_child(tmp_path, path, plain=False):
    """``(rows, warnings or bad records)`` of one lidar_gbx file read in a child
    process; fails, with the signal, if the child dies. ``rows`` is None when
    the read raised (the error is in the second item)."""
    import databricks.labs.gbx as gbx

    src = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(gbx.__file__)))
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            filter(None, [src, os.environ.get("PYTHONPATH")])
        ),
    }
    scratch = str(tmp_path / "bad_records")
    run = subprocess.run(
        [sys.executable, "-c", _CHILD, path, scratch, "1" if plain else "0"],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    assert run.returncode == 0, (
        f"the read killed its process (exit code {run.returncode}; a negative one "
        f"is a signal, -11 = SIGSEGV): {run.stderr[-500:]}"
    )
    rows, warned = json.loads(run.stdout.splitlines()[-1])
    return rows, (warned if plain else _records(scratch))


def _is_set_aside(rows, found, plain):
    """The read gave no row and said the verdict once: a warning (plain) or one
    unreadable-file bad record."""
    assert rows == 0
    if plain:
        assert len(found) == 1 and VERDICT in found[0]
        assert "skipping unreadable file" in found[0]
    else:
        assert len(found) == 1
        assert found[0]["record_index"] is None
        assert found[0]["error_class"] == "UnreadableFile"
        assert found[0]["error_message"] == VERDICT


def _verdict(path):
    """What the chunk table check says of a file: a str (why not) or a table."""
    import laspy

    with laspy.open(path) as r:
        return L._read_chunk_table(r.header, path, os.path.getsize(path))


# --- the crash: set aside, never a dead process ----------------------------------


@pytest.mark.parametrize("plain", [False, True], ids=["on_error", "plain"])
def test_the_minimal_crashing_file_is_set_aside_not_a_dead_process(tmp_path, plain):
    """1,663 bytes: the header, one point record, 1,286 bytes of 0xFF. It ends
    the decoder's process (SIGSEGV / SIGBUS) on every release of lazrs."""
    path = _minimal_repro(str(tmp_path / "min.laz"))
    assert os.path.getsize(path) == 1663
    _is_set_aside(*_read_in_child(tmp_path, path, plain), plain)


@pytest.mark.parametrize(
    "point_format,version", [(1, "1.2"), (3, "1.2"), (4, "1.3"), (5, "1.3")]
)
@pytest.mark.parametrize("plain", [False, True], ids=["on_error", "plain"])
def test_a_4_kib_run_of_ff_in_the_points_is_set_aside_not_a_dead_process(
    tmp_path, point_format, version, plain
):
    """4 KiB of 0xFF where the first chunk's compressed points start: the
    decoder's stack overflows (formats 4 and 5, LAS 1.3 with waveform packets,
    carry GPS time too and die the same way). The file is set aside, whole,
    with no row read."""
    path = _make_laz(tmp_path / "a.laz", point_format, version=version)
    _overwrite(path, 0, 0, b"\xff" * 4096)
    _is_set_aside(*_read_in_child(tmp_path, path, plain), plain)


def test_a_run_of_ff_in_a_later_chunk_is_found(tmp_path):
    """Every chunk a read needs is scanned: the damage is in the fourth of five."""
    path = _make_laz(tmp_path / "a.laz", 3, 210_000)
    assert len(_layout(path)[2]) >= 4
    _overwrite(path, 3, 20_000, b"\xff" * 4096)
    assert _verdict(path) == VERDICT


@pytest.mark.parametrize("point_format,version", [(6, "1.4"), (7, "1.4"), (8, "1.4")])
def test_the_layered_point_formats_with_gps_time_are_scanned(
    tmp_path, point_format, version
):
    """Formats 6-8 keep GPS time in a layer of their own; the scan covers them.

    This proves the scan sees the run, not that a crash is prevented: on a small
    file the same fill rarely lands in the GPS-time layer, so the decoder often
    survives it (the crash is shown for formats 1, 3, 4 and 5 above; the layered
    formats died in 4-5 of 18 reads of a 210,000-point file)."""
    path = _make_laz(tmp_path / "a.laz", point_format, 5000, version)
    _overwrite(path, 0, 2000, b"\xff" * 4096)
    assert _verdict(path) == VERDICT


# --- what is not the decoder's crash: read as before ------------------------------


@pytest.mark.parametrize("point_format", [0, 2])
@pytest.mark.parametrize("plain", [False, True], ids=["on_error", "plain"])
def test_formats_without_gps_time_are_not_scanned_and_read_as_before(
    tmp_path, point_format, plain
):
    """The same 4 KiB of 0xFF in a format with no GPS time cannot overflow the
    stack (it never crashed): not set aside by this check, and the read ends
    (rows, or the decoder's own error) in a live process, without the verdict."""
    path = _make_laz(tmp_path / "a.laz", point_format)
    _overwrite(path, 0, 0, b"\xff" * 4096)
    assert not isinstance(_verdict(path), str)
    rows, found = _read_in_child(tmp_path, path, plain)
    text = json.dumps(found, default=str)
    assert "0xFF" not in text and "would crash" not in text


@pytest.mark.parametrize(
    "point_format,version",
    [
        (0, "1.2"),
        (1, "1.2"),
        (2, "1.2"),
        (3, "1.2"),
        (4, "1.3"),
        (5, "1.3"),
        (6, "1.4"),
        (7, "1.4"),
        (8, "1.4"),
    ],
)
def test_a_valid_file_is_read_in_full(tmp_path, point_format, version):
    n = 120_000  # three chunks
    path = _make_laz(tmp_path / "a.laz", point_format, n, version)
    assert not isinstance(_verdict(path), str)
    rows, found = _read_in_child(tmp_path, path, plain=True)
    assert rows == n and found == []
    rows, found = _read_in_child(tmp_path, path, plain=False)
    assert rows == n and found == []


def test_a_first_point_of_ff_bytes_is_not_scanned(tmp_path):
    """The first record of every chunk is stored uncompressed and never reaches
    the decoder: extra bytes of 255 (a no-data fill) make 180 of its 214 bytes
    0xFF, and the file is sound."""
    path = _make_laz(tmp_path / "a.laz", 3, 5000, extra_bytes=180)
    start, vlr, _ = _layout(path)
    assert vlr.item_size() >= 214
    with open(path, "rb") as fh:
        fh.seek(start + 8)
        first = fh.read(vlr.item_size())
    assert first.count(b"\xff") >= 180  # 128 of a 256-byte block: it would be flagged
    assert not isinstance(_verdict(path), str)
    rows, found = _read_in_child(tmp_path, path, plain=True)
    assert rows == 5000 and found == []


# --- the threshold ---------------------------------------------------------------


def _verdict_with_block(tmp_path, ff_in_block, block=3):
    """Verdict of a file whose 256-byte block ``block`` (counted from the first
    byte of the point data) has ``ff_in_block`` bytes of 0xFF in a row at its
    start and random bytes in the rest of the point data."""
    path = _make_laz(tmp_path / f"b{ff_in_block}.laz", 3, 5000)
    start, vlr, entries = _layout(path)
    item = vlr.item_size()
    rng = np.random.default_rng(11)

    def body(rest):
        data = bytearray(rng.integers(0, 255, len(rest), dtype=np.uint8).tobytes())
        at = block * _BLOCK - item
        data[at : at + ff_in_block] = b"\xff" * ff_in_block
        return bytes(data)

    _body_of_first_chunk(path, body)
    return _verdict(path)


def test_a_block_at_least_half_ff_is_damaged(tmp_path):
    assert _verdict_with_block(tmp_path, 128) == VERDICT
    assert _verdict_with_block(tmp_path, 256) == VERDICT


def test_a_block_less_than_half_ff_is_not(tmp_path):
    """Random bytes hold about one 0xFF in a 256-byte block, a valid file's
    longest run is a few bytes; 127 of 256 is below the line."""
    assert not isinstance(_verdict_with_block(tmp_path, 127), str)
    assert not isinstance(_verdict_with_block(tmp_path, 0), str)


def test_the_scan_reads_in_pieces_and_still_finds_a_run_that_spans_them(
    tmp_path, monkeypatch
):
    """A scan buffer of 1 KiB: the run is found wherever it falls, and a file
    with no run is read in full without a verdict."""
    monkeypatch.setattr(L, "_FF_SCAN_BYTES", 4 * _BLOCK)
    path = _make_laz(tmp_path / "a.laz", 3, 5000)
    assert not isinstance(_verdict(path), str)
    _overwrite(path, 0, 3000, b"\xff" * 1500)
    assert _verdict(path) == VERDICT


# --- check_laz (the downloader, plot_static's point-cloud layer) -------------------


def test_check_laz_names_the_file_with_a_run_of_ff(tmp_path):
    path = _make_laz(tmp_path / "c.laz", 3, 120_000)
    with open(path, "rb") as fh:
        sound = fh.read()
    assert L.check_laz(path, "c.laz") is None and L.check_laz(sound, "c.laz") is None
    _overwrite(path, 1, 5000, b"\xff" * 4096)
    with open(path, "rb") as fh:
        damaged = fh.read()
    for source in (path, damaged):
        with pytest.raises(
            ValueError, match=r"^c\.laz: not read, compressed point data"
        ):
            L.check_laz(source, "c.laz")
    with pytest.raises(ValueError, match="a run of 0xFF bytes"):
        L.check_laz(path, "c.laz")
