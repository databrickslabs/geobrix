"""Generator for ``synthetic.gsb`` — a minimal, valid NTv2 grid-shift file.

This is a TEST FIXTURE generator, not a real datum grid. It produces a tiny
NTv2 ``.gsb`` that applies a CONSTANT horizontal shift of ``+30`` arc-seconds of
latitude (and ``0`` in longitude) over a small box around Great Britain. A
constant shift makes node ordering irrelevant — bilinear interpolation of
identical node values is that value — so any point inside the grid receives
exactly the constant shift. That is what lets a functional test assert an exact
shifted coordinate and thereby prove the grid was actually consulted by PROJ.

The committed ``synthetic.gsb`` is the output of ``build_gsb(...)`` with the
constants in ``__main__`` below. To regenerate::

    python3 gen_synthetic_gsb.py synthetic.gsb

NTv2 essentials used here (see the format spec):
  * Overview header: 11 records; subgrid header: 11 records; then GS_COUNT shift
    records of 4 float32 (lat_shift, lon_shift, lat_accuracy, lon_accuracy).
  * Each header record is an 8-byte ASCII tag + 8-byte value (int / double / char).
  * Units are seconds of arc; NTv2 longitude is POSITIVE-WEST.
"""
import struct
import sys


def _rec_int(name: bytes, val: int) -> bytes:
    # 8-byte ASCII tag + 4-byte int + 4-byte pad.
    return name.ljust(8)[:8] + struct.pack("<i", val) + b"\x00\x00\x00\x00"


def _rec_dbl(name: bytes, val: float) -> bytes:
    return name.ljust(8)[:8] + struct.pack("<d", val)


def _rec_str(name: bytes, val: bytes) -> bytes:
    return name.ljust(8)[:8] + val.ljust(8)[:8]


def build_gsb(
    lat_shift_sec: float,
    lon_shift_sec: float,
    s_lat: float,
    n_lat: float,
    e_long: float,
    w_long: float,
    lat_inc: float,
    long_inc: float,
) -> bytes:
    """Return the bytes of an NTv2 grid with a constant per-node shift.

    Bounds/increments are in seconds of arc; ``e_long``/``w_long`` are
    POSITIVE-WEST (so a geographic longitude of +1 deg EAST is ``-3600``).
    """
    ncols = int(round((w_long - e_long) / long_inc)) + 1
    nrows = int(round((n_lat - s_lat) / lat_inc)) + 1
    gs_count = ncols * nrows

    out = bytearray()
    # ---- Overview header (11 records) ----
    out += _rec_int(b"NUM_OREC", 11)
    out += _rec_int(b"NUM_SREC", 11)
    out += _rec_int(b"NUM_FILE", 1)
    out += _rec_str(b"GS_TYPE", b"SECONDS")
    out += _rec_str(b"VERSION", b"SYNTH1")
    out += _rec_str(b"SYSTEM_F", b"GBXSRC")
    out += _rec_str(b"SYSTEM_T", b"GBXDST")
    out += _rec_dbl(b"MAJOR_F", 6378137.0)      # GRS80 / WGS84 semi-major
    out += _rec_dbl(b"MINOR_F", 6356752.314)
    out += _rec_dbl(b"MAJOR_T", 6378137.0)
    out += _rec_dbl(b"MINOR_T", 6356752.314)
    # ---- Subgrid header (11 records) ----
    out += _rec_str(b"SUB_NAME", b"SYNTH")
    out += _rec_str(b"PARENT", b"NONE")
    out += _rec_str(b"CREATED", b"20260823")
    out += _rec_str(b"UPDATED", b"20260823")
    out += _rec_dbl(b"S_LAT", s_lat)
    out += _rec_dbl(b"N_LAT", n_lat)
    out += _rec_dbl(b"E_LONG", e_long)          # positive-west
    out += _rec_dbl(b"W_LONG", w_long)          # positive-west
    out += _rec_dbl(b"LAT_INC", lat_inc)
    out += _rec_dbl(b"LONG_INC", long_inc)
    out += _rec_int(b"GS_COUNT", gs_count)
    # ---- Shift records: constant over the whole grid ----
    for _ in range(gs_count):
        out += struct.pack("<ffff", lat_shift_sec, lon_shift_sec, 0.0, 0.0)
    # ---- End-of-file marker record ----
    out += _rec_str(b"END", b"\x00" * 8)
    return bytes(out)


# Constants of the committed synthetic.gsb: +30 arc-second latitude shift over a
# box spanning 50..53 N and 1 deg E .. 1 deg W, at 1-degree node spacing.
LAT_SHIFT_SEC = 30.0
LON_SHIFT_SEC = 0.0
PARAMS = dict(
    s_lat=50.0 * 3600,
    n_lat=53.0 * 3600,
    e_long=-1.0 * 3600,   # 1 deg EAST edge (positive-west negative)
    w_long=1.0 * 3600,    # 1 deg WEST edge
    lat_inc=1.0 * 3600,
    long_inc=1.0 * 3600,
)


def build_default() -> bytes:
    return build_gsb(LAT_SHIFT_SEC, LON_SHIFT_SEC, **PARAMS)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "synthetic.gsb"
    data = build_default()
    with open(path, "wb") as f:
        f.write(data)
    print(f"wrote {path} ({len(data)} bytes)")
