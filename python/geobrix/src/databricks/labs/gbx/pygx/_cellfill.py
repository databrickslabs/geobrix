"""Shared fill logic for the light-tier gbx_<grid>_cellfill grouped aggregators.

Binary format (big-endian, matching Java DataOutputStream / heavy CellFillAcc.serialize
applied to the FILLED result):

    [count: Int32BE]
    for each entry:
        [cellid: Int64BE][present: Bool8][value: Float64BE if present]

Cell IDs are always stored as int64. For BNG the UDF converts string IDs to int
before calling :func:`fill`; parity decoders call :func:`decode` and re-render.
"""

from __future__ import annotations

import struct
from typing import Callable, Dict, Iterable, List, Optional, Tuple


def collect(
    pairs: Iterable[Tuple[int, Optional[float]]],
) -> Dict[int, Optional[float]]:
    """``{cell_id: value}`` from ``(cell_id, value)`` pairs, one value per distinct cell.

    A cell id that repeats keeps its LARGEST non-null value, the same rule the cell
    rasterizers use for a repeated cell; ``None`` (covered but missing) survives only
    when every value of the cell is ``None``. Arrival order inside a Spark group is not
    fixed, so "the last row wins" gave a different fill from run to run.
    """
    cells: Dict[int, Optional[float]] = {}
    for cell_id, value in pairs:
        if cell_id not in cells or (
            value is not None and (cells[cell_id] is None or value > cells[cell_id])
        ):
            cells[cell_id] = value
    return cells


def fill(
    cells: Dict[int, Optional[float]],
    k: int,
    method: str,
    power: float,
    k_loop_fn: Callable[[int, int], List[int]],
) -> List[Tuple[int, Optional[float]]]:
    """Fill NULL cells in *cells* from valid neighbours within k rings.

    Mirrors heavy ``CellFill.fill`` semantics exactly:

    - Valid (non-None) cells pass through unchanged.
    - A None cell is filled ring-by-ring for d=1..k via ``k_loop_fn(cell_id, d)``.
      Ring index d is the IDW distance weight.
    - ``mean``: unweighted mean of all gathered valid neighbour values.
    - ``idw``: Σ(v · d^-power) / Σ(d^-power) over gathered (value, d) pairs.
      At k=1, all neighbours have d=1 so idw degenerates to mean.
    - A None cell with no valid neighbour within k rings stays None.
    - k_loop rings are disjoint hollow rings — no double-counting.
    - Only in-group (present in *cells*) neighbours contribute.
    - A cell id that repeats keeps its LARGEST non-null value (see :func:`collect`),
      so the result does not depend on row-arrival order; callers build *cells*
      with :func:`collect`.

    Args:
        cells: ``{int_cell_id: Optional[float]}`` — None = covered-but-missing.
        k: neighbourhood radius (>= 0); k=0 fills nothing.
        method: ``'mean'`` or ``'idw'`` (case-insensitive).
        power: IDW distance exponent (``idw`` only; ignored for ``mean``).
        k_loop_fn: ``callable(cell_id, d) -> list[int]`` — hollow ring at
            exactly ring-distance d from cell_id.

    Returns:
        ``list[(cell_id, Optional[float])]`` in ascending unsigned int64 order.
    """
    if k < 0:
        raise ValueError(f"cellfill: k must be >= 0; got {k}")
    m = method.lower()
    if m not in ("mean", "idw"):
        raise ValueError(f"cellfill: method must be 'mean' or 'idw'; got '{method}'")

    # Ascending unsigned int64 order (mirrors Scala Long.compareUnsigned sort).
    ordered = sorted(cells.keys(), key=lambda x: x & 0xFFFF_FFFF_FFFF_FFFF)

    result: List[Tuple[int, Optional[float]]] = []
    for cell_id in ordered:
        v = cells[cell_id]
        if v is not None:
            result.append((cell_id, v))  # valid cell: pass through unchanged
        else:
            # Gather (value, ring_distance) from valid in-group neighbours.
            gathered: List[Tuple[float, int]] = []
            for d in range(1, k + 1):
                for nb in k_loop_fn(cell_id, d):
                    nb_v = cells.get(nb)
                    if nb_v is not None:
                        gathered.append((nb_v, d))

            if not gathered:
                result.append((cell_id, None))  # no valid neighbour → stays None
            elif m == "mean":
                result.append((cell_id, sum(vv for vv, _ in gathered) / len(gathered)))
            else:  # idw
                num = sum(vv * (d**-power) for vv, d in gathered)
                den = sum(d**-power for _, d in gathered)
                result.append((cell_id, num / den))

    return result


def encode(filled: List[Tuple[int, Optional[float]]]) -> bytes:
    """Encode a fill result as BINARY (matches CellFillAcc.serialize format).

    Format: ``[count: >i]([cellid: >q][present: >?][value: >d if present])*``
    Big-endian throughout; cell IDs as signed int64.
    """
    out = [struct.pack(">i", len(filled))]
    for cell_id, value in filled:
        if value is None:
            out.append(struct.pack(">q?", cell_id, False))
        else:
            out.append(struct.pack(">q?d", cell_id, True, value))
    return b"".join(out)


def decode(data: bytes) -> List[Tuple[int, Optional[float]]]:
    """Decode BINARY fill result back to ``(cellid, value)`` list.

    Inverse of :func:`encode` — used by tests to verify the fill result.
    """
    offset = 0
    (count,) = struct.unpack_from(">i", data, offset)
    offset += 4
    result: List[Tuple[int, Optional[float]]] = []
    for _ in range(count):
        (cell_id,) = struct.unpack_from(">q", data, offset)
        offset += 8
        (present,) = struct.unpack_from(">?", data, offset)
        offset += 1
        if present:
            (value,) = struct.unpack_from(">d", data, offset)
            offset += 8
        else:
            value = None
        result.append((cell_id, value))
    return result
