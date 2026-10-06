"""Unit tests for the vendored siting policy helpers.

Pure functions only -- no SparkSession, no cluster, no geobrix JAR. The module
must import offline (spark/gbx imports are deferred into ``main``), so loading it
here exercises only the policy helpers the brief pins:
``quick_survivors`` / ``required_mast`` / ``rank_candidates`` (+ ``parse_args``).
"""

import importlib.util
import pathlib

import h3


def _load():
    p = pathlib.Path(__file__).parents[1] / "siting" / "siting.py"
    s = importlib.util.spec_from_file_location("_wc_siting", p)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


# --------------------------------------------------------------------------- #
# parse_args
# --------------------------------------------------------------------------- #


def test_parse_args():
    a = _load().parse_args(
        ["--catalog", "c", "--schema", "s", "--volume", "v", "--full-aoi", "false"]
    )
    assert a.catalog == "c" and a.schema == "s" and a.volume == "v"
    assert a.full_aoi == "false"


# --------------------------------------------------------------------------- #
# quick_survivors -- keep candidates whose coarse viewshed fraction >= thresh
# --------------------------------------------------------------------------- #


def test_quick_survivors_keeps_at_or_above_threshold():
    rows = [
        (-122.1, 37.1, 0.10),
        (-122.2, 37.2, 0.30),  # exactly at threshold -> kept (inclusive)
        (-122.3, 37.3, 0.55),
    ]
    kept = _load().quick_survivors(rows, 0.30)
    fracs = sorted(r[2] for r in kept)
    assert fracs == [0.30, 0.55]


def test_quick_survivors_all_below():
    rows = [(-1.0, 1.0, 0.05), (-2.0, 2.0, 0.1)]
    assert _load().quick_survivors(rows, 0.30) == []


# --------------------------------------------------------------------------- #
# required_mast -- bare-earth datum with nearest-ground ring fallback
# --------------------------------------------------------------------------- #


def test_required_mast_exact_hit():
    m = _load()
    cell = h3.latlng_to_cell(37.77, -122.47, 12)
    assert m.required_mast(cell, {cell: 12.5}, 3) == 12.5


def test_required_mast_nearest_ground_fallback():
    m = _load()
    cell = h3.latlng_to_cell(37.77, -122.47, 12)
    neighbor = next(c for c in h3.grid_disk(cell, 1) if c != cell)
    # cell itself has no ground; a ring-1 neighbor does -> fall back to it.
    assert m.required_mast(cell, {neighbor: 5.0}, 3) == 5.0


def test_required_mast_none_when_no_ground_within_k():
    m = _load()
    cell = h3.latlng_to_cell(37.77, -122.47, 12)
    far = h3.latlng_to_cell(40.0, -75.0, 12)  # many rings away
    assert m.required_mast(cell, {far: 5.0}, 3) is None
    assert m.required_mast(cell, {}, 3) is None


# --------------------------------------------------------------------------- #
# rank_candidates -- coverage desc, then mast asc (NULL mast last)
# --------------------------------------------------------------------------- #


def test_rank_candidates_coverage_then_mast():
    m = _load()
    rows = [
        {"cellid": 1, "viewshed_cells": 5, "required_mast": 1.0},
        {"cellid": 2, "viewshed_cells": 10, "required_mast": 20.0},
        {"cellid": 3, "viewshed_cells": 10, "required_mast": 10.0},
        {"cellid": 5, "viewshed_cells": 10, "required_mast": None},
        {"cellid": 4, "viewshed_cells": 8, "required_mast": None},
    ]
    order = [r["cellid"] for r in m.rank_candidates(rows)]
    # cells desc: the three 10s, then 8, then 5. Within the 10s: mast 10, 20,
    # then NULL last. The 8 (NULL mast) outranks the 5 on coverage.
    assert order == [3, 2, 5, 4, 1]
