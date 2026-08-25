import pandas as pd
import pytest
from shapely import box, from_wkb, get_srid, segmentize, set_srid, to_wkb

from databricks.labs.gbx.pyvx import _coverage as cov


def _wkb(g, srid=0):
    return to_wkb(set_srid(g, srid) if srid else g, include_srid=bool(srid))


# --- fixtures -------------------------------------------------------------
A = [box(0, 0, 1, 1), box(1, 0, 2, 1)]              # valid: share edge x=1
OVERLAP = [box(0, 0, 1, 1), box(0.9, 0, 1.9, 1)]    # invalid: overlap in x∈[0.9,1]
GAP = [box(0, 0, 1, 1), box(1.05, 0, 2.05, 1)]      # 0.05-wide gap between them


def test_coverage_is_valid_true_on_shared_edge():
    assert cov.coverage_is_valid_agg([_wkb(g) for g in A], 0.0) is True


def test_coverage_is_valid_false_on_overlap():
    assert cov.coverage_is_valid_agg([_wkb(g) for g in OVERLAP], 0.0) is False


def test_coverage_is_valid_gap_width_semantics():
    # gap_width=0 → gaps allowed (only overlaps invalid) → the gap pair is "valid"
    assert cov.coverage_is_valid_agg([_wkb(g) for g in GAP], 0.0) is True
    # gap_width wider than the 0.05 gap → the gap is flagged → invalid
    assert cov.coverage_is_valid_agg([_wkb(g) for g in GAP], 0.5) is False


def test_coverage_is_valid_empty_group_is_none():
    assert cov.coverage_is_valid_agg([], 0.0) is None
    assert cov.coverage_is_valid_agg([None], 0.0) is None


def test_coverage_is_valid_accepts_wkt_and_ewkb():
    wkt = [g.wkt for g in A]
    assert cov.coverage_is_valid_agg(wkt, 0.0) is True
    ewkb = [_wkb(g, 4326) for g in A]
    assert cov.coverage_is_valid_agg(ewkb, 0.0) is True


def test_invalid_edges_empty_geom_when_valid():
    out = cov.coverage_invalid_edges_agg([_wkb(g) for g in A], 0.0)
    assert out is not None
    assert from_wkb(out).is_empty


def test_invalid_edges_nonempty_on_overlap():
    out = cov.coverage_invalid_edges_agg([_wkb(g) for g in OVERLAP], 0.0)
    assert out is not None
    assert not from_wkb(out).is_empty


def test_invalid_edges_none_on_empty_group():
    assert cov.coverage_invalid_edges_agg([], 0.0) is None


def test_invalid_edges_preserves_srid():
    out = cov.coverage_invalid_edges_agg([_wkb(g, 4326) for g in OVERLAP], 0.0)
    assert get_srid(from_wkb(out)) == 4326


def test_invalid_edges_accepts_wkt_and_ewkb():
    wkt = [g.wkt for g in A]
    out = cov.coverage_invalid_edges_agg(wkt, 0.0)
    assert out is not None
    assert from_wkb(out).is_empty
    ewkb = [_wkb(g, 4326) for g in A]
    out2 = cov.coverage_invalid_edges_agg(ewkb, 0.0)
    assert out2 is not None
    assert from_wkb(out2).is_empty


def test_is_valid_agg_raises_on_malformed():
    with pytest.raises(ValueError):
        cov.coverage_is_valid_agg([_wkb(A[0]), b"not-wkb"], 0.0)


def test_invalid_edges_agg_raises_on_malformed():
    with pytest.raises(ValueError):
        cov.coverage_invalid_edges_agg([_wkb(A[0]), b"not-wkb"], 0.0)


def test_parse_group_drops_none_not_raises():
    # None alongside valid polygons: None is dropped, valid result returned (not raised).
    assert cov.coverage_is_valid_agg([None, _wkb(A[0]), _wkb(A[1])], 0.0) is True


def test_coverage_simplify_pdf_n_to_n_and_preserves_columns():
    pdf = pd.DataFrame(
        {"cov_id": ["c", "c"], "name": ["p0", "p1"], "geom": [_wkb(g) for g in A]}
    )
    out = cov.coverage_simplify_pdf(pdf, "geom", 0.0, True, "geom_simplified")
    assert len(out) == 2                                   # N→N
    assert list(out["name"]) == ["p0", "p1"]               # columns + order preserved
    assert "geom_simplified" in out.columns
    assert all(from_wkb(b).is_valid for b in out["geom_simplified"])


def test_coverage_simplify_pdf_preserves_shared_edge():
    # Densify each square with extra collinear midpoints (segmentize at 0.05 adds ~20
    # vertices per unit edge), then simplify at tolerance=0.1 which removes those
    # collinear extras. Assert (a) vertex count drops vs the densified input, and
    # (b) topology is preserved — the two neighbours still share a boundary.
    dense = [segmentize(g, 0.05) for g in A]
    n_before = sum(len(g.exterior.coords) for g in dense)
    pdf = pd.DataFrame({"cov_id": ["c", "c"], "geom": [_wkb(g) for g in dense]})
    out = cov.coverage_simplify_pdf(pdf, "geom", 0.1, True, "geom_simplified")
    g0, g1 = [from_wkb(b) for b in out["geom_simplified"]]
    n_after = len(g0.exterior.coords) + len(g1.exterior.coords)
    assert n_after < n_before                               # collinear vertices removed
    assert g0.touches(g1) or g0.intersection(g1).length > 0  # shared boundary intact


def test_coverage_simplify_pdf_raises_on_unparseable_row():
    pdf = pd.DataFrame({"cov_id": ["c", "c"], "geom": [_wkb(A[0]), b"not-wkb"]})
    with pytest.raises(ValueError):
        cov.coverage_simplify_pdf(pdf, "geom", 0.0, True, "geom_simplified")
