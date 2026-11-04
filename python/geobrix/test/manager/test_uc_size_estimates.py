"""The estimates that bound a UC array BEFORE its kernel builds it (uc.handlers, uc.tier_b): never under the real count,
and a refusal is raised without the kernel having run. The over-cap rows are in the oracle tables.
"""

import pytest
import shapely

from databricks.labs.gbx.bench.oracles import UcGeometry
from databricks.labs.gbx.pygx import _bng, _custom, _dilate, _h3
from databricks.labs.gbx.uc import handlers as H
from databricks.labs.gbx.uc.handlers import _Refusal


def _never(*_a, **_k):
    raise AssertionError("the kernel ran before the size was judged")


def test_the_estimates_never_fall_under_the_cells_the_kernels_make():
    shapes = [
        shapely.box(530000, 180000, 532900, 180900),
        shapely.Polygon(
            [(530000, 180000), (534000, 180000), (534000, 184000), (530000, 184000)],
            [[(531000, 181000), (533000, 181000), (533000, 183000), (531000, 183000)]],
        ),
        shapely.LineString([(530000, 180000), (533000, 182500)]),
        shapely.Point(530500, 180500),
    ]
    for g in shapes:
        for res, k in ((3, 0), (3, 2), (4, 1)):
            made = 0
            for mode in _dilate.MODES:
                for cov in _dilate.COVERAGE:
                    for kernel in (_bng.geometry_k_ring_str, _bng.geometry_k_loop_str):
                        try:
                            made = max(made, len(kernel(g, res, k, mode, cov)))
                        except (
                            Exception
                        ):  # noqa: BLE001 - a mode the shape has no cells for
                            pass
            assert H._bng_cells_bound(g, res, k, interior=False) >= made
            assert H._bng_cells_bound(g, res, 0) >= len(_bng.polyfill_str(g, res))
    for g in [
        shapely.box(-3, 50, 0, 53),
        shapely.Polygon([(0, 0), (2, 0), (1, 2)]),
        shapely.LineString([(0, 0), (1, 1), (2, 0.5)]),
        shapely.Point(1, 1),
        shapely.MultiPolygon(
            [shapely.box(0, 0, 0.5, 0.5), shapely.box(5, 5, 5.2, 5.2)]
        ),
    ]:
        for res, k in ((4, 0), (5, 2)):
            made = 0
            for mode in _dilate.MODES:
                for cov in _dilate.COVERAGE:
                    try:
                        made = max(
                            made, len(_h3.geom_expand("ring", g, res, k, mode, cov))
                        )
                    except Exception:  # noqa: BLE001
                        pass
            assert H._h3_cells_bound(g, res, k) >= made


def test_a_large_geometry_at_a_fine_resolution_is_refused_before_the_kernel_runs(
    monkeypatch,
):
    monkeypatch.setattr(_bng, "polyfill_str", _never)
    monkeypatch.setattr(_bng, "geometry_k_ring_str", _never)
    monkeypatch.setattr(_h3, "geom_expand", _never)
    monkeypatch.setattr(_custom, "polyfill", _never)
    uk = UcGeometry(shapely.to_wkb(shapely.box(0, 0, 700000, 1300000)), 27700)
    with pytest.raises(
        _Refusal, match=r"gbx_bng_polyfill.*Fix: use a coarser resolution"
    ):
        H.bng_polyfill(uk, 6)
    with pytest.raises(_Refusal, match=r"gbx_bng_geomkring.*Fix:.*session function"):
        H.bng_geomkring(uk, 6, 1)
    world = UcGeometry(shapely.to_wkb(shapely.box(-100, 20, -50, 50)), 4326)
    with pytest.raises(_Refusal, match=r"gbx_h3_geomkring.*Fix:.*session function"):
        H.h3_geomkring(world, 12, 1)


def test_a_wide_geometry_with_a_short_outline_is_not_refused_for_its_area(monkeypatch):
    """A dilation makes the cells near the outline: a country-sized polygon at 100 m cells is a thin ring, not its area."""
    seen = []
    monkeypatch.setattr(
        _bng, "geometry_k_ring_str", lambda *a, **k: seen.append(a) or ["TQ3080"]
    )
    uk = UcGeometry(shapely.to_wkb(shapely.box(100000, 100000, 600000, 1200000)), 27700)
    assert H.bng_geomkring(uk, 4, 1) == ["TQ3080"] and seen
