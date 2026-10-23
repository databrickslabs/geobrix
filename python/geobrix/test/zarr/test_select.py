import datetime as dt
import warnings

import numpy as np
import pytest

from databricks.labs.gbx.zarr._select import chunk_slices, resolve, touched_chunks

LAT = np.linspace(90, -90, 73)  # descending, ERA5 style
LON = np.arange(0, 360, 2.5)  # 0..357.5
TIME = np.arange(
    np.datetime64("2023-01-01T00"),
    np.datetime64("2023-01-03T00"),
    np.timedelta64(1, "h"),
).astype("datetime64[ns]")
DIMS = ("time", "latitude", "longitude")
SHAPE = (48, 73, 144)
COORDS = {"time": TIME, "latitude": LAT, "longitude": LON}


def test_unselected_is_full():
    ax = resolve(DIMS, SHAPE, COORDS, None, None)
    assert [a.ranges for a in ax] == [((0, 48),), ((0, 73),), ((0, 144),)]


def test_descending_lat_inclusive_slice():
    ax = resolve(DIMS, SHAPE, COORDS, {"latitude": slice(60, 50)}, None)
    lat = ax[1]
    assert lat.ranges == ((12, 17),) and lat.size == 5  # 60, 57.5, 55, 52.5, 50


def test_lon_wrap_two_ranges():
    ax = resolve(DIMS, SHAPE, COORDS, {"longitude": slice(-10, 10)}, None)
    assert ax[2].ranges == ((140, 144), (0, 5))  # 350..357.5 then 0..10


def test_time_scalar_iso_and_nearest():
    ax = resolve(DIMS, SHAPE, COORDS, {"time": "2023-01-01T05:00"}, None)
    assert ax[0].ranges == ((5, 6),) and ax[0].squeeze
    with pytest.raises(ValueError):
        resolve(DIMS, SHAPE, COORDS, {"time": "2024-01-01"}, None)


def test_isel_list_runs_and_bad_step():
    ax = resolve(DIMS, SHAPE, COORDS, None, {"time": [3, 1, 2, 10]})
    assert ax[0].ranges == ((1, 4), (10, 11))
    with pytest.raises(ValueError):
        resolve(DIMS, SHAPE, COORDS, None, {"time": slice(0, 10, 2)})


def test_empty_selection():
    ax = resolve(DIMS, SHAPE, COORDS, {"latitude": slice(95, 91)}, None)
    assert ax[1].ranges == () and touched_chunks(ax, (12, 37, 72)) == []


def test_chunks_and_slices_at_partial_edge():
    ax = resolve(DIMS, SHAPE, COORDS, {"latitude": slice(-80, -90)}, {"time": 0})
    ch = touched_chunks(ax, (12, 37, 72))
    assert ch == [(0, 1, 0), (0, 1, 1)]  # last lat chunk holds rows 37..72 (36 rows)
    pairs = chunk_slices(ax, (0, 1, 1), (12, 37, 72), SHAPE)
    ((inside, out),) = pairs
    # -80 is row 68, i.e. row 31 of the chunk that starts at row 37; -90 is row 72, the chunk's last
    assert inside[1] == slice(31, 36) and out[2] == slice(72, 144)


# ---- behaviour beyond the planned tests: output offsets, label types, error cases


def test_output_offsets_follow_label_order_across_wrapped_ranges():
    ax = resolve(
        DIMS, SHAPE, COORDS, {"longitude": slice(-10, 10)}, {"time": 0, "latitude": 0}
    )
    assert ax[2].size == 9
    # chunk 1 of 72-wide chunks holds columns 72..143: only 140..143 are selected, at output 0..3
    ((inside, out),) = chunk_slices(ax, (0, 0, 1), (12, 37, 72), SHAPE)
    assert inside[2] == slice(68, 72) and out[2] == slice(0, 4)
    # chunk 0 holds columns 0..4, which follow the first range in the output
    ((inside, out),) = chunk_slices(ax, (0, 0, 0), (12, 37, 72), SHAPE)
    assert inside[2] == slice(0, 5) and out[2] == slice(4, 9)


def test_chunk_slices_emits_one_box_per_range_in_a_chunk():
    ax = resolve(DIMS, SHAPE, COORDS, None, {"time": [1, 2, 5]})
    assert [
        (i[0], o[0]) for i, o in chunk_slices(ax, (0, 0, 0), (12, 37, 72), SHAPE)
    ] == [
        (slice(1, 3), slice(0, 2)),
        (slice(5, 6), slice(2, 3)),
    ]


def test_touched_chunks_for_wrapped_selection_is_sorted():
    ax = resolve(
        DIMS, SHAPE, COORDS, {"longitude": slice(-10, 10)}, {"time": 0, "latitude": 0}
    )
    assert touched_chunks(ax, (12, 37, 72)) == [(0, 0, 0), (0, 0, 1)]


def test_ascending_slice_open_ends_and_lon_bounds_without_wrap():
    assert resolve(DIMS, SHAPE, COORDS, {"longitude": slice(10, 20)}, None)[
        2
    ].ranges == ((4, 9),)
    assert resolve(DIMS, SHAPE, COORDS, {"longitude": slice(None, 5)}, None)[
        2
    ].ranges == ((0, 3),)
    assert resolve(DIMS, SHAPE, COORDS, {"longitude": slice(355, None)}, None)[
        2
    ].ranges == ((142, 144),)
    assert resolve(DIMS, SHAPE, COORDS, {"latitude": slice(None, 87)}, None)[
        1
    ].ranges == (
        (0, 2),
    )  # 90, 87.5: start..87 in axis order
    assert (
        resolve(DIMS, SHAPE, COORDS, {"latitude": slice(50, 60)}, None)[1].ranges == ()
    )  # against the axis order


def test_lon_negative_arcs_and_full_circle():
    def lon(s):
        return resolve(DIMS, SHAPE, COORDS, {"longitude": s}, None)[2]

    assert lon(slice(-120, -60)).ranges == ((96, 121),)  # 240..300, one range
    assert lon(slice(350, 10)).ranges == (
        (140, 144),
        (0, 5),
    )  # a > b with no negative bound
    assert lon(slice(-180, 180)).ranges == (
        (0, 144),
    )  # 360 degrees: whole axis, array order
    assert lon(slice(-75, -75)).ranges == ((114, 115),)  # 285


def test_lon_wrap_by_units_attribute_and_scalar_wrap():
    dims, shape, coords = ("lon",), (144,), {"lon": LON}
    assert resolve(dims, shape, coords, {"lon": -75.0}, None)[0].ranges == ((114, 115),)
    # a dimension that is not named like a longitude wraps only when its units say so
    dims, coords = ("col",), {"col": LON}
    assert resolve(dims, shape, coords, {"col": slice(-10, 10)}, None)[0].ranges == (
        (0, 5),
    )  # plain slice, no wrap
    ax = resolve(
        dims,
        shape,
        coords,
        {"col": slice(-10, 10)},
        None,
        coord_attrs={"col": {"units": "degrees_east"}},
    )
    assert ax[0].ranges == ((140, 144), (0, 5))


def test_no_wrap_for_a_regional_longitude_axis():
    lon = np.arange(100.0, 120.0, 1.0)
    assert resolve(("lon",), (20,), {"lon": lon}, {"lon": slice(-10, 105)}, None)[
        0
    ].ranges == ((0, 6),)


def test_time_slices_accept_iso_strings_and_datetime64():
    def t(s):
        return resolve(DIMS, SHAPE, COORDS, {"time": s}, None)[0].ranges

    assert t(slice("2023-01-01T02", "2023-01-01T04:00")) == ((2, 5),)
    assert t(slice(np.datetime64("2023-01-01T02"), np.datetime64("2023-01-01T04"))) == (
        (2, 5),
    )
    assert t(slice("2023-01-02", None)) == ((24, 48),)
    assert t(slice("2023-02-01", "2023-03-01")) == ()
    assert t(np.datetime64("2023-01-02T03")) == ((27, 28),)
    assert t("2023-01-01T05:20") == ((5, 6),)  # nearest, within half an hour


def test_nearest_scalar_and_list_labels():
    assert resolve(DIMS, SHAPE, COORDS, {"latitude": 59.0}, None)[1].ranges == (
        (12, 13),
    )
    ax = resolve(DIMS, SHAPE, COORDS, {"latitude": [50, 60, 57.5]}, None)[1]
    assert ax.ranges == ((12, 14), (16, 17)) and not ax.squeeze
    with pytest.raises(ValueError, match="not found in latitude"):
        resolve(DIMS, SHAPE, COORDS, {"latitude": 91.5}, None)


def test_float32_coordinates_match_python_float_bounds():
    lat = np.arange(0, 10, 0.1, dtype=np.float32)
    assert resolve(("lat",), (100,), {"lat": lat}, {"lat": slice(0.3, 0.7)}, None)[
        0
    ].ranges == ((3, 8),)
    assert resolve(("lat",), (100,), {"lat": lat}, {"lat": 0.3}, None)[0].ranges == (
        (3, 4),
    )


def test_isel_slice_negative_and_squeeze():
    ax = resolve(DIMS, SHAPE, COORDS, None, {"time": slice(-3, None), "latitude": -1})
    assert ax[0].ranges == ((45, 48),) and not ax[0].squeeze
    assert ax[1].ranges == ((72, 73),) and ax[1].squeeze and ax[1].size == 1


@pytest.mark.parametrize(
    "sel, isel",
    [
        ({"depth": 1}, None),  # unknown dimension
        ({"time": "2023-01-01"}, {"time": 0}),  # both ways
        ({"latitude": slice(60, 50, 2)}, None),  # label slice with a step
        (None, {"time": 99}),  # index out of range
        (None, {"time": [1.5]}),  # non-integer list
        ({"time": 3}, None),  # number for a time axis
        ({"latitude": "north"}, None),  # string for a numeric axis
    ],
)
def test_bad_selections_raise_value_error(sel, isel):
    with pytest.raises(ValueError, match="Fix:|not found"):
        resolve(DIMS, SHAPE, COORDS, sel, isel)


def test_non_monotonic_coordinate_rejects_slices_but_not_scalars():
    coords = {"k": np.array([3.0, 1.0, 2.0])}
    with pytest.raises(ValueError, match="not monotonic"):
        resolve(("k",), (3,), coords, {"k": slice(1, 2)}, None)
    assert resolve(("k",), (3,), coords, {"k": 2.0}, None)[0].ranges == ((2, 3),)


def test_missing_coordinate_points_at_isel():
    with pytest.raises(ValueError, match="isel"):
        resolve(DIMS, SHAPE, {"time": TIME}, {"latitude": 10}, None)


def test_scalar_array_has_one_empty_key_chunk():
    assert touched_chunks([], ()) == [()]
    assert chunk_slices([], (), (), ()) == [((), ())]


# ---- fix round 1: periodic longitudes, time bounds and date-string periods

LON_PM = np.arange(-180, 180, 2.5)  # -180..177.5
LON_INC = np.arange(
    0, 360.1, 2.5
)  # 0..360 inclusive: 145 labels, the last repeats the first


def lon_ranges(coord, s, dim="longitude", attrs=None):
    ax = resolve(
        (dim,),
        (len(coord),),
        {dim: coord},
        {dim: s},
        None,
        coord_attrs={dim: attrs} if attrs else None,
    )
    return ax[0].ranges


def time_ranges(s, coord=TIME):
    return resolve(("time",), (len(coord),), {"time": coord}, {"time": s}, None)[
        0
    ].ranges


def test_periodic_axis_stored_minus180_to_180_in_both_directions():
    assert lon_ranges(LON_PM, slice(170, -170)) == ((140, 144), (0, 5))
    assert lon_ranges(LON_PM, slice(-10, 10)) == ((68, 77),)  # plain, no seam
    assert lon_ranges(LON_PM, slice(350, 10)) == ((68, 77),)  # a 0..360 request
    assert lon_ranges(LON_PM, slice(190, 200)) == ((4, 9),)  # -170..-160
    assert lon_ranges(LON_PM, slice(-180, 180)) == ((0, 144),)
    # the same request on a 0..360 axis crosses its seam instead
    assert lon_ranges(LON, slice(-170, 170)) == ((76, 144), (0, 69))
    assert lon_ranges(LON, slice(-180, 180)) == ((0, 144),)


def test_periodic_scalars_wrap_and_match_by_circular_distance():
    def r(c, v):
        return resolve(
            ("longitude",), (len(c),), {"longitude": c}, {"longitude": v}, None
        )[0].ranges

    assert r(LON_PM, 285) == ((42, 43),)  # 285 is -75
    assert r(LON, -75.0) == ((114, 115),)
    assert r(LON, 359.5) == (
        (0, 1),
    )  # half a degree from 0, though 357.5 is the nearest label by value
    assert r(LON_INC, 360.0) == (
        (144, 145),
    )  # an exact label of an inclusive axis stays put


def test_inclusive_zero_to_360_axis_never_selects_the_repeat_twice():
    assert LON_INC.size == 145
    r = lon_ranges(LON_INC, slice(-10, 10))
    assert r == (
        (140, 144),
        (0, 5),
    )  # 350..357.5 then 0..10; index 144 (360) is left out
    covered = [i for s, e in r for i in range(s, e)]
    assert len(covered) == len(set(covered))
    assert lon_ranges(LON_INC, slice(355, 365)) == ((142, 144), (0, 3))
    assert lon_ranges(LON_INC, slice(0, 360)) == (
        (0, 145),
    )  # both ends asked for by name


def test_regional_and_projected_axes_never_wrap():
    lon = np.arange(100.0, 251.0, 1.0)  # 100..250
    assert lon_ranges(lon, slice(200, 150), "lon") == ()
    assert lon_ranges(lon, slice(-10, 10), "lon") == ()
    assert lon_ranges(lon, slice(-10, 120), "lon") == ((0, 21),)
    cells = np.arange(300.0)  # "x" counted in cells: span + step is 300
    assert lon_ranges(cells, slice(10, 0), "x") == ()
    assert lon_ranges(cells, slice(-10, 10), "x") == ((0, 11),)
    metres = np.arange(0, 500_000, 1000.0)  # "x" in metres: far more than 360 wide
    assert lon_ranges(metres, slice(-10, 5000), "x") == ((0, 6),)
    assert lon_ranges(metres, slice(5000, 0), "x") == ()
    almost = np.arange(0.0, 351.0, 1.0)  # 0..350: span + step is 351, not a full circle
    assert lon_ranges(almost, slice(340, 10), "lon") == ()
    # a descending axis is never periodic: the slice follows the axis order
    assert lon_ranges(np.arange(360.0)[::-1], slice(-10, 10), "lon") == ()


def test_longitude_like_by_attributes_and_not_by_name():
    for attrs in (
        {"units": "degrees_east"},
        {"standard_name": "longitude"},
        {"axis": "X"},
    ):
        assert lon_ranges(LON, slice(-10, 10), "col", attrs) == ((140, 144), (0, 5))
    assert lon_ranges(LON, slice(-10, 10), "col", {"units": "m"}) == ((0, 5),)
    # an attribute marks the axis longitude-like but a regional one still never wraps
    cells = np.arange(300.0)
    assert lon_ranges(cells, slice(10, 0), "col", {"axis": "X"}) == ()


def test_time_bounds_beyond_the_dtype_range_clamp_to_the_axis():
    assert time_ranges(slice("2023-01-01T20", "3000-01-01")) == ((20, 48),)
    assert time_ranges(slice("1000-01-01", "2023-01-01T02")) == ((0, 3),)
    assert time_ranges(
        slice(np.datetime64("1000-01-01"), np.datetime64("3000-01-01"))
    ) == ((0, 48),)
    assert time_ranges(slice("3000-01-01", None)) == ()
    assert time_ranges(slice(None, "1000-01-01")) == ()
    with pytest.raises(ValueError, match="not found in time.*Fix:"):
        time_ranges("3000-01-01")  # a scalar cannot be clamped
    # an axis in seconds reaches years a nanosecond axis cannot
    far = np.array(["2500-01-01", "2500-01-02", "2500-01-03"], dtype="datetime64[s]")
    assert time_ranges(slice("2500-01-02", "2500-01-02"), far) == ((1, 2),)
    assert time_ranges(slice("2023-01-01", "9000-01-01"), far) == ((0, 3),)
    assert time_ranges("2500-01-03", far) == ((2, 3),)


def test_date_strings_are_periods_in_slices():
    assert time_ranges(slice("2023-01-01", "2023-01-02")) == (
        (0, 48),
    )  # both whole days
    assert time_ranges(slice("2023-01-02", "2023-01-02")) == ((24, 48),)
    assert time_ranges(slice("2023-01", "2023-01")) == ((0, 48),)
    assert time_ranges(slice("2023", "2023")) == ((0, 48),)
    assert time_ranges(slice("2022", "2022")) == ()
    assert time_ranges(slice("2024", None)) == ()
    assert time_ranges(slice("2023-01-01T05", "2023-01-01T05")) == ((5, 6),)
    assert time_ranges(slice("2023-01-01T05:30", "2023-01-01T07:10")) == (
        (6, 8),
    )  # minute periods
    # months and years on a daily axis
    days = np.arange("2022-12-01", "2023-03-01", dtype="datetime64[D]").astype(
        "datetime64[ns]"
    )
    assert time_ranges(slice("2023-01", "2023-01"), days) == ((31, 62),)
    assert time_ranges(slice("2023", "2023"), days) == ((31, 90),)
    assert time_ranges(slice("2022", "2022"), days) == ((0, 31),)
    assert time_ranges(slice("2022-12", "2023-01"), days) == ((0, 62),)


def test_datetime_objects_stay_exact_instants_and_scalar_strings_pick_labels():
    assert time_ranges(
        slice(np.datetime64("2023-01-01"), np.datetime64("2023-01-02"))
    ) == ((0, 25),)
    assert time_ranges(slice(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2))) == (
        (0, 25),
    )
    assert time_ranges(slice(dt.date(2023, 1, 1), dt.date(2023, 1, 2))) == ((0, 25),)
    assert time_ranges("2023-01-02") == ((24, 25),)  # the label at the start of the day
    assert time_ranges(["2023-01-01", "2023-01-02T03"]) == ((0, 1), (27, 28))


def test_one_nanosecond_axis_compares_exactly():
    t = np.datetime64("2023-01-01T00:00:00", "ns") + np.arange(10).astype(
        "timedelta64[ns]"
    )

    def sl(coord, s):
        return resolve(("t",), (10,), {"t": coord}, {"t": s}, None)[0].ranges

    assert sl(t, slice(t[3], t[5])) == ((3, 6),)
    assert sl(t[::-1], slice(t[::-1][3], t[::-1][5])) == ((3, 6),)
    assert sl(t, t[4]) == ((4, 5),)
    assert sl(t, slice(t[9], None)) == ((9, 10),)


def test_slice_tolerance_never_double_counts_a_label():
    r = lon_ranges(LON, slice(10.0000001, 10.0))
    idx = [i for s, e in r for i in range(s, e)]
    assert len(idx) == len(set(idx)) and sorted(idx) == list(range(144))
    assert r == ((4, 144), (0, 4))  # almost a full turn, starting at 10
    r = lon_ranges(LON, slice(-10, 349.9999999))
    assert sum(e - s for s, e in r) == 144


def _assemble(axes, data, chunks):
    """Rebuild the selection from chunk_slices the way a reader will."""
    out = np.full(tuple(a.size for a in axes), -1, dtype=data.dtype)
    for cidx in touched_chunks(axes, chunks):
        extent = tuple(
            slice(c * k, min((c + 1) * k, n))
            for c, k, n in zip(cidx, chunks, data.shape)
        )
        block = data[extent]
        for inside, outside in chunk_slices(axes, cidx, chunks, data.shape):
            out[outside] = block[inside]
    return out


@pytest.mark.parametrize(
    "sel, isel, picks",
    [
        (  # seam-crossing longitude, descending latitude, several ranges across time chunks
            {"longitude": slice(-10, 10), "latitude": slice(60, 50)},
            {"time": [1, 2, 3, 10, 11, 30, 47]},
            {
                "time": [1, 2, 3, 10, 11, 30, 47],
                "latitude": range(12, 17),
                "longitude": [*range(140, 144), *range(0, 5)],
            },
        ),
        (  # squeezed time, latitude ending on the partial last chunk, whole longitude
            {"latitude": slice(-80, -90)},
            {"time": 5},
            {"time": [5], "latitude": range(68, 73), "longitude": range(144)},
        ),
        (  # label lists and a time window; the seam falls between two 72-wide chunks
            {
                "latitude": [50, 60, 57.5],
                "time": slice("2023-01-01T20", "2023-01-02T03"),
                "longitude": slice(170, 190),
            },
            None,
            {
                "time": range(20, 28),
                "latitude": [12, 13, 16],
                "longitude": range(68, 77),
            },
        ),
    ],
)
def test_assembled_chunks_equal_direct_numpy_selection(sel, isel, picks):
    data = np.arange(np.prod(SHAPE)).reshape(SHAPE)
    chunks = (12, 37, 72)
    axes = resolve(DIMS, SHAPE, COORDS, sel, isel)
    expected = data[np.ix_(*[np.array(list(picks[d])) for d in DIMS])]
    got = _assemble(axes, data, chunks)
    assert got.shape == expected.shape
    assert (got == expected).all()


def test_utc_offsets_parse_without_numpy_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert time_ranges("2023-01-01T05:00+05:00") == ((0, 1),)  # 00:00 UTC
        assert time_ranges("2023-01-01T00:00-05:00") == ((5, 6),)  # 05:00 UTC
        assert time_ranges("2023-01-01T05:00Z") == ((5, 6),)
        assert time_ranges("2023-01-01 05:00:00+0000") == ((5, 6),)
        assert time_ranges(
            slice("2023-01-01T05:00+05:00", "2023-01-01T07:30+02:00")
        ) == ((0, 6),)
        aware = dt.datetime(2023, 1, 1, 5, tzinfo=dt.timezone(dt.timedelta(hours=5)))
        assert time_ranges(slice(aware, aware)) == ((0, 1),)


def test_list_selection_prepares_the_axis_once(monkeypatch):
    from databricks.labs.gbx.zarr import _select

    calls = []
    real = _select._step

    def counting(num):
        calls.append(1)
        return real(num)

    monkeypatch.setattr(_select, "_step", counting)
    ax = resolve(DIMS, SHAPE, COORDS, {"latitude": [50, 60, 57.5, 55, 52.5]}, None)[1]
    assert ax.ranges == ((12, 17),) and len(calls) == 1


def test_isel_on_an_empty_dimension_says_so():
    with pytest.raises(ValueError, match="empty") as e:
        resolve(("t",), (0,), {}, None, {"t": 0})
    assert "[-0" not in str(e.value) and "Fix:" in str(e.value)


# ---- fix round 2: projected origins, float32 seams, CF unit spellings, non-finite bounds, offsets


def test_projected_x_with_a_large_origin_never_wraps():
    metres = 500000 + np.arange(
        360.0
    )  # about 360 units wide, but nowhere near longitudes
    assert lon_ranges(metres, slice(499990, 500010), "x") == ((0, 11),)
    assert lon_ranges(metres, slice(500340, 500400), "x") == ((340, 360),)
    assert lon_ranges(metres, slice(500010, 499990), "x") == ()
    tens = 1000 + np.arange(36) * 10.0
    assert lon_ranges(tens, slice(1340, 1400), "x") == ((34, 36),)
    assert lon_ranges(tens, slice(1340, 1000), "x") == ()


def test_periodic_origin_must_lie_within_one_turn_of_zero():
    wraps = lon_ranges(360 + np.arange(360.0), slice(350, 370), "lon")
    assert wraps == ((350, 360), (0, 11))  # origin 360: the boundary is included
    assert lon_ranges(-360 + np.arange(360.0), slice(-10, 10), "lon") == (
        (350, 360),
        (0, 11),
    )
    # origin 361 is out of range: a plain slice, 361..370 inclusive
    assert lon_ranges(361 + np.arange(360.0), slice(350, 370), "lon") == ((0, 10),)
    assert lon_ranges(-361 + np.arange(360.0), slice(-10, 10), "lon") == ((351, 360),)


def test_float32_axis_keeps_its_edge_labels_after_wrapping():
    lon64 = np.arange(3600) * 0.1
    lon32 = lon64.astype(np.float32)
    for lon in (lon64, lon32):
        assert lon_ranges(lon, slice(-10.1, 10)) == (
            (3499, 3600),
            (0, 101),
        )  # 349.9 .. 10.0
        assert lon_ranges(lon, slice(-10.1, -9.9)) == (
            (3499, 3502),
        )  # 349.9, 350.0, 350.1
        assert lon_ranges(lon, slice(349.9, 10.1)) == ((3499, 3600), (0, 102))


def test_float32_wrap_rounds_after_the_arithmetic_not_before():
    # float32(-293.2) is off by 1e-5, which would survive the +360 and drop the label at 66.8
    lon64 = np.arange(3600) * 0.1
    lon32 = lon64.astype(np.float32)
    for lon in (lon64, lon32):
        assert lon_ranges(lon, slice(45.8, -293.2)) == (
            (458, 669),
        )  # 45.8 .. 66.8, one turn short
        assert lon_ranges(lon, slice(305.3, -276.7)) == ((3053, 3600), (0, 834))
        assert lon_ranges(lon, slice(389.7, 366.4)) == ((297, 3600), (0, 65))


@pytest.mark.parametrize(
    "units",
    ["degrees_east", "degree_east", "degrees_E", "degree_E", "degreeE", "degreesE"],
)
def test_cf_longitude_unit_spellings(units):
    assert lon_ranges(LON, slice(-10, 10), "col", {"units": units}) == (
        (140, 144),
        (0, 5),
    )


def test_other_units_are_not_longitudes():
    for units in ("degrees_north", "degree_N", "m", ""):
        assert lon_ranges(LON, slice(-10, 10), "col", {"units": units}) == ((0, 5),)


@pytest.mark.parametrize(
    "bound", [float("nan"), float("inf"), float("-inf"), np.float32("nan")]
)
def test_non_finite_slice_bounds_are_rejected(bound):
    for coord, dim in ((LON, "longitude"), (LAT, "latitude"), (np.arange(10), "k")):
        for s in (slice(10, bound), slice(bound, 10), slice(None, bound)):
            with pytest.raises(ValueError, match=r"finite.*Fix:"):
                lon_ranges(coord, s, dim)


def test_non_finite_scalar_labels_are_rejected():
    for v in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match=r"finite.*Fix:"):
            resolve(DIMS, SHAPE, COORDS, {"latitude": v}, None)
        with pytest.raises(ValueError, match=r"finite.*Fix:"):
            resolve(DIMS, SHAPE, COORDS, {"latitude": [50, v]}, None)


def test_finite_bound_beyond_the_float_dtype_clamps_quietly():
    lon32 = LON.astype(np.float32)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert lon_ranges(lon32, slice(0, 1e300), "col") == ((0, 144),)
        assert lon_ranges(lon32, slice(-1e300, 10), "col") == ((0, 5),)
        assert lon_ranges(lon32, slice(-1e300, 10)) == (
            (0, 144),
        )  # periodic: 360 degrees or more


@pytest.mark.parametrize("off", ["+25:00", "-25:00", "+24:00", "+05:75", "+99"])
def test_utc_offset_out_of_range_cannot_be_parsed(off):
    text = "2023-01-01T05:00" + off
    for sel in (text, slice(text, None)):
        with pytest.raises(ValueError, match="cannot parse") as e:
            time_ranges(sel)
        assert "not found" not in str(e.value) and "Fix:" in str(e.value)


def test_utc_offset_just_inside_the_range_still_parses():
    assert time_ranges(slice("2023-01-01T05:00+23:59", None)) == ((0, 48),)
    assert time_ranges(slice("2023-01-01T05:00-23:59", None)) == ((29, 48),)


# ---- fix round 3: seams on pixel-centred float32 axes, out-of-range bounds, float noise

PIXEL64 = np.arange(-1799.5, 1800, 1.0) / 10  # -179.95 .. 179.95, step 0.1
PIXEL32 = PIXEL64.astype(np.float32)
EAST64 = np.arange(0.5, 3600, 1.0) / 10  # 0.05 .. 359.95, step 0.1
EAST32 = EAST64.astype(np.float32)
FINE64 = np.arange(7200) * 0.05 + 0.025  # 0.025 .. 359.975, step 0.05
FINE32 = FINE64.astype(np.float32)


def test_arcs_ending_on_the_first_label_keep_it_on_pixel_centred_float32_axes():
    for lon in (PIXEL64, PIXEL32):
        assert lon_ranges(lon, slice(179.95, -179.95)) == ((3599, 3600), (0, 1))
        assert lon_ranges(lon, slice(170.05, -179.95)) == ((3500, 3600), (0, 1))
    for lon in (EAST64, EAST32):
        assert lon_ranges(lon, slice(359.95, 0.05)) == ((3599, 3600), (0, 1))
        assert lon_ranges(lon, slice(-0.05, 0.05)) == ((3599, 3600), (0, 1))
    for lon in (FINE64, FINE32):
        assert lon_ranges(lon, slice(359.975, 0.025)) == ((7199, 7200), (0, 1))
        assert lon_ranges(lon, slice(-0.025, 0.075)) == ((7199, 7200), (0, 2))


def test_bounds_beyond_the_dtype_range_are_past_every_label():
    f32 = np.array(
        [0, 1e38, 3.4028235e38], dtype=np.float32
    )  # the last label is the dtype maximum
    f16 = np.array([0, 1, 65504], dtype=np.float16)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert lon_ranges(f32, slice(1e300, None), "k") == ()
        assert lon_ranges(f32, slice(5e38, None), "k") == ()
        assert lon_ranges(f16, slice(70000, None), "k") == ()
        assert lon_ranges(f32, slice(3.4028235e38, None), "k") == (
            (2, 3),
        )  # the maximum, spelt in decimal
        assert lon_ranges(f16, slice(65504, None), "k") == ((2, 3),)
        assert lon_ranges(f32, slice(None, 1e300), "k") == (
            (0, 3),
        )  # an upper bound past the end is open
        assert lon_ranges(f32, slice(-1e300, None), "k") == ((0, 3),)
        assert lon_ranges(f32, slice(None, -1e300), "k") == ()
        desc = f32[::-1]
        assert lon_ranges(desc, slice(1e300, None), "k") == ((0, 3),)
        assert lon_ranges(desc, slice(None, 1e300), "k") == ()
        assert lon_ranges(desc, slice(-1e300, None), "k") == ()
        with pytest.raises(ValueError, match="not found"):
            lon_ranges(f32, 1e300, "k")  # a scalar past the maximum is not the maximum


def test_non_finite_messages_fit_the_kind_of_selection():
    with pytest.raises(ValueError, match="Fix:") as e:
        resolve(DIMS, SHAPE, COORDS, {"latitude": float("nan")}, None)
    assert "slice end" not in str(e.value) and "isel" in str(e.value)
    with pytest.raises(ValueError, match="Fix:") as e:
        lon_ranges(LON, slice(10, float("nan")))
    assert "slice end" in str(e.value)


def test_float_noise_cannot_turn_a_zero_width_arc_into_a_near_full_turn():
    lon64 = np.arange(-1800, 1800) / 10  # -180 .. 179.9
    for lon in (lon64, lon64.astype(np.float32)):
        assert lon_ranges(lon, slice(-218.7, -578.7)) == (
            (3213, 3214),
        )  # both are 141.3
        assert lon_ranges(lon, slice(794.2, 74.2)) == ((2542, 2543),)  # both are 74.2
        assert lon_ranges(lon, slice(-10.1, 349.9)) == (
            (0, 3600),
        )  # exactly one turn wide
    assert lon_ranges(LON, slice(10.0000001, 10.0)) == (
        (4, 144),
        (0, 4),
    )  # a real near-full turn stays


def test_slice_ending_on_the_seam_is_the_same_on_float32_and_float64():
    for lon64, lon32 in ((PIXEL64, PIXEL32), (EAST64, EAST32), (FINE64, FINE32)):
        r64 = lon_ranges(lon64, slice(float(lon64[-1]), float(lon64[0]) + 360))
        r32 = lon_ranges(lon32, slice(float(lon64[-1]), float(lon64[0]) + 360))
        assert r64 == r32 == ((len(lon64) - 1, len(lon64)),)


def test_slice_ending_on_origin_plus_360_is_plain_whatever_the_dtype():
    # origin -92.15: float32 rounds it, so the decimal origin + 360 is not exactly the axis's own
    lon64 = (-92150 + np.arange(36000) * 10) / 1000
    for lon in (lon64, lon64.astype(np.float32)):
        assert lon_ranges(lon, slice(267.84, 267.85)) == ((35999, 36000),)
        assert lon_ranges(lon, slice(-92.14, 267.85)) == ((1, 36000),)


# ---- Task 9 carries (Ruling Z19): seam snap capped by the step, periodic reach, huge ints


def test_seam_snap_is_capped_by_the_label_step_on_a_float16_axis():
    # float16 rounds so coarsely that the uncapped snap (1.4 degrees) was wider than the 1 degree step
    c16, c64 = np.arange(360).astype(np.float16), np.arange(360.0)
    for s in (
        slice(-1, 3),
        slice(359, 3),
        slice(-1, -0.5),
        slice(357, 1),
        slice(-2, 3),
        slice(358.5, 2),
    ):
        assert lon_ranges(c16, s) == lon_ranges(c64, s), s


def test_seam_snap_is_capped_by_the_label_step_on_a_fine_float32_axis():
    # 1e-4 degree labels: the float32 snap (1.7e-4) is wider than the step
    c64 = np.arange(0, 360, 1e-4)
    c32 = c64.astype(np.float32)
    n = len(c64)
    assert lon_ranges(c32, slice(-1e-4, 2e-4)) == ((n - 1, n), (0, 3))
    assert lon_ranges(c32, slice(359.9999, 1e-4)) == ((n - 1, n), (0, 2))
    assert lon_ranges(c32, slice(359.9998, 2e-4)) == ((n - 2, n), (0, 3))


def test_the_snap_is_still_the_float_noise_allowance_on_ordinary_axes():
    # the cap only bites when the step is tiny: a 0.1 degree float32 axis keeps its full snap
    assert 4 * float(np.finfo(np.float32).eps) * 360 < 0.25 * 0.1
    assert lon_ranges(PIXEL32, slice(179.95, -179.95)) == ((3599, 3600), (0, 1))


@pytest.mark.parametrize("coord", [LON, LON.astype(np.float32)], ids=["f64", "f32"])
def test_periodic_slice_bounds_far_from_the_axis_are_refused(coord):
    for s in (slice(1e15, 10), slice(1e300, 1e300), slice(5e38, 10), slice(10, -1e7)):
        with pytest.raises(ValueError, match="too far from longitude") as e:
            lon_ranges(coord, s)
        assert "360 degree circle" in str(e.value) and "None" in str(e.value)
        assert "Fix:" in str(e.value)


def test_periodic_scalars_far_from_the_axis_are_refused():
    for coord, values in (
        (LON, (1e300, 5e38, 2e6, -2e6)),
        (LON.astype(np.float32), (1e300, 5e38, 2e6, -2e6)),
        (LON.astype(np.float16), (1e300, 7e4, 2e6)),  # 7e4 overflows float16 to inf
    ):
        for v in values:
            with pytest.raises(ValueError, match="too far from longitude") as e:
                lon_ranges(coord, v)
            assert "Fix:" in str(e.value) and "slice end" not in str(e.value)
    # a scalar a few turns away still wraps
    assert lon_ranges(LON, 722.5) == lon_ranges(LON, 2.5) == ((1, 2),)


def test_periodic_whole_axis_shortcut_and_open_ends_survive_the_reach_limit():
    for coord in (LON, LON.astype(np.float32)):
        assert lon_ranges(coord, slice(-1e300, 10)) == ((0, 144),)
        assert lon_ranges(coord, slice(10, 1e300)) == ((0, 144),)
        assert lon_ranges(coord, slice(None, 1e300)) == ((0, 144),)
        assert lon_ranges(coord, slice(-1e300, None)) == ((0, 144),)


def test_non_periodic_axes_keep_the_clamp_semantics_for_huge_values():
    lon32 = LON.astype(np.float32)
    assert lon_ranges(lon32, slice(0, 1e300), "col") == ((0, 144),)
    assert lon_ranges(lon32, slice(5e38, None), "col") == ()
    with pytest.raises(ValueError, match="not found"):
        lon_ranges(lon32, 5e38, "col")


def test_python_ints_beyond_int64_are_not_clamped_before_the_float_cast():
    c = np.array([0.0, 1e308, 1.7976931348623157e308])
    big = 10**400
    assert lon_ranges(c, slice(big, None), "k") == ()  # past every label
    assert lon_ranges(c, slice(None, big), "k") == ((0, 3),)  # an open end
    assert lon_ranges(c, slice(None, -big), "k") == ()
    assert lon_ranges(c, slice(-big, None), "k") == ((0, 3),)
    with pytest.raises(ValueError, match="not found"):
        lon_ranges(c, big, "k")
    # integer axes still compare as integers
    ci = np.arange(5)
    assert lon_ranges(ci, slice(10**30, None), "k") == ()
    assert lon_ranges(ci, slice(None, 10**30), "k") == ((0, 5),)
    assert lon_ranges(ci, slice(4, 2**63), "k") == ((4, 5),)


def test_huge_ints_on_a_periodic_axis_follow_the_reach_rule():
    big = 10**400
    assert lon_ranges(LON, slice(-big, 10)) == ((0, 144),)
    assert lon_ranges(LON, slice(10, big)) == ((0, 144),)
    with pytest.raises(ValueError, match="too far from longitude"):
        lon_ranges(LON, slice(big, 10))
    with pytest.raises(ValueError, match="too far from longitude"):
        lon_ranges(LON, big)
