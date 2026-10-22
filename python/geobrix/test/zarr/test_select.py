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
