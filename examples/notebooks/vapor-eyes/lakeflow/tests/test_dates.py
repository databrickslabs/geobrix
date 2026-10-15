from datetime import date
from land._dates import parse_window, asof_window, observation_date_from_item


def test_parse_window_splits_range():
    assert parse_window("2023-07-15/2023-08-20") == (date(2023, 7, 15), date(2023, 8, 20))


def test_asof_window_single_day():
    assert asof_window("2024-08-24", days=1) == "2024-08-23/2024-08-24"


def test_observation_date_s5p():
    # S5P item id embeds the sensing date as ..._YYYYMMDDT...
    got = observation_date_from_item(
        "S5P_OFFL_L2__CH4____20240823T193456_20240823T211626_...", "s5p")
    assert got == date(2024, 8, 23)


def test_observation_date_none_when_absent():
    assert observation_date_from_item("no-date-here", "s5p") is None
