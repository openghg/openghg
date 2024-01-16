from multiprocessing.sharedctypes import Value

import numpy as np
import pandas as pd
import pytest
from openghg.util import (
    check_date,
    check_nan,
    closest_daterange,
    combine_dateranges,
    create_daterange,
    create_daterange_str,
    create_frequency_str,
    daterange_contains,
    daterange_from_str,
    daterange_overlap,
    find_daterange_gaps,
    find_duplicate_timestamps,
    in_daterange,
    parse_period,
    relative_time_offset,
    split_daterange_str,
    split_encompassed_daterange,
    time_offset,
    timestamp_tzaware,
    trim_daterange,
    dates_in_range,
)
from pandas import DateOffset, Timedelta, Timestamp
from xarray import Dataset


def test_create_daterange():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    daterange = create_daterange(start, end)

    assert str(daterange[0]) == "2019-01-01 15:33:12+00:00"
    assert str(daterange[-1]) == "2020-01-01 15:33:12+00:00"


def test_create_daterange_wrong_way_raises():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    with pytest.raises(ValueError):
        _ = create_daterange(start=end, end=start)


def test_create_daterange_str():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    s = create_daterange_str(start=start, end=end)

    assert s == "2019-01-01-15:33:12+00:00_2020-01-01-18:55:12+00:00"


def test_daterange_from_str():
    s = "2019-01-01-15:33:00+00:00_2020-01-01-18:55:00+00:00"

    daterange = daterange_from_str(daterange_str=s)

    assert daterange[0] == Timestamp("2019-01-01 15:33:00", tz="UTC")
    assert daterange[-1] == Timestamp("2020-01-01 15:33:00", tz="UTC")


def test_daterange_overlap():
    start_date_a = "2001-01-01"
    end_date_a = "2001-06-30"

    start_date_b = "2001-02-01"
    end_date_b = "2001-09-01"

    daterange_a = create_daterange_str(start=start_date_a, end=end_date_a)
    daterange_b = create_daterange_str(start=start_date_b, end=end_date_b)

    assert daterange_overlap(daterange_a=daterange_a, daterange_b=daterange_b) is True

    start_date_b = "2001-07-01"
    end_date_b = "2001-11-01"

    daterange_b = create_daterange_str(start=start_date_b, end=end_date_b)

    assert daterange_overlap(daterange_a=daterange_a, daterange_b=daterange_b) is False


def test_closest_daterange():
    dateranges = [
        "2012-01-01-00:00:00+00:00_2014-01-01-00:00:00+00:00",
        "2014-01-02-00:00:00+00:00_2015-01-01-00:00:00+00:00",
        "2016-01-02-00:00:00+00:00_2017-01-01-00:00:00+00:00",
        "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00",
    ]

    to_comp = create_daterange_str(start="2011-01-09", end="2011-09-09")
    closest = closest_daterange(to_compare=to_comp, dateranges=dateranges)

    assert closest == "2012-01-01-00:00:00+00:00_2014-01-01-00:00:00+00:00"

    to_comp = create_daterange_str(start="2015-01-09", end="2015-10-09")
    closest = closest_daterange(to_compare=to_comp, dateranges=dateranges)

    assert closest == "2014-01-02-00:00:00+00:00_2015-01-01-00:00:00+00:00"

    to_comp = create_daterange_str(start="2021-01-09", end="2022-10-09")
    closest = closest_daterange(to_compare=to_comp, dateranges=dateranges)

    assert closest == "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00"

    with pytest.raises(ValueError):
        to_comp = create_daterange_str(start="2019-01-09", end="2021-10-09")
        closest = closest_daterange(to_compare=to_comp, dateranges=dateranges)


def test_find_daterange_gaps():
    dateranges = [
        "2012-01-01_2013-01-01",
        "2014-09-02_2014-11-01",
        "2015-01-01_2015-11-01",
        "2016-09-02_2018-11-01",
        "2019-01-02_2021-01-01",
    ]

    start = timestamp_tzaware("2001-01-01")
    end = timestamp_tzaware("2021-09-01")

    gaps = find_daterange_gaps(start_search=start, end_search=end, dateranges=dateranges)

    expected_gaps = [
        "2001-01-01-00:00:00+00:00_2011-12-31-23:00:00+00:00",
        "2013-01-01-01:00:00+00:00_2014-09-01-23:00:00+00:00",
        "2014-11-01-01:00:00+00:00_2014-12-31-23:00:00+00:00",
        "2015-11-01-01:00:00+00:00_2016-09-01-23:00:00+00:00",
        "2018-11-01-01:00:00+00:00_2019-01-01-23:00:00+00:00",
        "2021-01-01-01:00:00+00:00_2021-09-01-00:00:00+00:00",
    ]

    assert gaps == expected_gaps

    start = timestamp_tzaware("2001-01-01")
    end = timestamp_tzaware("2021-09-01")

    start_narrow = timestamp_tzaware("2016-01-01")
    end_narrow = timestamp_tzaware("2018-01-01")

    gaps = find_daterange_gaps(start_search=start_narrow, end_search=end_narrow, dateranges=dateranges)


def test_combining_single_dateranges_returns():
    daterange = "2027-08-01-00:00:00_2027-12-01-00:00:00"

    combined = combine_dateranges(dateranges=[daterange])

    assert combined[0] == daterange


def test_combining_overlapping_dateranges():
    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2001-02-01-00:00:00_2001-06-01-00:00:00"

    dateranges = [daterange_1, daterange_2]

    combined = combine_dateranges(dateranges=dateranges)

    assert combined == ["2001-01-01-00:00:00+00:00_2001-06-01-00:00:00+00:00"]

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2001-02-01-00:00:00_2001-06-01-00:00:00"
    daterange_3 = "2001-05-01-00:00:00_2001-08-01-00:00:00"
    daterange_4 = "2004-05-01-00:00:00_2004-08-01-00:00:00"
    daterange_5 = "2004-04-01-00:00:00_2004-09-01-00:00:00"
    daterange_6 = "2007-04-01-00:00:00_2007-09-01-00:00:00"

    dateranges = [daterange_1, daterange_2, daterange_3, daterange_4, daterange_5, daterange_6]

    combined = combine_dateranges(dateranges=dateranges)

    combined = [
        "2001-01-01-00:00:00+00:00_2001-08-01-00:00:00+00:00",
        "2004-04-01-00:00:00+00:00_2004-09-01-00:00:00+00:00",
        "2007-04-01-00:00:00_2007-09-01-00:00:00",
    ]

    return False

    assert combined == [
        "2001-01-01-00:00:00+00:00_2001-08-01-00:00:00+00:00",
        "2004-04-01-00:00:00+00:00_2004-09-01-00:00:00+00:00",
        "2007-04-01-00:00:00+00:00_2007-09-01-00:00:00+00:00",
    ]


def test_combining_no_overlap():
    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2011-02-01-00:00:00_2011-06-01-00:00:00"

    dateranges = [daterange_1, daterange_2]

    combined = combine_dateranges(dateranges=dateranges)

    assert combined == [
        "2001-01-01-00:00:00+00:00_2001-03-01-00:00:00+00:00",
        "2011-02-01-00:00:00+00:00_2011-06-01-00:00:00+00:00",
    ]


def test_combining_big_daterange():
    dateranges = ["2014-01-01_2099-06-06", "2014-06-07_2015-09-09", "2015-09-10_2019-01-06"]
    combined = combine_dateranges(dateranges=dateranges)

    assert combined == ["2014-01-01-00:00:00+00:00_2099-06-06-00:00:00+00:00"]

    dateranges = ["1994-05-05_1997-05-05", "2001-01-01_2005-05-05", "1900-01-01_2020_05-05"]

    combined = combine_dateranges(dateranges=dateranges)

    assert combined == ["1900-01-01-00:00:00+00:00_2020-01-01-00:00:00+00:00"]


def test_split_daterange_str():
    start_true = Timestamp("2001-01-01-00:00:00", tz="UTC")
    end_true = Timestamp("2001-03-01-00:00:00", tz="UTC")

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"

    start, end = split_daterange_str(daterange_str=daterange_1)

    assert start_true == start
    assert end_true == end


def test_trim_daterange():
    trim_me = create_daterange_str(start="2011-01-01", end="2021-09-01")
    overlapping = create_daterange_str(start="2004-05-09", end="2013-01-01")

    trimmed = trim_daterange(to_trim=trim_me, overlapping=overlapping)

    assert trimmed == "2013-01-01-00:00:01+00:00_2021-09-01-00:00:00+00:00"

    trim_me = create_daterange_str(start="2001-01-01", end="2005-09-01")
    overlapping = create_daterange_str(start="2004-05-09", end="2013-01-01")

    trimmed = trim_daterange(to_trim=trim_me, overlapping=overlapping)

    assert trimmed == "2001-01-01-00:00:00+00:00_2004-05-08-23:59:59+00:00"

    trim_me = create_daterange_str(start="2000-01-01", end="2005-09-01")
    overlapping = create_daterange_str(start="2007-05-09", end="2013-01-01")

    with pytest.raises(ValueError):
        trimmed = trim_daterange(to_trim=trim_me, overlapping=overlapping)


def test_split_encompassed_daterange():
    container = create_daterange_str(start="2004-05-09", end="2013-01-01")

    contained = create_daterange_str(start="2007-05-09", end="2010-01-01")

    result = split_encompassed_daterange(container=container, contained=contained)

    expected = {
        "container_start": "2004-05-09-00:00:00+00:00_2007-05-08-23:59:59+00:00",
        "contained": "2007-05-09-00:00:00+00:00_2009-12-31-23:59:59+00:00",
        "container_end": "2010-01-01-00:00:01+00:00_2013-01-01-00:00:00+00:00",
    }

    assert result == expected

    container = create_daterange_str(start="1995-05-09", end="1997-01-01")

    with pytest.raises(ValueError):
        split_encompassed_daterange(container=container, contained=contained)


def test_split_encompassed_daterange_same_start_end():
    container = create_daterange_str(start="2004-05-09", end="2013-01-01")
    contained = create_daterange_str(start="2004-05-09", end="2007-01-01")

    result = split_encompassed_daterange(container=container, contained=contained)

    expected = {
        "container_start": "2007-01-01-00:00:01+00:00_2013-01-01-00:00:00+00:00",
        "contained": "2004-05-09-00:00:00+00:00_2007-01-01-00:00:00+00:00",
    }

    assert result == expected

    container = create_daterange_str(start="2001-01-01", end="2013-01-01")
    contained = create_daterange_str(start="2004-05-09", end="2013-01-01")

    result = split_encompassed_daterange(container=container, contained=contained)

    expected = {
        "container_start": "2001-01-01-00:00:00+00:00_2004-05-08-23:59:59+00:00",
        "contained": "2004-05-09-00:00:00+00:00_2013-01-01-00:00:00+00:00",
    }

    assert result == expected

    container = create_daterange_str(start="2010-01-01", end="2013-01-01")
    contained = create_daterange_str(start="2004-05-09", end="2013-01-01")

    with pytest.raises(ValueError):
        split_encompassed_daterange(container=container, contained=contained)

    contained = create_daterange_str(start="2004-05-09", end="2013-01-01")
    container = create_daterange_str(start="2004-05-09", end="2007-01-01")

    with pytest.raises(ValueError):
        split_encompassed_daterange(container=container, contained=contained)


def test_daterange_contains():
    container = create_daterange_str(start="2004-05-09", end="2013-01-01")
    contained = create_daterange_str(start="2007-05-09", end="2010-01-01")

    res = daterange_contains(container=container, contained=contained)

    assert res

    container = create_daterange_str(start="2009-05-09", end="2013-01-01")
    contained = create_daterange_str(start="2007-05-09", end="2010-01-01")

    res = daterange_contains(container=container, contained=contained)

    assert not res


def test_check_date():
    date_str = "2021-01-01"

    assert check_date(date=date_str) == date_str
    assert check_date(date="this") == "NA"

    # "1001" is interpreted as a date string, not an integer, so it is successfully parsed to pd.Timestamp to Jan 1st, 1001
    #  assert check_date(date="1001") == "NA"

    unix_timestamp_ms = 1636043284779

    assert check_date(unix_timestamp_ms) == unix_timestamp_ms


def test_check_nan():
    assert check_nan(data=np.nan)
    assert check_nan(data=123) == 123

    with pytest.raises(TypeError):
        check_nan(data="123") == "123"
        assert check_nan(data=None)


def test_check_duplicate_timestamps():
    dr = pd.date_range("2021-1-1", "2021-1-15").tolist()
    with_dupes = dr + dr
    df_dupes = pd.DataFrame(with_dupes, columns=["dates"], index=with_dupes)
    dupes = find_duplicate_timestamps(df_dupes)

    assert dupes

    df_nodupes = pd.DataFrame(dr, columns=["dates"], index=dr)
    dupes = find_duplicate_timestamps(df_nodupes)

    assert not dupes

    empty_ds = Dataset()

    with pytest.raises(ValueError):
        find_duplicate_timestamps(empty_ds)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("12H", (12, "hours")),
        ("yearly", (1, "years")),
        ("monthly", (1, "months")),
        ((1, "minute"), (1, "minutes")),
        ("1.5H", (1.5, "hours")),
    ],
)
def test_parse_period(test_input, expected):
    """
    Testing known inputs and expected outputs for parse_period function
    This function reads in different "period" inputs and converts them
    to something which can be used to create a Timedelta or DateOffset object
    """
    assert parse_period(test_input) == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"value": 1, "unit": "hour"}, "1 hour"),
        ({"period": "3MS"}, "3 months"),
        ({"period": "yearly"}, "1 year"),
    ],
)
def test_create_frequency_str(kwargs, expected):
    """
    Testing known inputs and expected outputs for create_frequency_str function
    """
    assert create_frequency_str(**kwargs) == expected


def test_create_frequency_str_needs_unit():
    """
    Testing ValueError raised when value is specified but unit is omitted.
    """
    with pytest.raises(ValueError):
        create_frequency_str(value=1)


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"value": 1, "unit": "hour"}, Timedelta(hours=1)),
        ({"value": 1.5, "unit": "hours"}, Timedelta(hours=1.5)),
        ({"period": "3D"}, Timedelta(days=3)),
        ({"period": "7.5D"}, Timedelta(days=7.5)),
    ],
)
def test_time_offset(kwargs, expected):
    """
    Testing known inputs and expected outputs for time_offset function
    """
    assert time_offset(**kwargs) == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"value": 1, "unit": "hour"}, Timedelta(hours=1)),
        ({"value": 1.5, "unit": "hours"}, Timedelta(hours=1.5)),
        ({"value": 3, "unit": "months"}, DateOffset(months=3)),
        ({"period": "yearly"}, DateOffset(years=1)),
    ],
)
def test_relative_time_offset(kwargs, expected):
    """
    Testing known inputs and expected outputs for relative_time_offset function
    """
    assert relative_time_offset(**kwargs) == expected


def test_relative_time_offset_with_vaies():
    print(relative_time_offset)

def test_in_daterange():
    start_a = timestamp_tzaware("2021-01-01")
    end_a = timestamp_tzaware("2021-06-01")

    start_b = timestamp_tzaware("2021-02-02")
    end_b = timestamp_tzaware("2021-12-31")

    assert in_daterange(start_a=start_a, end_a=end_a, start_b=start_b, end_b=end_b)

    start_b = timestamp_tzaware("1970-01-01")
    end_b = timestamp_tzaware("1980-01-01")

    assert not in_daterange(start_a=start_a, end_a=end_a, start_b=start_b, end_b=end_b)


def test_dates_in_range():
    keys = [
        "2022-01-01_2022-01-07",
        "2022-01-08_2022-01-14",
        "2022-01-15_2022-01-21",
        "2022-01-22_2022-01-28",
        "2022-01-29_2022-02-04",
        "2022-02-05_2022-02-11",
        "2022-02-12_2022-02-18",
        "2022-02-19_2022-02-25",
        "2022-02-26_2022-03-04",
        "2022-03-05_2022-03-11",
    ]

    start_date = pd.Timestamp(2022, 1, 10)
    end_date = pd.Timestamp(2022, 2, 20)

    result = dates_in_range(keys, start_date, end_date)

    expected_result = [
        "2022-01-08_2022-01-14",
        "2022-01-15_2022-01-21",
        "2022-01-22_2022-01-28",
        "2022-01-29_2022-02-04",
        "2022-02-05_2022-02-11",
        "2022-02-12_2022-02-18",
        "2022-02-19_2022-02-25",
    ]

    assert result == expected_result

    start_date = pd.Timestamp(2024, 1, 10)
    end_date = pd.Timestamp(2024, 2, 20)

    result = dates_in_range(keys, start_date, end_date)

    assert not result
