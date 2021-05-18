import pytest
import pandas as pd
from openghg.util import (
    date_overlap,
    create_daterange_str,
    closest_daterange,
    find_daterange_gaps,
    timestamp_tzaware,
    combine_dateranges,
    split_datrange_str,
)


def test_date_overlap():
    start_date_a = "2001-01-01"
    end_date_a = "2001-06-30"

    start_date_b = "2001-02-01"
    end_date_b = "2001-09-01"

    daterange_a = create_daterange_str(start=start_date_a, end=end_date_a)
    daterange_b = create_daterange_str(start=start_date_b, end=end_date_b)

    assert date_overlap(daterange_a=daterange_a, daterange_b=daterange_b) is True

    start_date_b = "2001-07-01"
    end_date_b = "2001-11-01"

    daterange_b = create_daterange_str(start=start_date_b, end=end_date_b)

    assert date_overlap(daterange_a=daterange_a, daterange_b=daterange_b) is False


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
        "2001-01-01-00:00:00+00:00_2011-12-31-00:00:00+00:00",
        "2013-01-02-00:00:00+00:00_2014-09-01-00:00:00+00:00",
        "2014-11-02-00:00:00+00:00_2014-12-31-00:00:00+00:00",
        "2015-11-02-00:00:00+00:00_2016-09-01-00:00:00+00:00",
        "2018-11-02-00:00:00+00:00_2019-01-01-00:00:00+00:00",
        "2021-01-02-00:00:00+00:00_2021-09-01-00:00:00+00:00",
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


def test_split_daterange_str():

    start_true = pd.Timestamp("2001-01-01-00:00:00", tz="UTC")
    end_true = pd.Timestamp("2001-03-01-00:00:00", tz="UTC")

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"

    start, end = split_datrange_str(daterange_str=daterange_1)

    assert start_true == start
    assert end_true == end
