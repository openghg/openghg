import pytest
from openghg.util import date_overlap, create_daterange_str, closest_daterange


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
