import datetime
import os
import pytest
from pandas import Timestamp


from openghg import util


def test_date_overlap():
    a = "2014-01-30-10:52:30+00:00_2014-02-15-13:22:30+00:00"
    b = "2014-02-14-10:52:30+00:00_2014-08-30-13:22:30+00:00"

    result = util.date_overlap(daterange_a=a, daterange_b=b)

    assert result is True

    a = "2014-01-30-10:52:30+00:00_2014-02-15-13:22:30+00:00"
    b = "2024-02-28-10:52:30+00:00_2024-08-30-13:22:30+00:00"

    result = util.date_overlap(daterange_a=a, daterange_b=b)

    assert result is False


def test_create_aligned_timestamp():
    t = Timestamp("2001-01-01-15:33:33", tz="UTC")

    aligned = util.create_aligned_timestamp(t)

    assert aligned == Timestamp("2001-01-01 15:33:00", tz="UTC")

    t = datetime.datetime(2000, 1, 1, 1, 1, 1)

    aligned = util.create_aligned_timestamp(t)

    assert str(aligned.tzinfo) == "UTC"


def test_create_daterange():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    daterange = util.create_daterange(start, end)

    assert str(daterange[0]) == "2019-01-01 15:33:00+00:00"
    assert str(daterange[-1]) == "2020-01-01 18:55:00+00:00"


def test_create_daterange_wrong_way_raises():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    with pytest.raises(ValueError):
        _ = util.create_daterange(start=end, end=start)


def test_create_daterange_str():
    start = Timestamp("2019-1-1-15:33:12", tz="UTC")
    end = Timestamp("2020-1-1-18:55:12", tz="UTC")

    s = util.create_daterange_str(start=start, end=end)

    assert s == "2019-01-01-15:33:00+00:00_2020-01-01-18:55:00+00:00"


def test_daterange_from_str():
    s = "2019-01-01-15:33:00+00:00_2020-01-01-18:55:00+00:00"

    daterange = util.daterange_from_str(daterange_str=s)

    assert daterange[0] == Timestamp("2019-1-1-15:33:00", tz="UTC")
    assert daterange[-1] == Timestamp("2020-1-1-18:55:00", tz="UTC")


def test_read_header():
    filename = "header_test.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data"
    filepath = os.path.join(dir_path, test_data, filename)

    header = util.read_header(filepath=filepath)

    assert len(header) == 7


def test_valid_site():
    site = "BSD"
    result = util.valid_site(site=site)

    assert result is True

    site = "tac"
    result = util.valid_site(site=site)

    assert result is True

    site = "Dover"
    result = util.valid_site(site=site)

    assert result is False
    