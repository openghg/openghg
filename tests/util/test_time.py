from openghg.util import date_overlap, create_daterange_str


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

