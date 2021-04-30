from pandas import Timestamp

__all__ = [
    "timestamp_tzaware",
    "timestamp_now",
    "timestamp_epoch",
    "daterange_from_str",
    "daterange_to_str",
    "create_daterange_str",
    "create_daterange",
    "create_aligned_timestamp",
    "date_overlap",
]


def timestamp_tzaware(timestamp: Timestamp) -> Timestamp:
    """Returns the pandas Timestamp passed as a timezone (UTC) aware
    Timestamp.

    Args:
        timestamp (pandas.Timestamp): Timezone naive Timestamp
    Returns:
        pandas.Timestamp: Timezone aware
    """
    from pandas import Timestamp

    if not isinstance(timestamp, Timestamp):
        timestamp = Timestamp(timestamp)

    if timestamp.tzinfo is None:
        return timestamp.tz_localize(tz="UTC")
    else:
        return timestamp.tz_convert(tz="UTC")


def timestamp_now() -> Timestamp:
    """Returns a pandas timezone (UTC) aware Timestamp for the current time.

    Returns:
        pandas.Timestamp: Timestamp at current time
    """
    from pandas import Timestamp

    return timestamp_tzaware(Timestamp.now())


def timestamp_epoch():
    """Returns the UNIX epoch time
    1st of January 1970

    Returns:
        pandas.Timestamp: Timestamp object at epoch
    """
    from pandas import Timestamp

    return timestamp_tzaware(Timestamp("1970-1-1 00:00:00"))


def get_datetime(year, month, day, hour=None, minute=None, second=None):
    """Returns a timezone aware datetime object

    Args:
        year (int): Year
        month (int): Month of year
        day (int): Day of month
        hour (int, default=None): Hour of day
        minute (int, default=None): Minute of hour
        second (int, default=None): Second of minute
    Returns:
        datetime: Timezone aware datetime object
    """
    from datetime import datetime
    from Acquire.ObjectStore import datetime_to_datetime

    date = datetime(year=year, month=month, day=day)  # , hour=hour, minute=minute, second=second)

    return datetime_to_datetime(date)


def date_overlap(daterange_a, daterange_b):
    """Check if daterange_a is within daterange_b.

    For this logic see
    https://stackoverflow.com/a/325964

    Args:
        daterange_a (str): Timezone aware daterange string. Example:
        2014-01-30-10:52:30+00:00_2014-01-30-13:22:30+00:00
        daterange_b (str): As daterange_a
    Returns:
        bool: True if daterange included
    """
    from pandas import Timestamp

    daterange_a = daterange_a.split("_")
    daterange_b = daterange_b.split("_")

    start_a = Timestamp(ts_input=daterange_a[0], tz="UTC")
    end_a = Timestamp(ts_input=daterange_a[1], tz="UTC")

    start_b = Timestamp(ts_input=daterange_b[0], tz="UTC")
    end_b = Timestamp(ts_input=daterange_b[1], tz="UTC")

    return start_a <= end_b and end_a >= start_b


def create_aligned_timestamp(time):
    """Align the passed datetime / Timestamp object to the minute
    interval for use in dateranges and overlap checks.

    Args:
        time (str, pandas.Timestamp)
    Returns:
        pandas.Timestamp: Timestamp aligned to minute
        with UTC timezone
    """
    from pandas import Timedelta, Timestamp

    if not isinstance(time, Timestamp):
        time = Timestamp(ts_input=time)

    if time.tzinfo is None:
        t = time.tz_localize(tz="UTC")
    else:
        t = time.tz_convert(tz="UTC")

    t -= Timedelta(f"{t.second} s")

    return t


def create_daterange(start, end):
    """Create a minute aligned daterange

    Args:
        start (Timestamp)
        end (Timestamp)
    Returns:
        pandas.DatetimeIndex
    """
    from pandas import date_range

    if start > end:
        raise ValueError("Start date is after end date")

    start = create_aligned_timestamp(start)
    end = create_aligned_timestamp(end)

    return date_range(start=start, end=end, freq="min")


def create_daterange_str(start, end):
    """Convert the passed datetimes into a daterange string
    for use in searches and Datasource interactions

    Args:
        start_date (Timestamp)
        end_date (Timestamp)
    Returns:
        str: Daterange string
    """
    daterange = create_daterange(start=start, end=end)

    return daterange_to_str(daterange)


def daterange_from_str(daterange_str):
    """Get a Pandas DatetimeIndex from a string. The created
    DatetimeIndex has minute frequency.

    Args:
        daterange_str (str): Daterange string
        of the form 2019-01-01T00:00:00_2019-12-31T00:00:00
    Returns:
        pandas.DatetimeIndex: DatetimeIndex with minute frequency
    """
    from pandas import date_range

    split = daterange_str.split("_")

    # Align the seconds
    start = create_aligned_timestamp(split[0])
    end = create_aligned_timestamp(split[1])

    return date_range(start=start, end=end, freq="min")


def daterange_to_str(daterange):
    """Takes a pandas DatetimeIndex created by pandas date_range converts it to a
    string of the form 2019-01-01-00:00:00_2019-03-16-00:00:00

    Args:
        daterange (pandas.DatetimeIndex)
    Returns:
        str: Daterange in string format
    """
    start = str(daterange[0]).replace(" ", "-")
    end = str(daterange[-1]).replace(" ", "-")

    return "_".join([start, end])
