from pandas import Timestamp, DatetimeIndex
from typing import Dict, List, Tuple, Optional, Union

__all__ = [
    "timestamp_tzaware",
    "timestamp_now",
    "timestamp_epoch",
    "daterange_from_str",
    "daterange_to_str",
    "create_daterange_str",
    "create_daterange",
    "create_aligned_timestamp",
    "daterange_overlap",
    "combine_dateranges",
    "split_daterange_str",
    "closest_daterange",
    "valid_daterange",
    "find_daterange_gaps",
    "trim_daterange",
    "split_encompassed_daterange",
    "daterange_contains",
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


def timestamp_epoch() -> Timestamp:
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

    date = datetime(year=year, month=month, day=day)

    return datetime_to_datetime(date)


def daterange_overlap(daterange_a, daterange_b):
    """Check if daterange_a is within daterange_b.

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

    # For this logic see
    # https://stackoverflow.com/a/325964
    return start_a <= end_b and end_a >= start_b


def create_aligned_timestamp(time: Union[str, Timestamp]) -> Timestamp:
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


def create_daterange(start: Timestamp, end: Timestamp, freq: Optional[str] = "D") -> DatetimeIndex:
    """Create a minute aligned daterange

    Args:
        start: Start date
        end: End date
    Returns:
        pandas.DatetimeIndex
    """
    from pandas import date_range

    if start > end:
        raise ValueError("Start date is after end date")

    start = create_aligned_timestamp(start)
    end = create_aligned_timestamp(end)

    return date_range(start=start, end=end, freq=freq)


def create_daterange_str(start: Union[str, Timestamp], end: Union[str, Timestamp]) -> str:
    """Convert the passed datetimes into a daterange string
    for use in searches and Datasource interactions

    Args:
        start_date: Start date
        end_date: End date
    Returns:
        str: Daterange string
    """
    start = create_aligned_timestamp(start)
    end = create_aligned_timestamp(end)

    start = str(start).replace(" ", "-")
    end = str(end).replace(" ", "-")

    return "_".join((start, end))


def daterange_from_str(daterange_str: str, freq: Optional[str] = "D") -> DatetimeIndex:
    """Get a Pandas DatetimeIndex from a string. The created
    DatetimeIndex has minute frequency.

    Args:
        daterange_str (str): Daterange string
        of the form 2019-01-01T00:00:00_2019-12-31T00:00:00
    Returns:
        pandas.DatetimeIndex: DatetimeIndex covering daterange
    """
    from pandas import date_range

    split = daterange_str.split("_")

    # Align the seconds
    start = create_aligned_timestamp(split[0])
    end = create_aligned_timestamp(split[1])

    return date_range(start=start, end=end, freq=freq)


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


def combine_dateranges_old(dateranges: List[str]) -> List:
    """Checks a list of daterange strings for overlapping and combines
    those that do.

    Note : this function expects daterange strings in the form
    2019-01-01T00:00:00_2019-12-31T00:00:00

    Args:
        dateranges: List of strings
    Returns:
        list: List of dateranges with overlapping ranges combined
    """
    from collections import defaultdict
    from openghg.util import daterange_from_str, daterange_to_str, pairwise

    # Ensure there are no duplciates
    dateranges = list(set(dateranges))

    # We can't combine a single daterange
    if len(dateranges) < 2:
        return dateranges

    dateranges.sort()

    daterange_objects = [daterange_from_str(x) for x in dateranges]

    # We want lists of dateranges to combine
    groups = defaultdict(list)
    # Each group contains a number of dateranges that overlap
    group_n = 0
    # Do a pairwise comparison
    # "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    for a, b in pairwise(daterange_objects):
        if len(a.intersection(b)) > 0:
            groups[group_n].append(a)
            groups[group_n].append(b)
        else:
            # If the first pair don't match we want to keep both but
            # have them in separate groups
            if group_n == 0:
                groups[group_n].append(a)
                group_n += 1
                groups[group_n].append(b)
                continue

            # Otherwise increment the group number and just keep the second of the pair
            # The first of the pair was a previous second so will have been saved in the
            # last iteration
            group_n += 1
            groups[group_n].append(b)

    # Now we need to combine each group into a single daterange
    combined_dateranges = []
    for group_number, daterange_list in groups.items():
        combined = daterange_list[0].union_many(daterange_list[1:])
        combined_dateranges.append(combined)

    # Conver the dateranges backt to strings for storing
    combined_dateranges = [daterange_to_str(x) for x in combined_dateranges]

    return combined_dateranges

def combine_dateranges(dateranges: List[str]) -> List:
    """Checks a list of daterange strings for overlapping and combines
    those that do.

    Note : this function expects daterange strings in the form
    2019-01-01T00:00:00_2019-12-31T00:00:00

    Args:
        dateranges: List of strings
    Returns:
        list: List of dateranges with overlapping ranges combined
    """
    from collections import defaultdict
    from openghg.util import daterange_from_str, daterange_to_str, pairwise, daterange_overlap

    # Ensure there are no duplciates
    dateranges = list(set(dateranges))

    # We can't combine a single daterange
    if len(dateranges) < 2:
        return dateranges

    dateranges.sort()

    def group_dateranges(ranges):
        # We want lists of dateranges to combine
        groups = defaultdict(list)
        # Each group contains a number of dateranges that overlap
        group_n = 0
        # Do a pairwise comparison
        # "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        for a, b in pairwise(dateranges):
            if daterange_overlap(a, b) > 0:
                groups[group_n].append(a)
                groups[group_n].append(b)
            else:
                # If the first pair don't match we want to keep both but
                # have them in separate groups
                if group_n == 0:
                    groups[group_n].append(a)
                    group_n += 1
                    groups[group_n].append(b)
                    continue

                # Otherwise increment the group number and just keep the second of the pair
                # The first of the pair was a previous second so will have been saved in the
                # last iteration
                group_n += 1
                groups[group_n].append(b)

        return groups

    print(groups)

    return

    # Now we need to combine each group into a single daterange
    combined_dateranges = []
    for group_number, daterange_list in groups.items():
        combined = daterange_list[0].union_many(daterange_list[1:])
        combined_dateranges.append(combined)

    # Conver the dateranges backt to strings for storing
    combined_dateranges = [daterange_to_str(x) for x in combined_dateranges]

    return combined_dateranges


def split_daterange_str(daterange_str: str) -> Tuple[Timestamp, Timestamp]:
    """Split a daterange string to the component start and end
    Timestamps

    Args:
        daterange_str (str): Daterange string of the form

        2019-01-01T00:00:00_2019-12-31T00:00:00
    Returns:
        tuple (Timestamp, Timestamp): Tuple of start, end pandas Timestamps
    """
    from pandas import Timestamp

    split = daterange_str.split("_")

    start = Timestamp(split[0], tz="UTC")
    end = Timestamp(split[1], tz="UTC")

    return start, end


def valid_daterange(daterange: str) -> bool:
    """Check if the passed daterange is valid

    Args:
        daterange: Daterange string
    Returns:
        bool: True if valid
    """
    from openghg.util import split_daterange_str

    start, end = split_daterange_str(daterange)

    if start >= end:
        return False

    return True


def closest_daterange(to_compare: str, dateranges: Union[str, List]) -> str:
    """Finds the closest daterange in a list of dateranges

    Args:
        to_compare: Daterange (as a string) to compare
        dateranges: List of dateranges
    Returns:
        str: Daterange from dateranges that's the closest in time to to_compare
    """
    from openghg.util import split_daterange_str
    from pandas import Timedelta

    min_start = Timedelta("3650days")
    min_end = Timedelta("3650days")

    if not isinstance(dateranges, list):
        dateranges = [dateranges]

    dateranges = sorted(dateranges)

    start_comp, end_comp = split_daterange_str(daterange_str=to_compare)
    # We want to iterate over the dateranges and first check if they overlap
    # if they do, return that daterange
    # otherwise check how far apart the
    for daterange in dateranges:
        # If they're close to overlap the start and end will be close
        start, end = split_daterange_str(daterange_str=daterange)

        # Check for an overlap
        if start <= end_comp and end >= start_comp:
            raise ValueError("Overlapping daterange.")

        # Find the min between all the starts and all the ends
        diff_start_end = abs(start_comp - end)
        if diff_start_end < min_start:
            min_start = diff_start_end
            closest_daterange_start = daterange

        diff_end_start = abs(end_comp - start)
        if diff_end_start < min_end:
            min_end = diff_end_start
            closest_daterange_end = daterange

    if min_start < min_end:
        return closest_daterange_start
    else:
        return closest_daterange_end


def find_daterange_gaps(start_search: Timestamp, end_search: Timestamp, dateranges: List) -> List[str]:
    """Given a start and end date and a list of dateranges find the gaps.

    For example given a list of dateranges

    example = ['2014-09-02_2014-11-01', '2016-09-02_2018-11-01']

    start = timestamp_tzaware("2012-01-01")
    end = timestamp_tzaware("2019-09-01")

    gaps = find_daterange_gaps(start, end, example)

    gaps == ['2012-01-01-00:00:00+00:00_2014-09-01-00:00:00+00:00',
            '2014-11-02-00:00:00+00:00_2016-09-01-00:00:00+00:00',
            '2018-11-02-00:00:00+00:00_2019-09-01-00:00:00+00:00']

    Args:
        start_search: Start timestamp
        end_search: End timestamp
        dateranges: List of daterange strings
    Returns:
        list: List of dateranges
    """
    from pandas import Timedelta
    from openghg.util import pairwise

    sorted_dateranges = sorted(dateranges)

    # The difference between the start and end of the successived dateranges
    range_gap = "1day"
    # First find the gap between the start and the end
    start_first, end_first = split_daterange_str(sorted_dateranges[0])

    gaps = []
    if start_search < start_first:
        gap_start = start_search
        gap_end = start_first - Timedelta(range_gap)
        gap = create_daterange_str(start=gap_start, end=gap_end)
        gaps.append(gap)

    # Then find the gap between the end
    start_last, end_last = split_daterange_str(sorted_dateranges[-1])

    if end_search > end_last:
        gap_end = end_search
        gap_start = end_last + Timedelta(range_gap)
        gap = create_daterange_str(start=gap_start, end=gap_end)
        gaps.append(gap)

    for a, b in pairwise(sorted_dateranges):
        start_a, end_a = split_daterange_str(a)
        start_b, end_b = split_daterange_str(b)

        # Ignore any that are outside our search window
        if end_a < start_search or start_a > end_search:
            continue

        diff = start_b - end_a
        if diff > Timedelta(range_gap) and diff.value > 0:
            gap_start = end_a + Timedelta(range_gap)
            gap_end = start_b - Timedelta(range_gap)

            gap = create_daterange_str(start=gap_start, end=gap_end)
            gaps.append(gap)
        else:
            pass

    gaps.sort()

    return gaps


def daterange_contains(container: str, contained: str) -> bool:
    """Check if container contains contained

        Args:
            container: Daterange
            contained: Daterange
        Returns:
            bool
    """
    start_a, end_a = split_daterange_str(container)
    start_b, end_b = split_daterange_str(contained)

    return start_a < start_b and end_b < end_a


def trim_daterange(to_trim: str, overlapping: str) -> str:
    """Trims a daterange

    Args:
        to_trim: Daterange to trim down. Dates that overlap
        with overlap_daterange will be removed from to_trim
        overlap_daterange: Daterange containing dates we want to trim
        from to_trim
    Returns:
        str: Trimmed daterange
    """
    from pandas import Timedelta

    if not daterange_overlap(daterange_a=to_trim, daterange_b=overlapping):
        raise ValueError(f"Dateranges {to_trim} and {overlapping} do not overlap")

    # We need to work out which way round they overlap
    start_trim, end_trim = split_daterange_str(to_trim)
    start_overlap, end_overlap = split_daterange_str(overlapping)

    delta_gap = Timedelta("1s")

    # Work out if to_trim is before or after the overlap_daterange
    if end_trim > start_overlap and end_overlap > end_trim:
        new_end_trim = start_overlap - delta_gap
        return create_daterange_str(start=start_trim, end=new_end_trim)
    else:
        new_start_trim = end_overlap + delta_gap
        return create_daterange_str(start=new_start_trim, end=end_trim)


def split_encompassed_daterange(container: str, contained: str) -> Dict:
    """ Checks if one of the passed dateranges contains the other, if so, then
    split the larger daterange into three sections.

          <---a--->
    <---------b----------->

    Here b is split into three and we end up with:

    <-dr1-><---a---><-dr2->

    Args:
        daterange_a: Daterange
        daterange_b: Daterange
    Returns:
        dict: Dictionary of results
    """
    from pandas import Timedelta

    if not daterange_overlap(daterange_a=container, daterange_b=contained):
        raise ValueError("No overlap of dateranges.")

    container_start, container_end = split_daterange_str(daterange_str=container)
    contained_start, contained_end = split_daterange_str(daterange_str=contained)

    delta_gap = Timedelta("1s")

    dr1_start = container_start
    dr1_end = contained_start - delta_gap
    dr1 = create_daterange_str(start=dr1_start, end=dr1_end)

    dr3_start = contained_end + delta_gap
    dr3_end = container_end
    dr3 = create_daterange_str(start=dr3_start, end=dr3_end)

    # Trim a gap off the end of contained
    new_contained_end = contained_end - delta_gap
    new_contained = create_daterange_str(start=contained_start, end=new_contained_end)

    results = {}
    results["container_start"] = dr1
    results["contained"] = new_contained
    results["container_end"] = dr3

    return results
