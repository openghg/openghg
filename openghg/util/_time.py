from datetime import date
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
    "daterange_overlap",
    "combine_dateranges",
    "split_daterange_str",
    "closest_daterange",
    "valid_daterange",
    "find_daterange_gaps",
    "trim_daterange",
    "split_encompassed_daterange",
    "daterange_contains",
    "sanitise_daterange",
    "check_nan",
    "check_date",
    "first_last_dates",
]


def timestamp_tzaware(timestamp: Union[str, Timestamp]) -> Timestamp:
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


def daterange_overlap(daterange_a: str, daterange_b: str) -> bool:
    """Check if daterange_a is within daterange_b.

    Args:
        daterange_a (str): Timezone aware daterange string. Example:
        2014-01-30-10:52:30+00:00_2014-01-30-13:22:30+00:00
        daterange_b (str): As daterange_a
    Returns:
        bool: True if daterange included
    """
    from pandas import Timestamp

    split_a = daterange_a.split("_")
    split_b = daterange_b.split("_")

    start_a = Timestamp(ts_input=split_a[0], tz="UTC")
    end_a = Timestamp(ts_input=split_a[1], tz="UTC")

    start_b = Timestamp(ts_input=split_b[0], tz="UTC")
    end_b = Timestamp(ts_input=split_b[1], tz="UTC")

    # For this logic see
    # https://stackoverflow.com/a/325964
    return bool(start_a <= end_b and end_a >= start_b)


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

    start = timestamp_tzaware(start)
    end = timestamp_tzaware(end)

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
    start = timestamp_tzaware(start)
    end = timestamp_tzaware(end)

    if start > end:
        raise ValueError(f"Invalid daterange, start ({start}) > end ({end})")

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
    start = timestamp_tzaware(split[0])
    end = timestamp_tzaware(split[1])

    return date_range(start=start, end=end, freq=freq)


def daterange_to_str(daterange: DatetimeIndex) -> str:
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


def combine_dateranges(dateranges: List[str]) -> List[str]:
    """Combine dateranges

    Args:
        dateranges: Daterange strings
    Returns:
        list: List of combined dateranges

    Modified from
    https://codereview.stackexchange.com/a/69249
    """
    if len(dateranges) == 1:
        return dateranges

    def sort_key(tup: Tuple) -> Timestamp:
        return tup[0]

    intervals = [split_daterange_str(x) for x in dateranges]
    sorted_by_lower_bound = sorted(intervals, key=sort_key)

    combined: List[Timestamp] = []

    for higher in sorted_by_lower_bound:
        if not combined:
            combined.append(higher)
        else:
            lower = combined[-1]
            # Test for intersection between lower and higher:
            # We know via sorting that lower[0] <= higher[0]
            if higher[0] <= lower[1]:
                upper_bound = max(lower[1], higher[1])
                # Replace by combined interval
                combined[-1] = (lower[0], upper_bound)
            else:
                combined.append(higher)

    combined_strings = [create_daterange_str(start=a, end=b) for a, b in combined]

    return combined_strings


def split_daterange_str(
    daterange_str: str, date_only: bool = False
) -> Tuple[Union[Timestamp, date], Union[Timestamp, date]]:
    """Split a daterange string to the component start and end
    Timestamps

    Args:
        daterange_str: Daterange string of the form
        date_only: Return only the date portion of the Timestamp, removing
        the hours / seconds component

        2019-01-01T00:00:00_2019-12-31T00:00:00
    Returns:
        tuple (Timestamp / datetime.date, Timestamp / datetime.date): Tuple of start, end timestamps / dates
    """
    split = daterange_str.split("_")

    start = timestamp_tzaware(split[0])
    end = timestamp_tzaware(split[1])

    if date_only:
        start = start.date()
        end = end.date()

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


def closest_daterange(to_compare: str, dateranges: Union[str, List[str]]) -> str:
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


def find_daterange_gaps(start_search: Timestamp, end_search: Timestamp, dateranges: List[str]) -> List[str]:
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

    if not dateranges:
        return []

    sorted_dateranges = sorted(dateranges)

    # The difference between the start and end of the successive dateranges
    range_gap = Timedelta("1h")
    min_gap = Timedelta("30m")

    # First find the gap between the start and the end
    start_first, _ = split_daterange_str(sorted_dateranges[0])

    gaps = []
    if start_search < start_first:
        gap_start = start_search
        gap_end = start_first - range_gap

        if gap_end - gap_start > min_gap:
            gap = create_daterange_str(start=gap_start, end=gap_end)
            gaps.append(gap)

    # Then find the gap between the end
    _, end_last = split_daterange_str(sorted_dateranges[-1])

    if end_search > end_last:
        gap_end = end_search
        gap_start = end_last + range_gap

        if gap_end - gap_start > min_gap:
            gap = create_daterange_str(start=gap_start, end=gap_end)
            gaps.append(gap)

    for a, b in pairwise(sorted_dateranges):
        start_a, end_a = split_daterange_str(a)
        start_b, end_b = split_daterange_str(b)

        # Ignore any that are outside our search window
        if end_a < start_search or start_a > end_search:
            continue

        diff = start_b - end_a

        if diff > min_gap:
            gap_start = end_a + range_gap
            gap_end = start_b - range_gap

            gap = create_daterange_str(start=gap_start, end=gap_end)
            gaps.append(gap)
        else:
            pass

    gaps.sort()

    return gaps


def daterange_contains(container: str, contained: str) -> bool:
    """Check if the daterange container contains the daterange contained

    Args:
        container: Daterange
        contained: Daterange
    Returns:
        bool
    """
    start_a, end_a = split_daterange_str(container)
    start_b, end_b = split_daterange_str(contained)

    return bool(start_a <= start_b and end_b <= end_a)


def trim_daterange(to_trim: str, overlapping: str) -> str:
    """Removes overlapping dates from to_trim

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
    if start_trim < start_overlap and end_overlap > end_trim:
        new_end_trim = start_overlap - delta_gap
        return create_daterange_str(start=start_trim, end=new_end_trim)
    else:
        new_start_trim = end_overlap + delta_gap
        return create_daterange_str(start=new_start_trim, end=end_trim)


def split_encompassed_daterange(container: str, contained: str) -> Dict:
    """Checks if one of the passed dateranges contains the other, if so, then
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

    container_start, container_end = split_daterange_str(daterange_str=container)
    contained_start, contained_end = split_daterange_str(daterange_str=contained)

    # First check one contains the other
    if not (container_start <= contained_start and contained_end <= container_end):
        raise ValueError(f"Range {container} does not contain {contained}")

    # Gap to add between dateranegs so they don't overlap
    delta_gap = Timedelta("1s")
    # If the difference is less than this we'll assume they're the same timestamp
    tolerance = Timedelta("2h")

    results = {}
    # If one of them starts at the same point we just want to split the range in two
    if abs(contained_start - container_start) < tolerance:
        new_contained = create_daterange_str(start=contained_start, end=contained_end)
        dr1_start = contained_end + delta_gap
        dr1 = create_daterange_str(start=dr1_start, end=container_end)

        results["container_start"] = dr1
        results["contained"] = new_contained

        return results

    if abs(contained_end - container_end) < tolerance:
        new_contained = create_daterange_str(start=contained_start, end=contained_end)
        dr1_end = contained_start - delta_gap
        dr1 = create_daterange_str(start=container_start, end=dr1_end)

        results["container_start"] = dr1
        results["contained"] = new_contained

        return results

    dr1_start = container_start
    dr1_end = contained_start - delta_gap
    dr1 = create_daterange_str(start=dr1_start, end=dr1_end)

    dr3_start = contained_end + delta_gap
    dr3_end = container_end
    dr3 = create_daterange_str(start=dr3_start, end=dr3_end)

    # Trim a gap off the end of contained
    new_contained_end = contained_end - delta_gap
    new_contained = create_daterange_str(start=contained_start, end=new_contained_end)

    results["container_start"] = dr1
    results["contained"] = new_contained
    results["container_end"] = dr3

    return results


def sanitise_daterange(daterange: str) -> str:
    """Make sure the daterange is correct and return
    tzaware daterange.

    Args:
        daterange: Daterange str
    Returns:
        str: Timezone aware daterange str
    """
    start, end = split_daterange_str(daterange)

    if start >= end:
        raise ValueError("Invalid daterange, start after end date")

    return create_daterange_str(start=start, end=end)


def check_date(date: str) -> str:
    """Check if a date string can be converted to a pd.Timestamp
    and returns NA if not.

    Returns a string that can be JSON serialised.

    Args:
        date: String to test
    Returns:
        str: Returns NA if not a date, otherwise date string
    """
    from pandas import Timestamp, isnull

    try:
        d = Timestamp(date)
        if isnull(d):
            return "NA"

        return date
    except ValueError:
        return "NA"


def check_nan(data: Union[int, float]) -> Union[str, float, int]:
    """Check if a number is Nan.

    Returns a string that can be JSON serialised.

    Args:
        data: Number
    Returns:
        str, float, int: Returns NA if not a number else number
    """
    from math import isnan

    if isnan(data):
        return "NA"
    else:
        return round(data, 3)


def first_last_dates(keys: List) -> Tuple[Timestamp, Timestamp]:
    """Find the first and last timestamp from a list of keys

    Args:
        keys: List of keys
    Returns:
        tuple: First and last timestamp
    """

    def sorting_key(s: str) -> str:
        return s.split("/")[-1]

    sorted_keys = sorted(keys, key=sorting_key)

    first_daterange = sorted_keys[0].split("/")[-1]
    first_date = first_daterange.split("_")[0]

    last_daterange = sorted_keys[-1].split("/")[-1]
    last_date = last_daterange.split("_")[-1]

    first = timestamp_tzaware(first_date)
    last = timestamp_tzaware(last_date)

    return first, last
