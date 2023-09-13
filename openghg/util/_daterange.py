from __future__ import annotations

from types import NotImplementedType
from typing import cast, Optional, TypeVar, Union

import pandas as pd
import xarray as xr

from ._time import timestamp_tzaware, relative_time_offset


class DateRangeError(Exception):
    pass


DR = TypeVar("DR", bound="DateRange")


class DateRange:
    def __init__(self, start: pd.Timestamp, end: pd.Timestamp, period: Optional[str] = None) -> None:
        """Create DateRange object with given start and ends dates, extended to cover full periods
        if period provided.

        A "representative" DateRange covers the start - end time + any additional period that is covered
        by each time point.

        If there is only one time point (i.e. start and end datetimes are the same) and no period is
        supplied 1 additional second will be added to ensure these values are not identical.

        Args:
            start: start date of date range.
            end: end date of date range.
            period: Value representing a time period e.g. "12H", "1AS" "3MS". Should be suitable for
                creation of a pandas Timedelta or DataOffset object.

        Returns:
            None
        """
        if start > end:
            raise ValueError(f"Start date {start} after end date {end}.")
        self.start = start

        if period is not None:
            offset = relative_time_offset(period=period)
            end += offset  # end of time period covered by data
            end -= pd.Timedelta(seconds=1)  # exclude endpoint
        self.end = end

        if self.end == start:
            self.end += pd.Timedelta(seconds=1)  # make range longer than 0 seconds

        self.period = period

    def __str__(self) -> str:
        """Convert DateRange to string.

        Returns:
            str : String "start_end" in the format "YYYY-MM-DD-hh:mm:ss_YYYY-MM-DD-hh:mm:ss"
        """
        start = str(self.start).replace(" ", "-")
        end = str(self.end).replace(" ", "-")
        return start + "_" + end

    def __repr__(self) -> str:
        return f"DateRange({self.start!r}, {self.end!r}, period={self.period!r})"

    def __hash__(self) -> int:
        """Hash by start_end_period string."""
        period = self.period or ""
        hash_string = str(self) + "_" + period
        return hash(hash_string)

    def __eq__(self, other: object) -> Union[bool, NotImplementedType]:
        if not isinstance(other, DateRange):
            return NotImplemented
        else:
            cast(DateRange, other)
        return bool((self.start == other.start) and (self.end == other.end))

    def __lt__(self, other: DateRange) -> bool:
        """Compare (start, end) pairs in lexicographical order.

        (start1, end1) is less than (start2, end2) if either:
        start1 < start2, or start1 == start2 and end1 < end2.

        Args:
            other: DateRange to compare with self.

        Returns:
            True if self lexicographically less than other.
        """
        if self.start < other.start:
            return True
        elif self.start == other.start:
            return bool(self.end < other.end)
        else:
            return False

    @classmethod
    def from_dataset(cls: type[DR], dataset: xr.Dataset, period: Optional[str] = None) -> DR:
        """Get DateRange from xarray Dataset.

        Args:
            dataset: Data containing (at least) a time dimension. Used to extract start and end datetimes.
            period: Value representing a time period e.g. "12H", "1AS" "3MS". Should be suitable for
                creation of a pandas Timedelta or DataOffset object.

        Returns:
            DateRange with start and end date extracted from given Dataset.
        """
        try:
            start = timestamp_tzaware(dataset.time.min().values)
            end = timestamp_tzaware(dataset.time.max().values)
        except AttributeError:
            raise DateRangeError("This Dataset does not have a time attribute, unable to read date range")
        return cls(start, end, period)

    @classmethod
    def from_dataframe(cls: type[DR], dataframe: pd.DataFrame, period: Optional[str] = None) -> DR:
        """Get DateRange from pandas DataFrame.

        Args:
            dataframe: DataFrame with DatetimeIndex.
            period: Value representing a time period e.g. "12H", "1AS" "3MS". Should be suitable for
                creation of a pandas Timedelta or DataOffset object.

        Returns:
            DateRange with start and end date extracted from given Dataset.
        """
        if not isinstance(dataframe.index, pd.DatetimeIndex):
            raise DateRangeError("This DataFrame does not have a DatetimeIndex, unable to read date range.")

        start = timestamp_tzaware(cast(pd.Timestamp, dataframe.index.min()))  # cast okay by previous check
        end = timestamp_tzaware(cast(pd.Timestamp, dataframe.index.max()))
        return cls(start, end, period)

    @classmethod
    def from_string(cls: type[DR], daterange_str: str, period: Optional[str] = None) -> DR:
        """Inverse of DateRange.__str__ method.

        Args:
            daterange_str: string of the form "YYYY-MM-DD-hh:mm:ss_YYYY-MM-DD-hh:mm:ss"
            period: Value representing a time period e.g. "12H", "1AS" "3MS". Should be suitable for
                creation of a pandas Timedelta or DataOffset object.

        Returns:
            DateRange with start and end date extracted from given string.
        """
        start_str, end_str = daterange_str.split("_")
        return cls(pd.Timestamp(start_str), pd.Timestamp(end_str), period)

    def overlaps(self, other: DateRange) -> bool:
        """Return True if 'other' DateRange overlaps with this DateRange.

        Two date ranges are disjoint if one ends before the other starts,
        so they overlap is the opposite is true.

        Args:
            other: DateRange to compare with self.

        Returns:
            True if self and other overlap, False otherwise.
        """
        return bool(self.start <= other.end and self.end >= other.start)


def _clip_dateranges(dateranges: list[DateRange]) -> list[DateRange]:
    """Trim DateRanges in given list so that the DateRanges do not overlap.

    Args:
        dateranges: list of DateRanges sorted so that the start dates are
            increasing.

    Returns:
        list of DateRanges with same start datetimes, but trimmed end
            datetimes, so that the DateRanges do not overlap.
    """
    if len(dateranges) <= 1:
        return dateranges

    clipped_dateranges = []
    for dr1, dr2 in zip(dateranges, dateranges[1:]):
        if dr1.overlaps(dr2):
            clipped_end = dr2.start - pd.Timedelta(seconds=1)
            clipped_dateranges.append(DateRange(dr1.start, clipped_end))
        else:
            clipped_dateranges.append(dr1)
    clipped_dateranges.append(dateranges[-1])  # last daterange isn't clipped

    return clipped_dateranges


T = TypeVar("T")


def clip_daterange_keys(daterange_dict: dict[DateRange, T]) -> dict[DateRange, T]:
    """Trim DateRange keys in given dict so that the DateRanges do not overlap.

    Args:
        daterange_dict: dict keyed by  DateRanges. The keys must be sorted so that
            the start dates are increasing.

    Returns:
        dict keyed by DateRanges with same start datetimes, but trimmed end
            datetimes, so that the DateRanges do not overlap.
    """
    dateranges = _clip_dateranges(list(daterange_dict.keys()))
    return {k: v for k, v in zip(dateranges, daterange_dict.values())}
