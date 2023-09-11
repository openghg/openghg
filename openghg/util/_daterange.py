from __future__ import annotations

from typing import cast, TypeVar, Optional

import numpy as np
import pandas as pd
import xarray as xr

from ._time import timestamp_tzaware, relative_time_offset


class DateRangeError(Exception):
    pass


DR = TypeVar("DR", bound="DateRange")


class DateRange:
    def __init__(self, start: pd.Timestamp, end: pd.Timestamp, freq: Optional[pd.DateOffset] = None) -> None:
        if start > end:
            raise ValueError(f"Start date {start} after end date {end}.")
        self.start = start
        self.end = end
        self.freq = freq

    def __str__(self) -> str:
        start = str(self.start).replace(" ", "-")
        end = str(self.end).replace(" ", "-")
        return start + "_" + end

    def __repr__(self) -> str:
        return f"DateRange({self.start!r}, {self.end!r})"

    def __eq__(self, other: DateRange) -> bool:
        return (self.start == other.start) and (self.end == other.end)

    def __lt__(self, other: DateRange) -> bool:
        """Compare (start, end) pairs in lexicographical order.

        (start1, end1) is less than (start2, end2) if either:
        start1 < start2, or start1 == start2 and end1 < end2.
        """
        if self.start < other.start:
            return True
        elif self.start == other.start:
            return self.end < other.end
        else:
            return False

    @classmethod
    def from_dataset(cls: type[DR], dataset: xr.Dataset) -> DR:
        """Get DateRange from xarray Dataset."""
        try:
            start = timestamp_tzaware(dataset.time.min().values)
            end = timestamp_tzaware(dataset.time.max().values)
        except AttributeError:
            raise DateRangeError("This Dataset does not have a time attribute, unable to read date range")
        return cls(start, end)

    @classmethod
    def from_dataframe(cls: type[DR], dataframe: pd.DataFrame) -> DR:
        """Get DateRange from pandas DataFrame."""
        if not isinstance(dataframe.index, pd.DatetimeIndex):
            raise DateRangeError("This DataFrame does not have a DatetimeIndex, unable to read date range.")

        start = timestamp_tzaware(cast(pd.Timestamp, dataframe.index.min()))  # cast okay by previous check
        end = timestamp_tzaware(cast(pd.Timestamp, dataframe.index.max()))
        return cls(start, end)

    @classmethod
    def from_string(cls: type[DR], daterange_str: str) -> DR:
        """Inverse of DateRange.__str__ method."""
        start_str, end_str = daterange_str.split("_")
        return cls(pd.Timestamp(start_str), pd.Timestamp(end_str))

    def overlaps(self, other: DateRange) -> bool:
        """Return True if 'other' DateRange overlaps with this DateRange.

        Two date ranges are disjoint if one ends before the other starts,
        so they overlap is the opposite is true.
        """
        return bool(self.start <= other.end and self.end >= other.start)

    def make_representative(self, period: Optional[str] = None) -> DateRange:
        """Take period information into account."""
        start = self.start
        end = self.end

        if period is not None:
            offset = relative_time_offset(period=period)
            end += offset  # end of time period covered by data
            end -= pd.Timedelta(seconds=1)  # exclude endpoint

        if start == end:
            end += pd.Timedelta(seconds=1)  # make range longer than 0 seconds

        return DateRange(start, end)


def clip_dateranges(dateranges: list[DateRange]) -> list[DateRange]:
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
    clipped_dateranges.append(dateranges[-1])

    return clipped_dateranges
