from __future__ import annotations

from abc import ABC, abstractmethod
from typing import cast, TypeVar

import numpy as np
import pandas as pd
import xarray as xr


SIT = TypeVar("SIT", bound="StoreIndex")


class StoreIndex(ABC):
    """Interface for index of Store object.

    To decide if new data to be added to the Store conflicts
    with existing data in the Store, we need to decide when their
    time dimensions intersect. (Or more generally: when their append
    dimensions intersect.)

    Using the time coordinate or a pd.DatetimeIndex may be too simplistic,
    since often the time coordinate represents the start of a time interval.

    Further, we may want to ignore minor differences in Timestamps.
    """
    @abstractmethod
    def conflicts(self, other: SIT | xr.Dataset) -> SIT:
        """Return common index values from self and other."""
        ...

    @abstractmethod
    def nonconflicts(self, other: SIT | xr.Dataset) -> SIT:
        """Return index values in other, but not in self."""
        ...

    @abstractmethod
    def select(self, ds: xr.Dataset) -> xr.Dataset:
        """Restrict dataset to values in index."""
        ...

    def select_conflicts(self, ds: xr.Dataset, conflicts: StoreIndex | None = None) -> xr.Dataset:
        """Return dataset restricted to conflicting index values."""
        if conflicts is None:
            conflicts = self.conflicts(ds)
        return conflicts.select(ds)  # type:ignore

    def select_nonconflicts(self, ds: xr.Dataset, nonconflicts: StoreIndex | None = None) -> xr.Dataset:
        """Return dataset restricted to nonconflicting index values."""
        if nonconflicts is None:
            nonconflicts = self.nonconflicts(ds)
        return nonconflicts.select(ds)  # type:ignore


class DatetimeStoreIndex(StoreIndex):
    """Store index based on pd.DatetimeIndex."""
    def __init__(self, times: np.ndarray | pd.DatetimeIndex) -> None:
        super().__init__()
        self.index = pd.DatetimeIndex(times)

    @classmethod
    def from_dataset(cls: type[DatetimeStoreIndex], ds: xr.Dataset) -> DatetimeStoreIndex:
        return cls(ds.time.values)

    def conflicts(self, other: DatetimeStoreIndex | xr.Dataset) -> DatetimeStoreIndex:
        if isinstance(other, xr.Dataset):
            other = DatetimeStoreIndex.from_dataset(other)
        intersection = self.index.intersection(other.index)
        intersection = cast(pd.DatetimeIndex, intersection)
        result = DatetimeStoreIndex(intersection)
        return result

    def nonconflicts(self, other: DatetimeStoreIndex | xr.Dataset) -> DatetimeStoreIndex:
        if isinstance(other, xr.Dataset):
            other = DatetimeStoreIndex.from_dataset(other)
        diff = other.index.difference(self.index)
        diff = cast(pd.DatetimeIndex, diff)
        result = DatetimeStoreIndex(diff)
        return result

    def select(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.sel(time=self.index)
