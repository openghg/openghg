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
    def __len__(self) -> int:
        """Return number of entries in the index."""
        ...

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Return number of entries in the index."""
        ...

    @abstractmethod
    def conflicts(self, other: SIT | xr.Dataset) -> SIT:
        """Return common index values from self and other."""
        ...

    def conflicts_found(self, other: StoreIndex | xr.Dataset) -> bool:
        """Return True if common index values found in self and other."""
        return len(self.conflicts(other)) > 0

    @abstractmethod
    def nonconflicts(self, other: SIT | xr.Dataset) -> SIT:
        """Return index values in other, but not in self."""
        ...

    def nonconflicts_found(self, other: StoreIndex | xr.Dataset) -> bool:
        """Return True if there are index values in other that are not in self ."""
        return len(self.nonconflicts(other)) > 0

    @abstractmethod
    def select(self, ds: xr.Dataset) -> xr.Dataset:
        """Restrict dataset to values in index."""
        ...

    def select_conflicts(self, ds: xr.Dataset, conflicts: StoreIndex | None = None) -> xr.Dataset:
        """Return dataset restricted to conflicting index values.

        Note: this method may need to align the index values after restricting; the result of this
        method must result in a dataset that will exactly overwrite the existing data.
        """
        if conflicts is None:
            conflicts = self.conflicts(ds)
        return conflicts.select(ds)  # type:ignore

    def select_nonconflicts(self, ds: xr.Dataset, nonconflicts: StoreIndex | None = None) -> xr.Dataset:
        """Return dataset restricted to nonconflicting index values."""
        if nonconflicts is None:
            nonconflicts = self.nonconflicts(ds)
        return nonconflicts.select(ds)  # type:ignore

    @abstractmethod
    def to_array(self) -> np.ndarray:
        ...


class DatetimeStoreIndex(StoreIndex):
    """Store index based on pd.DatetimeIndex."""
    def __init__(self, times: np.ndarray | pd.DatetimeIndex | None = None) -> None:
        super().__init__()
        if times is None:
            self.index = pd.DatetimeIndex([])
        else:
            self.index = pd.DatetimeIndex(times)

    @classmethod
    def from_dataset(cls: type[DatetimeStoreIndex], ds: xr.Dataset) -> DatetimeStoreIndex:
        return cls(ds.time.values)

    def __len__(self) -> int:
        return len(self.index)

    def __eq__(self, other) -> bool:
        if not isinstance(other, DatetimeStoreIndex):
            return NotImplemented
        return (self.index == other.index).all()

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

    def to_array(self) -> np.ndarray:
        return self.index.values


class FloorDatetimeStoreIndex(StoreIndex):
    """Store index based on pd.DatetimeIndex with times rounded down to a frequency.

    This uses `pd.DatetimeIndex.floor` to drop any time units more precise than
    the specified frequency.

    This could be used to avoid duplicating data when there are small differences in
    time coordinates, but it is mostly to demonstrate how `StoreIndex` should be
    implemented in a more complicated case where `select_conflicts` needs to transform
    the time coordinate.

    # TODO: re-implement this with `pd.Index.get_indexer` and a tolerance?
    """
    def __init__(self, times: np.ndarray | pd.DatetimeIndex, freq: str = "s") -> None:
        super().__init__()
        self.index = pd.DatetimeIndex(times)
        self.freq = freq
        self.floored = pd.Series(data=np.arange(len(times)), index=self.index.floor(freq=self.freq))

    @classmethod
    def from_dataset(cls: type[FloorDatetimeStoreIndex], ds: xr.Dataset, freq: str = "s") -> FloorDatetimeStoreIndex:
        return cls(ds.time.values, freq)

    def __len__(self) -> int:
        return len(self.index)

    def __eq__(self, other) -> bool:
        if not isinstance(other, FloorDatetimeStoreIndex):
            return NotImplemented
        return (self.index == other.index).all() and (self.freq == other.freq)

    def conflicts(self, other: FloorDatetimeStoreIndex | xr.Dataset) -> FloorDatetimeStoreIndex:
        """Return the times from `other` that conflict, after rounding down to self.freq"""
        if isinstance(other, xr.Dataset):
            other = FloorDatetimeStoreIndex.from_dataset(other, self.freq)

        floored_intersection = self.floored.index.intersection(other.floored.index)
        other_floored_intersection_idxs = other.floored[floored_intersection]

        other_intersection = other.index[other_floored_intersection_idxs]
        other_intersection = cast(pd.DatetimeIndex, other_intersection)
        result = FloorDatetimeStoreIndex(other_intersection, self.freq)  # NOTE: not sure how this will work if self and other have different freqs
        return result

    def nonconflicts(self, other: FloorDatetimeStoreIndex | xr.Dataset) -> FloorDatetimeStoreIndex:
        if isinstance(other, xr.Dataset):
            other = FloorDatetimeStoreIndex.from_dataset(other, self.freq)

        floored_diff = other.floored.index.difference(self.floored.index)
        other_floored_diff_idxs = other.floored[floored_diff]

        other_diff = other.index[other_floored_diff_idxs]
        other_diff = cast(pd.DatetimeIndex, other_diff)
        result = FloorDatetimeStoreIndex(other_diff, self.freq)  # NOTE: not sure how this will work if self and other have different freqs
        return result

    def select(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.sel(time=self.index)

    def select_conflicts(self, ds: xr.Dataset, conflicts: StoreIndex | None = None) -> xr.Dataset:
        """Restrict `ds` to intersection of floored times and align to existing times."""
        ds_restricted = super().select_conflicts(ds, conflicts)
        other = FloorDatetimeStoreIndex.from_dataset(ds, self.freq)

        floored_intersection = self.floored.index.intersection(other.floored.index)
        self_floored_intersection_idxs = self.floored[floored_intersection]

        self_intersection = self.index[self_floored_intersection_idxs]

        return ds_restricted.assign_coords(time=self_intersection)

    def to_array(self) -> np.ndarray:
        return self.index.values
