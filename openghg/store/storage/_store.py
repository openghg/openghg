from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Protocol, TypeVar

import xarray as xr

from openghg.store.storage._index import StoreIndex, DatetimeStoreIndex
from openghg.types import DataOverlapError, UpdateError





class Store(ABC):
    """Interface for means of storing a single dataset."""
    @property
    @abstractmethod
    def index(self) -> StoreIndex:
        """Get index for store."""
        ...

    @abstractmethod
    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        """Insert an xr.Dataset to the store.

        If no data is present in the Store, this method should initialise storage.

        Args:
            data: xr.Dataset to add to Store
            on_conflict: if "error", raise DataOverlapError if any conflicts found. If "ignore", then
                ignore any conflicting values in `data`, and insert only non-conflicting values.

        Returns:
            None

        Raises:
            DataOverlapError if conflicts found and `on_conflict` == "error".
        """
        if on_conflict == "error" and self.index.conflicts_found(data):
            raise DataOverlapError

    @abstractmethod
    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        """Update the data in the Store with data in an xr.Dataset.

        Note: by default, only existing data can be updated, so an error is raised if there are
        non-conflicting times in the new data. This can be overridden.

        Args:
            data: xr.Dataset to add to Store
            on_nonconflict: if "error", raise IndexError if any non-conflicts found. If "ignore", then
                ignore any non-conflicting values in `data`, and insert only conflicting values.

        Returns:
            None

        Raises:
            UpdateError if nonconflicts found and `on_nonconflict` == "error".
        """
        if on_nonconflict == "error" and self.index.nonconflicts_found(data):
            raise UpdateError("To update with data that contains values outside the existing data index, use `on_nonconflict = 'error'`.")

    def upsert(self, data: xr.Dataset) -> None:
        """Add data to Store, inserting at new index values and updating at existing index values."""
        self.insert(data, on_conflict="ignore")
        self.update(data, on_nonconflict="ignore")

    @abstractmethod
    def get(self) -> xr.Dataset:
        """Return the stored data."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear data from store."""
        ...

    def overwrite(self, data: xr.Dataset) -> None:
        """Write data to Store, deleting any existing data."""
        self.clear()
        self.insert(data, on_conflict="ignore")  # "ignore" to avoid checking for conflicts

    @abstractmethod
    def delete(self) -> None:
        """Delete the store.

        Note: override this method if there are other artefacts
        to remove (for instance, a directory that held the data).
        """
        self.clear()

    def copy(self, other: Store) -> None:
        """Copy data from self to other.

        Note: this overwrites the data in `other`.
        """
        ds = self.get()
        other.overwrite(ds)

    @abstractmethod
    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the store."""
        ...


T = TypeVar("T")

class SupportsVersioning(Protocol):
    @classmethod
    def new_version(cls: type[T], version_root: Path, version_key: str) -> T:
        # TODO: `Path` probably isn't the right type here...
        ...


class MemoryStore(Store):
    """Simple in-memory implementation of Store interface."""
    def __init__(self, data: xr.Dataset | None = None) -> None:
        super().__init__()
        self.data = data

    def clear(self) -> None:
        self.data = None

    @property
    def index(self) -> DatetimeStoreIndex:
        if self.data is None:
            return DatetimeStoreIndex()
        return DatetimeStoreIndex.from_dataset(self.data)

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            self.data = data
        else:
            super().insert(data, on_conflict) # error checking
            data_nonconflicts = self.index.select_nonconflicts(data)
            self.data = xr.concat([self.data, data_nonconflicts], dim="time").sortby("time")

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            raise UpdateError("Cannot update empty Store.")
        else:
            super().update(data, on_nonconflict) # error checking
            data_conflicts = self.index.select_conflicts(data)
            self.data.update(data_conflicts)

    def get(self) -> xr.Dataset:
        if self.data is None:
            return xr.Dataset()
        return self.data
