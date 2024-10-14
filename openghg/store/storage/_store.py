from __future__ import annotations

from abc import ABC, abstractmethod

from xarray import Dataset

from openghg.store.storage._index import StoreIndex


class Store(ABC):
    """Interface for means of storing a single dataset."""
    @abstractmethod
    def add(self, data: Dataset) -> None:
        """Add an xr.Dataset to the store."""
        ...

    @abstractmethod
    def delete(self) -> None:
        """Delete the store."""
        ...

    @abstractmethod
    def get(self) -> Dataset:
        """Return the stored data."""
        ...

    @property
    @abstractmethod
    def index(self) -> StoreIndex:
        """Get index for store."""
        ...

    def copy(self, other: Store) -> None:
        """Copy data from self to other."""
        ds = self.get()
        other.add(ds)

    @abstractmethod
    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the store."""
        ...
