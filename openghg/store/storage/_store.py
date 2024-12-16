from abc import ABC, abstractmethod
from typing import Any
from xarray import Dataset
from collections.abc import Iterator


class Store(ABC):
    """Interface for storing data in a Datasource. This may be in a zarr directory
    store, compressed NetCDF, a sparse storage format or others."""

    @abstractmethod
    def add(
        self,
        version: str,
        dataset: Dataset,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> None:
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def delete_version(self, version: str) -> None:
        """Delete a version from the store"""
        pass

    @abstractmethod
    def delete_all(self) -> None:
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def keys(self, version: str) -> Iterator[str]:
        """Keys of data stored in the zarr store"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the zarr store."""
        pass

    @abstractmethod
    def store_key(self, version: str) -> str:
        """Return the key of this zarr store"""
        pass

    @abstractmethod
    def version_exists(self, version: str) -> bool:
        """Check if a version exists in the current store"""
        pass

    @abstractmethod
    def get(self, version: str) -> Dataset:
        """Get the version of the dataset stored in the zarr store."""
        pass

    @abstractmethod
    def _pop(self, version: str) -> Dataset:
        """Pop some data from the store. This removes the data at this version from the store
        and returns it."""
        pass

    @abstractmethod
    def update(self, version: str, dataset: Dataset, compressor: Any | None, filters: Any | None) -> None:
        """Update the data at the given key"""
        pass

    @abstractmethod
    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store"""
        pass
