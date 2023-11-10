from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Iterable
from xarray import Dataset
from collections.abc import Iterator


class Store(ABC):
    """Interface for storing data in a Datasource. This may be in a zarr directory
    store, compressed NetCDF, a sparse storage format or others."""

    @abstractmethod
    def add(
        self,
        key: str,
        version: str,
        dataset: Dataset,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def delete(self, key: str, version: str) -> None:
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def delete_all(self) -> None:
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def keys(self) -> Iterator[str]:
        """Keys of data stored in the zarr store"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the zarr store."""
        pass

    @abstractmethod
    def store_key(self) -> str:
        """Return the key of this zarr store"""
        pass

    @abstractmethod
    def pop(self, key: str, version: str) -> Dataset:
        """Pop some data from the store."""
        pass

    @abstractmethod
    def copy_to_memorystore(self, keys: Iterable, version: str) -> List[Dict]:
        """Copies the compressed data from the filesystem store to a list of in-memory stores.
        This preserves the compression and chunking of the data and creates a list
        that can be opened as a single dataset.
        """

    @abstractmethod
    def update(
        self, key: str, version: str, dataset: Dataset, compressor: Optional[Any], filters: Optional[Any]
    ) -> None:
        """Update the data at the given key"""
        pass

    @abstractmethod
    def hash(self, data: str) -> str:
        """Hash the data at the given key"""
        pass

    @abstractmethod
    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        pass

    @abstractmethod
    def hash_equal(self, key: str, dataset: Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        pass

    @abstractmethod
    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store"""
        pass
