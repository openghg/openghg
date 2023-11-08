"""
So this is a prototype for using zarr with the
"""
from __future__ import annotations
from abc import ABC, abstractmethod
import collections
import logging
from typing import Any, Dict, Literal, Iterable, Generator, List, Union, Optional, MutableMapping
import xarray as xr
import zarr

from openghg.types import KeyExistsError
from pathlib import Path
from openghg.objectstore import get_folder_size
from openghg.store.spec import get_zarr_encoding

logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)

StoreLike = Union[zarr.storage.BaseStore, MutableMapping]


class ZarrStore(ABC):
    """Interface for our zarr stores."""

    @abstractmethod
    def add(self):
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def update(self):
        """Update the data at the given key"""
        pass

    @abstractmethod
    def delete(self):
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def hash(self):
        """Hash the data at the given key"""
        pass

    @abstractmethod
    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        pass

    @abstractmethod
    def hash_equal(self, key: str, dataset: xr.Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        pass

    @abstractmethod
    def bytes_stored(self):
        """Return the number of bytes stored in the zarr store"""
        pass


class LocalZarrStore(ZarrStore):
    def __init__(self, bucket: str, datasource_uuid: str, mode: Literal["rw", "r"] = "rw") -> None:
        self._bucket = bucket
        self._store_key = f"data/{datasource_uuid}/zarr"
        self._mode = mode
        dir_path = Path(bucket, self._store_key)
        self._store = zarr.storage.NestedDirectoryStore(dir_path)
        self._pop_keys = collections.deque()
        self._memory_store = {}

    def __bool__(self) -> bool:
        return bool(self._store)

    def _create_key(self, key: str, version: str) -> str:
        """Create a key for the zarr store."""
        return f"{version}/{key}"

    def keys(self) -> Generator:
        """Keys of data stored in the zarr store.

        Returns:
            Generator: Generator object
        """
        return self._store.keys()

    def close(self) -> None:
        """Close the zarr store.

        Returns:
            None
        """
        self._store.close()

    def store_key(self) -> str:
        """Return the key of this zarr Store

        Returns:
            str: Key of zarr store
        """
        return self._store_key

    def store_path(self) -> Path:
        """Return the path of this zarr Store

        Returns:
            Path: Path of zarr store
        """
        return Path(self._bucket, self._store_key)

    def add(
        self,
        key: str,
        version: str,
        dataset: xr.Dataset,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Add an xr.Dataset to the zarr store.

        Args:
            key: Key to add data under
            version: Version of data to add
            dataset: xr.Dataset to add
            compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
            filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
        Returns:
            None
        """
        if self._mode == "r":
            raise ValueError("Cannot add to a read-only zarr store")

        versioned_key = self._create_key(key=key, version=version)
        # TODO - check if the key already exists and then ?
        if versioned_key in self._store:
            raise KeyExistsError("Cannot overwrite key in zarr store using add - use update method")

        encoding = get_zarr_encoding(data_vars=dataset.data_vars, filters=filters, compressor=compressor)
        dataset.to_zarr(store=self._store, group=versioned_key, encoding=encoding)

    def pop(self, key: str, version: str) -> xr.Dataset:
        """Pop some data from the store.

        Args:
            key: Key to pop data
            version: Version of data
        Returns:
            Dataset: Dataset popped from the store
        """
        if self._mode == "r":
            raise ValueError("Cannot pop from a read-only zarr store")

        self._pop_keys.append((key, version))
        versioned_key = self._create_key(key=key, version=version)
        # Let's copy the data we want to pop into a memory store and return it from there
        zarr.convenience.copy_store(
            source=self._store, dest=self._memory_store, source_path=versioned_key, dest_path=versioned_key
        )
        self.delete(key=key, version=version)
        return xr.open_zarr(store=self._memory_store, group=versioned_key, consolidated=False)

    def copy_to_memorystore(self, keys: Iterable, version: str) -> List[Dict]:
        """Copies the compressed data from the filesystem store to a list of in-memory stores.
        This preserves the compression and chunking of the data and creates a list
        that can be opened as a single dataset.

        Args:
            keys: List of keys
            version: Version of data to copy
        Returns:
            List: List of dictionaries
        """
        memory_stores = []
        for daterange_key in keys:
            key = self._create_key(key=daterange_key, version=version)
            store = {}
            zarr.copy_store(source=self._store, dest=store, source_path=key)
            memory_stores.append(store)

        return memory_stores

    def delete(self, key: str, version: str) -> None:
        """Remove data from the zarr store

        Args:
            key: Key of data in store
            version: Version of data
        Returns:
            None
        """
        if self._mode == "r":
            raise ValueError("Cannot delete from a read-only zarr store")

        versioned_key = self._create_key(key=key, version=version)
        del self._store[versioned_key]

    def delete_all(self) -> None:
        """Delete all data from the zarr store.

        Returns:
            None
        """
        if self._mode == "r":
            raise ValueError("Cannot delete from a read-only zarr store")

        for key in self._store.keys():
            del self._store[key]

    def update(
        self, key: str, version: str, dataset: xr.Dataset, compressor: Optional[Any], filters: Optional[Any]
    ) -> None:
        """Update the data at the given key.

        Args:
            key: Key of data in store
            version: Version of data
            dataset: xr.Dataset to add
            compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
            filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
        Returns:
            None
        """
        if self._mode == "r":
            raise ValueError("Cannot update a read-only zarr store")

        versioned_key = self._create_key(key=key, version=version)
        if versioned_key not in self._store:
            raise KeyError(f"Key {versioned_key} not found in zarr store")

        self.delete(key=key, version=version)
        self.add(key=key, version=version, dataset=dataset, compressor=compressor, filters=filters)

    def hash(self, data: str) -> str:
        """Hash the data at the given key"""
        raise NotImplementedError

    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        raise NotImplementedError

    def hash_equal(self, key: str, dataset: xr.Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        raise NotImplementedError

    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store"""
        return get_folder_size(folder_path=self.store_path())
