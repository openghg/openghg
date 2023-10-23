"""
So this is a prototype for using zarr with the
"""
from __future__ import annotations
from typing import Dict, Literal, List, Union, Optional, MutableMapping
from types import TracebackType
from pydantic import BaseModel
from numcodecs import Blosc
import collections
from openghg.objectstore import set_object_from_json, exists, get_object_from_json
import xarray as xr
import zarr
import logging
from abc import ABC, abstractmethod

from openghg.types import KeyExistsError

logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

# NOTE - this is a prototype

StoreLike = Union[zarr.storage.BaseStore, MutableMapping]


class ZarrStore(ABC):
    """Interface for our zarr stores."""

    @abstractmethod
    def add(self, key: str, dataset: xr.Dataset):
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def add_multiple(self, datasets: Dict[str, xr.Dataset]):
        """Add multiple xr.Datasets to the zarr store."""
        pass

    @abstractmethod
    def update(self, key: str, dataset: xr.Dataset):
        """Update the data at the given key"""
        pass

    @abstractmethod
    def update_multiple(self, datasets: Dict[str, xr.Dataset]):
        """Update multiple xr.Datasets in the zarr store."""
        pass

    @abstractmethod
    def delete(self, key: str):
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def delete_multiple(self, keys: List[str]):
        """Remove multiple keys from the zarr store"""
        pass

    @abstractmethod
    def hash(self, data: str) -> str:
        """Hash the data at the given key"""
        pass

    @abstractmethod
    def hash_multiple(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, str]:
        """Hash multiple xr.Datasets"""
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


class LocalZarrStore:
    _store_root = "zarr_data"

    def __init__(self, bucket: str, datasource_uuid: str, mode: Literal["rw", "r"] = "rw") -> None:
        store_path = f"{bucket}/data/{datasource_uuid}/zarr"

        self._mode = mode
        self._bucket = bucket
        self._store = zarr.storage.NestedDirectoryStore(store_path)
        self._pop_keys = collections.deque()
        self._memory_store = {}

    def __bool__(self) -> bool:
        return bool(self._store)

    def _create_key(self, key: str, version: str) -> str:
        """Create a key for the zarr store."""
        return f"{version}/{key}"

    def add(self, key: str, version: str, dataset: xr.Dataset):
        """Add an xr.Dataset to the zarr store."""
        from openghg.store.spec import get_zarr_encoding

        if self._mode == "r":
            raise ValueError("Cannot add to a read-only zarr store")

        versioned_key = self._create_key(key=key, version=version)
        # TODO - check if the key already exists and then ?
        if versioned_key in self._store:
            raise KeyExistsError("Cannot overwrite key in zarr store using add - use update method")

        encoding = get_zarr_encoding(data_vars=dataset.data_vars)
        dataset.to_zarr(store=self._store, group=versioned_key, encoding=encoding)

    def pop(self, key: str, version: str) -> xr.Dataset:
        """Pop (but not actually remove, just add to a list to be removed) some data from the store."""
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

    def get(self, key: str, version: str) -> xr.Dataset:
        """Get an xr.Dataset from the zarr store."""
        versioned_key = self._create_key(key=key, version=version)
        return xr.open_zarr(store=self._store, group=versioned_key)

    def delete(self, key: str, version: str):
        """Remove data from the zarr store"""
        if self._mode == "r":
            raise ValueError("Cannot delete from a read-only zarr store")

        versioned_key = self._create_key(key=key, version=version)
        del self._store[versioned_key]

    def delete_multiple(self, keys: List[str]):
        """Remove multiple keys from the zarr store"""
        for key in keys:
            del self._store[key]

    def latest_keys(self):
        """Return the latest keys in the store"""
        raise NotImplementedError
        return self.version_keys(version="latest")

    def version_keys(self, version: str):
        raise NotImplementedError
        return [k for k in list(self._store.keys()) if version in k]

    def add_multiple(self, version: str, datasets: Dict[str, xr.Dataset]):
        """Add multiple xr.Datasets to the zarr store."""
        for key, dataset in datasets.items():
            self.add(key=key, version=version, dataset=dataset)

    def update(self, key: str, dataset: xr.Dataset):
        """Update the data at the given key"""
        pass

    def update_multiple(self, datasets: Dict[str, xr.Dataset]):
        """Update multiple xr.Datasets in the zarr store."""
        pass

    def hash(self, data: str) -> str:
        """Hash the data at the given key"""
        raise NotImplementedError

    def hash_multiple(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, str]:
        """Hash multiple xr.Datasets"""
        raise NotImplementedError

    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        pass

    def hash_equal(self, key: str, dataset: xr.Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        pass

    def bytes_stored(self):
        """Return the number of bytes stored in the zarr store"""
        pass
