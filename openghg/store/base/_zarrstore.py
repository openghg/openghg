"""
So this is a prototype for using zarr with the
"""
from __future__ import annotations
from typing import Dict, Union, Optional, MutableMapping
from types import TracebackType
from pydantic import BaseModel
from numcodecs import Blosc
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
    def delete_multiple(self, keys: list[str]):
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


class LocalZarrStore(zarr.storage.NestedDirectoryStore):
    # These should only be stored when the instance is created
    # and not be loaded in or stored to the object store
    # TODO - happy for this name to change
    # Maybe just replace this with store if we're only storing a few things?
    DO_NOT_STORE = ["_bucket", "_key", "_store", "_to_be_replaced", "_pop_keys"]
    _store_root = "zarr_data"

    def __init__(self, bucket: str, datasource_uuid: str) -> None:
        """ """
        store_path = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/zarrstore"
        key = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/metadata"

        super().__init__(store_path)

        # Store hashes of Datasets added to the store for easy checking
        self._hashes = {}

        # Does this need to be stored? Maybe for dataset hashes?
        if exists(bucket=bucket, key=key):
            result = get_object_from_json(bucket, key=key)
            self.__dict__.update(result)

        self._bucket = bucket
        self._key = key
        # TODO - move this to the objectstore submodule
        self._key = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/metadata"
        self._store = zarr.storage.NestedDirectoryStore(store_path)
        self._pop_keys = []

    def _create_key(self, key: str, version: str) -> str:
        """Create a key for the zarr store."""
        return f"{key}/{version}"

    def close(self):
        """Close object store connection.
        This closes the metastore and writes internal metadata.
        If an Datastore is used without a context manager
        ("with" statement), then it must be closed manually.
        """
        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in self.DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self._key, data=internal_metadata)

    def add(self, key: str, version: str, dataset: xr.Dataset):
        """Add an xr.Dataset to the zarr store."""
        from openghg.store.spec import get_zarr_encoding
        from openghg.util import daterange_overlap

        if self._pop_keys:
            first = self._pop_keys.pop(0)
            if not daterange_overlap(first, key):
                raise ValueError("Cannot add this key as we expect it to overlap a popped key")

        versioned_key = self._create_key(key=key, version=version)
        # TODO - check if the key already exists and then ?
        if versioned_key in self._store:
            raise KeyExistsError("Cannot overwrite key in zarr store using add - use update method")

        encoding = get_zarr_encoding(data_vars=dataset.data_vars)
        dataset.to_zarr(store=self._store, group=versioned_key, encoding=encoding)

    def pop_but_not(self, key: str, version: str) -> xr.Dataset:
        """Pop (but not actually remove, just add to a list to be removed) some data from the store."""
        # We'll need to replace or delete these keys before closing the store
        versioned_key = self._create_key(key=key, version=version)
        self._pop_keys.append(versioned_key)
        return xr.open_zarr(store=self._store, group=versioned_key)

    def get(self, key: str, version: str) -> xr.Dataset:
        """Get an xr.Dataset from the zarr store."""
        versioned_key = self._create_key(key=key, version=version)
        return xr.open_zarr(store=self._store, group=versioned_key)

    def latest_keys(self):
        """Return the latest keys in the store"""
        return self.version_keys(version="latest")

    def version_keys(self, version: str):
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

    def delete(self, key: str):
        """Remove data from the zarr store"""
        del self._store[key]

    def delete_multiple(self, keys: list[str]):
        """Remove multiple keys from the zarr store"""
        for key in keys:
            del self._store[key]

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
