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
    DO_NOT_STORE = ["_bucket", "_key", "_store"]
    _store_root = "zarrstore"

    def __init__(self, bucket: str, datasource_uuid: str) -> None:
        """ """
        store_path = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/zarrstore"
        key = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/metadata"

        super().__init__(store_path)

        if exists(bucket=bucket, key=key):
            result = get_object_from_json(self._bucket, key=key)
            self.__dict__.update(result)

        self._bucket = bucket
        self._key = key
        # TODO - move this to the objectstore submodule
        self._key = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/metadata"
        self._store = zarr.storage.NestedDirectoryStore(store_path)

    def close(self):
        """Close object store connection.
        This closes the metastore and writes internal metadata.
        If an Datastore is used without a context manager
        ("with" statement), then it must be closed manually.
        """
        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in self.DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self._key, data=internal_metadata)

    def add(self, key: str, dataset: xr.Dataset):
        """Add an xr.Dataset to the zarr store."""

        # Should we add to an in memory store and then write to disk?
        # Could have a limit of a number of keys added and then flush to disk
        # TODO - check if the key already exists and then ?
        if key in self._store:
            raise KeyExistsError("Cannot overwrite key in zarr store using add - use update method")

        # TODO - pull the compression out so user can set
        zarr_compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)
        comp = {"compressor": zarr_compressor}
        encoding = {var: comp for var in dataset.data_vars}
        dataset.to_zarr(store=self._store, group=key, encoding=encoding)

    def get(self, key: str) -> xr.Dataset:
        """Get an xr.Dataset from the zarr store."""
        return xr.open_zarr(store=self._store, group=key)

    def add_multiple(self, datasets: Dict[str, xr.Dataset]):
        """Add multiple xr.Datasets to the zarr store."""
        for key, dataset in datasets.items():
            self.add(key=key, dataset=dataset)

    def update(self, key: str, dataset: xr.Dataset):
        """Update the data at the given key"""
        pass

    def update_multiple(self, datasets: Dict[str, xr.Dataset]):
        """Update multiple xr.Datasets in the zarr store."""
        pass

    def delete(self, key: str):
        """Remove data from the zarr store"""
        pass

    def delete_multiple(self, keys: list[str]):
        """Remove multiple keys from the zarr store"""
        pass

    def hash(self, data: str) -> str:
        """Hash the data at the given key"""
        pass

    def hash_multiple(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, str]:
        """Hash multiple xr.Datasets"""
        pass

    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        pass

    def hash_equal(self, key: str, dataset: xr.Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        pass

    def bytes_stored(self):
        """Return the number of bytes stored in the zarr store"""
        pass
