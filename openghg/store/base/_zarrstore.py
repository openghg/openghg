"""
So this is a prototype for using zarr with the 
"""
from __future__ import annotations
from typing import Union, Optional, MutableMapping
from types import TracebackType
from pydantic import BaseModel
from numcodecs import Blosc
from openghg.objectstore import set_object_from_json, exists, get_object_from_json
import xarray as xr
import zarr
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

# NOTE - this is a prototype

StoreLike = Union[zarr.storage.BaseStore, MutableMapping]


class ZarrStore(ABC):
    """Interface for our zarr stores."""

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
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
    def add(self, key: str, dataset: xr.Dataset):
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def delete(self, key: str):
        """Remove data from the zarr store"""
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
    # These should only be stored when the instance is created
    # and not be loaded in or stored to the object store
    # TODO - happy for this name to change
    # Maybe just replace this with store if we're only storing a few things?
    DO_NOT_STORE = ["_bucket", "_key", "_datakey", "_store"]
    _store_root = "zarrstore"

    def __init__(self, bucket: str, datasource_uuid: str) -> None:
        """ """
        self._bucket = bucket
        self._hashes = {}
        # TODO - move this to the objectstore submodule
        self._store_path = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/zarrstore"
        self._key = f"{bucket}/{LocalZarrStore._store_root}/{datasource_uuid}/metadata"
        self._store = zarr.storage.NestedDirectoryStore(self._store_path)

        if exists(bucket=self._bucket, key=self._key):
            result = get_object_from_json(self._bucket, key=self._key)
            self.__dict__.update(result)

    def __enter__(self):
        return self

    def __contains__(self, key: str) -> bool:
        raise NotImplementedError

    # def __iter__(self):
    #     yield from ...

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            logger.error(msg=f"{exc_type}: {exc_value}")
            logger.error(msg=f"Traceback:\n{traceback}")
        else:
            self.close()

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
        # TODO - check if the key already exists and then ???

        # TODO - pull the compression out so user can set
        zarr_compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)
        comp = {"compressor": zarr_compressor}
        encoding = {var: comp for var in dataset.data_vars}
        dataset.to_zarr(store=self._store, group=key, encoding=encoding)

    def delete(self, key: str):
        """Remove data from the zarr store"""
        raise NotImplementedError

    def hash(self, dataset: xr.Dataset) -> str:
        """Hash the data at the given key"""
        raise NotImplementedError

    def get_hash(self, key: str) -> str:
        """Get the hash of the data at the given key"""
        raise NotImplementedError

    def hash_equal(self, key: str, dataset: xr.Dataset) -> bool:
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        raise NotImplementedError
