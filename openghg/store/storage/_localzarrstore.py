"""
A zarr store on the local filesystem. This is used by Datasource to handle the
storage of data on the local filesystem and different versions of data.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Literal, Iterator, Union, Optional, MutableMapping
import xarray as xr
import zarr
import os
import re
import shutil

from openghg.types import ZarrStoreError
from pathlib import Path
from openghg.objectstore import get_folder_size
from openghg.store.storage import Store

logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)

StoreLike = Union[zarr.storage.BaseStore, MutableMapping]


class LocalZarrStore(Store):
    def __init__(self, bucket: str, datasource_uuid: str, mode: Literal["rw", "r"] = "rw") -> None:
        self._bucket = bucket
        self._mode = mode
        self._root_store_key = f"data/{datasource_uuid}/zarr"
        self._stores_path = Path(bucket, self._root_store_key).expanduser().resolve()

        # QUESTION - is this better than storing more JSON with the path names?
        # It means we don't have to worry about saving of the store
        # and is similar to the way that zarr does it with their directory stores
        self._stores = {}
        if not self._stores_path.exists():
            self._stores_path.mkdir(parents=True)
        else:
            compiled_reg = re.compile(r"v\d+")
            for f in sorted(os.listdir(self._stores_path)):
                if compiled_reg.match(str(f)):
                    full_path = Path(self._stores_path, f).expanduser().resolve()
                    self._stores[f] = zarr.storage.NestedDirectoryStore(full_path)

        self._memory_store: Dict[str, bytes] = {}

    def __bool__(self) -> bool:
        return any(self._stores)

    def keys(self, version: str) -> Iterator[str]:
        """Keys of data stored in the zarr store.

        Returns:
            Generator: Generator object
        """
        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        iterator: Iterator = self._stores[version].keys()
        return iterator

    def close(self) -> None:
        """Close the zarr store.

        Returns:
            None
        """
        for store in self._stores.values():
            store.close()

    def store_key(self, version: str) -> str:
        """Return the key of this zarr Store

        Returns:
            str: Key of zarr store
        """
        if version.lower() not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        return str(Path(self._root_store_key, version))

    def store_path(self, version: str) -> Path:
        """Return the path of this zarr Store

        Returns:
            Path: Path of zarr store
        """
        return Path(self._stores_path, version)

    def version_exists(self, version: str) -> bool:
        """Check if a version exists in the current store

        Args:
            version: Version e.g. v0, v1
        Returns:
            bool: True if version exists
        """
        return version.lower() in self._stores

    def add(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        append_dim: str = "time",
    ) -> None:
        """Add an xr.Dataset to the zarr store.

        Args:
            key: Key to add data under
            version: Version of data to add
            dataset: xr.Dataset to add
            compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
            Defaults to using the Blosc compressor with ztd compression level 5
            filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
        Returns:
            None
        """
        from openghg.store.storage import get_zarr_encoding

        version = version.lower()

        if self._mode == "r":
            raise PermissionError("Cannot add to a read-only zarr store")

        # Used to append new data to the current version's zarr store
        if version in self._stores:
            dataset.to_zarr(
                store=self._stores[version], mode="a", consolidated=True, append_dim=append_dim, compute=True
            )
        else:
            if not self._stores and version != "v0":
                raise ValueError("First version must be v0")

            self._stores[version] = zarr.storage.NestedDirectoryStore(self.store_path(version=version))
            encoding = get_zarr_encoding(data_vars=dataset.data_vars, filters=filters, compressor=compressor)
            dataset.to_zarr(
                store=self._stores[version], mode="w", encoding=encoding, consolidated=True, compute=True
            )

    def get(self, version: str) -> xr.Dataset:
        """Open the version of the dataset stored in the zarr store.
        This should only be used when no data will be changed in the store
        as changes to data in the store will result in errors with an open
        dataset.

        Note that this function should not be used if the store is to be modified.
        Please use copy_to_memorystore for that otherwise data may be lost.

        Args:
            version: Version of data to get
        Returns:
            xr.Dataset: Dataset from the store
        """
        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        store = self._stores[version]
        ds: xr.Dataset = xr.open_zarr(store=store, consolidated=True)
        return ds

    def pop(self, version: str) -> xr.Dataset:
        """Pop some data from the store. This copies the data in the version specified
        to a memory store, deletes the version and returns an xarray Dataset loaded from the
        memory store.

        Note that this will delete the data from the store on the local filesystem.

        Args:
            version: Version of data
        Returns:
            Dataset: Dataset popped from the store
        """
        if self._mode == "r":
            raise PermissionError("Cannot pop from a read-only zarr store")

        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        self._memory_store.clear()
        store = self._stores[version]
        # Let's copy the data we want to pop into a memory store and return it from there
        zarr.convenience.copy_store(source=store, dest=self._memory_store)
        self.delete_version(version=version)
        ds: xr.Dataset = xr.open_zarr(store=self._memory_store)
        return ds

    def copy_to_memorystore(self, version: str) -> Dict:
        """Copies the compressed data from the filesystem store to an in-memory store.
        This preserves the compression and chunking of the data and the store
        can be opened as a single dataset.

        Note that this function should be used if the store is to be modified.

        Args:
            version: Version of data to copy
        Returns:
            Dict: In-memory copy of compressed data
        """
        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        store = self._stores[version]
        mem_store: Dict[str, bytes] = {}
        zarr.copy_store(source=store, dest=mem_store)
        return mem_store

    def delete(self, key: str, version: str) -> None:
        """Remove a specific piece of data from the zarr store. The key will need to be
        retrieved from the keys method of the store.

        Args:
            key: Key of data in store
            version: Version of data
        Returns:
            None
        """
        if self._mode == "r":
            raise PermissionError("Cannot delete from a read-only zarr store")

        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        try:
            del self._stores[version][key]
        except KeyError:
            raise ZarrStoreError(f"Key {key} not found in zarr store")

    def delete_version(self, version: str) -> None:
        """Delete a version from the store

        Args:
            version: Version to delete
        Returns:
            None
        """
        version = version.lower()
        if self._mode == "r":
            raise PermissionError("Cannot delete from a read-only zarr store")

        if version not in self._stores:
            raise KeyError(f"Invalid version - {version}")

        path = self.store_path(version=version)
        shutil.rmtree(path)
        del self._stores[version]

    def delete_all(self) -> None:
        """Delete all data from the zarr store.

        Returns:
            None
        """
        if self._mode == "r":
            raise PermissionError("Cannot delete a read-only zarr store")

        self._stores.clear()
        shutil.rmtree(self._stores_path, ignore_errors=True)

    def update(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Update a version of the data. This first deletes the current version
        and then adds the new version. To update a version in place, keeping the current data,
        use the append function.

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
            raise PermissionError("Cannot update a read-only zarr store")

        self.delete_version(version=version)
        self.add(version=version, dataset=dataset, compressor=compressor, filters=filters)

    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store

        Returns:
            int: Number of bytes stored in zarr store
        """
        bytes_stored = 0
        for version in self._stores:
            bytes_stored += get_folder_size(folder_path=self.store_path(version=version))

        return bytes_stored

    def get_chunking(self, version: str) -> Dict:
        """Get the chunking of a version of the data in the store

        Args:
            version: Version of data
        Returns:
            dict: Chunking of data
        """
        if version.lower() not in self._stores:
            raise KeyError(f"Invalid version - {version}")
        return dict(self.get(version=version).chunks)

    def _check_chunking(self, dataset: xr.Dataset, version: str) -> None:
        """Ensure that chunks of incoming data and that already stored are the same

        Args:
            version: Version of data
        Returns:
            None
        """
        raise NotImplementedError("This function is not yet fully implemented.")

        incoming_chunks = dict(dataset.chunks)
        if not incoming_chunks:
            return None

        stored_chunks = dict(self.get(version=version).chunks)
        # Only take chunks that aren't whole dimensions
        actually_chunked = {k: v for k, v in stored_chunks.items() if len(v) > 1}

        errors = []
        for k, v in incoming_chunks.items():
            if k not in actually_chunked:
                errors.append(f"Chunking for {k} not found in stored data")

            if v != max(stored_chunks[k]):
                error = f"For {k} we have {v} but stored data has {max(v)}"
                errors.append(error)

        if errors:
            suggestion = f"Current chunking of data: {actually_chunked}"
            raise ValueError("\n".join(errors))

        return None
