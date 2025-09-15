from __future__ import annotations

from collections.abc import Iterator
import logging
import os
from pathlib import Path
import re
import shutil
from typing import Any, Literal, cast

import xarray as xr
import zarr

from openghg.objectstore import get_folder_size
from openghg.storage import get_versioned_zarr_directory_store
from openghg.store.storage._encoding import get_zarr_encoding
from openghg.store.storage._store import Store
from openghg.types import ZarrStoreError


logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)


class LocalZarrStore(Store):
    def __init__(self, bucket: str, datasource_uuid: str, mode: Literal["rw", "r"] = "rw") -> None:
        self._bucket = bucket
        self._mode = mode
        self._datasource_uuid = datasource_uuid
        self._root_store_key = f"data/{datasource_uuid}/zarr"
        self._stores_path = Path(bucket, self._root_store_key).expanduser().resolve()

        # Here we ensure we have the correct directory structure for the zarr stores
        # and do a lookup for existing version of the store
        versions = []

        if not self._stores_path.exists():
            self._stores_path.mkdir(parents=True)
        else:
            compiled_reg = re.compile(r"v\d+")
            for f in sorted(self._stores_path.iterdir()):
                if compiled_reg.match(str(f.name)):
                    versions.append(f.name)

        self._vzds = get_versioned_zarr_directory_store(path=self._stores_path, versions=versions)

    def __bool__(self) -> bool:
        """Return True if any version of the store is not empty."""
        return any(self._vzds._versions.values())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._bucket}, {self._datasource_uuid}, {self._mode})"

    def version_exists(self, version: str) -> bool:
        """Check if a version exists in the current store.

        Args:
            version: Data version

        Returns:
            bool: True if version exists
        """
        return version.lower() in self._vzds.versions

    def _check_version(self, version: str) -> str:
        """Check if the given version exists in the store.

        Args:
            version: Data version

        Returns:
            str: Lowercase version

        Raises:
            ZarrStoreError if the version does not exist.

        """
        if not self.version_exists(version):
            raise ZarrStoreError(f"Invalid version - {version}")

        return version.lower()

    def _check_writable(self) -> None:
        """Check if the store is writable."""
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only zarr store")

    def keys(self, version: str) -> Iterator[str]:
        """Keys of data stored in the zarr store.

        Args:
            version: Data version

        Returns:
            Iterator over keys stored in zarr store with given version.

        """
        version = self._check_version(version)
        self._vzds.checkout_version(version)
        return cast(Iterator, self._vzds.store.keys())

    def close(self) -> None:
        """Close the zarr store.

        Returns:
            None
        """
        # zarr directory stores do not need to be closed
        pass

    def store_path(self, version: str) -> Path:
        """Return the path of this zarr Store.

        Args:
            version: Data version

        Returns:
            Path: Path of zarr store

        """
        version = self._check_version(version)

        return self._stores_path / version

    def add(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Any | None = None,
        filters: Any | None = None,
        append_dim: str = "time",
    ) -> None:
        """Add an xr.Dataset to the zarr store.

        Args:
            version: Data version
            dataset: xr.Dataset to add
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
            append_dim: Dimension to append to

        Returns:
            None

        Raises:
            ValueError if first version is not "v1".

        """
        if self._vzds.append_dim != append_dim:
            logger.warning(f"Updating LocalZarrStore append dim. to {append_dim}.")
            self._vzds.append_dim = append_dim

        self._check_writable()
        version = version.lower()

        # Append new data to the zarr store for the current version
        if self.version_exists(version):
            self._vzds.checkout_version(version)
        # Otherwise we create a new zarr Store for the version
        else:
            if not self._vzds.versions and version != "v1":
                raise ValueError("First version must be v1")
            self._vzds.create_version(version, checkout=True)

            # set encoding
            # TODO: make this a method; do we want to overwrite or update?
            encoding = get_zarr_encoding(data_vars=dataset.data_vars, filters=filters, compressor=compressor)
            self._vzds.to_zarr_kwargs = {"encoding": encoding}

        self._vzds.insert(dataset)

    def get(self, version: str) -> xr.Dataset:
        """Get the version of the dataset stored in the zarr store.

        Args:
            version: Data version
        Returns:
            xr.Dataset: Dataset from the store
        """
        version = self._check_version(version)
        self._vzds.checkout_version(version)
        return self._vzds.get()  # pass option `consolidated=True`?

    def delete_version(self, version: str) -> None:
        """Delete a version from the store

        Args:
            version: Data version
        Returns:
            None
        """
        self._check_writable()
        version = self._check_version(version)
        self._vzds.delete_version(version)

    def delete_all(self) -> None:
        """Delete all data from the zarr store.

        Returns:
            None
        """
        self._check_writable()

        for version in self._vzds.versions:
            self._vzds.delete_version(version)

        if self._stores_path.exists():
            self._stores_path.rmdir()

    def overwrite(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> None:
        """Update a version of the data.

        This first deletes the current version and then adds the new version.
        To update a version in place, keeping the current data, use the append function.

        Args:
            version: Data version
            dataset: Dataset to add
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding

        Returns:
            None

        """
        self._check_writable()
        self._vzds.checkout_version(version.lower())

        # set encoding
        encoding = get_zarr_encoding(data_vars=dataset.data_vars, filters=filters, compressor=compressor)
        self._vzds.to_zarr_kwargs = {"encoding": encoding}

        self._vzds.overwrite(dataset)

    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store

        Returns:
            int: Number of bytes stored in zarr store
        """
        bytes_stored = 0
        for version in self._vzds.versions:
            bytes_stored += get_folder_size(folder_path=self.store_path(version=version))

        return bytes_stored


class LocalZarrStore2(Store):
    """A zarr based data store on the local filesystem.
    This is used by Datasource to handle the storage of versioned data.

    Args:
        bucket: path to object store bucket
        datasource_uuid: Datasource UUID
        mode: Opening mode for store, "rw" for read-write, "r" for read-only
    """

    def __init__(self, bucket: str, datasource_uuid: str, mode: Literal["rw", "r"] = "rw") -> None:
        self._bucket = bucket
        self._mode = mode
        self._root_store_key = f"data/{datasource_uuid}/zarr"
        self._stores_path = Path(bucket, self._root_store_key).expanduser().resolve()

        # Here we ensure we have the correct directory structure for the zarr stores
        # and do a lookup for existing stores, populating the _stores dictionary.
        self._stores = {}
        if not self._stores_path.exists():
            self._stores_path.mkdir(parents=True)
        else:
            compiled_reg = re.compile(r"v\d+")
            for f in sorted(os.listdir(self._stores_path)):
                if compiled_reg.match(str(f)):
                    full_path = Path(self._stores_path, f).expanduser().resolve()
                    self._stores[f] = zarr.storage.DirectoryStore(full_path)

    def __bool__(self) -> bool:
        return any(self._stores)

    def _check_version(self, version: str) -> str:
        """Check if the given version exists in the store.
        Raises a ZarrStoreError if the version does not exist.

        Args:
            version: Data version
        Returns:
            str: Lowercase version
        """
        if version.lower() not in self._stores:
            raise ZarrStoreError(f"Invalid version - {version}")

        return version.lower()

    def _check_writable(self) -> None:
        """Check if the store is writable

        Returns:
            None
        """
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only zarr store")

    def keys(self, version: str) -> Iterator[str]:
        """Keys of data stored in the zarr store.

        Args:
            version: Data version
        Returns:
            Generator: Generator object
        """
        version = self._check_version(version)
        keys: Iterator = self._stores[version].keys()
        return keys

    def close(self) -> None:
        """Close the zarr store.

        Returns:
            None
        """
        for store in self._stores.values():
            store.close()

    def store_path(self, version: str) -> Path:
        """Return the path of this zarr Store

        Args:
            version: Data version
        Returns:
            Path: Path of zarr store
        """
        return Path(self._stores_path, version)

    def version_exists(self, version: str) -> bool:
        """Check if a version exists in the current store

        Args:
            version: Data version
        Returns:
            bool: True if version exists
        """
        return version.lower() in self._stores

    def add(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Any | None = None,
        filters: Any | None = None,
        append_dim: str = "time",
    ) -> None:
        """Add an xr.Dataset to the zarr store.

        Args:
            version: Data version
            dataset: xr.Dataset to add
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
            append_dim: Dimension to append to
        Returns:
            None
        """
        from openghg.store.storage import get_zarr_encoding

        self._check_writable()
        version = version.lower()

        # Append new data to the zarr store for the current version
        if version in self._stores:
            # We want to ensure the chunking of the incoming data matches the chunking of the stored data
            chunking = self.match_chunking(version=version, dataset=dataset)
            if chunking:
                dataset = dataset.chunk(chunking)

            dataset.to_zarr(
                store=self._stores[version],
                mode="a",
                consolidated=True,
                append_dim=append_dim,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
                safe_chunks=False,
            )
        # Otherwise we create a new zarr Store for the version
        else:
            if not self._stores and version != "v1":
                raise ValueError("First version must be v1")

            self._stores[version] = zarr.storage.DirectoryStore(self.store_path(version=version))
            encoding = get_zarr_encoding(data_vars=dataset.data_vars, filters=filters, compressor=compressor)
            dataset.to_zarr(
                store=self._stores[version],
                mode="w",
                encoding=encoding,
                consolidated=True,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
            )

    def get(self, version: str) -> xr.Dataset:
        """Get the version of the dataset stored in the zarr store.

        Args:
            version: Data version
        Returns:
            xr.Dataset: Dataset from the store
        """
        version = self._check_version(version)
        store = self._stores[version]
        ds: xr.Dataset = xr.open_zarr(store=store, consolidated=True)
        return ds

    def delete_version(self, version: str) -> None:
        """Delete a version from the store

        Args:
            version: Data version
        Returns:
            None
        """
        self._check_writable()
        version = self._check_version(version)

        path = self.store_path(version=version)
        shutil.rmtree(path)
        del self._stores[version]

    def delete_all(self) -> None:
        """Delete all data from the zarr store.

        Returns:
            None
        """
        self._check_writable()
        self._stores.clear()

        # make sure we're not deleting too much...
        bucket_path = Path(self._bucket).expanduser().resolve()
        if self._stores_path.parent not in [bucket_path, bucket_path / "data"]:
            shutil.rmtree(self._stores_path.parent, ignore_errors=True)
        else:
            shutil.rmtree(self._stores_path, ignore_errors=True)

    def overwrite(
        self,
        version: str,
        dataset: xr.Dataset,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> None:
        """Update a version of the data. This first deletes the current version
        and then adds the new version. To update a version in place, keeping the current data,
        use the append function.

        Args:
            version: Data version
            dataset: Dataset to add
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
        Returns:
            None
        """
        self._check_writable()
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

    def match_chunking(self, version: str, dataset: xr.Dataset) -> dict[str, int]:
        """Ensure that chunks of incoming data match the chunking of the stored data.

        If no chunking is found then an empty dictionary is returned.
        If there is no mismatch then an empty dictionary is returned.
        Returns the chunking scheme of the stored data if there is a mismatch.

        Args:
            version: Data version
            dataset: Incoming dataset
        Returns:
            dict: Chunking scheme
        """
        version = self._check_version(version)

        incoming_chunks = dict(dataset.unify_chunks().chunks)
        incoming_actually_chunked = {k: max(v) for k, v in incoming_chunks.items() if len(v) > 1}

        stored_chunks = {str(k): v for k, v in self.get(version=version).chunksizes.items()}
        # Only take chunks that aren't whole dimensions
        stored_actually_chunked = {k: max(v) for k, v in stored_chunks.items() if len(v) > 1}

        if not incoming_actually_chunked and not stored_actually_chunked:
            return {}

        if incoming_actually_chunked != stored_actually_chunked:
            if not incoming_actually_chunked:
                msg = (
                    f"Incoming data is not chunked, using stored chunking schema: {stored_actually_chunked}."
                )
            else:
                msg = (
                    f"Chunking scheme of incoming data ({incoming_actually_chunked}) does not match stored data."
                    + f"\nUsing stored chunking schema: {stored_actually_chunked}."
                    + "\nThis may result in an increased processing time. Not passing the chunks argument may be faster."
                )

            logger.warning(msg)

            # For coordinates we haven't chunked over we'll use the full size
            for k in dataset.dims:
                k = str(k)  # xr.Dataset.dims returns a Mapping with Hashable keys, which may not be strings
                if k not in stored_actually_chunked:
                    stored_actually_chunked[k] = dataset.sizes[k]

            return stored_actually_chunked

        return {}

    # TODO: implement attribute matching for zarr store
    def match_attributes(self, version: str, dataset: xr.Dataset) -> dict:
        """Ensure the attributes of the stored and incoming data are matched,
        any attributes that differ will be added as a new key with a number appended.
        e.g. author_1 for a differing author value.

        Args:
            version: Data version
            dataset: Incoming dataset
        Returns:
            dict: Dictionary of compared and updated attributes
        """
        raise NotImplementedError()
