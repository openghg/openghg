from __future__ import annotations

from collections.abc import Iterator
import logging
from pathlib import Path
from typing import Any, Literal, cast

import xarray as xr

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

        self._vzds = get_versioned_zarr_directory_store(path=self._stores_path)

    def __bool__(self) -> bool:
        """Return True if any version of the store is not empty."""
        return bool(self._vzds)

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
        try:
            self._vzds.checkout_version(version)
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e

        return self._vzds.get()  # pass option `consolidated=True`?

    def delete_version(self, version: str) -> None:
        """Delete a version from the store.

        Args:
            version: Data version
        Returns:
            None
        """
        self._check_writable()

        try:
            self._vzds.delete_version(version)
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e

    def delete_all(self) -> None:
        """Delete all data from the zarr store.

        Returns:
            None
        """
        self._check_writable()
        self._vzds.delete_all_versions()

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
        """Return the number of bytes stored in the zarr store.

        Returns:
            int: Number of bytes stored in zarr store
        """
        return self._vzds.bytes_stored()
