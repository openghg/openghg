from collections.abc import Callable, Iterable
import logging
from pathlib import Path
from typing import Any, cast, Generic, Literal, TypeVar

import xarray as xr
import zarr
import zarr.convenience
from zarr._storage.store import Store as AbstractZarrStore

from openghg.types import DataOverlapError
from openghg.util._versioning import SimpleVersioning
from ._indexing import ConflictDeterminer
from ._store import Store, UpdateError


logger = logging.getLogger("openghg.storage")
logger.setLevel(logging.DEBUG)


def parse_to_zarr_kwargs(to_zarr_kwargs: dict) -> dict:
    accepted_keys = ["write_empty_chunks", "zarr_format", "storage_options", "encoding"]
    result = {}
    for k, v in to_zarr_kwargs.items():
        if k in accepted_keys:
            result[k] = v
    return result


ZST = TypeVar("ZST", bound=AbstractZarrStore)


class ZarrStore(Store, Generic[ZST]):
    """Zarr store for storing a single dataset."""

    def __init__(
        self,
        zarr_store: ZST,
        append_dim: str = "time",
        index_options: dict | None = None,
        **to_zarr_kwargs: Any,
    ) -> None:
        """Pass an instantiated Zarr Store.

        Note: for commonly used types of ZarrStore, we can create convenience functions
        to create ZarrStore objects.

        Args:
            zarr_store: instantiated Zarr Store.
            append_dim: dimension to insert new data along.
            index_options: options for index, such as `method = "nearest"`
            to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.
              Not all parameters will be passed on. See here for the full description
              of the parameters: https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html

              Accepted arguments are:
              - `write_empty_chunks`
              - `zarr_format` (automatically inferred by default)
              - `storage_options`: only relevant to cloud storage, see
                 https://github.com/pydata/xarray/pull/5615
              - `encoding`: dictionary mapping data variables to encoding dictionary

        """
        super().__init__()
        self._store = zarr_store
        self.append_dim = append_dim
        self.index_options = index_options or {}
        self.to_zarr_kwargs = to_zarr_kwargs

    # use property to control assignment of `to_zarr_kwargs`
    @property
    def to_zarr_kwargs(self) -> dict:
        return self._to_zarr_kwargs

    @to_zarr_kwargs.setter
    def to_zarr_kwargs(self, value: dict) -> None:
        self._to_zarr_kwargs = parse_to_zarr_kwargs(value)

    @property
    def store(self) -> ZST:
        """Underlying Zarr storage."""
        return self._store

    @property
    def _conflict_determiner(self) -> ConflictDeterminer:
        index = self.get().get_index(self.append_dim)
        return ConflictDeterminer(index=index, **self.index_options)

    def __bool__(self) -> bool:
        return bool(self.store)

    def clear(self) -> None:
        self.store.rmdir()

    def bytes_stored(self) -> int:
        try:
            return cast(int, self.store.getsize())
        except AttributeError:
            return 0

    def get(self) -> xr.Dataset:
        if not bool(self):
            return xr.Dataset()

        # need to sort to be consistent with MemoryStore
        result = xr.open_zarr(self.store, consolidated=True).sortby(self.append_dim)
        return cast(xr.Dataset, result)

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        if not self.store:
            data.to_zarr(
                store=self.store,
                mode="w",
                consolidated=True,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
                **self.to_zarr_kwargs,
            )
        else:
            if self._conflict_determiner.has_conflicts(data.get_index(self.append_dim)):
                if on_conflict == "error":
                    raise DataOverlapError("Cannot insert data with conflicts if `on_conflict` == 'error'")

                # otherwise, select non-conflicts
                data = self._conflict_determiner.select_nonconflicts(data, self.append_dim)

            data.to_zarr(
                store=self.store,
                mode="a",
                append_dim=self.append_dim,
                consolidated=True,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
                safe_chunks=False,
                **self.to_zarr_kwargs,
            )

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        if not self.store:
            raise UpdateError("Cannot update empty Store.")
        else:
            if self._conflict_determiner.has_nonconflicts(data.get_index(self.append_dim)):
                if on_nonconflict == "error":
                    raise UpdateError("Cannot add new values with `update`.")

                # otherwise, select conflicts/overlapping values
                data = self._conflict_determiner.select_conflicts(data, self.append_dim)

            # nothing to update
            if not bool(data):
                logger.warning("No data to update with")
                return None

            try:
                data.to_zarr(
                    store=self.store,
                    mode="r+",
                    region="auto",
                    consolidated=True,
                    compute=True,
                    synchronizer=zarr.ThreadSynchronizer(),
                    safe_chunks=False,
                )
            except (ValueError, IndexError) as e:
                # possible issue with non-contiguous data
                raise NotImplementedError(
                    "Cannot update Zarr store, possibly due to non-contiguous data."
                    "Updating with non-contiguous data is currently not supported."
                ) from e


def get_zarr_directory_store(
    path: Path, append_dim: str = "time", index_options: dict | None = None, **to_zarr_kwargs: Any
) -> ZarrStore[zarr.DirectoryStore]:
    """Factory function to create ZarrStore objects based on a zarr.DirectoryStore.

    Args:
        path: path to Zarr store location.
        append_dim: dimension to append new data along.
        index_options: options for index along append dimension; for instance
          `method = "nearest"`.
        to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.

    Returns:
        ZarrStore based on zarr.DirectoryStore.

    """
    store = zarr.DirectoryStore(path)
    return ZarrStore[zarr.DirectoryStore](
        store, append_dim=append_dim, index_options=index_options, **to_zarr_kwargs
    )


def get_zarr_memory_store(
    append_dim: str = "time", index_options: dict | None = None, **to_zarr_kwargs: Any
) -> ZarrStore[zarr.MemoryStore]:
    """Factory function to create ZarrStore objects based on a zarr.MemoryStore.

    Args:
        append_dim: dimension to append new data along.
        index_options: options for index along append dimension; for instance
          `method = "nearest"`.
        to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.

    Returns:
        ZarrStore based on zarr.MemoryStore.

    """
    store = zarr.MemoryStore()
    return ZarrStore[zarr.MemoryStore](
        store, append_dim=append_dim, index_options=index_options, **to_zarr_kwargs
    )


class VersionedZarrStore(SimpleVersioning[ZST], ZarrStore[ZST]):
    """Zarr storage with versions.

    This class uses the methods from `ZarrStore` but overrides the `._store` attribute
    to point to the current version of the zarr store held by the `SimpleVersioning` class.

    Overriding `._store`
    """

    def __init__(
        self,
        factory: Callable[[str], ZST],
        versions: Iterable[str] | None = None,
        append_dim: str = "time",
        index_options: dict | None = None,
        **to_zarr_kwargs: Any,
    ) -> None:
        """Create VersionedZarrStore object.

        The `factory` and `versions` arguments are used to set up the
        versioning, and the remaining arguments are passed to `ZarrStore`.

        Args:
            factory: function that produces a zarr store, given a version. For
              instance, "v1" might map to a zarr directory store with path
              `root_path / "v1"`.
            versions: Versions to instantiate; these are loaded using the
              `factory` function.
            append_dim: dimension to append new data along.
            index_options: options for the index used to resolve conflicts when
              adding data to the store.
            to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.
              Not all parameters will be passed on. See here for the full description
              of the parameters: https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html

              Accepted arguments are:
              - `write_empty_chunks`
              - `zarr_format` (automatically inferred by default)
              - `storage_options`: only relevant to cloud storage, see
                 https://github.com/pydata/xarray/pull/5615
              - `encoding`: dictionary mapping data variables to encoding dictionary

        """
        super().__init__(
            factory=factory,
            versions=versions,
        )

        # manually set attributes for underlying ZarrStore (except for `self._store`,
        # which is delegated to the current version).
        self.append_dim = append_dim
        self.index_options = index_options or {}
        self.to_zarr_kwargs = parse_to_zarr_kwargs(to_zarr_kwargs)

    # make ._store an alias for ._current
    @property
    def _store(self) -> ZST:
        return self._current

    @_store.setter
    def _store(self, value: ZST) -> None:
        self._current = value

    def copy_to_version(self, v: str) -> None:
        """Copy current version to specified version.

        The version "v" is created if it doesn't exist, and is overwritten otherwise.

        This overrides the default method using `.deepcopy` to use Zarr's built in
        copying method.

        Args:
            v: version to copy to

        Raises:
            VersionError if no version is currently checked out.

        """
        if v not in self.versions:
            self._versions[v] = self.factory(v)
        source = self._current  # will raise VersionError if no version checked out
        dest = self._versions[v]
        zarr.convenience.copy_store(source, dest)


def get_versioned_zarr_directory_store(
    path: Path,
    versions: Iterable[str] | None = None,
    append_dim: str = "time",
    index_options: dict | None = None,
    **to_zarr_kwargs: Any,
) -> VersionedZarrStore[zarr.DirectoryStore]:
    """Factory function to create VersionedZarrStore objects based on a zarr.DirectoryStore.

    Args:
        path: root path where zarr `DirectoryStore`s will be based.
        versions: list of versions to load.
        append_dim: dimension to append new data along.
        index_options: options for the index used to resolve conflicts when
          adding data to the store.
        to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.

    Returns:
        VersionedZarrStore object with zarr.DirectoryStore as the underlying
        storage.

    """

    def factory(v: str) -> zarr.DirectoryStore:
        """Factory for versioning."""
        return zarr.DirectoryStore(path / v)

    return VersionedZarrStore[zarr.DirectoryStore](
        factory=factory,
        versions=versions,
        append_dim=append_dim,
        index_options=index_options,
        **to_zarr_kwargs,
    )


def get_versioned_zarr_memory_store(
    versions: Iterable[str] | None = None,
    append_dim: str = "time",
    index_options: dict | None = None,
    **to_zarr_kwargs: Any,
) -> VersionedZarrStore[zarr.MemoryStore]:
    """Factory function to create VersionedZarrStore objects based on a zarr.MemoryStore.

    Args:
        versions: list of versions to load.
        append_dim: dimension to append new data along.
        index_options: options for the index used to resolve conflicts when
          adding data to the store.
        to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.

    Returns:
        VersionedZarrStore object with zarr.MemoryStore as the underlying
        storage.

    """

    def factory(_: str) -> zarr.MemoryStore:
        """Factory for versioning."""
        return zarr.MemoryStore()

    return VersionedZarrStore[zarr.MemoryStore](
        factory=factory,
        versions=versions,
        append_dim=append_dim,
        index_options=index_options,
        **to_zarr_kwargs,
    )
