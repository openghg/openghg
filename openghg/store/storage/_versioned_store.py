from __future__ import annotations

from abc import ABC, abstractmethod, update_abstractmethods
from collections.abc import Iterator
from typing import Any, Callable, Generic, Optional, TypeVar

from xarray import Dataset

from ._store import Store


VersionType = TypeVar("VersionType")


class VersionError(Exception):
    pass


class VersionedStoreNew(Store, Generic[VersionType]):
    """Interface for Stores that keep track of versions.

    A VersionedStore has all of the methods that a Store has (such as `insert`, `update`, `delete`),
    but modifications will only be applied to the "current version".

    Checking out a different version will apply the Store methods to that version.

    Versions can be created, read, updated, and deleted.
    """

    @property
    @abstractmethod
    def versions(self) -> list[VersionType]:
        """List versions."""
        ...

    @abstractmethod
    def create_version(self, v: VersionType | None = None, checkout: bool = False) -> None:
        """Create new version.

        Initially, the new version will contain the same data as the current version.

        Args:
            v: tag for new version. If not specified, one will be created.
            checkout: if True, checkout the newly created version.

        Raises:
            ValueError if specfied version already exists.
        """
        if v in self.versions:
            raise ValueError(f"Cannot create version {v}; it already exists.")

    @property
    @abstractmethod
    def current_version(self) -> VersionType:
        """Return current version."""
        ...

    @property
    @abstractmethod
    def parent_version(self) -> VersionType:
        """Return parent of current version."""
        ...

    @abstractmethod
    def checkout_version(self, v: VersionType) -> None:
        """Set current version to given version."""
        ...

    def delete_all(self) -> None:
        """Delete all versions and remove any artefacts stored by the VersionedStore."""
        for v in self.versions:
            self.checkout_version(v)
            self.delete()


ST = TypeVar("ST", bound=Store)


class SimpleVersionedStore(VersionedStoreNew[str], Generic[ST]):
    """Note: `copy` only copies the current version to a new store."""
    def __init__(self, store_factory: Callable[[str], ST], versions: list[str] | None = None) -> None:
        super().__init__()
        self.factory = store_factory

        if versions is None:
            self._versions = {"v1": self.factory("v1")}
        else:
            # TODO: do we want to load all of the stores now?
            self._versions = {v: self.factory(v) for v in versions}

        self._current_version = self.latest_version

    @property
    def versions(self) -> list[str]:
        return list(self._versions.keys())

    @property
    def latest_version(self) -> str:
        return max(self.versions, key=lambda x: int(x[1:]))

    @property
    def current_version(self) -> str:
        return self._current_version

    @property
    def _current(self) -> ST:
        return self._versions[self.current_version]

    @property
    def parent_version(self) -> str:
        version_number = int(self._current_version[1:])
        if version_number == 1:
            raise VersionError(f"Version {self.current_version} has no parent.")
        return f"v{version_number - 1}"

    def create_version(self, v: str | None = None, checkout: bool = False, copy_current: bool = True) -> None:
        """
        Create a new version.

        Note: version string `v` is ignored.

        New versions can only be created starting after the latest version.
        The contents of the current version will be copied to the newly created version
        """
        if not self._versions:
            v = "v1"
            copy_current = False
            checkout = True
        else:
            # ignore `v`, if specified; new versions in SimpleVersionedStore always come after
            # latest version
            latest_version_number = int(self.latest_version[1:])
            v = f"v{latest_version_number + 1}"

        super().create_version(v)  # error checking

        self._versions[v] = self.factory(v)

        if copy_current:
            self._current.copy(self._versions[v])

        if checkout:
            self.checkout_version(v)

    def checkout_version(self, v: str) -> None:
        if v not in self.versions:
            raise ValueError(f"Version {v} does not exist.")
        self._current_version = v

    def __bool__(self) -> bool:
        return bool(self._current)

    def clear(self) -> None:
        self._current.clear()

    def delete(self) -> None:
        if self.current_version != self.latest_version:
            raise VersionError("Only the latest version can be deleted. Checkout latest version first.")

        try:
            second_latest_version = self.parent_version
        except VersionError:
            second_latest_version = None

        latest_version = self.latest_version
        self._current.delete()
        del self._versions[latest_version]

        if second_latest_version:
            self.checkout_version(second_latest_version)

    @property
    def index(self) -> DatetimeStoreIndex:
        return self._current.index

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        self._current.insert(data, on_conflict=on_conflict)

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        self._current.update(data, on_nonconflict=on_nonconflict)

    def get(self) -> xr.Dataset:
        return self._current.get()

    def copy(self, other: Store) -> None:
        """Note: this only copies the current version to another store."""
        self._current.copy(other)


class VersionedStore(ABC):
    """Interface for storing data in a Datasource. This may be in a zarr directory
    store, compressed NetCDF, a sparse storage format or others."""

    @abstractmethod
    def add(
        self,
        version: str,
        dataset: Dataset,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def delete_version(self, version: str) -> None:
        """Delete a version from the store"""
        pass

    @abstractmethod
    def delete_all(self) -> None:
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def keys(self, version: str) -> Iterator[str]:
        """Keys of data stored in the zarr store"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the zarr store."""
        pass

    @abstractmethod
    def store_key(self, version: str) -> str:
        """Return the key of this zarr store"""
        pass

    @abstractmethod
    def version_exists(self, version: str) -> bool:
        """Check if a version exists in the current store"""
        pass

    @abstractmethod
    def get(self, version: str) -> Dataset:
        """Get the version of the dataset stored in the zarr store."""
        pass

    @abstractmethod
    def _pop(self, version: str) -> Dataset:
        """Pop some data from the store. This removes the data at this version from the store
        and returns it."""
        pass

    @abstractmethod
    def update(
        self, version: str, dataset: Dataset, compressor: Optional[Any], filters: Optional[Any]
    ) -> None:
        """Update the data at the given key"""
        pass

    @abstractmethod
    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the zarr store"""
        pass
