"""Storage for a single Xarray Dataset.

This module contains the interface (abstract base class) `Store`, as well
as two concrete implementations:
- `MemoryStore`
- `VersionedMemoryStore`

These concrete implementations are mainly used for testing.
Their value is that they produce the same results as the Zarr implementations of
the `Store` interface, using Xarray operations.

"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Literal

import pandas as pd
import xarray as xr

from openghg.types import DataOverlapError
from openghg.util._versioning import SimpleVersioning
from ._indexing import ConflictDeterminer


# TODO: this should be in openghg.types (?)
class UpdateError(Exception): ...


class Store(ABC):
    """Interface for means of storing a single dataset."""

    @abstractmethod
    def __bool__(self) -> bool:
        """Return True is Store is not empty."""
        ...

    def __repr__(self) -> str:
        return "Store()"

    @abstractmethod
    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        """Insert an xr.Dataset to the store.

        If no data is present in the Store, this method should initialise storage.

        Args:
            data: xr.Dataset to add to Store
            on_conflict: if "error", raise DataOverlapError if any conflicts found. If "ignore", then
                ignore any conflicting values in `data`, and insert only non-conflicting values.

        Returns:
            None

        Raises:
            DataOverlapError if conflicts found and `on_conflict` == "error".
        """
        ...

    @abstractmethod
    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        """Update the data in the Store with data in an xr.Dataset.

        Note: by default, only existing data can be updated, so an error is raised if there are
        non-conflicting times in the new data. This can be overridden.

        Args:
            data: xr.Dataset to add to Store
            on_nonconflict: if "error", raise IndexError if any non-conflicts found. If "ignore", then
                ignore any non-conflicting values in `data`, and insert only conflicting values.

        Returns:
            None

        Raises:
            UpdateError if nonconflicts found and `on_nonconflict` == "error".
        """
        ...

    def upsert(self, data: xr.Dataset) -> None:
        """Add data to Store, inserting at new index values and updating at existing index values.

        Note: the order of updating and inserting can matter. Override this method if this order does not
        have the desired effect.
        """
        self.update(data, on_nonconflict="ignore")
        self.insert(data, on_conflict="ignore")

    @abstractmethod
    def get(self) -> xr.Dataset:
        """Return the stored data."""
        if not bool(self):
            return xr.Dataset()  # TODO: should this raise an error instead?
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        """Clear data from store."""
        ...

    def overwrite(self, data: xr.Dataset) -> None:
        """Write data to Store, deleting any existing data."""
        self.clear()
        self.insert(data, on_conflict="ignore")  # "ignore" to avoid checking for conflicts

    def delete(self) -> None:
        """Delete the store.

        Note: override this method if there are other artefacts
        to remove (for instance, a directory that held the data).
        """
        self.clear()

    def copy_to(self, other: Store) -> None:
        """Copy data from self to other.

        Note: this overwrites the data in `other`.
        """
        ds = self.get()
        other.overwrite(ds)

    def bytes_stored(self) -> int:
        """Return the number of bytes stored in the store."""
        raise NotImplementedError


class MemoryStore(Store):
    """Simple in-memory implementation of Store interface."""

    def __init__(
        self, data: xr.Dataset | None = None, append_dim: str = "time", index_options: dict | None = None
    ) -> None:
        super().__init__()
        self.data = data
        self.append_dim = append_dim
        self.index_options = index_options or {}

    def __repr__(self) -> str:
        return f"MemoryStore({self.data!r}, append_dim={self.append_dim})"

    def __bool__(self) -> bool:
        return self.data is not None

    def clear(self) -> None:
        self.data = None

    @property
    def _conflict_determiner(self) -> ConflictDeterminer:
        index = self.data.get_index(self.append_dim) if self.data else pd.Index([])
        return ConflictDeterminer(index=index, **self.index_options)

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            self.data = data
        else:
            if self._conflict_determiner.has_conflicts(data.get_index(self.append_dim)):
                if on_conflict == "error":
                    raise DataOverlapError("Cannot insert data with conflicts if `on_conflict` == 'error'")

                # otherwise, select non-conflicts
                data = self._conflict_determiner.select_nonconflicts(data, self.append_dim)

            self.data = xr.concat([self.data, data], dim=self.append_dim).sortby(self.append_dim)

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            raise UpdateError("Cannot update empty Store.")
        else:
            if self._conflict_determiner.has_nonconflicts(data.get_index(self.append_dim)):
                if on_nonconflict == "error":
                    raise UpdateError("Cannot add new values with `update`.")

                # otherwise, select conflicts/overlapping values
                data = self._conflict_determiner.select_conflicts(data, self.append_dim)

            # merge new data with existing data from other time points
            # NOTE: this might not work properly if index options have been used to set
            # a tolerance for defining conflicts
            existing_data = self.data.drop_sel({self.append_dim: data.coords[self.append_dim]})
            self.data = xr.merge([existing_data, data])

    def get(self) -> xr.Dataset:
        return self.data if self.data is not None else xr.Dataset()

    def bytes_stored(self) -> int:
        return self.data.nbytes if self.data is not None else 0  # type: ignore


class VersionedMemoryStore(SimpleVersioning[xr.Dataset | None], MemoryStore):
    """Versioned in-memory storage of Xarray Datasets."""

    def __init__(
        self,
        data: xr.Dataset | None = None,
        append_dim: str = "time",
        index_options: dict | None = None,
        default_version: str = "v1",
    ) -> None:
        # if data is not None, we need to create an initial version
        versions = [default_version] if data is not None else None

        super().__init__(
            factory=lambda _: None,  # empty MemoryStore initialised with data = None
            versions=versions,
        )
        self.append_dim = append_dim
        self.index_options = index_options or {}

    # make .data an alias for ._current
    @property
    def data(self) -> xr.Dataset | None:
        return self._current

    @data.setter
    def data(self, value: xr.Dataset | None) -> None:
        self._current = value
