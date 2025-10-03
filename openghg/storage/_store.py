"""Storage for a single Xarray Dataset.

This module contains the interface (abstract base class) ``Store``, as well
as two concrete implementations:

- ``MemoryStore``
- ``VersionedMemoryStore``

These concrete implementations are mainly used for testing.
Their value is that they produce the same results as the Zarr implementations of
the ``Store`` interface, using Xarray operations.

The ``Store`` class provides a basic "CRUD" interface for storing Xarray ``Dataset``s:

- ``Store.insert`` creates/initialises storage if no data is present, otherwise it appends
  new data to the store. If the new data "overlaps" with existing data, this is an error
  by default: existing data should not be modified by insert. There is an option to ignore
  any overlaps in the new data.
- ``Store.update`` updates existing data. This method cannot add new data, so an error is raised
  if the data provided does not "overlap" with the existing data. There is an option to ignore
  "non-overlaps".
- ``Store.get`` returns the stored data as an ``xr.Dataset``
- ``Store.clear`` clears the stored data, and ``Store.delete`` clears the data and removes
   any other artefacts (files, directories, etc.) created when storage is initialised.

The interface ``Store`` does not define what constitutes a "overlap" or "non-overlap", but
for data with a time coordinate, an overlap occurs when new data (passed to ``insert`` or ``update``)
has time coordinate values that "match" stored time coordinate values.
Match might mean exact equality, or equality up to some tolerance.

``Store`` has some other methods for convenience:

- ``Store.upsert`` does an update followed by an insert (similar to updating a dictionary)
- ``Store.overwrite`` clears the store then inserts data

"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Literal

import pandas as pd
import xarray as xr

from openghg.types import DataOverlapError, UpdateError
from openghg.util._versioning import SimpleVersioning
from ._indexing import OverlapDeterminer


class Store(ABC):
    """Interface for means of storing a single dataset."""

    @abstractmethod
    def __bool__(self) -> bool:
        """Return True is Store is not empty."""
        ...

    def __repr__(self) -> str:
        return "Store()"

    @abstractmethod
    def insert(self, data: xr.Dataset, on_overlap: Literal["error", "ignore"] = "error") -> None:
        """Insert an xr.Dataset to the store.

        If no data is present in the Store, this method should initialise storage.

        Args:
            data: xr.Dataset to add to Store
            on_overlap: if "error", raise DataOverlapError if any overlaps found. If "ignore", then
                ignore any overlaping values in `data`, and insert only non-overlaping values.

        Returns:
            None

        Raises:
            DataOverlapError: if overlaps found and `on_overlap` == "error".
        """
        ...

    @abstractmethod
    def update(self, data: xr.Dataset, on_nonoverlap: Literal["error", "ignore"] = "error") -> None:
        """Update the data in the Store with data in an xr.Dataset.

        Note: by default, only existing data can be updated, so an error is raised if there are
        non-overlaping times in the new data. This can be overridden.

        Args:
            data: xr.Dataset to add to Store
            on_nonoverlap: if "error", raise UpdateError if any non-overlaps found. If "ignore", then
                ignore any non-overlaping values in `data`, and insert only overlaping values.

        Returns:
            None

        Raises:
            UpdateError: if nonoverlaps found and `on_nonoverlap` == "error".
        """
        ...

    def upsert(self, data: xr.Dataset) -> None:
        """Add data to Store, inserting at new index values and updating at existing index values.

        Note: the order of updating and inserting can matter. Override this method if this order does not
        have the desired effect.
        """
        self.update(data, on_nonoverlap="ignore")
        self.insert(data, on_overlap="ignore")

    @abstractmethod
    def get(self) -> xr.Dataset:
        """Return the stored data.

        If Store is empty, return empty Dataset.
        """
        if not bool(self):
            return xr.Dataset()
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        """Clear data from store."""
        ...

    def overwrite(self, data: xr.Dataset) -> None:
        """Write data to Store, deleting any existing data."""
        self.clear()
        self.insert(data, on_overlap="ignore")  # "ignore" to avoid checking for conflicts

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
    def _overlap_determiner(self) -> OverlapDeterminer:
        index = self.data.get_index(self.append_dim) if self.data else pd.Index([])
        return OverlapDeterminer(index=index, **self.index_options)

    def insert(self, data: xr.Dataset, on_overlap: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            # TODO should we check if the data has the append dim? ...zarr wouldn't
            self.data = data
        else:
            if self._overlap_determiner.has_overlaps(data.get_index(self.append_dim)):
                if on_overlap == "error":
                    raise DataOverlapError("Cannot insert data with conflicts if `on_conflict` == 'error'")

                # otherwise, select non-overlaps
                data = self._overlap_determiner.select_nonoverlaps(data, self.append_dim)

            self.data = xr.concat([self.data, data], dim=self.append_dim).sortby(self.append_dim)

    def update(self, data: xr.Dataset, on_nonoverlap: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            raise UpdateError("Cannot update empty Store.")

        # we assume that the data has a coordinate for the append dim below
        if self.append_dim not in data.coords:
            raise ValueError(f"Provided data does not have append dim {self.append_dim}")

        if self._overlap_determiner.has_nonoverlaps(data.get_index(self.append_dim)):
            if on_nonoverlap == "error":
                raise UpdateError("Cannot add new values with `update`.")

            # otherwise, select overlaps/overlapping values
            data = self._overlap_determiner.select_overlaps(data, self.append_dim)

        # merge new data with existing data from other time points
        # NOTE: this might not work properly if index options have been used to set
        # a tolerance for defining overlaps
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
