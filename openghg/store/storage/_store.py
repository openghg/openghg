from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Generic, Literal, TypeVar

import dask
import numpy as np
import xarray as xr
from xarray.backends.zarr import ZarrStore as xrZarrStore
from xarray.conventions import decode_cf_variable
import zarr
from zarr._storage.store import Store as AbstractZarrStore

from ._index import StoreIndex, DatetimeStoreIndex
from openghg.types import DataOverlapError, UpdateError


logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)


class Store(ABC):
    """Interface for means of storing a single dataset."""

    @property
    @abstractmethod
    def index(self) -> StoreIndex:
        """Get index for store."""
        ...

    def __len__(self) -> int:
        """Return number of index values."""
        return len(self.index)

    @abstractmethod
    def __bool__(self) -> bool:
        """Return True is Store is not empty."""
        ...

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
        if on_conflict == "error" and self.index.conflicts_found(data):
            raise DataOverlapError

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
        if on_nonconflict == "error" and self.index.nonconflicts_found(data):
            raise UpdateError(
                "To update with data that contains values outside the existing data index, use `on_nonconflict = 'error'`."
            )

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
        ...

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

    def copy(self, other: Store) -> None:
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

    def __init__(self, data: xr.Dataset | None = None) -> None:
        super().__init__()
        self.data = data

    def __bool__(self) -> bool:
        return self.data is not None

    def clear(self) -> None:
        self.data = None

    @property
    def index(self) -> DatetimeStoreIndex:
        if self.data is None:
            return DatetimeStoreIndex()
        return DatetimeStoreIndex.from_dataset(self.data)

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            self.data = data
        else:
            super().insert(data, on_conflict)  # error checking
            data_nonconflicts = self.index.select_nonconflicts(data)
            self.data = xr.concat([self.data, data_nonconflicts], dim="time").sortby("time")

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        if self.data is None:
            raise UpdateError("Cannot update empty Store.")
        else:
            super().update(data, on_nonconflict)  # error checking
            data_conflicts = self.index.select_conflicts(data)
            self.data.update(data_conflicts)

    def get(self) -> xr.Dataset:
        if self.data is None:
            return xr.Dataset()
        return self.data


ZST = TypeVar("ZST", bound=AbstractZarrStore)


class ZarrStore(Store, Generic[ZST]):
    """Zarr store for storing a single dataset."""

    def __init__(self, zarr_store: ZST) -> None:
        """Pass an instantiated Zarr Store.

        Note: for commonly used types of ZarrStore, we can create convenience functions
        to create ZarrStore objects.
        """
        super().__init__()
        self.store = zarr_store

        # TODO: add filters and encoding

    def __bool__(self) -> bool:
        return bool(self.store)

    @property
    def index(self) -> DatetimeStoreIndex:
        if self.store:
            zgroup = zarr.group(self.store)
            zs = xrZarrStore(zgroup)
            time = zs.open_store_variable("time")
            time = decode_cf_variable("time", time)
            return DatetimeStoreIndex(time.values)  # type: ignore
        return DatetimeStoreIndex()

    def clear(self) -> None:
        self.store.rmdir()

    def get(self) -> xr.Dataset:
        return xr.open_zarr(self.store, consolidated=True).sortby(
            "time"
        )  # need to sort to be consistent with MemoryStore

    def insert(self, data: xr.Dataset, on_conflict: Literal["error", "ignore"] = "error") -> None:
        if not self.store:
            data.to_zarr(
                store=self.store,
                mode="w",
                consolidated=True,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
            )
        else:
            super().insert(data, on_conflict)  # error checking

            data_nonconflicts = self.index.select_nonconflicts(data)
            data_nonconflicts = self._match_chunking(data_nonconflicts)

            data_nonconflicts.to_zarr(
                store=self.store,
                mode="a",
                append_dim="time",
                consolidated=True,
                compute=True,
                synchronizer=zarr.ThreadSynchronizer(),
            )

    def update(self, data: xr.Dataset, on_nonconflict: Literal["error", "ignore"] = "error") -> None:
        if not self.store:
            raise UpdateError("Cannot update empty Store.")
        else:
            super().update(data, on_nonconflict)  # error checking

            data_conflicts = self.index.select_conflicts(data)
            data_conflicts = self._match_chunking(data_conflicts)

            try:
                data_conflicts.to_zarr(
                    store=self.store,
                    mode="r+",
                    region="auto",
                    consolidated=True,
                    compute=True,
                    synchronizer=zarr.ThreadSynchronizer(),
                )
            except ValueError as e:
                # possible issue with non-contiguous data
                source_regions, target_regions = self._find_contiguous_regions(data)

                delayed = []
                vars_to_drop = [dv for dv in data.data_vars if "time" not in data[dv].dims]

                for source_region, target_region in zip(source_regions, target_regions):
                    target_slice = slice(target_region[0], target_region[-1] + 1)
                    res = (
                        data_conflicts.isel(time=source_region)
                        .drop_vars(vars_to_drop)
                        .to_zarr(
                            store=self.store,
                            mode="r+",
                            region={"time": target_slice},
                            consolidated=True,
                            compute=True,
                            synchronizer=zarr.ThreadSynchronizer(),
                        )
                    )
                    delayed.append(res)

                dask.compute(*delayed)

    def _match_chunking(self, dataset: xr.Dataset) -> xr.Dataset:
        """Ensure that chunks of incoming data match the chunking of the stored data.

        If no chunking is found then an empty dictionary is returned.
        If there is no mismatch then an empty dictionary is returned.
        Returns the chunking scheme of the stored data if there is a mismatch.

        Args:
            dataset: Incoming dataset
        Returns:
            dict: Chunking scheme
        """
        incoming_chunks = dict(dataset.chunks)
        incoming_actually_chunked = {k: max(v) for k, v in incoming_chunks.items() if len(v) > 1}

        stored_chunks = {str(k): v for k, v in self.get().chunksizes.items()}
        # Only take chunks that aren't whole dimensions
        stored_actually_chunked = {k: max(v) for k, v in stored_chunks.items() if len(v) > 1}

        if not incoming_actually_chunked and not stored_actually_chunked:
            return dataset

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

            logger.warning(msg)  # TODO: use warning.warn?

            # For coordinates we haven't chunked over we'll use the full size
            for k in dataset.dims:
                k = str(k)  # xr.Dataset.dims returns a Mapping with Hashable keys, which may not be strings
                if k not in stored_actually_chunked:
                    stored_actually_chunked[k] = dataset.sizes[k]

            return dataset.chunk(stored_actually_chunked)

        return dataset

    def _find_contiguous_regions(self, dataset: xr.Dataset) -> tuple[list, list]:
        """Return list of contiguous regions in dataset.

        Each contiguous region is a list of indices for the `time` coordinate
        of the given dataset.

        For instance, if

        contiguous_regions = self._find_contiguous_regions(dataset)
        contig_times0 = dataset.time.isel(contiguous_regions[0])

        then no times in the stored data will be strictly between two values in `contig_times0`.
        """
        source_regions = []
        target_regions = []

        idxs = self.index.index.get_indexer(dataset.time.values)

        if (idxs == -1).any():
            raise ValueError("Can only find contiguous regions for a subset of the existing index.")

        diffs = np.diff(idxs)

        src_current = [0]
        target_current = [idxs[0]]
        for i, idx, diff in zip(range(1, len(diffs) + 1), idxs[1:], diffs):
            if diff == 1:
                src_current.append(i)
                target_current.append(idx)
            else:
                # TODO: diff tells us the size of the gap (+ 1), we
                # could use this to optmise by loading data to fill small gaps
                source_regions.append(src_current)
                target_regions.append(target_current)
                src_current = [i]
                target_current = [idx]

        if src_current:
            source_regions.append(src_current)

        if target_current:
            target_regions.append(target_current)

        return source_regions, target_regions


def get_zarr_directory_store(path: Path) -> ZarrStore[zarr.DirectoryStore]:
    """Factory function to create ZarrStore objects based on a zarr.DirectoryStore

    Uses DatetimeStoreIndex for indexing.
    """
    store = zarr.DirectoryStore(path)
    return ZarrStore[zarr.DirectoryStore](store)
