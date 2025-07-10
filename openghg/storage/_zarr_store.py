import logging
from pathlib import Path
from typing import cast, Generic, Literal, TypeVar

import xarray as xr
import zarr
from zarr._storage.store import Store as AbstractZarrStore

from openghg.types import DataOverlapError
from ._indexing import ConflictDeterminer
from ._store import Store, UpdateError


logger = logging.getLogger("openghg.storage")
logger.setLevel(logging.DEBUG)


ZST = TypeVar("ZST", bound=AbstractZarrStore)


class ZarrStore(Store, Generic[ZST]):
    """Zarr store for storing a single dataset."""

    def __init__(
        self, zarr_store: ZST | None = None, append_dim: str = "time", index_options: dict | None = None
    ) -> None:
        """Pass an instantiated Zarr Store.

        Note: for commonly used types of ZarrStore, we can create convenience functions
        to create ZarrStore objects.
        """
        super().__init__()
        self._store = zarr_store
        self.append_dim = append_dim
        self.index_options = index_options or {}

    @property
    def store(self) -> ZST:
        if self._store is None:
            raise AttributeError("Zarr store not set.")
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
        if not self.__bool__():
            return xr.Dataset()

        return xr.open_zarr(self.store, consolidated=True).sortby(
            self.append_dim
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
    path: Path, append_dim: str = "time", index_options: dict | None = None
) -> ZarrStore[zarr.DirectoryStore]:
    """Factory function to create ZarrStore objects based on a zarr.DirectoryStore."""
    store = zarr.DirectoryStore(path)
    return ZarrStore[zarr.DirectoryStore](store, append_dim=append_dim, index_options=index_options)


def get_zarr_memory_store(
    append_dim: str = "time", index_options: dict | None = None
) -> ZarrStore[zarr.MemoryStore]:
    """Factory function to create ZarrStore objects based on a zarr.MemoryStore."""
    store = zarr.MemoryStore()
    return ZarrStore[zarr.MemoryStore](store, append_dim=append_dim, index_options=index_options)
