"""Synchronous access to the Zarr 2 mapping and Zarr 3 async storage APIs."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, TypeAlias

import zarr
import zarr.storage

# Zarr 2 and 3 have incompatible store interfaces; narrow them inside these helpers.
ZarrStoreLike: TypeAlias = Any


def zarr_major_version() -> int:
    """Return the installed zarr-python major version, not the data format."""
    return int(zarr.__version__.split(".", maxsplit=1)[0])


def zarr_has_async_store_api() -> bool:
    """Return whether the installed zarr-python uses asynchronous stores."""
    return zarr_major_version() >= 3


def make_memory_store() -> ZarrStoreLike:
    """Create an empty in-memory store using the installed Zarr API."""
    return zarr.storage.MemoryStore()


def make_local_store(path: str | Path) -> ZarrStoreLike:
    """Create a store rooted at a filesystem path without clearing existing data."""
    if zarr_has_async_store_api():
        return zarr.storage.LocalStore(path)
    return zarr.storage.DirectoryStore(path)


def iter_store_keys(store: ZarrStoreLike, prefix: str | None = None) -> Iterator[str]:
    """Iterate keys at or below a slash-delimited prefix, excluding sibling names.

    An empty prefix selects the whole store. Keys are materialised before returning
    so callers can delete them during iteration.
    """
    prefix = (prefix or "").strip("/")
    if zarr_has_async_store_api():
        from zarr.core.sync import sync

        async def collect() -> list[str]:
            """Collect keys using the native asynchronous store API."""
            return [key async for key in store.list_prefix(prefix)]

        keys = sync(collect())
    else:
        keys = list(store)
    return (key for key in keys if not prefix or key == prefix or key.startswith(prefix + "/"))


def store_is_empty(store: ZarrStoreLike, prefix: str | None = None) -> bool:
    """Return whether the store or slash-delimited prefix contains no keys."""
    return next(iter_store_keys(store, prefix), None) is None


def clear_store(store: ZarrStoreLike, prefix: str | None = None) -> None:
    """Remove keys at or below a prefix, or the entire store when none is given.

    Clearing a whole local store also removes its root directory, matching Zarr 2
    DirectoryStore behaviour. The store object can subsequently be written again.
    """
    prefix = (prefix or "").strip("/")
    if zarr_has_async_store_api():
        from zarr.core.sync import sync

        if not prefix:
            if isinstance(store, zarr.storage.LocalStore) and not store.root.exists() and not store.read_only:
                return
            sync(store.clear())
            if isinstance(store, zarr.storage.LocalStore) and store.root.exists():
                store.root.rmdir()
        else:
            for key in iter_store_keys(store, prefix):
                sync(store.delete(key))
    elif not prefix:
        store.rmdir()
    else:
        for key in iter_store_keys(store, prefix):
            del store[key]


def store_byte_size(store: ZarrStoreLike, prefix: str | None = None) -> int:
    """Sum stored bytes at or below a prefix, including metadata and encoded chunks.

    Returns zero for Zarr 2 stores without a getsize method, retaining the existing
    storage accounting behaviour.
    """
    if not hasattr(store, "getsize"):
        return 0
    if zarr_has_async_store_api():
        from zarr.core.sync import sync

        return sum(sync(store.getsize(key)) for key in iter_store_keys(store, prefix))
    return sum(store.getsize(key) for key in iter_store_keys(store, prefix))
