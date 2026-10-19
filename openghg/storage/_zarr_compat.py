"""The small synchronous boundary between Zarr 2 and 3 store APIs."""

from pathlib import Path
from typing import Any, TypeAlias

import zarr
import zarr.storage

# Zarr 2 stores are mappings; Zarr 3 stores expose asynchronous methods instead.
ZarrStoreLike: TypeAlias = Any


def has_async_store_api() -> bool:
    return int(zarr.__version__.split(".", maxsplit=1)[0]) >= 3


def make_local_store(path: Path) -> ZarrStoreLike:
    if has_async_store_api():
        return zarr.storage.LocalStore(path)
    return getattr(zarr.storage, "DirectoryStore")(path)


def make_memory_store() -> ZarrStoreLike:
    return zarr.storage.MemoryStore()


def store_is_empty(store: ZarrStoreLike) -> bool:
    if has_async_store_api():
        from zarr.core.sync import sync

        return bool(sync(store.is_empty("")))
    return not bool(store)


def clear_store(store: ZarrStoreLike) -> None:
    if has_async_store_api():
        from zarr.core.sync import sync

        if isinstance(store, zarr.storage.LocalStore) and not store.root.exists() and not store.read_only:
            return
        sync(store.clear())
        # Retain DirectoryStore's directory-removal behavior for datasource deletion.
        if isinstance(store, zarr.storage.LocalStore) and store.root.exists():
            store.root.rmdir()
    else:
        store.rmdir()


def store_byte_size(store: ZarrStoreLike) -> int:
    if has_async_store_api():
        from zarr.core.sync import sync

        return int(sync(store.getsize_prefix("")))
    return sum(store.getsize(key) for key in store) if hasattr(store, "getsize") else 0


def copy_store(source: ZarrStoreLike, dest: ZarrStoreLike) -> None:
    """Copy encoded keys between distinct stores without decoding arrays."""
    if has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        async def copy_keys() -> None:
            prototype = default_buffer_prototype()
            async for key in source.list():
                value = await source.get(key, prototype=prototype)
                if value is None:
                    raise KeyError(key)
                # Memory stores must not share mutable chunk buffers across versions.
                await dest.set(key, prototype.buffer.from_bytes(value.to_bytes()))

        sync(copy_keys())
    else:
        from zarr.convenience import copy_store as copy_zarr2_store

        copy_zarr2_store(source, dest)
