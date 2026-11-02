"""The small synchronous boundary between Zarr 2 and 3 store APIs."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import zarr
import zarr.storage

if TYPE_CHECKING:
    from zarr.abc.store import Store as ZarrStoreBase
else:
    # The compatibility tests import this module with Zarr 2, where zarr.abc
    # does not exist. Static checking uses Store; the runtime fallback is inert.
    ZarrStoreBase = object


def has_async_store_api() -> bool:
    return int(zarr.__version__.split(".", maxsplit=1)[0]) >= 3


def make_local_store(path: Path) -> ZarrStoreBase:
    path = path.absolute()
    if has_async_store_api():
        return zarr.storage.LocalStore(path)
    return cast(ZarrStoreBase, getattr(zarr.storage, "DirectoryStore")(path))


def make_memory_store() -> ZarrStoreBase:
    return zarr.storage.MemoryStore()


def clear_store(store: ZarrStoreBase) -> None:
    if has_async_store_api():
        from zarr.core.sync import sync

        if isinstance(store, zarr.storage.LocalStore) and not store.root.exists() and not store.read_only:
            return
        sync(store.clear())
        # Retain DirectoryStore's directory-removal behavior for datasource deletion.
        if isinstance(store, zarr.storage.LocalStore) and store.root.exists():
            store.root.rmdir()
    else:
        cast(Any, store).rmdir()


def copy_store(source: ZarrStoreBase, dest: ZarrStoreBase) -> None:
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

        copy_zarr2_store(cast(Any, source), cast(Any, dest))
