"""Compatibility helpers for zarr-python storage APIs.

This module centralises the zarr v2/v3 store API differences needed by
OpenGHG storage. It provides factories for memory and local filesystem stores,
helpers for key iteration, prefix clearing, empty checks, byte sizing, and a
copy wrapper. Zarr v3 store methods are asynchronous, so helpers synchronise
awaitable store operations before returning to the synchronous OpenGHG storage
API.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Coroutine, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
import inspect
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol, TypeVar, cast

import zarr
import zarr.storage


class ZarrStoreLike(Protocol):
    """Minimal mapping protocol for zarr stores used by OpenGHG.

    Zarr v2 stores expose mapping methods directly, while zarr v3 stores expose
    asynchronous methods for most I/O. Compatibility helpers handle those
    asynchronous methods where needed; this protocol records the current
    mapping-shaped surface that xarray and OpenGHG's zarr v2 tests still use.
    """

    def __getitem__(self, key: str) -> Any: ...

    def __setitem__(self, key: str, value: Any) -> None: ...

    def __delitem__(self, key: str) -> None: ...

    def __iter__(self) -> Iterator[str]: ...

    def __bool__(self) -> bool: ...


T = TypeVar("T")


def zarr_version() -> str:
    """Return the installed zarr package version.

    Returns:
        Installed zarr version string.
    """
    try:
        return version("zarr")
    except PackageNotFoundError:
        return str(zarr.__version__)


def zarr_major_version() -> int:
    """Return the installed zarr major version.

    Returns:
        Major version number parsed from the installed zarr version.
    """
    return int(zarr_version().split(".", maxsplit=1)[0])


def zarr_has_async_store_api() -> bool:
    """Check whether zarr stores use the v3 asynchronous store API.

    Returns:
        True when the installed zarr version uses the v3 store API.
    """
    return zarr_major_version() >= 3


def make_memory_store() -> ZarrStoreLike:
    """Create a zarr memory store for the installed zarr version.

    Returns:
        Zarr memory store instance.
    """
    store_cls = getattr(zarr.storage, "MemoryStore", None) or getattr(zarr, "MemoryStore")
    return cast(ZarrStoreLike, store_cls())


def make_local_store(path: str | Path) -> ZarrStoreLike:
    """Create a local filesystem zarr store for the installed zarr version.

    Args:
        path: Filesystem path to the store root.

    Returns:
        Zarr local filesystem store instance.
    """
    store_cls = getattr(zarr.storage, "LocalStore", None)
    if store_cls is None:
        store_cls = getattr(zarr.storage, "DirectoryStore", None) or getattr(zarr, "DirectoryStore")

    return cast(ZarrStoreLike, store_cls(path))


def store_is_empty(store: ZarrStoreLike, prefix: str | None = None) -> bool:
    """Check whether a zarr store or prefix contains any keys.

    Args:
        store: Zarr store instance.
        prefix: Optional key prefix to check.

    Returns:
        True if no keys exist in the store or prefix.
    """
    normalised_prefix = _normalise_prefix(prefix)
    is_empty = getattr(store, "is_empty", None)
    if is_empty is not None:
        return bool(_call_maybe_async(is_empty, normalised_prefix or ""))

    return not any(iter_store_keys(store, prefix=normalised_prefix))


def clear_store(store: ZarrStoreLike, prefix: str | None = None) -> None:
    """Clear all keys from a zarr store or from a prefix.

    For zarr v2 local stores, clearing the whole store preserves the historical
    ``DirectoryStore.rmdir()`` behavior, which removes the store directory itself.

    Args:
        store: Zarr store instance.
        prefix: Optional key prefix to clear.
    """
    normalised_prefix = _normalise_prefix(prefix)
    rmdir = getattr(store, "rmdir", None)
    if normalised_prefix is None and rmdir is not None:
        rmdir()
        return None

    if normalised_prefix is None:
        clear = getattr(store, "clear", None)
        if clear is None:
            _delete_store_keys(store, iter_store_keys(store))
        else:
            _call_maybe_async(clear)
        return None

    _delete_store_keys(store, iter_store_keys(store, prefix=normalised_prefix))
    return None


def iter_store_keys(store: ZarrStoreLike, prefix: str | None = None) -> Iterator[str]:
    """Iterate keys in a zarr store.

    Args:
        store: Zarr store instance.
        prefix: Optional key prefix to filter keys by.

    Returns:
        Iterator over keys relative to the store root.
    """
    normalised_prefix = _normalise_prefix(prefix)

    if normalised_prefix is not None:
        list_prefix = getattr(store, "list_prefix", None)
        if list_prefix is not None:
            keys = _collect_iterable(list_prefix(normalised_prefix))
            return (key for key in keys if _key_matches_prefix(key, normalised_prefix))

    list_all = getattr(store, "list", None)
    if list_all is not None:
        keys = _collect_iterable(list_all())
    else:
        keys_method = getattr(store, "keys", None)
        if keys_method is not None:
            keys = [str(key) for key in keys_method()]
        else:
            keys = [str(key) for key in cast(Iterable[str], store)]

    if normalised_prefix is None:
        return iter(keys)

    return (key for key in keys if _key_matches_prefix(key, normalised_prefix))


def store_byte_size(store: ZarrStoreLike, prefix: str | None = None) -> int:
    """Return the number of bytes stored in a zarr store or prefix.

    Args:
        store: Zarr store instance.
        prefix: Optional key prefix to measure.

    Returns:
        Total number of stored bytes.
    """
    normalised_prefix = _normalise_prefix(prefix)
    getsize_prefix = getattr(store, "getsize_prefix", None)
    if normalised_prefix is not None and getsize_prefix is not None:
        return int(_call_maybe_async(getsize_prefix, normalised_prefix))

    getsize = getattr(store, "getsize", None)
    if getsize is None:
        return 0

    total = 0
    for key in iter_store_keys(store, prefix=normalised_prefix):
        total += int(_call_maybe_async(getsize, key))
    return total


def copy_store(source: ZarrStoreLike, dest: ZarrStoreLike) -> None:
    """Copy all zarr store keys from one store to another.

    This currently delegates to zarr-python's copy helper when available. Zarr
    v3 does not yet provide the same helper, so this wrapper is the compatibility
    point for the OpenGHG copy implementation planned in the next migration step.

    Args:
        source: Source zarr store.
        dest: Destination zarr store.

    Raises:
        NotImplementedError: If the installed zarr version has no store copy helper.
    """
    copy_store_fn = getattr(zarr, "copy_store", None)
    if copy_store_fn is None:
        raise NotImplementedError("zarr.copy_store is not available for this zarr version.")

    copy_store_fn(source, dest)


def _normalise_prefix(prefix: str | None) -> str | None:
    """Normalise empty or slash-delimited prefixes to internal form."""
    if prefix is None:
        return None

    normalised = prefix.strip("/")
    return normalised or None


def _key_matches_prefix(key: str, prefix: str) -> bool:
    """Check whether a key is exactly under a normalised prefix."""
    return key == prefix or key.startswith(f"{prefix}/")


def _call_maybe_async(func: Any, *args: Any) -> Any:
    """Call a store method and synchronously resolve awaitable results.

    Args:
        func: Store method or callable to invoke.
        args: Positional arguments to pass to ``func``.

    Returns:
        Direct return value, or awaited return value for asynchronous methods.
    """
    result = func(*args)
    if inspect.isawaitable(result):
        return _sync(cast(Awaitable[Any], result))

    return result


def _collect_iterable(value: Any) -> list[str]:
    """Collect synchronous or asynchronous store key iterables.

    Args:
        value: Synchronous iterable or asynchronous iterator of store keys.

    Returns:
        Store keys converted to strings.
    """
    if hasattr(value, "__aiter__"):
        return _sync(_collect_async_iterable(cast(AsyncIterator[str], value)))

    return [str(item) for item in value]


async def _collect_async_iterable(value: AsyncIterator[str]) -> list[str]:
    """Collect an asynchronous store key iterator."""
    return [str(item) async for item in value]


def _delete_store_keys(store: ZarrStoreLike, keys: Iterable[str]) -> None:
    """Delete keys from a zarr store.

    Args:
        store: Zarr store instance.
        keys: Keys to delete from the store.
    """
    delete = getattr(store, "delete", None)
    if delete is not None:
        for key in keys:
            _call_maybe_async(delete, key)
        return None

    for key in keys:
        del store[key]
    return None


def _sync(awaitable: Awaitable[T]) -> T:
    """Synchronously wait for an awaitable store operation.

    Uses zarr's own sync helper when available. Otherwise, it runs the awaitable
    with ``asyncio.run``; if an event loop is already running, the awaitable is
    resolved in a short-lived worker thread.

    Args:
        awaitable: Awaitable store operation.

    Returns:
        Awaited result.
    """
    try:
        from zarr.core.sync import sync as zarr_sync
    except ImportError:
        zarr_sync = None

    if zarr_sync is not None:
        return cast(T, zarr_sync(awaitable))

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _asyncio_run(awaitable)

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(_asyncio_run, awaitable).result()


def _asyncio_run(awaitable: Awaitable[T]) -> T:
    """Run an awaitable through ``asyncio.run`` with a coroutine cast."""
    return asyncio.run(cast(Coroutine[Any, Any, T], awaitable))
