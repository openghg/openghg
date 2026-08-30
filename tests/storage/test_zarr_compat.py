"""Tests for zarr storage compatibility helpers."""

from __future__ import annotations

import ast
from collections.abc import AsyncIterator
from pathlib import Path

import zarr

from openghg.storage._zarr_compat import (
    clear_store,
    iter_store_keys,
    make_local_store,
    make_memory_store,
    store_byte_size,
    store_is_empty,
    zarr_has_async_store_api,
    zarr_major_version,
)


class AsyncPrefixStore:
    """Small v3-style async store fake for prefix helper tests."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    async def list(self) -> AsyncIterator[str]:
        """Yield all keys."""
        for key in self.values:
            yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        """Yield keys using zarr v3's broad prefix semantics."""
        for key in self.values:
            if key.startswith(prefix):
                yield key

    async def getsize(self, key: str) -> int:
        """Return the size of a stored byte value."""
        return len(self.values[key])

    async def delete(self, key: str) -> None:
        """Delete a stored key."""
        del self.values[key]


def _put_bytes(store: object, key: str, value: bytes) -> None:
    """Write a byte value to a mapping-like zarr store."""
    store[key] = value  # type: ignore[index]


def test_zarr_version_capability_detection() -> None:
    """Check that zarr version helpers reflect the installed package."""
    assert zarr_major_version() == int(zarr.__version__.split(".", maxsplit=1)[0])
    assert zarr_has_async_store_api() is (zarr_major_version() >= 3)


def test_memory_store_helpers() -> None:
    """Check memory store creation, key iteration, size, and clearing."""
    store = make_memory_store()

    assert store_is_empty(store)

    _put_bytes(store, "a", b"abc")
    _put_bytes(store, "group/b", b"de")

    assert not store_is_empty(store)
    assert sorted(iter_store_keys(store)) == ["a", "group/b"]
    assert sorted(iter_store_keys(store, prefix="group")) == ["group/b"]
    assert store_byte_size(store) == 5
    assert store_byte_size(store, prefix="group") == 2

    clear_store(store, prefix="group")

    assert sorted(iter_store_keys(store)) == ["a"]
    assert store_byte_size(store) == 3

    clear_store(store)

    assert store_is_empty(store)


def test_prefix_helpers_do_not_match_sibling_prefixes() -> None:
    """Check prefix helpers do not include keys from similarly named siblings."""
    store = make_memory_store()
    _put_bytes(store, "group/a", b"a")
    _put_bytes(store, "grouped/a", b"bb")

    assert sorted(iter_store_keys(store, prefix="group")) == ["group/a"]
    assert store_byte_size(store, prefix="group") == 1

    clear_store(store, prefix="group")

    assert sorted(iter_store_keys(store)) == ["grouped/a"]
    assert store_byte_size(store) == 2


def test_async_prefix_helpers_filter_v3_prefix_results() -> None:
    """Check v3-style broad prefix listings are filtered to path boundaries."""
    store = AsyncPrefixStore()
    store.values["group/a"] = b"a"
    store.values["grouped/a"] = b"bb"

    assert sorted(iter_store_keys(store, prefix="group")) == ["group/a"]
    assert store_byte_size(store, prefix="group") == 1

    clear_store(store, prefix="group")

    assert sorted(iter_store_keys(store)) == ["grouped/a"]
    assert store_byte_size(store) == 2


def test_local_store_helpers(tmp_path: Path) -> None:
    """Check local store creation, sizing, and clearing."""
    store = make_local_store(tmp_path)

    assert store_is_empty(store)

    _put_bytes(store, "data/.zarray", b"{}")
    _put_bytes(store, "data/0", b"1234")

    assert sorted(iter_store_keys(store)) == ["data/.zarray", "data/0"]
    assert store_byte_size(store) == 6

    clear_store(store)

    assert store_is_empty(store)


def test_zarr_store_uses_compat_layer() -> None:
    """Check _zarr_store avoids zarr v2-only imports and factories."""
    source = Path("openghg/storage/_zarr_store.py").read_text()
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names = {alias.name for alias in node.names}
            assert "zarr.convenience" not in imported_names

        if isinstance(node, ast.ImportFrom):
            assert node.module != "zarr._storage.store"

        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "zarr":
            assert node.attr not in {"DirectoryStore", "MemoryStore"}
