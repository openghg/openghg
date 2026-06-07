"""Tests for zarr storage compatibility helpers."""

from __future__ import annotations

import ast
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


def _put_bytes(store: object, key: str, value: bytes) -> None:
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
