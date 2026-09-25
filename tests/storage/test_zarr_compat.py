"""Exercise real memory and local stores under each supported Zarr runtime."""

from pathlib import Path
from typing import Any

import pytest
import zarr

from openghg.storage._zarr_compat import (
    clear_store,
    iter_store_keys,
    make_local_store,
    make_memory_store,
    store_byte_size,
    store_is_empty,
    zarr_has_async_store_api,
)


def put_bytes(store: Any, key: str, value: bytes) -> None:
    """Write raw bytes through the installed Zarr API."""
    if zarr_has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        sync(store.set(key, default_buffer_prototype().buffer.from_bytes(value)))
    else:
        store[key] = value


@pytest.mark.parametrize("local", [False, True], ids=["memory", "local"])
def test_store_helpers(tmp_path: Path, local: bool) -> None:
    """Count and clear complete path prefixes without touching similarly named siblings."""
    store = make_local_store(tmp_path / "store") if local else make_memory_store()
    assert store_is_empty(store)
    put_bytes(store, "group/a", b"abc")
    put_bytes(store, "grouped/a", b"12345")
    put_bytes(store, "other", b"de")
    assert not store_is_empty(store)
    assert not store_is_empty(store, "group")
    assert sorted(iter_store_keys(store)) == ["group/a", "grouped/a", "other"]
    assert list(iter_store_keys(store, "/group/")) == ["group/a"]
    assert store_byte_size(store) == 10
    assert store_byte_size(store, "group") == 3
    clear_store(store, "group")
    assert store_is_empty(store, "group")
    assert not store_is_empty(store, "grouped")
    assert store_byte_size(store, "group") == 0
    assert store_byte_size(store) == 7
    clear_store(store)
    assert store_is_empty(store)
    if local:
        assert not (tmp_path / "store").exists()
    put_bytes(store, "new", b"1")
    assert list(iter_store_keys(store)) == ["new"]


def test_runtime_detection() -> None:
    """Detect the storage API independently of the data's Zarr format."""
    assert zarr_has_async_store_api() == (int(zarr.__version__.split(".")[0]) >= 3)


def test_clear_nonexistent_local_store(tmp_path: Path) -> None:
    """Empty destination cleanup is repeatable before and after its first write."""
    path = tmp_path / "missing" / "store"
    store = make_local_store(path)
    clear_store(store)
    assert not path.exists()
    put_bytes(store, "array/0", b"chunk")
    clear_store(store)
    clear_store(store)
    assert not path.exists()
    assert store_is_empty(store)
