"""Exercise raw stores through the synchronous Zarr 2/3 compatibility boundary."""

import asyncio
from pathlib import Path
import zlib

import numpy as np
import pytest

from openghg.storage._zarr_compat import (
    clear_store,
    copy_store,
    has_async_store_api,
    make_local_store,
    make_memory_store,
)


def write_keys(store, values):
    if has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        for key, value in values.items():
            sync(store.set(key, default_buffer_prototype().buffer.from_bytes(value)))
    else:
        store.update(values)


def read_keys(store):
    if has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        async def read():
            return {
                key: (await store.get(key, prototype=default_buffer_prototype())).to_bytes()
                async for key in store.list()
            }

        return sync(read())
    return dict(store)


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path):
    if request.param == "memory":
        return make_memory_store()
    return make_local_store(tmp_path / "store")


@pytest.fixture(params=["memory-memory", "memory-local", "local-memory", "local-local"])
def stores(request, tmp_path):
    return tuple(
        make_memory_store() if kind == "memory" else make_local_store(tmp_path / name)
        for name, kind in zip(("source", "dest"), request.param.split("-"))
    )


def test_store_clear_removes_all_keys_and_allows_reuse(store, tmp_path):
    assert read_keys(store) == {}
    write_keys(store, {"group/a": b"abc", "grouped/a": b"12345", "other": b"de"})

    clear_store(store)

    assert read_keys(store) == {}
    assert not (tmp_path / "store").exists()
    clear_store(store)
    write_keys(store, {"new/key": b"reused"})
    assert read_keys(store) == {"new/key": b"reused"}


def test_local_store_resolves_relative_path_and_clears_missing_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = make_local_store(Path("store"))
    clear_store(store)
    assert not (tmp_path / "store").exists()

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    write_keys(store, {"nested/key": b"data"})

    assert (tmp_path / "store" / "nested" / "key").read_bytes() == b"data"
    assert not (elsewhere / "store").exists()
    clear_store(store)
    assert not (tmp_path / "store").exists()


def test_copy_preserves_encoded_chunks_and_metadata(stores):
    source, dest = stores
    values = {
        ".zgroup": b'{ "zarr_format" : 2 }',
        ".zattrs": b'{"description":"preserve formatting"}',
        ".zmetadata": b'{"metadata": {}}',
        "array/.zarray": b'{"dtype":"<f8", "shape":[3]}',
        "array/0": zlib.compress(b"encoded chunk\x00" * 30),
        "nested/array/1": b"\x00\xff\x01",
    }
    write_keys(source, values)

    copy_store(source, dest)

    assert read_keys(dest) == values
    assert read_keys(source) == values


def test_copy_empty_store(stores):
    source, dest = stores
    copy_store(source, dest)
    assert read_keys(source) == {}
    assert read_keys(dest) == {}


def test_synchronous_copy_from_running_event_loop(stores):
    source, dest = stores
    write_keys(source, {"chunk": b"\x00\xffencoded"})

    async def copy():
        copy_store(source, dest)

    asyncio.run(copy())
    assert read_keys(dest) == {"chunk": b"\x00\xffencoded"}


@pytest.mark.skipif(not has_async_store_api(), reason="Zarr 3 memory stores retain mutable buffers")
def test_memory_copy_detaches_mutable_source_buffer():
    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    source, dest = make_memory_store(), make_memory_store()
    data = np.array([0, 255, 1], dtype="uint8")
    buffer = default_buffer_prototype().buffer.from_array_like(data)
    sync(source.set("array/0", buffer))

    copy_store(source, dest)
    stored_buffer = sync(source.get("array/0", prototype=default_buffer_prototype()))
    stored_buffer.as_numpy_array()[0] = 42

    assert read_keys(source) == {"array/0": b"\x2a\xff\x01"}
    assert read_keys(dest) == {"array/0": b"\x00\xff\x01"}
