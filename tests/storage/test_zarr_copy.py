import asyncio
import zlib

import pytest

from openghg.storage._zarr_compat import (
    iter_store_keys,
    make_local_store,
    make_memory_store,
    zarr_has_async_store_api,
)
from openghg.storage._zarr_copy import copy_zarr_store


def write_keys(store, values):
    if zarr_has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        for key, value in values.items():
            sync(store.set(key, default_buffer_prototype().buffer.from_bytes(value)))
    else:
        store.update(values)


def read_keys(store):
    if zarr_has_async_store_api():
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        return {
            key: sync(store.get(key, prototype=default_buffer_prototype())).to_bytes()
            for key in iter_store_keys(store)
        }
    return dict(store)


@pytest.fixture(params=["memory", "local"])
def stores(request, tmp_path):
    if request.param == "memory":
        return make_memory_store(), make_memory_store()
    return make_local_store(tmp_path / "source"), make_local_store(tmp_path / "dest")


def test_copy_preserves_encoded_bytes(stores):
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

    assert copy_zarr_store(source, dest) == (len(values), 0, sum(map(len, values.values())))
    assert read_keys(dest) == values
    assert read_keys(source) == values


def test_copy_prefixes_respect_path_boundaries(stores):
    source, dest = stores
    write_keys(source, {"foo/a": b"a", "foo/b/c": b"bc", "foobar/a": b"wrong"})
    write_keys(dest, {"archive-extra/a": b"keep"})

    assert copy_zarr_store(source, dest, source_path="/foo/", dest_path="/archive/") == (2, 0, 3)
    assert read_keys(dest) == {"archive/a": b"a", "archive/b/c": b"bc", "archive-extra/a": b"keep"}


def test_copy_normalizes_separators(stores):
    source, dest = stores
    write_keys(source, {"group/sub/a": b"data", "group/submarine/a": b"keep"})

    assert copy_zarr_store(source, dest, source_path=r"\group//sub\\", dest_path=r"/archive\\nested//") == (
        1,
        0,
        4,
    )
    assert read_keys(dest) == {"archive/nested/a": b"data"}


@pytest.mark.parametrize("path", ["..", "../escaped", "./group", r"group\..\escaped"])
@pytest.mark.parametrize("argument", ["source_path", "dest_path"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_copy_rejects_dot_segments_before_writing(stores, tmp_path, path, argument, dry_run):
    source, dest = stores
    write_keys(source, {"a": b"data"})
    write_keys(dest, {"existing": b"keep"})

    with pytest.raises(ValueError, match="segment"):
        copy_zarr_store(source, dest, **{argument: path}, dry_run=dry_run)

    assert read_keys(dest) == {"existing": b"keep"}
    assert not (tmp_path / "escaped").exists()


@pytest.mark.parametrize("if_exists", ["raise", "replace", "skip"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_copy_conflict_policies(stores, if_exists, dry_run):
    source, dest = stores
    write_keys(source, {"a": b"new", "b": b"added"})
    before = {"a": b"old", "unrelated": b"keep"}
    write_keys(dest, before)

    if if_exists == "raise":
        if zarr_has_async_store_api():
            error = FileExistsError
        else:
            from zarr.errors import CopyError

            error = CopyError
        with pytest.raises(error, match="exists in destination"):
            copy_zarr_store(source, dest, if_exists=if_exists, dry_run=dry_run)
        assert read_keys(dest) == before
        return

    counts = (1, 1, 0 if dry_run else 5) if if_exists == "skip" else (2, 0, 0 if dry_run else 8)
    assert copy_zarr_store(source, dest, if_exists=if_exists, dry_run=dry_run) == counts
    expected = before if dry_run else {**before, "b": b"added"}
    if not dry_run and if_exists == "replace":
        expected["a"] = b"new"
    assert read_keys(dest) == expected


def test_copy_empty_store_and_invalid_policy(stores):
    source, dest = stores
    assert copy_zarr_store(source, dest) == (0, 0, 0)
    with pytest.raises(ValueError, match="if_exists"):
        copy_zarr_store(source, dest, if_exists="unknown")
    assert read_keys(dest) == {}


def test_copy_from_running_event_loop(stores):
    source, dest = stores
    write_keys(source, {"a": b"encoded"})

    async def copy():
        return copy_zarr_store(source, dest)

    assert asyncio.run(copy()) == (1, 0, 7)
    assert read_keys(dest) == {"a": b"encoded"}


def test_copy_v2_delegates(stores, monkeypatch):
    if zarr_has_async_store_api():
        pytest.skip("Zarr 2 delegation")
    import zarr.convenience

    calls = []

    def copy(source, dest, **kwargs):
        calls.append((source, dest, kwargs))
        return (2, 3, 4)

    monkeypatch.setattr(zarr.convenience, "copy_store", copy)
    assert copy_zarr_store(*stores, source_path="a", dest_path="b", if_exists="skip", dry_run=True) == (
        2,
        3,
        4,
    )
    assert calls == [(*stores, {"source_path": "a", "dest_path": "b", "if_exists": "skip", "dry_run": True})]
