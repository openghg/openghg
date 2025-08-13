"""
This module implements a MetaStore based on TinyDB, with custom
middleware based on `CachingMiddleware`.

The metastore can be accessed given a bucket and data type.
 In a context manager, use `open_metastore`. Outside of a context
 manager, use `ClassicMetaStore.from_bucket`; in this case, the
metastore must be closed using the `close` method.

Closing the metastore is necessary, since `CachingMiddleware`
doesn't write to disk unless at least 1000 writes have been made.

The `ClassicMetaStore` is organised in the same way that the
metastore was organised in OpenGHG <= v 6.2: there are separate
TinyDB databases for each data type.

Besides taking a data type parameter in the `__init__` method,
`ClassicMetaStore` only extends `TinyDBMetastore` by adding
a `from_bucket` class method, and a `close` method.
These methods are only needed by `store.BaseStore`.

"""

from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import contextmanager
import contextlib
from pathlib import Path
from types import TracebackType
from typing import Literal
from typing_extensions import Self
import warnings

import tinydb
from filelock import FileLock as _FileLock
from openghg.objectstore import get_object_lock_path, get_readable_buckets
from openghg.objectstore.metastore import TinyDBMetaStore
from openghg.objectstore.metastore._tiny_db import BucketKeyStorage, MultiJSONStorage, SafetyCachingMiddleware
from openghg.types._errors import ObjectStoreError


def get_metakey(data_type: str) -> str:
    """Return the metakey for a given data type.

    Args:
        data_type: data type to get metakey for.

    Returns:
        Metakey string for given data type, if found, or "default"
            if data type not found.
    """
    from openghg.store import data_class_info

    object_store_data_classes = data_class_info()
    try:
        result = object_store_data_classes[data_type]
    except KeyError:
        return "default"
    return f"{result['_root']}/uuid/{result['_uuid']}/metastore"


def get_metastore_path(bucket: str, data_type: str) -> Path:
    from openghg.objectstore import get_object_data_path, get_readable_buckets

    key = get_metakey(data_type)

    bucket = get_readable_buckets().get(bucket, bucket)

    try:
        path = get_object_data_path(bucket, key)
    except ObjectStoreError as e:
        raise ObjectStoreError(f"No data of type {data_type} in store {bucket}.") from e
    else:
        return path


def get_metastore_paths(
    *bucket_data_type_pairs: tuple[str, str],
    suppress_object_store_errors: bool = False,
) -> dict[str, Path]:
    if not bucket_data_type_pairs:
        return {}

    def make_key(pair: tuple[str, str]) -> str:
        return "__".join(pair)

    if not suppress_object_store_errors:
        return {make_key(pair): get_metastore_path(*pair) for pair in bucket_data_type_pairs}

    result = {}
    for pair in bucket_data_type_pairs:
        with contextlib.suppress(ObjectStoreError):
            result[make_key(pair)] = get_metastore_path(*pair)
    return result


@contextmanager
def _open_metastores(
    updater: Callable[[str, int, dict], None] | None = None, **json_info: str | bytes | Path
) -> Generator[TinyDBMetaStore, None, None]:
    """Context manager for TinyDBMetaStore based on OpenGHG v<=6.2 set-up for keys
    and TinyDB.

    Args:
        **json_info: mapping of source names to json path or json str/bytes.

    Yields:
        ClassicMetaStore instance.
    """
    with tinydb.TinyDB(storage=MultiJSONStorage, updater=updater, **json_info) as db:
        metastore = TinyDBMetaStore(database=db)
        yield metastore


@contextmanager
def open_multi_metastore(
    buckets: str | list[str] | None = None,
    data_types: str | list[str] | None = None,
    bucket_data_type_pairs: tuple[str, str] | list[tuple[str, str]] | None = None,
    suppress_object_store_errors: bool = False,
) -> Generator[TinyDBMetaStore, None, None]:
    # if given specific pairs, use these
    if bucket_data_type_pairs is not None:
        if not isinstance(bucket_data_type_pairs, list):
            bucket_data_type_pairs = [bucket_data_type_pairs]
    else:
        if buckets is None:
            buckets = list(get_readable_buckets().keys())
        elif isinstance(buckets, str):
            buckets = [buckets]

        if data_types is None:
            from openghg.store.spec._specification import define_data_types

            data_types = list(define_data_types())
        elif isinstance(data_types, str):
            data_types = [data_types]

        bucket_data_type_pairs = [(bucket, dtype) for bucket in buckets for dtype in data_types]

    json_info = get_metastore_paths(
        *bucket_data_type_pairs, suppress_object_store_errors=suppress_object_store_errors
    )

    def updater(name: str, doc_id: int, doc: dict) -> None:
        bucket_name, dtype = name.split("__")
        bucket = get_readable_buckets().get(bucket_name, bucket_name)  # try to get path

        if len(bucket_data_type_pairs) == 1:
            doc.update({"object_store": bucket})
        else:
            # make unique identifier
            multi_uuid = f"{name}__{doc['uuid']}"
            doc.update(
                {
                    "object_store": bucket,
                    "data_type": dtype,
                    "object_store_name": bucket_name,
                    "multi_uuid": multi_uuid,
                }
            )

    with _open_metastores(updater=updater, **json_info) as metastore:
        yield metastore


@contextmanager
def open_metastore(
    bucket: str, data_type: str | None = None, mode: Literal["r", "rw"] = "rw"
) -> Generator[TinyDBMetaStore, None, None]:
    """Context manager for TinyDBMetaStore based on OpenGHG v<=6.2 set-up for keys
    and TinyDB.

    Args:
        bucket: path to object store
        data_type: data type of metastore to open; if None, then mode must be read-only.
        mode: 'rw' for read/write, 'r' for read only

    Yields:
        ClassicMetaStore instance.
    """
    if data_type is None:
        from openghg.objectstore import get_object
        from openghg.store.spec import define_data_types

        if mode != "r":
            warnings.warn("Metastore cannot be opened to write without data type; opening as read-only.")

        json_info = {}
        for dtype in define_data_types():
            try:
                path = get_object(bucket, get_metakey(dtype))
            except ObjectStoreError:
                pass
            else:
                json_info[dtype] = path

        with _open_metastores(**json_info) as metastore:
            yield metastore
    else:
        key = get_metakey(data_type)
        with tinydb.TinyDB(bucket, key, mode, storage=SafetyCachingMiddleware(BucketKeyStorage)) as db:
            metastore = TinyDBMetaStore(database=db)
            yield metastore


class LockingError(Exception): ...


class FileLock:
    """Convenience wrapper around filelock.FileLock."""

    def __init__(self, bucket: str, key: str) -> None:
        lock_path = get_object_lock_path(bucket, key)

        # If lock is created for first time, make sure group has 'rw' permissions
        if not lock_path.exists():
            lock_path.touch(mode=0o664)

        try:
            self.lock = _FileLock(lock_path, timeout=600, mode=0o664)  # file lock with 10 minute timeout
        except PermissionError as e:
            raise LockingError(
                "You do not have the correct permissions to add data to this object store."
                f"Ask {lock_path.owner()} to set group permissions for {lock_path} to 'rw',"
                f"e.g. using `chmod +664 {lock_path}`"
            ) from e

    def acquire(self) -> None:
        self.lock.acquire(poll_interval=1)

    def release(self) -> None:
        self.lock.release()

    def is_locked(self) -> bool:
        return self.lock.is_locked


class DataClassMetaStore(TinyDBMetaStore):
    """Class that allows:
    - creating a TinyDB MetaStore via bucket and data type
    - locking the MetaStore

    There is also a `close` method, for use inside the `BaseStore` context-manager.
    """

    def __init__(self, bucket: str, data_type: str) -> None:
        self.data_type = data_type
        self.key = get_metakey(data_type)
        database = tinydb.TinyDB(
            bucket, self.key, mode="rw", storage=SafetyCachingMiddleware(BucketKeyStorage)
        )
        super().__init__(database=database)

        self.lock = FileLock(bucket, self.key)

    def __enter__(self) -> Self:
        self.lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.lock.release()

    def close(self) -> None:
        """Close the underlying TinyDB database."""
        self.lock.release()
        self._db.close()
