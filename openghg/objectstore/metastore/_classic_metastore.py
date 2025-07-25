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

import json
from collections.abc import Generator
from contextlib import contextmanager
from types import TracebackType
from typing import Any, Literal, cast

import tinydb
from filelock import FileLock as _FileLock
from openghg.objectstore import exists, get_object, get_object_lock_path, set_object_from_json
from openghg.objectstore.metastore import TinyDBMetaStore
from openghg.types import MetastoreError
from openghg.util import hash_string
from tinydb.middlewares import Middleware
from typing_extensions import Self


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


class BucketKeyStorage(tinydb.Storage):
    """Custom TinyDB storage class.

    Uses methods in `_local_store` module to read/write files via bucket and key.
    """

    def __init__(self, bucket: str, key: str, mode: Literal["r", "rw"]) -> None:
        """Create BucketKeyStorage object.

        Args:
            bucket: path to object store bucket (as string)
            key: metastore key
            mode: "r" for read-only, "rw" for read/write

        Returns:
            None
        """
        valid_modes = ("r", "rw")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, please choose one of {valid_modes}.")

        self._key = key
        self._bucket = bucket
        self._mode = mode

    def read(self) -> dict | None:
        """Read data from database.

        Returns:
            Dictionary version of JSON database, or None if database has
            not been initialised. (Returning None is required by the TinyDB
            interface.)
        """
        key = self._key

        if not exists(bucket=self._bucket, key=key):
            return None

        data = get_object(bucket=self._bucket, key=self._key)

        try:
            json_data: dict = json.loads(data)
            return json_data
        except json.JSONDecodeError:
            return None

    def write(self, data: dict) -> None:
        """Write data to metastore.

        Args:
            data: dictonary of data to add

        Returns:
            None

        Raises:
            MetastoreError if metastore opened in read-only mode.
        """
        if self._mode == "r":
            raise MetastoreError("Cannot write to metastore in read-only mode.")

        key = self._key
        set_object_from_json(bucket=self._bucket, key=key, data=data)

    def close(self) -> None:
        pass


class SafetyCachingMiddleware(Middleware):
    """Middleware that caches changes to the database, and writes
    these changes when the database is closed. Changes are only written
    if the underlying file has not changed. (The underlying file is the
    persistent record of the database that is read in by the storage class.)

    This differs from CachingMiddleware in two ways:
    1) changes are never written before closing the database, whereas
    CachingMiddleware will write after 1000 changes by default.

    2) CachingMiddleware does not check if the underlying file
    has changed since it was first accessed by the storage class.
    """

    def __init__(self, storage_cls: type[tinydb.Storage]) -> None:
        """Follows the standard pattern for middleware.

        Args:
            storage_cls: tinydb storage class to wrap with Middleware
        Returns:
            None
        """
        super().__init__(storage_cls)
        self.cache = None  # in-memory version of database
        self.database_hash = None  # hash taken when database first read
        self.writes_made = False  # flag to check if writes made

    def read(self):
        """Read the database from the cache, if present, otherwise load
        the database from the underlying storage and save a hash of the result.
        """
        if self.cache is None:
            self.cache = self.storage.read()
            self.database_hash = hash_string(str(self.cache))

        return self.cache

    def write(self, data):
        """Store data in the cache.

        Args:
            data: data to store (this is used internally by TinyDB)
        """
        self.cache = data
        self.writes_made = True

    def close(self):
        """Close the database. If writes have been made, and the underlying
        file has not changed, writes will be saved to disk at this point.

        Raises: MetaStoreError if writes have been made and the underlying file *has* been changed.
        """
        if self.writes_made:
            # we know that the cache is a dictionary of dictionaries
            self.cache = cast(dict[str, dict[str, Any]], self.cache)

            # check if stored hash matches current hash
            if self.database_hash == hash_string(str(self.storage.read())):
                # if underlying file not changed, write data
                self.storage.write(self.cache)
            else:
                raise MetastoreError(
                    "Could not write to object store: object store modified while write in progress."
                )

        # if close is called explicitly, rather than through a context manager,
        # then the cache should be empty, otherwise if the metastore instance is reused
        # it won't reflect the actual state of the metastore. (This is an edge case.)
        self.cache = None
        self.writes_made = False

        # let underlying storage clean up
        self.storage.close()


@contextmanager
def open_metastore(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[TinyDBMetaStore, None, None]:
    """Context manager for TinyDBMetaStore based on OpenGHG v<=6.2 set-up for keys
    and TinyDB.

    Args:
        bucket: path to object store
        data_type: data type of metastore to open
        mode: 'rw' for read/write, 'r' for read only

    Yields:
        ClassicMetaStore instance.
    """
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
