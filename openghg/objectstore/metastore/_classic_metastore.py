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

from collections.abc import Generator
from contextlib import contextmanager
import json
from typing import Literal, Optional, TypeVar

import tinydb
from tinydb.middlewares import CachingMiddleware

from openghg.objectstore import exists, get_object, set_object_from_json
from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.types import MetastoreError


object_store_data_classes = {  # TODO: move this to central location after ObjectStore PR
    "surface": {"_root": "ObsSurface", "_uuid": "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"},
    "column": {"_root": "ObsColumn", "_uuid": "5c567168-0287-11ed-9d0f-e77f5194a415"},
    "emissions": {"_root": "Emissions", "_uuid": "c5c88168-0498-40ac-9ad3-949e91a30872"},
    "footprints": {"_root": "Footprints", "_uuid": "62db5bdf-c88d-4e56-97f4-40336d37f18c"},
    "boundary_conditions": {"_root": "BoundaryConditions", "_uuid": "4e787366-be91-4fc5-ad1b-4adcb213d478"},
    "eulerian_model": {"_root": "EulerianModel", "_uuid": "63ff2365-3ba2-452a-a53d-110140805d06"},
}


def get_metakey(data_type: str) -> str:
    """Return the metakey for a given data type.

    Args:
        data_type: data type to get metakey for.

    Returns:
        Metakey string for given data type, if found, or "default"
            if data type not found.
    """
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

    def read(self) -> Optional[dict]:
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


@contextmanager
def open_metastore(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[ClassicMetaStore, None, None]:
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
    with tinydb.TinyDB(bucket, key, mode, storage=CachingMiddleware(BucketKeyStorage)) as db:
        metastore = ClassicMetaStore(database=db, data_type=data_type)
        yield metastore


CM = TypeVar("CM", bound="ClassicMetaStore")


class ClassicMetaStore(TinyDBMetaStore):
    """TinyDBMetaStore using set-up for keys and TinyDB from OpenGHG <=v6.2"""

    def __init__(self, database: tinydb.TinyDB, data_type: str) -> None:
        super().__init__(database=database)
        self.data_type = data_type

    @classmethod
    def from_bucket(cls: type[CM], bucket: str, data_type: str) -> CM:
        """Initialise a ClassicMetaStore given a bucket and data type.

        A ClassicMetaStore opened with this method must be closed to
        ensure that writes are saved.

        Args:
            bucket: path to object store
            data_type: data type of metastore to open

        Returns:
            ClassicMetastore object for given bucket and data type.
        """
        key = get_metakey(data_type)
        database = tinydb.TinyDB(bucket, key, mode="rw", storage=CachingMiddleware(BucketKeyStorage))
        return cls(database=database, data_type=data_type)

    def close(self) -> None:
        """Close the underlying TinyDB database."""
        self._db.close()