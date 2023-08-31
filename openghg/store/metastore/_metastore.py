"""
This module defines an interface for metastores.

Metastores store metadata and associated uuid's.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable
from typing import Any, Generic, TypeVar
from uuid import uuid4

import tinydb


T = TypeVar("T", bound="BucketUUIDLoadable")


@runtime_checkable
class BucketUUIDLoadable(Protocol):
    """Protocol for objects that can be created via a `load` class method
    that takes a bucket and a uuid as arguments.
    """

    @classmethod
    @abstractmethod
    def load(cls: type[T], bucket: str, uuid: str) -> T:
        pass


class MetaStore(ABC, Generic[T]):
    def __init__(self, storage_object: type[T], bucket: str) -> None:
        self._storage_object = storage_object
        self._bucket = bucket

    @abstractmethod
    def search(self, search_terms: dict[str, Any]) -> list[Any]:
        """Search for data using a dictionary of search terms.

        TODO: need to specify output format.
        """
        pass

    @abstractmethod
    def add(self, metadata: dict[str, Any]) -> str:
        """Add new metadata to the object store.

        Add creates an uuid for the associated data, stores the metadata together
        with the uuid, and returns the uuid.
        """
        pass

    def get(self, uuid: str) -> T:
        """Get data stored at given uuid."""
        return self._storage_object.load(self._bucket, uuid)


def get_new_uuid() -> str:
    """Return a new uuid as a string."""
    return str(uuid4())


class TinyDBMetaStore(MetaStore[T]):
    def __init__(self, storage_object: type[T], bucket: str, session: tinydb.TinyDB) -> None:
        super().__init__(storage_object=storage_object, bucket=bucket)
        self._metastore = session

    def search(self, search_terms: dict[str, Any] = dict()) -> list[Any]:
        search_terms = {k.lower(): v for k, v in search_terms.items()}
        query = tinydb.Query().fragment(search_terms)
        return list(self._metastore.search(query))  # TODO: find better way to deal with mypy than casting...

    def add(self, metadata: dict[str, Any]) -> str:
        uuid = get_new_uuid()
        metadata = {k.lower(): v for k, v in metadata.items()}
        metadata["uuid"] = uuid
        self._metastore.insert(metadata)
        return uuid
