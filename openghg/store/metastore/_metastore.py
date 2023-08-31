"""
This module defines an interface for metastores.

Metastores store metadata and associated uuid's.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable
from typing import Any, Generic, TypeVar


T = TypeVar('T', bound='BucketUUIDLoadable')


@runtime_checkable
class BucketUUIDLoadable(Protocol):
    """Protocol for objects that can be created
    via a `load` class method that takes a bucket and
    a uuid as arguments.
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
        """Search for data using a dictionary of search terms."""
        pass

    @abstractmethod
    def add(self, metadata: dict[str, Any]) -> str:
        """Add new metadata to the object store.

        Add creates an uuid for the associated data,
        stores the metadata together with the uuid,
        and returns the uuid.
        """
        pass

    def get(self, uuid: str) -> T:
        """Get data stored at given uuid."""
        return self._storage_object.load(self._bucket, uuid)
