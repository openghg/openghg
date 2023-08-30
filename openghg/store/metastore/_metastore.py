"""
This module defines an interface for metastores.

Metastores store metadata and associated uuid's.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable
from typing import Any, Generic, TypeVar

from openghg.dataobjects import SearchResults


T = TypeVar('T', bound='DataObject')


@runtime_checkable
class DataObject(Protocol):
    """Protocol for objects that can be created
    via a `load` class method that takes a bucket and
    a uuid as arguments.
    """
    @classmethod
    @abstractmethod
    def load(cls: type[T], bucket: str, uuid: str) -> T:
        pass


class MetaStore(Generic[T]):
    data_object: type[T]

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    @abstractmethod
    def search(self, search_terms: dict[str, Any]) -> SearchResults:
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
        return self.data_object.load(self.bucket, uuid)
