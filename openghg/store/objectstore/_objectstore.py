"""
This module defines an interface for object stores.

Object stores comprise a metastore for storing metadata
and a collecton of data, which is accessible via the metadata.

Data is organized into logical units called "objects" or "datasources".
Datasources are accessed by a UUID, which is stored in the metastore along
with metadata used to search for a particular datasource.

An ObjectStore object coordinates the metastore and storage of data.
In particular, it manages UUIDs and controls any operation that involves
both metadata and data.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable
from typing import Generic, TypeVar
from typing import Any, Dict, List, Optional

from openghg.store.metastore._metastore import MetaStore
from openghg.types import ObjectStoreError


M = TypeVar("M", bound=MetaStore)
L = TypeVar("L", bound="SupportsBucketUUIDLoad")
D = TypeVar("D", bound="SupportsDelete")
DO = TypeVar("DO", bound="DataObject")


MetaData = Dict[str, Any]
QueryResults = List[Any]
UUID = str
Data = Any
Bucket = str

@runtime_checkable
class SupportsBucketUUIDLoad(Protocol):
    """Protocol for objects that can be created via a `load` class method
    that takes a bucket and a uuid as arguments.
    """

    @classmethod
    @abstractmethod
    def load(cls: type[L], bucket: str, uuid: str) -> L:
        pass


@runtime_checkable
class SupportsDelete(Protocol):
    """Protocol for objects that have a `delete` method.
    """

    @abstractmethod
    def delete(self) -> None:
        pass


@runtime_checkable
class DataObject(SupportsBucketUUIDLoad, SupportsDelete, Protocol):
    """Protocol for objects that can be loaded via a bucket and
    uuid, as well as be deleted.
    """
    pass


class ObjectStore(ABC, Generic[M, DO]):
    def __init__(self, metastore: type[M], data_object: type[DO], bucket: Bucket) -> None:
        self.metastore = metastore(bucket=bucket)
        self.data_object = data_object
        self.bucket = bucket

    @abstractmethod
    def create(self, metadata: MetaData, data: Data) -> None:
        if (result := self.metastore.search(metadata)):
            raise ObjectStoreError(f'Cannot create new Data Object: this metadata is already associated with UUID f{result[0]["uuid"]}.')

    @abstractmethod
    def update(self, uuid: UUID, metadata: Optional[MetaData] = None, data: Optional[Data] = None) -> None:
        if not self.metastore.search({'uuid': uuid}):
            raise ObjectStoreError(f'Cannot update: UUID {uuid} not found.')


    def get(self, uuid: UUID) -> DO:
        """Get data stored at given uuid."""
        return self.data_object.load(self.bucket, uuid)

    def delete(self, uuid: UUID) -> None:
        """Delete data and metadata with given UUID."""
        data = self.get(uuid)
        data.delete()
        self.metastore.delete({"uuid": uuid})
