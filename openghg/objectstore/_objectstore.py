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
from typing import Generic, TypeVar
from typing import Any, Dict, List, Optional
from uuid import uuid4

from openghg.store.metastore._metastore import MetaStore
from openghg.types import ObjectStoreError


M = TypeVar("M", bound=MetaStore)
DS = TypeVar("DS", bound='AbstractDatasource')


MetaData = Dict[str, Any]
QueryResults = List[Any]
UUID = str
Data = Any
Bucket = str


class AbstractDatasource(ABC):
    """Interface for Datasource-like objects.

    The data stored in a Datasource is assumed to be related in some way.

    For instance, a data source might contain time series data for concentrations
    of a particular gas, measured from a specific instrument, at a specific
    inlet height, and at a specific site.

    Datasources are stored by UUID within buckets, and must have a `load` classmethod
    to support this.
    """
    def __init__(self, uuid: UUID) -> None:
        self.uuid = uuid

    @classmethod
    @abstractmethod
    def load(cls: type[DS], bucket: str, uuid: str) -> DS:
        pass

    @abstractmethod
    def add(self, data: Data) -> None:
        """Add data to the datasource.

        TODO: add `overwrite` argument, with expected error type
        if trying to overwrite data without saying so.
        """
        pass

    @abstractmethod
    def delete(self) -> None:
        """Delete all of the data stored by this datasource."""
        pass

    @abstractmethod
    def save(self, bucket: Bucket) -> None:
        """Save changes to datasource made by `add` method."""
        pass


class ObjectStore(Generic[DS]):
    def __init__(self, metastore: MetaStore, datasource_class: type[DS], bucket: Bucket) -> None:
        self.metastore = metastore
        self.datasource_class = datasource_class
        self.bucket = bucket

    def search(self, metadata: MetaData) -> QueryResults:
        """Search the metastore.

        NOTE: currently this is a thin wrapper around MetaStore.search,
        but could be expanded to include information contained in datasources.
        """
        return self.metastore.search(metadata)

    def get_uuids(self, metadata: MetaData = dict()) -> list[UUID]:
        results = self.metastore.search(metadata)
        return [result['uuid'] for result in results]

    def create(self, metadata: MetaData, data: Data) -> None:
        """Create a new datasource and store its metadata and UUID in the metastore.

        Args:
            metadata: metadata that should uniquely identify this datasource.
            data: data to store in datasource.

        Returns:
            None

        Raises:
            ObjectStoreError if the given metadata is already associated with a UUID.
        """
        if (uuids := self.get_uuids(metadata)):
            raise ObjectStoreError(f'Cannot create new Datasource: this metadata is already associated with UUID f{uuids[0]}.')

        uuid: UUID = str(uuid4())
        datasource = self.datasource_class(uuid)
        try:
            datasource.add(data)
        except Exception as e:
            raise e
        else:
            metadata['uuid'] = uuid
            self.metastore.add(metadata)
            del metadata['uuid']  # don't mutate the metadata
            datasource.save(bucket=self.bucket)

    def update(self, uuid: UUID, metadata: Optional[MetaData] = None, data: Optional[Data] = None) -> None:
        """Update metadata and/or data associated with a given UUID.

        Args:
            uuid: UUID of datasource to update
            metadata: metadata to add/overwrite metadata in metastore record associated with the given UUID.
            data: data to store in datasource with given UUID.

        Returns:
            None

        Raises:
            ObjectStoreError if the given UUID is not found.
        """
        if not self.metastore.search({'uuid': uuid}):
            raise ObjectStoreError(f'Cannot update: UUID {uuid} not found.')

        if metadata:
            self.metastore.update(record_to_update={'uuid': uuid}, metadata_to_add=metadata)

        if data:
            datasource = self.get_data(uuid)
            datasource.add(data)
            datasource.save(bucket=self.bucket)

    def get_data(self, uuid: UUID) -> DS:
        """Get data stored at given uuid."""
        return self.datasource_class.load(self.bucket, uuid)

    def delete(self, uuid: UUID) -> None:
        """Delete data and metadata with given UUID."""
        data = self.get_data(uuid)
        data.delete()
        self.metastore.delete({"uuid": uuid})
