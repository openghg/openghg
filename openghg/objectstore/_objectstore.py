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

from typing import Generic, TypeVar
from typing import Any, Dict, List, Optional
from uuid import uuid4

from openghg.objectstore._datasource import Datasource
from openghg.objectstore.metastore import MetaStore
from openghg.types import ObjectStoreError


DS = TypeVar("DS", bound="Datasource")


MetaData = Dict[str, Any]
QueryResults = List[Any]
UUID = str
Data = Any
Bucket = str


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
        return [result["uuid"] for result in results]

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
        if uuids := self.get_uuids(metadata):
            raise ObjectStoreError(
                f"Cannot create new Datasource: this metadata is already associated with UUID f{uuids[0]}."
            )

        uuid: UUID = str(uuid4())
        datasource = self.datasource_class(uuid)
        try:
            datasource.add(data)
        except Exception as e:
            raise e
        else:
            metadata["uuid"] = uuid
            self.metastore.add(metadata)
            del metadata["uuid"]  # don't mutate the metadata
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
        if not self.metastore.search({"uuid": uuid}):
            raise ObjectStoreError(f"Cannot update: UUID {uuid} not found.")

        if metadata:
            self.metastore.update(where={"uuid": uuid}, to_update=metadata)

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
