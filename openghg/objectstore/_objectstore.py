"""This module defines an interface for object stores.

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

from collections.abc import Generator
from contextlib import contextmanager
from types import TracebackType
from typing import Any, Generic, Literal, TypeVar
from typing_extensions import Self
from uuid import uuid4

from openghg.objectstore._datasource import AbstractDatasource, DatasourceFactory
from openghg.objectstore._legacy_datasource import Datasource, get_legacy_datasource_factory
from openghg.objectstore.metastore import MetaStore, open_metastore
from openghg.objectstore.metastore._classic_metastore import DataClassMetaStore, FileLock, LockingError
from openghg.types import ObjectStoreError


DS = TypeVar("DS", bound="AbstractDatasource")


MetaData = dict[str, Any]
QueryResults = list[Any]
UUID = str
Data = Any
Bucket = str


class ObjectStore(Generic[DS]):
    def __init__(self, metastore: MetaStore, datasource_factory: DatasourceFactory) -> None:
        self.metastore = metastore
        self.datasource_factory = datasource_factory

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.close()

    def close(self) -> None:
        self.metastore.close()

    def search(self, metadata: MetaData) -> QueryResults:
        """Search the metastore.

        NOTE: currently this is a thin wrapper around MetaStore.search,
        but could be expanded to include information contained in datasources.
        """
        return self.metastore.search(metadata)

    def get_uuids(self, metadata: MetaData | None = None) -> list[UUID]:
        metadata = metadata or {}
        results = self.metastore.search(metadata)
        return [result["uuid"] for result in results]

    def create(self, metadata: MetaData, data: Data, **kwargs: Any) -> UUID:
        """Create a new datasource and store its metadata and UUID in the metastore.

        Args:
            metadata: metadata that should uniquely identify this datasource.
            data: data to store in datasource.
            kwargs: keyword args to pass to underlying Datasource storage method.

        Returns:
            UUID of newly added datasource.

        Raises:
            ObjectStoreError if the given metadata is already associated with a UUID.
        """
        if uuids := self.get_uuids(metadata):
            raise ObjectStoreError(
                f"Cannot create new Datasource: this metadata is already associated with UUID f{uuids[0]}."
            )

        uuid: UUID = str(uuid4())
        datasource = self.datasource_factory.new(uuid)

        try:
            # HACK to preserve current behaviour
            if hasattr(datasource, "add_metadata"):
                datasource.add_metadata(metadata=metadata, extend_keys=kwargs.get("extend_keys"))  # type: ignore
            # END HACK

            datasource.add(data, **kwargs)

            # HACK
            if hasattr(datasource, "metadata"):
                metadata = metadata.copy()
                metadata.update(datasource.metadata())
            # END HACK
        except Exception as e:
            raise e
        else:
            metadata["uuid"] = uuid

            self.metastore.insert(metadata)
            del metadata["uuid"]  # don't mutate the metadata
            datasource.save()

        return uuid

    def update(
        self, uuid: UUID, metadata: MetaData | None = None, data: Data | None = None, **kwargs: Any
    ) -> None:
        """Update metadata and/or data associated with a given UUID.

        Args:
            uuid: UUID of datasource to update
            metadata: metadata to add/overwrite metadata in metastore record associated with the given UUID.
            data: data to store in datasource with given UUID.
            kwargs: keyword args to pass to underlying Datasource storage method.

        Returns:
            None

        Raises:
            ObjectStoreError if the given UUID is not found.
        """
        if not self.metastore.search({"uuid": uuid}):
            raise ObjectStoreError(f"Cannot update: UUID {uuid} not found.")

        if metadata:
            # TODO: might need more sophisticated update method, e.g. for updating lists?
            self.metastore.update(where={"uuid": uuid}, to_update=metadata)

            # HACK to preserve current behaviour
            datasource = self.get_datasource(uuid)
            if hasattr(datasource, "add_metadata"):
                datasource.add_metadata(metadata=metadata, extend_keys=kwargs.get("extend_keys"))  # type: ignore
            if hasattr(datasource, "metadata"):
                self.metastore.update(where={"uuid": uuid}, to_update=datasource.metadata())  # type: ignore
            datasource.save()
            # END HACK

        if data:
            datasource = self.get_datasource(uuid)
            datasource.add(data, **kwargs)
            datasource.save()

            # HACK to preserve current behaviour
            if hasattr(datasource, "metadata"):
                self.metastore.update(where={"uuid": uuid}, to_update=datasource.metadata())  # type: ignore
            # END HACK

    def get_datasource(self, uuid: UUID) -> DS:
        """Get data stored at given uuid."""
        return self.datasource_factory.load(uuid)

    def delete(self, uuid: UUID) -> None:
        """Delete data and metadata with given UUID."""
        data = self.get_datasource(uuid)
        data.delete()
        self.metastore.delete({"uuid": uuid})


# Helper functions for creating object stores
@contextmanager
def open_object_store(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[ObjectStore[Datasource], None, None]:
    with open_metastore(bucket=bucket, data_type=data_type, mode=mode) as ms:
        ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)
        object_store = ObjectStore[Datasource](metastore=ms, datasource_factory=ds_factory)
        yield object_store


# Object store with locks
class LockingObjectStore(ObjectStore[DS]):
    """ObjectStore with lock that can be acquired and released with a context manager.

    The context manager (`with` statement) must be used to create, update, and delete data.
    """

    def __init__(self, metastore: MetaStore, datasource_factory: DatasourceFactory, lock: FileLock) -> None:
        super().__init__(metastore=metastore, datasource_factory=datasource_factory)
        self.lock = lock

    def __enter__(self) -> Self:
        self.lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.close()

    def close(self) -> None:
        super().close()
        self.lock.release()

    def create(self, metadata: MetaData, data: Data, **kwargs: Any) -> UUID:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to add new data.")
        return super().create(metadata, data, **kwargs)

    def update(
        self, uuid: UUID, metadata: MetaData | None = None, data: Data | None = None, **kwargs: Any
    ) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to update data.")

        return super().update(uuid, metadata, data, **kwargs)

    def delete(self, uuid: UUID) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to delete data.")

        return super().delete(uuid)


def locking_object_store(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> LockingObjectStore[Datasource]:
    ms = DataClassMetaStore(bucket=bucket, data_type=data_type)
    ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)
    object_store = LockingObjectStore[Datasource](metastore=ms, datasource_factory=ds_factory, lock=ms.lock)

    return object_store
