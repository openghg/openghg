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

from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from types import TracebackType
from typing import Any, Generic, Literal, TypeAlias, TypeVar
from typing_extensions import Self
from uuid import uuid4

import xarray as xr

from openghg.objectstore._datasource import DatasourceFactory, DatasourceT
from openghg.objectstore._legacy_datasource import Datasource, get_legacy_datasource_factory
from openghg.objectstore.metastore import MetaStore, open_metastore
from openghg.objectstore.metastore._classic_metastore import DataClassMetaStore, FileLock, LockingError
from openghg.types import ObjectStoreError
from openghg.util import split_function_inputs


MetaData = dict[str, Any]
QueryResults = list[Any]
UUID = str
T = TypeVar("T")
Bucket = str


class ObjectStore(Generic[DatasourceT, T]):
    def __init__(
        self,
        metastore: MetaStore,
        datasource_factory: DatasourceFactory[DatasourceT],
        metadata_updater: (
            Callable[[QueryResults, Iterable[DatasourceT]], tuple[QueryResults, Iterable[DatasourceT]]] | None
        ) = None,
    ) -> None:
        self.metastore = metastore
        self.datasource_factory = datasource_factory

        def default_metadata_updater(
            metadata: QueryResults, datasources: Iterable[DatasourceT]
        ) -> tuple[QueryResults, Iterable[DatasourceT]]:
            return metadata, datasources

        self.metadata_updater = metadata_updater if metadata_updater is not None else default_metadata_updater

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

    def _search(self, metadata: MetaData | None = None, **kwargs: Any) -> QueryResults:
        """Internal metastore search.

        TODO: fix the arguments: metastore search is now more complex...

        This only retrieves metadata from the metastore. The public `search` method adds
        metadata from `Datasources` to this.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            Query results (list of search results)
        """
        metadata = metadata or {}

        # get arguments for search function
        params, remainder = split_function_inputs({**metadata, **kwargs}, self.metastore.search)

        if "search_terms" in params:
            params["search_terms"].update(**remainder)
        else:
            params["search_terms"] = remainder

        return self.metastore.search(**params)

    def get_datasource(self, uuid: UUID) -> DatasourceT:
        """Get data stored at given uuid."""
        return self.datasource_factory.load(uuid)

    def _retrieve(
        self, metadata: MetaData | None = None, **kwargs: Any
    ) -> tuple[QueryResults, Iterable[DatasourceT]]:
        """Internal retrieve.

        Searches the metastore then gets the Datasources corresponding to the search results.
        The search results and datasources are updated by `self.metadata_updater`, before being
        returned.

        This allows adding metadata to the search results from the datasources, and adding metadata from
        the metastore to the datasources.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            Query results and corresponding Datasources, updated by `self.metadata_updater`
        """
        search_results = self._search(metadata, **kwargs)
        datasources = (self.get_datasource(r["uuid"]) for r in search_results)
        return self.metadata_updater(search_results, datasources)

    def search(self, metadata: MetaData | None = None, **kwargs: Any) -> QueryResults:
        """Search the metastore.

        TODO: fix the arguments: metastore search is now more complex...

        NOTE: currently this is a thin wrapper around MetaStore.search,
        but could be expanded to include information contained in datasources.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            Query results (list of search results)
        """
        search_results, _ = self._retrieve(metadata, **kwargs)
        return search_results

    def retrieve(self, metadata: MetaData | None = None, **kwargs: Any) -> list[DatasourceT]:
        """Retrieve Datasources from the ObjectStore.

        Searching works the same as the `.search` method.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            Query results (list of search results)
        """
        _, datasources = self._retrieve(metadata, **kwargs)
        return list(datasources)

    def get_uuids(self, metadata: MetaData | None = None) -> list[UUID]:
        metadata = metadata or {}
        results = self.metastore.search(metadata)
        return [result["uuid"] for result in results]

    @property
    def uuids(self) -> list[UUID]:
        """UUIDs stored in ObjectStore."""
        return self.get_uuids()

    def create(self, metadata: MetaData, data: T, **kwargs: Any) -> UUID:
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
        self,
        uuid: UUID,
        metadata: MetaData | None = None,
        data: T | None = None,
        keys_to_delete: str | list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Update metadata and/or data associated with a given UUID.

        Args:
            uuid: UUID of datasource to update
            metadata: metadata to add/overwrite metadata in metastore record associated with the given UUID.
            data: data to store in datasource with given UUID.
            keys_to_delete: metadata keys to delete.
            kwargs: keyword args to pass to underlying Datasource storage method.

        Returns:
            None

        Raises:
            ObjectStoreError if the given UUID is not found.
        """
        if not self.metastore.search({"uuid": uuid}):
            raise ObjectStoreError(f"Cannot update: UUID {uuid} not found.")

        # Don't allow UUID to be deleted
        if keys_to_delete is not None:
            if isinstance(keys_to_delete, str):
                keys_to_delete = [keys_to_delete]
            if "uuid" in keys_to_delete:
                raise ValueError("Cannot delete UUID.")

        if metadata is not None and "uuid" in metadata:
            raise ValueError("Cannot update UUID.")

        if metadata or keys_to_delete:
            # TODO: might need more sophisticated update method, e.g. for updating lists?
            self.metastore.update(where={"uuid": uuid}, to_update=metadata, to_delete=keys_to_delete)

            # HACK to preserve current behaviour
            datasource = self.get_datasource(uuid)
            if hasattr(datasource, "add_metadata") and metadata is not None:
                datasource.add_metadata(metadata=metadata, extend_keys=kwargs.get("extend_keys"))  # type: ignore
            if hasattr(datasource, "_metadata") and keys_to_delete is not None:
                for k in keys_to_delete:
                    del datasource._metadata[k]  # type: ignore
            datasource.save()
            if hasattr(datasource, "metadata"):
                self.metastore.update(where={"uuid": uuid}, to_update=datasource.metadata())  # type: ignore
            # END HACK

        if data:
            datasource = self.get_datasource(uuid)
            datasource.add(data, **kwargs)
            datasource.save()

            # HACK to preserve current behaviour
            if hasattr(datasource, "metadata"):
                self.metastore.update(where={"uuid": uuid}, to_update=datasource.metadata())  # type: ignore
            # END HACK

    def delete(self, uuid: UUID) -> None:
        """Delete data and metadata with given UUID."""
        data = self.get_datasource(uuid)
        data.delete()
        self.metastore.delete({"uuid": uuid})


# Helper functions for creating object stores
@contextmanager
def open_object_store(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[ObjectStore[Datasource, xr.Dataset], None, None]:
    with open_metastore(bucket=bucket, data_type=data_type, mode=mode) as ms:
        ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)
        object_store = ObjectStore[Datasource, xr.Dataset](metastore=ms, datasource_factory=ds_factory)
        yield object_store


def get_datasource(bucket: str, uuid: str, data_type: str | None = None) -> Datasource:
    """Open Datasource with given bucket and uuid.

    Passing the data type is slightly more efficient, but not necessary.

    Args:
        bucket: location of object store
        uuid: uuid of Datasource
        data_type: optional data type of Datasource

    Returns:
        specified Datasource

    Raises:
        ObjectStoreError: if no Datasource with the given UUID is found the
        object store.

    """
    if data_type is not None:
        with open_object_store(bucket=bucket, data_type=data_type, mode="r") as objstore:
            return objstore.get_datasource(uuid=uuid)
    else:
        # try iterating over all data types
        from openghg.store.spec import define_data_types

        for dtype in define_data_types():
            try:
                with open_object_store(bucket=bucket, data_type=dtype, mode="r") as objstore:
                    result = objstore.get_datasource(uuid=uuid)
            except ObjectStoreError:
                continue
            else:
                return result

    # search over all data types failed
    raise ObjectStoreError(f"Datasource with uuid {uuid} not found in bucket {bucket}.")


# Object store with locks
class LockingObjectStore(ObjectStore[DatasourceT, T]):
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

    def create(self, metadata: MetaData, data: T, **kwargs: Any) -> UUID:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to add new data.")
        return super().create(metadata, data, **kwargs)

    def update(
        self,
        uuid: UUID,
        metadata: MetaData | None = None,
        data: T | None = None,
        keys_to_delete: str | list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to update data.")

        return super().update(uuid, metadata, data, keys_to_delete, **kwargs)

    def delete(self, uuid: UUID) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to delete data.")

        return super().delete(uuid)


LockingObjectStoreType: TypeAlias = LockingObjectStore[Datasource, xr.Dataset]


def locking_object_store(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> LockingObjectStoreType:
    ms = DataClassMetaStore(bucket=bucket, data_type=data_type)
    ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)
    object_store = LockingObjectStore[Datasource, xr.Dataset](
        metastore=ms, datasource_factory=ds_factory, lock=ms.lock
    )

    return object_store
