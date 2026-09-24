"""This module defines an interface for object stores.

Object stores comprise a metastore for storing metadata
and a collection of data, which is accessible via the metadata.

Data is organized into logical units called "objects" or "datasources".
Datasources are accessed by a UUID, which is stored in the metastore along
with metadata used to search for a particular datasource.

An ObjectStore object coordinates the metastore and storage of data.
In particular, it manages UUIDs and controls any operation that involves
both metadata and data.

The metastore attached to an ObjectStore is the raw persisted metastore.
Search and retrieve operations use a private metastore view to include
metadata stored on datasources without changing the persisted records.

"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
import inspect
from types import TracebackType
from typing import Any, TYPE_CHECKING, Generic, Literal, TypeAlias, TypeVar
import warnings
from typing_extensions import Self
from uuid import uuid4

import tinydb
from openghg.objectstore._datasource import DatasourceFactory, DatasourceT
from openghg.objectstore._legacy_datasource import Datasource, get_legacy_datasource_factory
from openghg.objectstore.metastore import MetaStore, TinyDBMetaStore, open_metastore
from openghg.objectstore.metastore._classic_metastore import DataClassMetaStore, FileLock, LockingError
from openghg.types import ObjectStoreError
from openghg.util import split_function_inputs

if TYPE_CHECKING:
    import xarray as xr

MetaData = dict[str, Any]
QueryResults = list[MetaData]
UUID = str
T = TypeVar("T")
Bucket = str

MetadataUpdaterT = Callable[[QueryResults, Iterable[DatasourceT]], tuple[QueryResults, Iterable[DatasourceT]]]
"""Type for function that transforms query results and a list of datasources."""

DATASOURCE_METADATA_OVERRIDE_KEYS = {
    "end_date",
    "latest_version",
    "sampling_period",
    "start_date",
    "tag",
    "timestamp",
    "versions",
}
"""Datasource-managed canonical metadata keys that override raw metastore values.

Raw metastore records remain authoritative for descriptor keys. These keys are
maintained by datasource update paths, where values may be normalised or
list-expanded before being returned in search and retrieve results.
"""


def _default_metadata_updater(
    metadata: QueryResults, datasources: Iterable[DatasourceT]
) -> tuple[QueryResults, Iterable[DatasourceT]]:
    """Default metadata updater for ObjectStore."""
    return metadata, datasources


@contextmanager
def _memory_metastore(records: QueryResults) -> Generator[TinyDBMetaStore, None, None]:
    """Create an in-memory TinyDB metastore populated with metadata records.

    Args:
        records: Metadata records to make searchable.

    Yields:
        TinyDBMetaStore backed by a temporary MemoryStorage database.
    """
    with tinydb.TinyDB(storage=tinydb.storages.MemoryStorage) as database:
        metastore = TinyDBMetaStore(database=database)
        formatted_records = [{key.lower(): value for key, value in record.items()} for record in records]
        if formatted_records:
            database.insert_multiple(formatted_records)
        yield metastore


class _MergedMetadataView(Generic[DatasourceT]):
    """Read view that combines raw metastore records with datasource metadata.

    This class is not a persistence layer and does not implement MetaStore. It
    reads raw candidates through the wrapped metastore, applies the metadata
    updater, then filters the merged records. Search arguments named
    `search_terms`, `search_functions`, `negative_lookup_keys`, and
    `search_list_keys` mirror the search arguments used by ObjectStore and
    TinyDBMetaStore.

    Args:
        metastore: Raw persisted metastore used for first-pass candidate lookup.
        datasource_factory: Factory used to load datasources for candidate UUIDs.
        metadata_updater: Callable that combines raw metadata and datasources
            before final filtering.
    """

    def __init__(
        self,
        metastore: MetaStore,
        datasource_factory: DatasourceFactory[DatasourceT],
        metadata_updater: MetadataUpdaterT,
    ) -> None:
        self._metastore = metastore
        self._datasource_factory = datasource_factory
        self._metadata_updater = metadata_updater

    def _load_datasource(self, uuid: UUID) -> DatasourceT:
        return self._datasource_factory.load(uuid)

    def _retrieve_candidates(
        self, search_terms: MetaData | None = None, skip_uuids: set[UUID] | None = None
    ) -> tuple[QueryResults, list[DatasourceT], QueryResults]:
        """Retrieve raw candidates and apply datasource metadata updates.

        Args:
            search_terms: Exact search terms used for the first-pass raw
                metastore lookup.
            skip_uuids: UUIDs to exclude from the returned candidates.

        Returns:
            Tuple of updated metadata records, their corresponding datasources,
            and raw records whose datasources could not be loaded.
        """
        skip_uuids = skip_uuids or set()
        raw_results = []
        datasources = []
        missing_datasource_results = []
        for result in self._metastore.search(search_terms=search_terms):
            uuid = result["uuid"]
            if uuid in skip_uuids:
                continue
            try:
                datasource = self._load_datasource(uuid)
            except (ObjectStoreError, LookupError):
                missing_datasource_results.append(dict(result))
                continue
            raw_results.append(dict(result))
            datasources.append(datasource)

        updated_results, updated_datasources = self._metadata_updater(raw_results, datasources)
        return list(updated_results), list(updated_datasources), missing_datasource_results

    @staticmethod
    def _candidate_search_terms(search_terms: MetaData) -> list[MetaData | None]:
        """Generate raw metastore lookup terms for a merged metadata search.

        Args:
            search_terms: Exact metadata terms requested by the caller.

        Returns:
            Search terms for candidate lookups, ordered from narrowest to
            broadest. The final `None` lookup loads all raw records when exact
            terms were supplied.
        """
        if not search_terms:
            return [{}]

        broadened_terms = [
            {key: value for key, value in search_terms.items() if key != key_to_drop}
            for key_to_drop in search_terms
        ]
        return [search_terms, *broadened_terms, None]

    @staticmethod
    def _filter_results(
        results: QueryResults,
        search_terms: MetaData | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
        search_list_keys: dict | None = None,
    ) -> QueryResults:
        """Filter candidate records using TinyDBMetaStore search semantics.

        Args:
            results: Merged metadata records to filter.
            search_terms: Exact metadata terms to match.
            search_functions: Metadata keys and predicate functions to match.
            negative_lookup_keys: Metadata keys that must be absent.
            search_list_keys: Metadata list keys and values that must be present
                in those lists.

        Returns:
            Records from `results` that match the search criteria.
        """
        if not results:
            return []

        with _memory_metastore(results) as metastore:
            filtered_results = metastore.search(
                search_terms=search_terms,
                search_functions=search_functions,
                negative_lookup_keys=negative_lookup_keys,
                search_list_keys=search_list_keys,
            )
            if filtered_results or not (search_terms or search_list_keys):
                return filtered_results

            from openghg.util import to_lowercase

            formatted_search_terms = to_lowercase(search_terms) if search_terms else search_terms
            formatted_search_list_keys = (
                to_lowercase(search_list_keys) if search_list_keys else search_list_keys
            )
            return metastore.search(
                search_terms=formatted_search_terms,
                search_functions=search_functions,
                negative_lookup_keys=negative_lookup_keys,
                search_list_keys=formatted_search_list_keys,
            )

    @staticmethod
    def _filter_datasources(
        filtered_results: QueryResults,
        candidate_results: QueryResults,
        datasources: list[DatasourceT],
        attach_metadata: bool = False,
    ) -> list[DatasourceT]:
        """Return datasources corresponding to filtered metadata records.

        Args:
            filtered_results: Metadata records that survived final filtering.
            candidate_results: Candidate metadata records before filtering.
            datasources: Datasources corresponding to `candidate_results`.
            attach_metadata: If True, update returned datasource objects in
                memory with the corresponding merged metadata records.

        Returns:
            Datasources corresponding to `filtered_results`, preserving filtered
            result order.
        """
        datasource_lookup = {
            result["uuid"]: datasource for result, datasource in zip(candidate_results, datasources)
        }
        filtered_datasources = [datasource_lookup[result["uuid"]] for result in filtered_results]
        if attach_metadata:
            for result, datasource in zip(filtered_results, filtered_datasources):
                metadata = getattr(datasource, "metadata", None)
                if isinstance(metadata, dict):
                    metadata.clear()
                    metadata.update({key: value for key, value in result.items() if key != "object_store"})
        return filtered_datasources

    def retrieve(
        self,
        search_terms: MetaData | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
        search_list_keys: dict | None = None,
        attach_metadata: bool = True,
    ) -> tuple[QueryResults, list[DatasourceT]]:
        """Retrieve merged metadata records and datasources matching search parameters.

        Candidate lookup starts with exact raw metastore terms, broadens by
        dropping exact terms one at a time, then loads all raw records. Final
        filtering is applied once to the de-duplicated merged candidate set so
        mixed fresh and stale raw metadata records can all be returned.

        Args:
            search_terms: Exact metadata terms to match.
            search_functions: Metadata keys and predicate functions to match.
            negative_lookup_keys: Metadata keys that must be absent.
            search_list_keys: Metadata list keys and values that must be present
                in those lists.
            attach_metadata: If True, update returned datasource objects in
                memory with their corresponding merged metadata records.

        Returns:
            Tuple containing matched merged metadata records and their
            corresponding datasources.
        """
        search_terms = search_terms or {}

        candidate_results: QueryResults = []
        datasources: list[DatasourceT] = []
        seen_uuids: set[UUID] = set()
        exact_missing_datasource_results: QueryResults = []
        exact_loaded_candidate_count = 0

        for index, candidate_terms in enumerate(self._candidate_search_terms(search_terms)):
            new_results, new_datasources, missing_datasource_results = self._retrieve_candidates(
                search_terms=candidate_terms, skip_uuids=seen_uuids
            )
            if index == 0:
                exact_missing_datasource_results = missing_datasource_results
                exact_loaded_candidate_count = len(new_results)
            candidate_results.extend(new_results)
            datasources.extend(new_datasources)
            seen_uuids.update(result["uuid"] for result in new_results)

        filtered_results = self._filter_results(
            candidate_results,
            search_terms=search_terms,
            search_functions=search_functions,
            negative_lookup_keys=negative_lookup_keys,
            search_list_keys=search_list_keys,
        )
        if filtered_results:
            return filtered_results, self._filter_datasources(
                filtered_results, candidate_results, datasources, attach_metadata=attach_metadata
            )

        if exact_missing_datasource_results and exact_loaded_candidate_count == 0:
            uuid = exact_missing_datasource_results[0]["uuid"]
            raise ObjectStoreError(f"No Datasource with uuid {uuid} found")

        return [], []

    def search(
        self,
        search_terms: MetaData | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
        search_list_keys: dict | None = None,
    ) -> QueryResults:
        """Search merged metastore and datasource metadata.

        Args:
            search_terms: Exact metadata terms to match.
            search_functions: Metadata keys and predicate functions to match.
            negative_lookup_keys: Metadata keys that must be absent.
            search_list_keys: Metadata list keys and values that must be present
                in those lists.

        Returns:
            Merged metadata records matching the search criteria.
        """
        results, _ = self.retrieve(
            search_terms=search_terms,
            search_functions=search_functions,
            negative_lookup_keys=negative_lookup_keys,
            search_list_keys=search_list_keys,
            attach_metadata=False,
        )
        return results

    def select(self, key: str) -> list[Any]:
        """Select values from merged metadata records.

        Args:
            key: Metadata key to select.

        Returns:
            Values stored at `key` in all merged metadata records.
        """
        return [result[key] for result in self.search()]


class ObjectStore(Generic[DatasourceT, T]):
    def __init__(
        self,
        metastore: MetaStore,
        datasource_factory: DatasourceFactory[DatasourceT],
        metadata_updater: MetadataUpdaterT | None = None,
    ) -> None:
        self.metastore = metastore
        self.datasource_factory = datasource_factory

        # use default metadata updater if None is passed
        self.metadata_updater = metadata_updater or _default_metadata_updater
        self._metadata_view = _MergedMetadataView(
            metastore=self.metastore,
            datasource_factory=self.datasource_factory,
            metadata_updater=self.metadata_updater,
        )

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

    def _search_params(self, metadata: MetaData | None = None, **kwargs: Any) -> dict:
        """Prepare metastore search parameters from metadata and keyword arguments."""
        metadata = metadata or {}
        params, remainder = split_function_inputs({**metadata, **kwargs}, self._metadata_view.search)

        if "search_terms" in params:
            params["search_terms"] = params["search_terms"] or {}
            params["search_terms"].update(**remainder)
        else:
            params["search_terms"] = remainder
        return params

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
        params = self._search_params(metadata, **kwargs)
        search_parameters = inspect.signature(self.metastore.search).parameters
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in search_parameters.values()):
            return self.metastore.search(**params)

        compatible_params = {key: value for key, value in params.items() if key in search_parameters}
        return self.metastore.search(**compatible_params)

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
        """Search object store metadata.

        Uses the metastore as the first-pass index, then filters the merged
        metastore and Datasource metadata returned to users.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            Query results (list of search results)
        """
        params = self._search_params(metadata, **kwargs)
        try:
            search_results = self._metadata_view.search(**params)
        except ObjectStoreError as e:
            # Datasource not found? just warn...
            warnings.warn(f"Metadata found without corresponding Datasource {e}.")
            search_results = self._search(metadata, **kwargs)

        return list(search_results)

    def retrieve(self, metadata: MetaData | None = None, **kwargs: Any) -> list[DatasourceT]:
        """Retrieve Datasources from the ObjectStore.

        Searching works the same as the `.search` method.

        Args:
            metadata: metadata to narrow search by
            **kwargs: keyword arg version of search metadata

        Returns:
            list of Datasources corresponding to query.
        """
        params = self._search_params(metadata, **kwargs)
        _, datasources = self._metadata_view.retrieve(**params)
        return list(datasources)

    def get_uuids(self, metadata: MetaData | None = None) -> list[UUID]:
        metadata = metadata or {}
        if not metadata:
            results = self.metastore.search()
        else:
            try:
                results = self._metadata_view.search(search_terms=metadata)
            except ObjectStoreError as e:
                warnings.warn(f"Metadata found without corresponding Datasource {e}.")
                results = self._search(metadata)
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
        existing_results = self._search(metadata)
        existing_uuids = [result["uuid"] for result in existing_results]
        merged_uuids = [uuid for uuid in self.get_uuids(metadata) if uuid not in existing_uuids]

        if uuids := [*existing_uuids, *merged_uuids]:
            raise ObjectStoreError(
                f"Cannot create new Datasource: this metadata is already associated with UUID {uuids[0]}."
            )

        uuid: UUID = str(uuid4())
        datasource = self.datasource_factory.new(uuid)

        datasource.add(data, **kwargs)
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
        extend_keys: list[str] | None = None,
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
            to_extend = None
            if extend_keys and metadata is not None:
                to_extend = {}
                for key in extend_keys:
                    if key in metadata:
                        to_extend[key] = metadata.pop(key)

            self.metastore.update(
                where={"uuid": uuid}, to_update=metadata, to_delete=keys_to_delete, to_extend=to_extend
            )

        if data:
            datasource = self.get_datasource(uuid)
            datasource.add(data, **kwargs)
            datasource.save()

    def delete(self, uuid: UUID) -> None:
        """Delete data and metadata with given UUID."""
        data = self.get_datasource(uuid)
        data.delete()
        self.metastore.delete({"uuid": uuid})


# Helper functions for creating object stores
def _update_one(
    r: MetaData, d: DatasourceT, skip_keys: list | None = None, extend_keys: list | None = None
) -> tuple[Any, DatasourceT]:
    """Helper function for make_metadata_updater_fn.

    Updates one pair of metadata and datasource.

    Metastore metadata is normalised using the same lowercasing rules as
    Datasource metadata and takes precedence for descriptor keys in returned
    records. Datasource metadata fills keys missing from the metastore, extends
    configured list keys, and takes precedence for Datasource-managed storage
    keys. The Datasource itself is not persisted.
    """
    if not hasattr(d, "metadata"):
        return dict(r), d

    from openghg.util import merge_and_extend_dict, to_lowercase

    def list_metadata(metadata: MetaData) -> MetaData:
        """Return list-key metadata with string values wrapped in lists."""
        metadata_extend = {}
        for key in extend_keys:
            if key in metadata:
                value = metadata.pop(key)
                if isinstance(value, str):
                    value = [value]
                metadata_extend[key] = value
        return metadata_extend

    extend_keys = extend_keys or []
    raw_metadata = to_lowercase(dict(r), skip_keys=skip_keys)
    datasource_metadata = to_lowercase(d.metadata, skip_keys=skip_keys)  # type: ignore
    raw_extend = list_metadata(raw_metadata)
    datasource_extend = list_metadata(datasource_metadata)

    merged_metadata = dict(raw_metadata)
    for key, value in datasource_metadata.items():
        if key not in merged_metadata or key in DATASOURCE_METADATA_OVERRIDE_KEYS:
            merged_metadata[key] = value

    return merge_and_extend_dict(merge_and_extend_dict(merged_metadata, raw_extend), datasource_extend), d


def make_metadata_updater_fn(
    skip_keys: list | None = None, extend_keys: list | None = None
) -> MetadataUpdaterT:
    """Create metadata updater function using given `skip_keys` and `extend_keys`.

    Since `extend_keys` depends on the context (e.g. data type), this function
    helps create metadata updaters to suit different contexts.

    Args:
        skip_keys: keys whose values should not be lowercased when added to
        Datasource metadata.
        extend_keys: keys whose values are lists, and should be extended rather
        than overwritten.

    Returns:
        metadata updater function.

    """

    def metadata_updater(
        search_results: QueryResults, datasources: Iterable[DatasourceT]
    ) -> tuple[QueryResults, Iterable[DatasourceT]]:
        """Merge metastore and datasource metadata in returned search records.

        The returned metastore results include metadata from both sources.
        Metastore metadata takes precedence over Datasource metadata for
        descriptor keys; Datasource metadata takes precedence for
        Datasource-managed storage keys. Datasource objects are returned
        unchanged.

        Args:
            search_results: results of metastore search
            datasources: iterable of datasources corresponding to metastore
            search results.

        Returns:
            updated search results and corresponding datasources.

        """
        # handle empty search
        if not search_results:
            return search_results, datasources

        zipped_result = (
            _update_one(r, d, skip_keys, extend_keys) for r, d in zip(search_results, datasources)
        )
        search_iter, datasources_iter = list(
            zip(*zipped_result)
        )  # turn iterator of tuples into list of two lists
        return list(search_iter), datasources_iter

    return metadata_updater


@contextmanager
def open_object_store(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[ObjectStore[Datasource, xr.Dataset], None, None]:
    with open_metastore(bucket=bucket, data_type=data_type, mode=mode) as ms:
        ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)

        try:
            from openghg.store import find_list_metakeys

            list_keys = find_list_metakeys(data_type=data_type, bucket=bucket)
        except (ObjectStoreError, ValueError):
            list_keys = None
        metadata_updater = make_metadata_updater_fn(extend_keys=list_keys)

        object_store = ObjectStore(
            metastore=ms, datasource_factory=ds_factory, metadata_updater=metadata_updater
        )
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
            return objstore.retrieve(uuid=uuid)[0]
    else:
        # try iterating over all data types
        from openghg.store.spec import define_data_types

        for dtype in define_data_types():
            try:
                with open_object_store(bucket=bucket, data_type=dtype, mode="r") as objstore:
                    result = objstore.retrieve(uuid=uuid)[0]
            except (ObjectStoreError, IndexError):
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

    def __init__(
        self,
        metastore: MetaStore,
        datasource_factory: DatasourceFactory,
        metadata_updater: MetadataUpdaterT,
        lock: FileLock,
    ) -> None:
        super().__init__(
            metastore=metastore, datasource_factory=datasource_factory, metadata_updater=metadata_updater
        )
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
        extend_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to update data.")

        return super().update(uuid, metadata, data, keys_to_delete, extend_keys, **kwargs)

    def delete(self, uuid: UUID) -> None:
        if not self.lock.is_locked:
            raise LockingError("Object store must be locked to delete data.")

        return super().delete(uuid)


if TYPE_CHECKING:
    LockingObjectStoreType: TypeAlias = LockingObjectStore[Datasource, xr.Dataset]
else:
    LockingObjectStoreType: TypeAlias = LockingObjectStore


def locking_object_store(
    bucket: str,
    data_type: str,
    mode: Literal["r", "rw"] = "rw",
    skip_keys: list | None = None,
    extend_keys: list | None = None,
) -> LockingObjectStoreType:
    ms = DataClassMetaStore(bucket=bucket, data_type=data_type)
    ds_factory = get_legacy_datasource_factory(bucket=bucket, data_type=data_type, mode=mode)
    metadata_updater = make_metadata_updater_fn(skip_keys=skip_keys, extend_keys=extend_keys)
    object_store = LockingObjectStore(
        metastore=ms, datasource_factory=ds_factory, metadata_updater=metadata_updater, lock=ms.lock
    )

    return object_store
