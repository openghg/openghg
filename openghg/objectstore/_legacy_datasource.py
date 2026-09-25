from __future__ import annotations
from collections import defaultdict
from collections.abc import Callable, MutableMapping
from copy import deepcopy
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, TYPE_CHECKING, cast, Literal, TypeVar
from typing_extensions import Self
from types import TracebackType
import logging

from openghg.objectstore import exists, get_object_from_json
from openghg.objectstore._local_store import delete_object
from openghg.types import DataOverlapError, ObjectStoreError, ZarrStoreError

from ._datasource import AbstractDatasource, DatasourceFactory

if TYPE_CHECKING:
    from ._datasource_edit import DatasourceEdit
    from ._payload_edit import PayloadEdit
    from openghg.storage._zarr_store import VersionedZarrStore
    from pandas import Timestamp
    import xarray as xr
    from xarray import Dataset as XrDataset
else:
    XrDataset = Any

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(logging.DEBUG)

__all__ = ["Datasource"]

WriteMethodT = TypeVar("WriteMethodT", bound=Callable[..., Any])


def _requires_write(method: WriteMethodT) -> WriteMethodT:
    """Prevent write methods from running on read-only Datasources."""

    @wraps(method)
    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only datasource")
        guard = getattr(self, "_edit_context_guard", None)
        if guard is not None:
            guard()
        return method(self, *args, **kwargs)

    return cast(WriteMethodT, wrapped)


def _requires_legacy_mutation(method: WriteMethodT) -> WriteMethodT:
    """Keep legacy mutation methods from changing immutable publications."""

    @wraps(method)
    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        self._check_legacy_mutation()
        return method(self, *args, **kwargs)

    return cast(WriteMethodT, wrapped)


TimedDataAction = Literal["insert", "copy_insert", "replace", "upsert", "error_overlap"]


@dataclass(frozen=True)
class TimedDataUpdatePlan:
    """Storage action for adding time-indexed data to a Datasource."""

    action: TimedDataAction
    new_version: bool


def plan_timed_data_update(
    *,
    if_exists: str,
    new_version: bool,
    has_existing_data: bool,
    overlapping: bool,
) -> TimedDataUpdatePlan:
    """Plan the concrete store operation after overlap detection."""
    if not has_existing_data:
        return TimedDataUpdatePlan(action="insert", new_version=True)

    if if_exists == "new":
        action: TimedDataAction = "insert" if new_version else "replace"
        return TimedDataUpdatePlan(action=action, new_version=new_version)

    if if_exists == "combine":
        action = "upsert" if overlapping or not new_version else "copy_insert"
        return TimedDataUpdatePlan(action=action, new_version=new_version)

    if if_exists == "auto":
        if overlapping:
            return TimedDataUpdatePlan(action="error_overlap", new_version=False)
        return TimedDataUpdatePlan(action="copy_insert" if new_version else "insert", new_version=new_version)

    raise ValueError("Invalid if_exists option. Please use 'auto', 'new', or 'combine'.")


class Datasource(AbstractDatasource[XrDataset]):
    """A Datasource holds data relating to a single source.

    For instance, a specific species at a certain height on a specific
    instrument could be a single "Datasource".
    """

    _datasource_root = "datasource"
    _runtime_state_keys = frozenset(
        {
            "_store",
            "_root_store_key",
            "_stores_path",
            "_bucket",
            "_mode",
            "_status",
            "_start_date",
            "_end_date",
            "_active_edit",
            "_loaded_state",
            "_edit_context_guard",
            "_edit_failed",
        }
    )

    def __init__(self, bucket: str, uuid: str, mode: Literal["r", "rw"] = "rw", data_type: str = "") -> None:
        from openghg.util._time import timestamp_now

        self._uuid = uuid
        self._creation_datetime = str(timestamp_now())
        self._metadata: dict[str, str | list | dict] = {}
        self._start_date = None
        self._end_date = None
        self._status: dict | None = None
        self._data_keys = defaultdict(list)  # dict mapping version to lists of daterange strings
        self._data_type = data_type
        # Hold information regarding the versions of the data
        self._latest_version: str = ""
        self._timestamps: dict[str, str] = {}
        self._active_edit: DatasourceEdit | None = None
        self._loaded_state: dict | None = None
        self._edit_context_guard: Callable[[], None] | None = None
        self._edit_failed = False
        self._versioning_policy = "legacy"
        self._next_version = 1
        self._commits: dict[str, dict] = {}
        if not hasattr(self, "_version_paths"):
            self._version_paths: dict[str, str] = {}

        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")

        self._mode = mode
        self._bucket = bucket
        self._store = self._create_store()

        self.update_daterange()

    def _create_store(self) -> VersionedZarrStore[Any]:
        """Open versioned payload storage; subclasses may supply another Zarr store."""
        from openghg.storage import get_versioned_zarr_directory_store
        from openghg.storage._zarr_compat import make_local_store
        from openghg.storage._zarr_store import VersionedZarrStore

        self._root_store_key = f"data/{self.uuid}/zarr"
        self._stores_path = Path(self._bucket, self._root_store_key).expanduser().resolve()
        options = {}
        if hasattr(self, "_store"):
            options = {
                "append_dim": self._store.append_dim,
                "index_options": deepcopy(self._store.index_options),
                "compressor": self._store.compressor,
                "filters": self._store.filters,
                "encoding": deepcopy(self._store.encoding),
                **self._store.to_zarr_kwargs,
            }
        if self._versioning_policy == "immutable":
            return VersionedZarrStore(
                factory=lambda version: make_local_store(self._stores_path / self._version_paths[version]),
                versions=self._data_keys,
                **options,
            )
        return get_versioned_zarr_directory_store(path=self._stores_path, **options)

    @classmethod
    def _read_state(cls, bucket: str, uuid: str) -> dict:
        """Read persisted datasource state, raising ObjectStoreError when absent."""
        key = f"{cls._datasource_root}/uuid/{uuid}"
        if not exists(bucket=bucket, key=key):
            raise ObjectStoreError(f"No Datasource with uuid {uuid} found in bucket {bucket}")
        return get_object_from_json(bucket=bucket, key=key)

    def _write_state(self, state: dict) -> None:
        """Atomically replace local datasource state; publication supplies its lock."""
        import json
        import os
        from uuid import uuid4

        path = Path(self._bucket, self.key + "._data")
        path.parent.mkdir(parents=True, exist_ok=True)
        contents = json.dumps(state)
        temporary = path.with_name(f".{path.name}-{uuid4().hex}")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as f:
                if path.exists():
                    os.fchmod(f.fileno(), path.stat().st_mode & 0o777)
                f.write(contents)
                f.flush()
                os.fsync(f.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def _current_persisted_state(self) -> dict | None:
        """Read local publication state, including when this datasource is new."""
        if not exists(bucket=self._bucket, key=self.key):
            return None
        return self._read_state(bucket=self._bucket, uuid=self.uuid)

    def _check_legacy_mutation(self) -> None:
        """Reject legacy writes after opt-in, including through stale handles."""
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only datasource")
        if self._edit_context_guard is not None:
            self._edit_context_guard()
        if self._edit_failed:
            raise ObjectStoreError("Publication failed; reload this datasource before writing again.")
        state = self._current_persisted_state()
        if (
            self._versioning_policy == "immutable"
            or (state is not None and state.get("_versioning_policy") == "immutable")
            or self._active_edit is not None
        ):
            raise ObjectStoreError("Saved versions are immutable; use begin_edit() and an explicit commit.")

    def _edit_state(self) -> dict:
        """Copy serializable datasource state without sharing mutable metadata."""
        state = deepcopy({k: v for k, v in self.__dict__.items() if k not in self._runtime_state_keys})
        if "versions" in state["_metadata"]:
            state["_metadata"]["versions"] = deepcopy(state["_metadata"]["versions"])
        return state

    def _begin_payload_edit(self, base: str | None) -> PayloadEdit:
        """Allocate one stable local generation, copying the selected base once."""
        from uuid import uuid4

        from openghg.storage._zarr_compat import clear_store, make_local_store
        from openghg.storage._zarr_copy import copy_zarr_store
        from openghg.storage._zarr_store import ZarrStore
        from ._payload_edit import PayloadEdit

        expected = deepcopy(self._loaded_state)
        context_guard = self._edit_context_guard

        def check_current() -> None:
            if self._mode == "r":
                raise PermissionError("Cannot modify a read-only datasource")
            if context_guard is not None:
                context_guard()
            if self._edit_failed:
                raise ObjectStoreError("Publication failed; reload this datasource before writing again.")
            if self._current_persisted_state() != expected:
                raise ObjectStoreError("Datasource changed; reload before starting or committing an edit.")

        check_current()
        if base is not None and base not in self._data_keys:
            raise ZarrStoreError(f"Invalid version: {base}")
        reference = ".generations/" + str(uuid4())
        destination = make_local_store(self._stores_path / reference)

        def discard() -> None:
            import errno

            clear_store(destination)
            try:
                (self._stores_path / ".generations").rmdir()
            except OSError as exc:
                if exc.errno not in (errno.ENOENT, errno.ENOTEMPTY):
                    raise

        payload = PayloadEdit(
            ZarrStore(
                destination,
                append_dim=self._store.append_dim,
                index_options=deepcopy(self._store.index_options),
                compressor=self._store.compressor,
                filters=self._store.filters,
                encoding=deepcopy(self._store.encoding),
                **self._store.to_zarr_kwargs,
            ),
            reference,
            check_current=check_current,
            abort=discard,
        )
        try:
            if base is not None:
                source = make_local_store(self._stores_path / self._version_paths.get(base, base))
                copy_zarr_store(source, destination)
        except BaseException:
            payload.abort()
            raise
        return payload

    def _publish_edit(self, payload: PayloadEdit, state: dict, version: str) -> None:
        """Publish a completed generation under a per-datasource state lock.

        Direct edits serialize here. Concurrent legacy mutations must use the
        ObjectStore manager's catalog lock; their payload writes are not staged.
        """
        from filelock import FileLock
        from openghg.objectstore import get_object_lock_path

        candidate = deepcopy(state)
        paths = {v: self._version_paths.get(v, v) for v in self._data_keys}
        paths[version] = payload.reference
        candidate["_version_paths"] = paths
        with FileLock(str(get_object_lock_path(self._bucket, self.key))):
            payload.check_current()
            if version in self._data_keys:
                raise ObjectStoreError(f"Saved version {version} already exists.")
            payload.mark_publishing()
            try:
                self._write_state(candidate)
            except BaseException:
                self._edit_failed = True
                raise
        self.__dict__.update(candidate)
        self._data_keys = defaultdict(list, self._data_keys)
        self._loaded_state = deepcopy(candidate)
        self._store = self._create_store()
        self._start_date = self._end_date = None
        self.update_daterange()

    @_requires_write
    def begin_edit(self, base: str | None = "latest") -> DatasourceEdit:
        """Start an unpublished working copy of a saved version.

        Args:
            base: Saved version to copy, ``"latest"`` for the newest saved
                version, or None for an empty working dataset. With no saved
                versions, ``"latest"`` also starts empty.

        Returns:
            An editor whose explicit ``commit`` publishes one new version.
            Exiting the editor's context without committing discards its changes.

        The first successful commit opts this datasource into immutable saved
        versions. Subsequent changes must use this API; legacy writes and plain
        ``save`` cannot publish edits. See :doc:`/development/datasource_versioning` for
        manager ownership and working-preview lifetime.
        """
        from ._datasource_edit import DatasourceEdit

        return DatasourceEdit(self, base)

    def _delete_state(self) -> None:
        """Remove persisted datasource state."""
        delete_object(bucket=self._bucket, key=self.key)

    def _delete_store_directory(self) -> None:
        """Remove the local payload directory after deleting all versions."""
        if self._stores_path.exists():
            self._stores_path.rmdir()

    def _set_store_encoding(self, compressor: Any | None = None, filters: Any | None = None) -> None:
        """Set encoding options used for newly written zarr variables."""
        if compressor:
            self._store.compressor = compressor
        if filters:
            self._store.filters = filters

    def _checkout_version(self, version: str) -> None:
        """Checkout a store version, wrapping versioning errors as ZarrStoreError."""
        try:
            self._store.checkout_version(version.lower())
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e

    def _ensure_store_version(self, version: str, *, copy_current: bool = False) -> None:
        """Create a zarr version if needed, otherwise check out the existing version."""
        version = version.lower()
        if version in self._store.versions:
            self._store.checkout_version(version)
        else:
            if not self._store.versions and version != "v1":
                raise ValueError("First version must be v1")
            if copy_current and not self._store.versions:
                raise ValueError("Cannot copy current version when creating the first version.")
            self._store.create_version(version, checkout=True, copy_current=copy_current)

    # Methods to satisfy AbstractDatasource ABC
    @classmethod
    def load(cls, uuid: str, bucket: str, mode: Literal["r", "rw"] = "rw", data_type: str = "") -> Self:
        stored_data = cls._read_state(bucket=bucket, uuid=uuid)

        ds = cls(bucket, uuid, mode, data_type)
        ds.__dict__.update(stored_data)
        ds._mode = mode
        ds._data_keys = defaultdict(list, ds._data_keys)
        ds._loaded_state = deepcopy(stored_data)
        ds._store = ds._create_store()
        ds._start_date = ds._end_date = None
        ds.update_daterange()

        return ds

    @_requires_legacy_mutation
    def save(self) -> None:
        """Persist datasource state through the backend's `_write_state` hook.

        Subclasses with additional runtime fields must extend `_runtime_state_keys`
        so that sessions and other non-persistent objects are excluded.
        """
        internal_metadata = self._edit_state()
        self._write_state(internal_metadata)
        self._loaded_state = deepcopy(internal_metadata)

    @_requires_write
    @_requires_legacy_mutation
    def add(self, data: xr.Dataset, **kwargs) -> None:
        if (period := kwargs.pop("period", None)) is not None:
            self._metadata["period"] = period

        self.add_data(metadata={}, data=data, data_type=self._data_type, **kwargs)

    def get_data(self, version: str = "latest") -> xr.Dataset:
        """Read a saved version as a lazy Xarray dataset.

        Args:
            version: Published version label, such as ``"v1"``, or ``"latest"``.
        Returns:
            Dataset backed by the selected saved payload. Immutable versions
            stay pinned across later commits; reading one does not change an
            editor's base. Closing the dataset does not close its datasource or
            manager. Remote backends may require a session factory for reads
            after the manager context exits.
        Raises:
            ZarrStoreError: If the requested version is not published.
        """
        if version == "latest":
            version = self._latest_version
        version = version.lower()
        if version not in self._data_keys:
            raise ZarrStoreError(f"Invalid version: {version}")

        if self._versioning_policy == "immutable":
            from openghg.storage._zarr_store import ZarrStore

            return ZarrStore(
                self._store._versions[version],
                append_dim=self._store.append_dim,
                **self._store.to_zarr_kwargs,
            ).get()
        self._checkout_version(version)
        return self._store.get()

    @_requires_write
    def delete(self) -> None:
        self.delete_all_data()
        self._delete_state()

    @_requires_write
    @_requires_legacy_mutation
    def update_attributes(
        self,
        version: str = "latest",
        data_vars: str | list[str] | None = None,
        update_global: bool = True,
        to_update: dict | None = None,
        to_delete: str | list[str] | None = None,
    ) -> bool:
        """Edit dataset attributes in one stored version and consolidate metadata.

        Args:
            version: Version to edit, or ``"latest"``.
            data_vars: Variables whose attributes should be edited, if any.
            update_global: Whether to also edit global dataset attributes.
            to_update: Attribute values to add or replace.
            to_delete: Attribute names to remove. Missing names raise KeyError.

        Returns:
            Whether attributes were edited. Call `save` afterwards to commit any
            backend-specific state associated with the edit.

        Raises:
            PermissionError: If this datasource is read-only.
            ZarrStoreError: If the requested version does not exist.
        """
        import zarr

        if not (to_update or to_delete) or (not update_global and data_vars is None):
            return False

        def updater(attrs: MutableMapping) -> bool:
            if to_delete:
                keys = [to_delete] if isinstance(to_delete, str) else to_delete
                for key in keys:
                    attrs.pop(key)
            if to_update:
                attrs.update(to_update)
            return bool(to_delete or to_update)

        self._checkout_version(self.latest_version if version == "latest" else version)
        store = self._store.store
        group = zarr.open_group(store)
        updated = updater(group.attrs) if update_global else False
        if data_vars is not None:
            variables = [data_vars] if isinstance(data_vars, str) else data_vars
            for variable in variables:
                try:
                    array = group[variable]
                except KeyError:
                    logger.warning(f"Data variable {variable} not present in zarr store. Skipping.")
                else:
                    updated = updater(array.attrs) or updated

        if updated:
            zarr.consolidate_metadata(store)
            logger.info(f"Modified attributes for {self.uuid}.")
        return updated

    # Context manager
    def __enter__(self) -> Datasource:
        return self

    def __exit__(
        self,
        exc_type: BaseException | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._active_edit is not None:
            self._active_edit.abort()
            return
        if exc_type is not None:
            logger.error(msg=f"{exc_type}, {exc_tb}")
        elif self._mode != "r" and self._versioning_policy != "immutable":
            self.save()

    # properties
    @property
    def start_date(self) -> Timestamp:
        """Start datetime for the data in this Datasource."""
        return self._start_date

    @property
    def end_date(self) -> Timestamp:
        """End datetime for the data in this Datasource."""
        return self._end_date

    @property
    def key(self) -> str:
        """Key for Datasource in object store."""
        return f"{self._datasource_root}/uuid/{self._uuid}"

    @property
    def uuid(self) -> str:
        """UUID of this object."""
        return self._uuid

    @property
    def metadata(self) -> dict:
        """Metadata of this Datasource."""
        if self._versioning_policy == "immutable":
            return deepcopy(self._metadata)
        return self._metadata

    @property
    def data_type(self) -> str:
        """Data type held by this Datasource."""
        return self._data_type

    @property
    def latest_version(self) -> str:
        """String of the latest version."""
        return self._latest_version

    @property
    def period(self) -> str | None:
        """Period from metadata for creating a pandas Timedelta or DataOffset object."""
        # Extract period associated with data from metadata
        # This will be the "sampling_period" for obs and "time_period" for other
        # TODO: May want to add period as a potential data variable so would need to extract from there if needed
        from openghg.util._metadata_util import get_period

        if "period" not in self._metadata:
            self._metadata["period"] = get_period(self._metadata)

        return cast(str | None, self._metadata["period"])

    @property
    def nbytes(self) -> int:
        """Size of data stored in bytes."""
        return self._store.bytes_stored()

    # Methods related storing, getting, deleting data
    @_requires_write
    @_requires_legacy_mutation
    def add_data(
        self,
        metadata: dict,
        data: xr.Dataset,
        data_type: str,
        sort: bool = False,
        drop_duplicates: bool = False,
        skip_keys: list | None = None,
        extend_keys: list | None = None,
        new_version: bool = True,
        if_exists: str = "auto",
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> None:
        """Add data to this Datasource and segment the data by size.
        The data is stored as a tuple of the data and the daterange it covers.

        Args:
            metadata: Metadata on the data for this Datasource
            data: xarray.Dataset
            data_type: Type of data, one of ["boundary_conditions", "column", "emissions", "flux", "flux_timeseries", "footprints", "surface", "eulerian_model"].
            sort: Sort data in time dimension
            drop_duplicates: Drop duplicate timestamps, keeping the first value
            skip_keys: Keys to not standardise as lowercase
            extend_keys: Keys to add to to current keys (extend a list), if present.
            new_version: Create a new version of the data
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
        Returns:
            None
        """
        self.add_metadata(metadata=metadata, skip_keys=skip_keys, extend_keys=extend_keys)

        if "time" in data.coords:
            return self.add_timed_data(
                data=data,
                data_type=data_type,
                sort=sort,
                drop_duplicates=drop_duplicates,
                new_version=new_version,
                if_exists=if_exists,
                compressor=compressor,
                filters=filters,
            )
        else:
            raise NotImplementedError()

    @_requires_write
    @_requires_legacy_mutation
    def add_timed_data(
        self,
        data: xr.Dataset,
        data_type: str,
        sort: bool,
        drop_duplicates: bool,
        new_version: bool = True,
        if_exists: str = "auto",
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> None:
        """Add data to this Datasource

        Args:
            data: An xarray.Dataset
            data_type: Name of data_type defined by
                openghg.store.spec.define_data_types()
            sort: If True sort by time, may load all data into memory
            drop_duplicates: If True drop duplicates, keeping first found duplicate
            new_version: Create a new version of the data
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
        Returns:
            None
        """
        from openghg.util._time import get_representative_daterange_str, timestamp_now

        # Ensure data is in time order
        time_coord = "time"
        new_daterange_str = get_representative_daterange_str(dataset=data, period=self.period)

        # Save details of current Datasource status
        self._status = {}

        if sort and drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first").sortby(time_coord)
        elif sort:
            data = data.sortby(time_coord)
        elif drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first")

        has_existing_data = bool(self._store)
        if has_existing_data:
            self._checkout_version(self._latest_version)
            overlapping = self._store._overlap_determiner.has_overlaps(data.get_index(self._store.append_dim))
        else:
            overlapping = False
        plan = plan_timed_data_update(
            if_exists=if_exists,
            new_version=new_version,
            has_existing_data=has_existing_data,
            overlapping=overlapping,
        )

        if self._latest_version and not plan.new_version:
            version_str = self._latest_version
        else:
            next_version = max((int(version[1:]) for version in self._data_keys), default=0) + 1
            version_str = f"v{next_version}"

        current_date_keys = (
            list(self._data_keys[self._latest_version])
            if self._data_keys and self._latest_version in self._data_keys
            else []
        )

        # TODO: what does the following comment mean? (BM Jan 2026)
        # We'll only need to sort the new dataset if the data we add comes before the current data

        if plan.action == "error_overlap":
            raise DataOverlapError(
                "Unable to add new data, because it overlaps with current data and `if_exists` is set to 'auto'. "
                "To update current data in object store use `if_exists` input (see options in documentation)."
            )

        self._set_store_encoding(compressor=compressor, filters=filters)

        if plan.action == "insert":
            self._ensure_store_version(version_str)
            self._store.insert(data)
            if has_existing_data and plan.new_version:
                date_keys = [new_daterange_str]
            else:
                date_keys = [*current_date_keys, new_daterange_str]
        elif plan.action == "copy_insert":
            self._ensure_store_version(version_str, copy_current=True)
            self._store.insert(data)
            date_keys = [*current_date_keys, new_daterange_str]
        elif plan.action == "replace":
            logger.info("Updating store to include new added data only.")
            self._checkout_version(version_str)
            self._store.overwrite(data)
            date_keys = [new_daterange_str]
        elif plan.action == "upsert":
            logger.info("Updating store by combining new data with existing.")
            if not self._store.versions:
                raise ValueError("Cannot update empty Zarr store.")
            self._ensure_store_version(version_str, copy_current=True)
            self._store.upsert(data)
            date_keys = [get_representative_daterange_str(self._store.get(), period=self.period)]

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)

        self._status["updates"] = True
        self._status["if_exists"] = if_exists
        self._latest_version = version_str

        # We'll store the daterange for this version of the data and update the latest to the current version
        timestamp_str_now = str(timestamp_now())
        self._data_keys[version_str] = sorted(date_keys)
        self._timestamps[version_str] = timestamp_str_now
        self.add_metadata_key(key="latest_version", value=version_str)
        self.add_metadata_key(key="timestamp", value=timestamp_str_now)

        self.update_daterange()
        # Store the start and end date of the most recent data
        start, end = self.daterange()
        self.add_metadata_key(key="start_date", value=str(start))
        self.add_metadata_key(key="end_date", value=str(end))
        # Store the version data, it's less information now and we can then
        # present version data to the users
        self._metadata["versions"] = self._data_keys

        self._last_updated = timestamp_str_now

    @_requires_write
    @_requires_legacy_mutation
    def delete_all_data(self) -> None:
        """Delete datasource entirely.

        Deletes the zarr store that contains all the data
        associated with this Datasource, clears out all keys
        stored in this Datasource, and removes the uuid
        from the `data` path.

        Returns:
            None
        """
        self._store.delete_all_versions()
        self._delete_store_directory()
        self._data_keys.clear()
        self._metadata.clear()
        self._timestamps.clear()

    @_requires_write
    @_requires_legacy_mutation
    def delete_version(self, version: str) -> None:
        """Delete a specific version of data.

        Args:
            bucket: Bucket containing data
            version: Version string
        Returns:
            None
        """
        if version == "latest":
            raise ValueError("Specific version required for deletion.")

        if version not in self._data_keys:
            raise KeyError("Invalid version.")

        try:
            self._store.delete_version(version.lower())
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e

        del self._data_keys[version]
        del self._timestamps[version]
        self._metadata["versions"] = self._data_keys

    # Metadata methods
    @_requires_legacy_mutation
    def add_metadata_key(self, key: str, value: str) -> None:
        """Add a label to the metadata dictionary with the key value pair
        This will overwrite any previous entry stored at that key.

        Args:
            key: Key for dictionary
            value: Value for dictionary
        Returns:
            None
        """
        value = str(value)
        self._metadata[key.lower()] = value.lower()

    @_requires_legacy_mutation
    def add_metadata(
        self, metadata: dict, skip_keys: list | None = None, extend_keys: list | None = None
    ) -> None:
        """Add all metadata in the dictionary to this Datasource.
        This will overwrite any previously stored values for keys of the same name.

        Args:
            metadata: Dictionary of metadata
            skip_keys: Keys to not standardise as lowercase
            extend_keys: Keys to add in addition to current keys (extend a list) if present.

        Returns:
            None
        """
        from openghg.util import to_lowercase, merge_dict, merge_and_extend_dict

        try:
            del metadata["object_store"]
        except KeyError:
            pass
        else:
            logger.warning("object_store should not be added to the metadata, removing.")

        if extend_keys is None:
            extend_keys = []

        lowercased: dict = to_lowercase(metadata, skip_keys=skip_keys)
        metadata_to_add = {key: value for key, value in lowercased.items() if key not in extend_keys}

        merged_metadata = merge_dict(
            self._metadata, metadata_to_add, on_overlap="ignore", on_conflict="right"
        )

        # Extend current keys with new values
        metadata_extend = {}
        for key in extend_keys:
            if key in metadata:
                value = metadata[key]
                if isinstance(value, str):
                    value = [value]
                metadata_extend[key] = value

        merged_and_extended_metadata = merge_and_extend_dict(merged_metadata, metadata_extend)

        self._metadata = merged_and_extended_metadata

    # Date range (and "data keys") methods
    def data_keys(self, version: str = "latest") -> list:
        """Returns the dateranges of data covered by a specific version of the data stored.

        Args:
            version: Version of keys to retrieve
        Returns:
            list: List of data keys
        """
        if version == "latest":
            version = self._latest_version

        if version not in self._data_keys:
            raise KeyError(f"Invalid version, valid versions {list(self._data_keys.keys())}")
        keys = self._data_keys[version]

        return list(keys) if self._versioning_policy == "immutable" else keys

    def all_data_keys(self) -> dict:
        """Return a summary of the versions of data stored for
        this Datasource

        Returns:
            dict: Dictionary of versions
        """
        return deepcopy(self._data_keys) if self._versioning_policy == "immutable" else self._data_keys

    def update_daterange(self) -> None:
        """Update the dates stored by this Datasource

        Returns:
            None
        """
        from openghg.util._time import split_daterange_str

        if not self._data_keys:
            return

        date_keys = sorted(self._data_keys[self._latest_version])

        start, _ = split_daterange_str(daterange_str=date_keys[0])
        _, end = split_daterange_str(daterange_str=date_keys[-1])

        self._start_date = start  # type: ignore
        self._end_date = end  # type: ignore

    def daterange(self) -> tuple[Timestamp, Timestamp]:
        """Get the daterange the data in this Datasource covers as tuple
        of start, end datetime objects

        Returns:
            tuple (Timestamp, Timestamp): Start, end timestamps
        """
        if self.start_date is None and self._data_keys is not None:
            self.update_daterange()

        return self.start_date, self.end_date

    def daterange_str(self) -> str:
        """Get the daterange this Datasource covers as a string in
        the form start_end

        Returns:
            str: Daterange covered by this Datasource
        """
        from openghg.util._time import create_daterange_str

        start, end = self.daterange()

        return create_daterange_str(start=start, end=end)

    # Integrity check
    def integrity_check(self) -> None:
        """Checks to ensure all data stored by this Datasource exists in the object store.

        Returns:
            None
        """
        from pandas import Timedelta
        from openghg.util._time import split_daterange_str, timestamp_tzaware

        for version, dateranges in self._data_keys.items():
            start_date, _ = split_daterange_str(daterange_str=dateranges[0])
            _, end_date = split_daterange_str(daterange_str=dateranges[-1])

            if version not in self._store.versions:
                raise ObjectStoreError(f"{version} not found in object store.")

            self._store.checkout_version(version)
            with self._store.get() as ds:
                if ds.time.size == 1:
                    start_keys = timestamp_tzaware(start_date)
                    start_data = timestamp_tzaware(ds.time[0].values)

                    assert start_keys.year == start_data.year

                    continue

                start_keys = timestamp_tzaware(start_date)
                start_data = timestamp_tzaware(ds.time[0].values)

                if abs(start_keys - start_data) > Timedelta(minutes=1):
                    raise ValueError(
                        f"Timestamp mismatch between expected ({start_keys}) and stored {start_data}"
                    )

                end_keys = timestamp_tzaware(end_date)
                end_data = timestamp_tzaware(ds.time[-1].values)

                if abs(end_keys - end_data) > Timedelta(minutes=1):
                    raise ValueError(
                        f"Timestamp mismatch between expected ({end_keys}) and stored {end_data}"
                    )


def get_legacy_datasource_factory(
    bucket: str,
    data_type: str,
    new_kwargs: dict | None = None,
    load_kwargs: dict | None = None,
    **kwargs: Any,
) -> DatasourceFactory[Datasource]:
    """Create DatasourceFactory for legacy Datasource class.

    Args:
        bucket: bucket of object store
        data_type: data type for saved data
        new_kwargs: keyword args for creating new Datasources (e.g. `mode`)
        load_kwargs: keyword args for loading existing Datasources (e.g. `mode`)
        kwargs: keyword args to add to both new_kwargs and load_kwargs

    Returns:
        DatasourceFactory for creating/loading Datasources from specified
        bucket.

    """
    kwargs_to_add = {"bucket": bucket, "data_type": data_type}
    kwargs_to_add.update(kwargs)

    new_kwargs = new_kwargs or {}
    new_kwargs.update(kwargs_to_add)

    load_kwargs = load_kwargs or {}
    load_kwargs.update(kwargs_to_add)

    return DatasourceFactory[Datasource](Datasource, new_kwargs, load_kwargs)
