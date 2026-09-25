"""Experimental iRODS ObjectStore with catalog metadata and versioned Zarr data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
import hashlib
import json
import threading
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from typing_extensions import Self
import xarray as xr

from openghg.objectstore._datasource import DatasourceFactory
from openghg.objectstore._irods_metastore import IRODSMetaStore, PublicationConflictError
from openghg.objectstore._irods_storage import (
    IRODSKVStore,
    IRODSZarrMapping,
    SessionFactory,
    _snapshot,
    read_document,
    validate_collection,
    validate_ordinary_collection,
    write_document,
)
from openghg.objectstore._legacy_datasource import Datasource
from openghg.objectstore._objectstore import ObjectStore, make_metadata_updater_fn
from openghg.storage._zarr_store import VersionedZarrStore
from openghg.types import ObjectStoreError


class _Sessions:
    """Reuse the context's connection, reopening connections for deferred reads."""

    def __init__(self, factory: SessionFactory) -> None:
        self.factory = factory
        self.active: Any = None

    @contextmanager
    def __call__(self) -> Iterator[Any]:
        if self.active is not None:
            yield self.active
        else:
            with self.factory() as session:
                yield session


class _Documents:
    def __init__(self, store: IRODSObjectStore) -> None:
        self.store = store
        self._revisions: dict[str, str | None] = {}

    @staticmethod
    def _key(key: str) -> str:
        return "store-" + hashlib.sha256(key.encode()).hexdigest()

    def _read(self, key: str) -> tuple[str | None, dict[str, Any] | None]:
        try:
            document = read_document(self.store._sessions, self.store.collection, self._key(key))
        except KeyError:
            return None, None
        if document.get("openghg_document_schema") == 1:
            return document["revision"], document["value"]
        # Legacy unwrapped documents are migrated on their first locked write.
        revision = "legacy:" + hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()
        return revision, document

    def read(self, key: str) -> dict[str, Any] | None:
        revision, value = self._read(key)
        self._revisions[key] = revision
        return value

    def write(self, key: str, value: dict[str, Any]) -> None:
        self.store._require_write()
        current, _ = self._read(key)
        if key in self._revisions and self._revisions[key] != current:
            raise PublicationConflictError(f"iRODS document {key!r} changed; reload before retrying.")
        revision = str(uuid4())
        write_document(
            self.store._sessions,
            self.store.collection,
            self._key(key),
            {
                "openghg_document_schema": 1,
                "revision": revision,
                "value": value,
            },
        )
        self._revisions[key] = revision


class IRODSDatasource(Datasource):
    """A pinned publication with immutable payload generations and guarded saves.

    Loaded datasets remain readable after updates and logical deletion. A stale
    datasource must be reloaded before mutation; failed writes also require reload.
    """

    _runtime_state_keys = Datasource._runtime_state_keys | {
        "_owner",
        "_collection",
        "_revision",
        "_record",
        "_version_paths",
        "_staging",
        "_failed",
    }

    def __init__(self, uuid: str, owner: IRODSObjectStore, publication: dict[str, Any] | None = None) -> None:
        self._owner = owner
        self._collection = owner.metastore.path(uuid)
        self._revision = publication["revision"] if publication else None
        self._record = publication["record"] if publication else {"uuid": uuid}
        self._version_paths = dict(publication["versions"]) if publication else {}
        self._staging: set[str] = set()
        self._failed = False
        super().__init__(owner.collection, uuid, owner.mode, owner.data_type)
        if publication:
            self.__dict__.update(
                {k: v for k, v in publication["datasource"].items() if k not in self._runtime_state_keys}
            )
            self._data_keys = defaultdict(list, self._data_keys)

    @property
    def revision(self) -> str | None:
        """Opaque publication revision for explicit optimistic conflict checks."""
        return self._revision

    def _check_current(self) -> None:
        self._owner._require_write()
        if self._failed:
            raise ObjectStoreError("A datasource mutation failed; reload before retrying.")
        current = self._owner.metastore.publication(self._uuid)
        if (current["revision"] if current else None) != self._revision:
            raise PublicationConflictError(f"iRODS datasource {self._uuid} changed; reload before retrying.")

    def _mapping(self, path: str, writable: bool = False) -> IRODSZarrMapping:
        def guard() -> None:
            self._owner._require_write(_worker=True)
            if path not in self._staging or self._failed:
                raise PermissionError("Published iRODS generations are immutable.")

        return IRODSZarrMapping(
            self._owner._sessions,
            path,
            self._owner.cache_dir,
            read_only=not writable,
            resource=self._owner.resource,
            write_guard=guard,
        )

    def _new_generation(self) -> str:
        self._owner._require_write()
        path = self._collection + "/generations/" + str(uuid4())
        self._staging.add(path)
        return path

    def _create_store(self) -> VersionedZarrStore[IRODSKVStore]:
        def factory(version: str) -> IRODSKVStore:
            if version not in self._version_paths:
                self._version_paths[version] = self._new_generation()
            path = self._version_paths[version]
            return IRODSKVStore(self._mapping(path, writable=path in self._staging))

        versions = sorted(self._version_paths, key=lambda version: int(version[1:]))
        return VersionedZarrStore(factory=factory, versions=versions)

    def _stage_version(self, version: str) -> None:
        import zarr

        path = self._version_paths[version]
        if path in self._staging:
            return
        destination = self._new_generation()
        mapping = IRODSKVStore(self._mapping(destination, writable=True))
        zarr.copy_store(self._store._versions[version], mapping)
        self._version_paths[version] = destination
        self._store._versions[version] = mapping

    def mapping(self, version: str = "latest") -> IRODSZarrMapping:
        """Return a read-only, pinned transport for cache and provenance inspection."""
        if version == "latest":
            version = self.latest_version
        return self._mapping(self._version_paths[version])

    @classmethod
    def load(cls, uuid: str, owner: IRODSObjectStore, **kwargs: Any) -> Self:  # type: ignore[override]
        """Pin metadata, datasource state, and generations from one atomic document."""
        publication = owner.metastore.publication(uuid)
        if publication is None or publication["record"] is None:
            raise ObjectStoreError(f"No published iRODS datasource with UUID {uuid}")
        return cls(uuid, owner, publication)

    def add(self, data: xr.Dataset, **kwargs: Any) -> None:
        self._check_current()
        try:
            super().add(data, **kwargs)
        except BaseException:
            self._failed = True
            raise

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
        self._check_current()
        try:
            super().add_data(
                metadata,
                data,
                data_type,
                sort,
                drop_duplicates,
                skip_keys,
                extend_keys,
                new_version,
                if_exists,
                compressor,
                filters,
            )
        except BaseException:
            self._failed = True
            raise

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
        self._check_current()
        try:
            if not new_version and self._latest_version:
                self._stage_version(self._latest_version)
            super().add_timed_data(
                data, data_type, sort, drop_duplicates, new_version, if_exists, compressor, filters
            )
        except BaseException:
            self._failed = True
            raise

    def update_attributes(
        self,
        version: str = "latest",
        data_vars: str | list[str] | None = None,
        update_global: bool = True,
        to_update: dict | None = None,
        to_delete: str | list[str] | None = None,
    ) -> bool:
        """Stage an attribute edit; save publishes it without changing pinned readers."""
        self._check_current()
        if not (to_update or to_delete) or (not update_global and data_vars is None):
            return False
        try:
            self._stage_version(self.latest_version if version == "latest" else version)
            return super().update_attributes(version, data_vars, update_global, to_update, to_delete)
        except BaseException:
            self._failed = True
            raise

    def _write_state(self, state: dict) -> None:
        self._check_current()
        # Freeze staged transports before the atomic publication can become visible,
        # including when the server commits but its acknowledgement is lost.
        self._staging.clear()
        try:
            publication = self._owner.metastore.publish(
                self._uuid, self._record, state, self._version_paths, self._revision
            )
        except BaseException:
            self._failed = True
            raise
        self._revision = publication["revision"]
        self._record = publication["record"]
        # Existing lazy arrays keep their old mapping; future mutations replace it.
        for version, path in self._version_paths.items():
            self._store._versions[version] = IRODSKVStore(self._mapping(path))

    def delete_version(self, version: str) -> None:
        """Unlink one version on save; its generations remain available to readers."""
        self._check_current()
        if version == "latest":
            raise ValueError("Specific version required for deletion.")
        if version not in self._data_keys:
            raise KeyError("Invalid version.")
        del self._version_paths[version]
        del self._store._versions[version]
        del self._data_keys[version]
        del self._timestamps[version]
        self._metadata["versions"] = self._data_keys
        if version == self._latest_version:
            self._latest_version = max(self._data_keys, key=lambda v: int(v[1:]), default="")
            self._metadata["latest_version"] = self._latest_version
            self._store._current_version = self._latest_version or None
            self._start_date = self._end_date = None
            if self._latest_version:
                self.update_daterange()
                self._metadata.update(
                    {
                        "start_date": str(self._start_date),
                        "end_date": str(self._end_date),
                        "timestamp": self._timestamps[self._latest_version],
                    }
                )
            else:
                for key in ("start_date", "end_date", "timestamp"):
                    self._metadata.pop(key, None)

    def delete_all_data(self) -> None:
        """Unlink every version on save without reclaiming immutable generations."""
        self._check_current()
        for version in list(self._data_keys):
            self.delete_version(version)

    def delete(self) -> None:
        """Publish a tombstone while preserving already loaded readers."""
        self._check_current()
        self._owner.delete(self._uuid, expected_revision=self._revision)


class IRODSObjectStore(ObjectStore[IRODSDatasource, xr.Dataset]):
    """ObjectStore using an ordinary iRODS collection as its catalog root.

    Args:
        session: Borrowed authenticated session, or None with ``session_factory``.
        collection: Existing absolute logical collection dedicated to this store.
        cache_dir: Opt-in directory for verified Zarr keys and receipts. None
            (the default) reads into memory without a persistent data cache.
        mode: Read-only (``r``) or read/write (``rw``) access.
        resource: Optional registered iRODS resource for new payload objects.
        data_type: OpenGHG data type, or empty for store-level documents.
        session_factory: Context-manager factory that opens authenticated sessions.
        skip_keys: Metadata keys excluded from normalization.
        extend_keys: Metadata keys merged as lists.

    Writers must use a context manager. A catalog collection serializes writers
    across clients; a crashed writer leaves a lock for deliberate operator
    recovery. Writers stage immutable generations before atomically publishing one datasource.
    Readers pin that publication; catalog searches across several datasources and
    store-level documents are not multi-datasource transactions.
    """

    def __init__(
        self,
        session: Any,
        collection: str,
        cache_dir: str | Path | None = None,
        mode: Literal["r", "rw"] = "r",
        resource: str | None = None,
        *,
        data_type: str = "surface",
        session_factory: SessionFactory | None = None,
        skip_keys: list | None = None,
        extend_keys: list | None = None,
    ) -> None:
        if mode not in ("r", "rw"):
            raise ValueError("mode must be 'r' or 'rw'.")
        if session_factory is None:
            if session is None:
                raise ValueError("Provide a session or a session_factory.")

            def session_factory() -> Any:
                return nullcontext(session)
        elif session is not None:
            raise ValueError("Provide only one of session and session_factory.")
        self._sessions = _Sessions(session_factory)
        self.collection = validate_collection(collection)
        if data_type and (not data_type.isidentifier() or not data_type.isascii()):
            raise ValueError("data_type must be a simple ASCII identifier.")
        self.data_type = data_type
        self.cache_dir = Path(cache_dir).expanduser().resolve() if cache_dir is not None else None
        self.mode = mode
        self.resource = resource
        self._connection: Any = None
        self._depth = 0
        self._context_thread: int | None = None
        self._context_guard = threading.RLock()
        self._locked = False
        self.lock_path = collection + "/.openghg-write-lock"
        with self._sessions() as connection:
            validate_ordinary_collection(connection, collection)
        metastore = IRODSMetaStore(
            self._sessions,
            collection + ("/" + data_type if data_type else ""),
            mode,
            write_guard=self._require_write,
        )
        merge_metadata = make_metadata_updater_fn(skip_keys=skip_keys, extend_keys=extend_keys)

        def pinned_metadata(records: Any, datasources: Any) -> Any:
            sources = list(datasources)
            return merge_metadata([source._record for source in sources], sources)

        super().__init__(
            metastore,
            DatasourceFactory(IRODSDatasource, new_kwargs={"owner": self}, load_kwargs={"owner": self}),
            pinned_metadata,
            documents=_Documents(self),
        )

    def __enter__(self) -> Self:
        from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME

        if not self._context_guard.acquire(blocking=False):
            raise ObjectStoreError("An iRODS ObjectStore context cannot be shared between threads.")
        if self._depth:
            self._depth += 1
            return self
        try:
            connection = self._sessions.factory()
            active = connection.__enter__()
        except BaseException:
            self._context_guard.release()
            raise
        self._connection = connection
        self._sessions.active = active
        try:
            if self.mode == "rw":
                try:
                    self._sessions.active.collections.create(self.lock_path, recurse=False)
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME as exc:
                    raise ObjectStoreError(
                        "The iRODS store has an active or abandoned writer lock. "
                        "Retry after the writer exits; an operator must inspect a crashed writer's lock."
                    ) from exc
                self._locked = True
            self._depth = 1
            self._context_thread = threading.get_ident()
            return self
        except BaseException:
            connection = self._connection
            self._sessions.active = None
            self._connection = None
            try:
                connection.__exit__(None, None, None)
            finally:
                self._context_guard.release()
            raise

    def close(self) -> None:
        """Release this context's writer lock and connection; deferred reads reopen."""
        if not self._depth:
            return
        if self._context_thread != threading.get_ident():
            raise ObjectStoreError("An iRODS ObjectStore context must close in its owning thread.")
        if self._depth > 1:
            self._depth -= 1
            self._context_guard.release()
            return
        try:
            if self._locked:
                self._sessions.active.collections.remove(self.lock_path, recurse=False, force=True)
                self._locked = False
        finally:
            self._locked = False
            self._depth = 0
            self._context_thread = None
            if self._connection is not None:
                connection = self._connection
                self._connection = None
                self._sessions.active = None
                try:
                    connection.__exit__(None, None, None)
                finally:
                    self._context_guard.release()

    def _require_write(self, *, _worker: bool = False) -> None:
        if self.mode != "rw":
            raise PermissionError("This iRODS ObjectStore is read-only.")
        if not self._locked:
            raise ObjectStoreError("Use a with statement to acquire the iRODS writer lock.")
        if not _worker and self._context_thread != threading.get_ident():
            raise ObjectStoreError("Mutations must run in the thread owning the iRODS writer context.")

    def create(self, metadata: dict[str, Any], data: xr.Dataset, **kwargs: Any) -> str:
        """Stage a datasource, then publish metadata and generations together."""
        self._require_write()
        if not self.data_type:
            raise ValueError("A data_type is required for datasource operations.")
        if self._search(metadata) or self.get_uuids(metadata):
            raise ObjectStoreError(
                "Cannot create new Datasource: this metadata is already associated with a UUID."
            )
        uuid = str(uuid4())
        datasource = self.datasource_factory.new(uuid)
        datasource._record = {**metadata, "uuid": uuid}
        datasource.add(data, **kwargs)
        datasource.save()
        return uuid

    def update(
        self,
        uuid: str,
        metadata: dict[str, Any] | None = None,
        data: xr.Dataset | None = None,
        keys_to_delete: str | list[str] | None = None,
        extend_keys: list[str] | None = None,
        *,
        expected_revision: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Publish one complete update, optionally rejecting a stale expected revision."""
        from openghg.objectstore._objectstore import _memory_metastore

        self._require_write()
        datasource = self.get_datasource(uuid)
        if expected_revision is not None and datasource.revision != expected_revision:
            raise PublicationConflictError(f"iRODS datasource {uuid} changed; reload before retrying.")
        if (metadata and "uuid" in metadata) or (
            keys_to_delete
            and "uuid" in ([keys_to_delete] if isinstance(keys_to_delete, str) else keys_to_delete)
        ):
            raise ValueError("Cannot update or delete UUID.")
        if metadata or keys_to_delete:
            updated = dict(metadata or {})
            extended = {key: updated.pop(key) for key in (extend_keys or []) if key in updated}
            with _memory_metastore([datasource._record]) as metastore:
                metastore.update({"uuid": uuid}, updated, keys_to_delete, extended)
                datasource._record = metastore.search()[0]
        if data is not None:
            datasource.add(data, **kwargs)
        datasource.save()

    def delete(self, uuid: str, *, expected_revision: str | None = None) -> None:
        """Publish a tombstone; retain payloads so existing readers stay valid."""
        self._require_write()
        snapshot = self.metastore.publication(uuid)
        if snapshot is None or snapshot["record"] is None:
            raise ObjectStoreError(f"No published iRODS datasource with UUID {uuid}")
        if expected_revision is not None and snapshot["revision"] != expected_revision:
            raise PublicationConflictError(f"iRODS datasource {uuid} changed; reload before retrying.")
        self.metastore.publish(uuid, None, snapshot["datasource"], snapshot["versions"], snapshot["revision"])

    def replicate(self, uuid: str, resource: str) -> None:
        """Replicate every Zarr object to a registered resource under its existing data ID."""
        from irods import keywords as kw

        self._require_write()
        if not isinstance(resource, str) or not resource.strip():
            raise ValueError("A registered destination resource is required.")
        datasource = self.get_datasource(uuid)
        with self._sessions() as session:
            for version in datasource._store.versions:
                mapping = datasource.mapping(version)
                for key in mapping:
                    path = mapping.collection + "/" + key
                    before = _snapshot(session, path)
                    session.data_objects.replicate(
                        path,
                        **{kw.DEST_RESC_NAME_KW: resource, kw.UPDATE_REPL_KW: "", kw.VERIFY_CHKSUM_KW: ""},
                    )
                    after = _snapshot(session, path)
                    identity_keys = ("data_id", "logical_path", "checksum", "size")
                    if any(before[k] != after[k] for k in identity_keys):
                        raise ObjectStoreError("Replica creation changed the source identity or content.")
                    obj = session.data_objects.get(path)
                    if not any(
                        str(r.status) == "1"
                        and resource in r.resc_hier.split(";")
                        and r.checksum == before["checksum"]
                        and int(r.size) == before["size"]
                        for r in obj.replicas
                    ):
                        raise ObjectStoreError("The destination has no verified good replica.")


def irods_object_store(
    *,
    bucket: str,
    data_type: str,
    mode: Literal["r", "rw"] = "r",
    skip_keys: list | None = None,
    extend_keys: list | None = None,
    environment_file: str | None = None,
    cache_dir: str | Path | None = None,
    resource: str | None = None,
    **session_options: Any,
) -> IRODSObjectStore:
    """Configured factory using a native iRODS environment and optional credentials.

    ``bucket`` is an absolute iRODS collection path. ``session_options`` are
    passed to python-irodsclient's iRODSSession, including a ``password`` resolved
    by OpenGHG's ``credentials_env`` configuration when needed. Credentials are
    retained only in memory and never written into dataset state or cache receipts.
    Persistent data caching is disabled unless ``cache_dir`` is supplied; use a
    private directory with sufficient space and inodes when opting in.
    """
    import os

    try:
        from irods.session import iRODSSession
    except ImportError as exc:
        raise ImportError("Install the optional client with pip install 'openghg[irods]'.") from exc
    environment_file = environment_file or os.environ.get(
        "IRODS_ENVIRONMENT_FILE", "~/.irods/irods_environment.json"
    )

    def sessions() -> Any:
        return iRODSSession(irods_env_file=str(Path(environment_file).expanduser()), **session_options)

    return IRODSObjectStore(
        None,
        bucket,
        cache_dir,
        mode,
        resource,
        data_type=data_type,
        session_factory=sessions,
        skip_keys=skip_keys,
        extend_keys=extend_keys,
    )
