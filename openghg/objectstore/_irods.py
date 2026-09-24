"""Experimental iRODS ObjectStore with catalog metadata and versioned Zarr data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
import hashlib
from pathlib import Path
from typing import Any, Literal

from typing_extensions import Self
import xarray as xr

from openghg.objectstore._datasource import DatasourceFactory
from openghg.objectstore._irods_metastore import IRODSMetaStore
from openghg.objectstore._irods_storage import (
    IRODSKVStore,
    IRODSZarrMapping,
    SessionFactory,
    _snapshot,
    delete_document,
    list_collections,
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

    @staticmethod
    def _key(key: str) -> str:
        return "store-" + hashlib.sha256(key.encode()).hexdigest()

    def read(self, key: str) -> dict[str, Any] | None:
        try:
            return read_document(self.store._sessions, self.store.collection, self._key(key))
        except KeyError:
            return None

    def write(self, key: str, value: dict[str, Any]) -> None:
        self.store._require_write()
        write_document(self.store._sessions, self.store.collection, self._key(key), value)


class IRODSDatasource(Datasource):
    """Use OpenGHG's datasource behaviour with iRODS persistence hooks.

    Datasources obtained from a configured factory support lazy reads after the
    store context exits. A borrowed session must remain open for those reads.
    """

    _runtime_state_keys = Datasource._runtime_state_keys | {"_owner", "_collection"}

    def __init__(self, uuid: str, owner: IRODSObjectStore) -> None:
        self._owner = owner
        self._collection = owner.metastore.path(uuid)
        super().__init__(owner.collection, uuid, owner.mode, owner.data_type)

    def _create_store(self) -> VersionedZarrStore[IRODSKVStore]:
        versions = [
            path.rsplit("/", 1)[1] for path in list_collections(self._owner._sessions, self._collection)
        ]
        versions = [v for v in versions if v.startswith("v") and v[1:].isdigit()]
        versions.sort(key=lambda version: int(version[1:]))

        def factory(version: str) -> IRODSKVStore:
            return IRODSKVStore(self.mapping(version))

        return VersionedZarrStore(factory=factory, versions=versions)

    def mapping(self, version: str = "latest") -> IRODSZarrMapping:
        """Return a version's transport for chunk cache and provenance inspection."""
        if version == "latest":
            version = self.latest_version
        if not version.startswith("v") or not version[1:].isdigit():
            raise ValueError("A version must have the form v1, v2, ...")
        return IRODSZarrMapping(
            self._owner._sessions,
            self._collection + "/" + version,
            self._owner.cache_dir,
            read_only=self._mode == "r",
            resource=self._owner.resource,
            write_guard=self._owner._require_write,
        )

    @classmethod
    def load(cls, uuid: str, owner: IRODSObjectStore, **kwargs: Any) -> Self:  # type: ignore[override]
        """Load catalog state and version names without transferring data chunks."""
        try:
            state = read_document(owner._sessions, owner.metastore.path(uuid), "datasource")
        except KeyError as exc:
            raise ObjectStoreError(f"No iRODS datasource state found for {uuid}.") from exc
        if state.get("_uuid") != uuid:
            raise ObjectStoreError("The catalog datasource UUID does not match its collection.")
        ds = cls(uuid, owner)
        ds.__dict__.update({k: v for k, v in state.items() if k not in cls._runtime_state_keys})
        ds._data_keys = defaultdict(list, ds._data_keys)
        return ds

    def _write_state(self, state: dict) -> None:
        self._owner._require_write()
        write_document(self._owner._sessions, self._collection, "datasource", state)

    def _delete_state(self) -> None:
        self._owner._require_write()
        delete_document(self._owner._sessions, self._collection, "datasource")

    def _delete_store_directory(self) -> None:
        # The enclosing store removes the UUID collection after its raw record.
        pass


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
    recovery. Multi-object writes are not transactions. Readers need external
    coordination with writers when a consistent whole-dataset snapshot matters.
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
        super().__init__(
            metastore,
            DatasourceFactory(IRODSDatasource, new_kwargs={"owner": self}, load_kwargs={"owner": self}),
            make_metadata_updater_fn(skip_keys=skip_keys, extend_keys=extend_keys),
            documents=_Documents(self),
        )

    def __enter__(self) -> Self:
        from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME

        if self._depth:
            self._depth += 1
            return self
        connection = self._sessions.factory()
        active = connection.__enter__()
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
            return self
        except BaseException:
            connection = self._connection
            self._sessions.active = None
            self._connection = None
            connection.__exit__(None, None, None)
            raise

    def close(self) -> None:
        """Release this context's writer lock and connection; deferred reads reopen."""
        if self._depth > 1:
            self._depth -= 1
            return
        try:
            if self._locked:
                self._sessions.active.collections.remove(self.lock_path, recurse=False, force=True)
                self._locked = False
        finally:
            self._locked = False
            self._depth = 0
            if self._connection is not None:
                connection = self._connection
                self._connection = None
                self._sessions.active = None
                connection.__exit__(None, None, None)

    def _require_write(self) -> None:
        if self.mode != "rw":
            raise PermissionError("This iRODS ObjectStore is read-only.")
        if not self._locked:
            raise ObjectStoreError("Use a with statement to acquire the iRODS writer lock.")

    def create(self, metadata: dict[str, Any], data: xr.Dataset, **kwargs: Any) -> str:
        self._require_write()
        if not self.data_type:
            raise ValueError("A data_type is required for datasource operations.")
        return super().create(metadata, data, **kwargs)

    def update(
        self,
        uuid: str,
        metadata: dict[str, Any] | None = None,
        data: xr.Dataset | None = None,
        keys_to_delete: str | list[str] | None = None,
        extend_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self._require_write()
        super().update(uuid, metadata, data, keys_to_delete, extend_keys, **kwargs)

    def delete(self, uuid: str) -> None:
        self._require_write()
        super().delete(uuid)
        with self._sessions() as session:
            session.collections.remove(self.metastore.path(uuid), recurse=True)

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
