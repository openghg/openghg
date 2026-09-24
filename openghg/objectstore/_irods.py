"""Experimental catalog-backed storage of immutable NetCDF snapshots.

The supplied iRODS session is borrowed. Keep it open while using the store or
its datasources. See the developer guide for deployment and cache semantics.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path, PurePosixPath
import tempfile
from typing import Any, Literal
from uuid import uuid4

from filelock import FileLock
import xarray as xr

from openghg.objectstore._datasource import AbstractDatasource, DatasourceFactory
from openghg.objectstore._irods_metastore import IRODSMetaStore, encode_metadata
from openghg.objectstore._objectstore import ObjectStore
from openghg.types import ObjectStoreError

logger = logging.getLogger(__name__)


def _digest(path: Path, checksum: str) -> str:
    """Hash local bytes using an iRODS SHA-256 or legacy MD5 checksum format."""
    if checksum.startswith("sha2:"):
        hasher = hashlib.sha256()
    elif len(checksum) == 32 and all(c in "0123456789abcdef" for c in checksum.lower()):
        hasher = hashlib.md5()  # noqa: S324 - compatibility with iRODS catalogs
    else:
        raise ObjectStoreError("A supported catalog checksum (SHA-256 or MD5) is required.")
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    if checksum.startswith("sha2:"):
        return "sha2:" + base64.b64encode(hasher.digest()).decode("ascii")
    return hasher.hexdigest()


def _snapshot(session: Any, path: str) -> dict[str, Any]:
    """Read catalog identity and select a good, checksummed physical replica."""
    obj = session.data_objects.get(path)
    replicas = [r for r in obj.replicas if str(r.status) == "1"]
    if not replicas or any(not r.checksum for r in replicas):
        raise ObjectStoreError("The object needs a good replica with a registered checksum.")
    if len({(r.checksum, int(r.size)) for r in replicas}) != 1:
        raise ObjectStoreError("Good replicas disagree about checksum or size; repair the remote object.")
    replica = min(replicas, key=lambda r: int(r.number))
    return {
        "host": session.host,
        "port": session.port,
        "zone": path.split("/")[1],
        "data_id": str(obj.id),
        "logical_path": path,
        "checksum": replica.checksum,
        "size": int(replica.size),
        "replica_number": int(replica.number),
        "resource": replica.resource_name,
    }


class IRODSDatasource(AbstractDatasource[xr.Dataset]):
    """Catalog reference whose payload is downloaded only on explicit access.

    Obtain instances through :class:`IRODSObjectStore`. ``get_data`` loads the
    whole snapshot into memory; ``local_path`` permits application-managed lazy
    xarray reads. Both require an open session, even for cache hits.
    """

    def __init__(self, uuid: str, store: IRODSObjectStore) -> None:
        super().__init__(uuid)
        self._store = store
        self.metadata = store.metastore.record(uuid)
        self._provenance: dict[str, Any] | None = None

    @classmethod
    def load(cls, uuid: str, store: IRODSObjectStore) -> IRODSDatasource:
        """Read catalog metadata without transferring dataset bytes."""
        return cls(uuid=uuid, store=store)

    def local_path(self) -> Path:
        """Return a verified cached snapshot, downloading it when necessary.

        Every call rechecks remote identity and hashes cached bytes. Corrupt
        cache entries are replaced only after a successful verified download.
        A JSON receipt beside the NetCDF records remote identity and metadata;
        this local copy is not a registered iRODS replica. No offline fallback
        is performed on connection, permission, or integrity failures.
        """
        from irods import keywords as kw

        store = self._store
        self.metadata = store.metastore.record(self.uuid)
        remote_path = store.metastore.path(self.uuid)
        identity = _snapshot(store.session, remote_path)
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        directory = store.cache_dir / key
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / "data.nc"
        receipt = directory / "provenance.json"
        with FileLock(str(directory / "download.lock")):
            if target.exists() and receipt.exists():
                try:
                    provenance = json.loads(receipt.read_text())
                    if (
                        isinstance(provenance, dict)
                        and provenance.get("schema_version") == 1
                        and provenance.get("kind") == "verified_local_cache"
                        and provenance.get("uuid") == self.uuid
                        and isinstance(provenance.get("metadata"), dict)
                        and isinstance(provenance.get("downloaded_at"), str)
                        and provenance.get("source") == identity
                        and target.stat().st_size == identity["size"]
                        and _digest(target, identity["checksum"]) == identity["checksum"]
                    ):
                        self._provenance = provenance
                        return target
                except (ValueError, KeyError, TypeError):
                    pass

            with tempfile.TemporaryDirectory(dir=directory) as temporary:
                staged = Path(temporary) / "data.nc"
                store.session.data_objects.get(
                    remote_path,
                    str(staged),
                    num_threads=1,
                    **{kw.REPL_NUM_KW: str(identity["replica_number"])},
                )
                if (
                    staged.stat().st_size != identity["size"]
                    or _digest(staged, identity["checksum"]) != identity["checksum"]
                ):
                    raise ObjectStoreError("Downloaded bytes do not match the catalog checksum and size.")
                if _snapshot(store.session, remote_path) != identity:
                    raise ObjectStoreError("Remote object changed during download; retry the read.")
                provenance = {
                    "schema_version": 1,
                    "kind": "verified_local_cache",
                    "uuid": self.uuid,
                    "source": identity,
                    "metadata": self.metadata,
                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                }
                staged_receipt = Path(temporary) / "provenance.json"
                staged_receipt.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
                staged.replace(target)
                staged_receipt.replace(receipt)
                self._provenance = provenance
        return target

    @property
    def provenance(self) -> dict[str, Any]:
        """Return a copy of the receipt, fetching and verifying data if needed."""
        self.local_path()
        return json.loads(json.dumps(self._provenance))

    def get_data(self) -> xr.Dataset:
        """Load the complete verified NetCDF snapshot into memory."""
        with xr.open_dataset(self.local_path(), engine="h5netcdf") as data:
            return data.load()

    def add(self, data: xr.Dataset, **kwargs: Any) -> None:
        """Reject mutation: publish another snapshot using the store's create method."""
        raise NotImplementedError("iRODS snapshots are immutable; create a new datasource.")

    def save(self) -> None:
        """Reject independent saves; the store publishes payload and metadata together."""
        raise NotImplementedError("Use IRODSObjectStore.create to publish a snapshot.")

    def delete(self) -> None:
        """Move the remote snapshot to iRODS trash; retain existing local caches."""
        self._store.delete(self.uuid)


class IRODSObjectStore(ObjectStore[IRODSDatasource, xr.Dataset]):
    """Experimental ObjectStore using the iRODS catalog and data objects.

    Args:
        session: Borrowed authenticated python-irodsclient session. Closing this
            store does not close the session.
        collection: Existing absolute logical collection, dedicated to this store.
        cache_dir: Local directory for checksum-verified downloads and receipts.
        mode: ``r`` for read-only access (default), or ``rw`` for publication,
            metadata updates, trash deletion and managed-resource replication.
        resource: Optional registered resource to use for new uploads.

    Payloads are immutable NetCDF snapshots with JSON-compatible catalog
    metadata. Writes assume one application writer. This explicit API does not
    register a backend for the standardise/get_obs workflows or local config.
    """

    def __init__(
        self,
        session: Any,
        collection: str,
        cache_dir: str | Path,
        mode: Literal["r", "rw"] = "r",
        resource: str | None = None,
    ) -> None:
        try:
            import irods  # noqa: F401
        except ImportError as exc:
            raise ImportError("Install the optional client with pip install 'openghg[irods]'.") from exc
        if mode not in ("r", "rw"):
            raise ValueError("mode must be 'r' or 'rw'.")
        collection = collection.rstrip("/")
        if (
            not collection.startswith("/")
            or collection.startswith("//")
            or str(PurePosixPath(collection)) != collection
            or ".." in PurePosixPath(collection).parts
            or any(ord(char) < 32 or char in "'\\" for char in collection)
        ):
            raise ValueError(
                "collection must be an absolute normalized iRODS path without quotes or controls."
            )
        session.collections.get(collection)
        self.session = session
        self.cache_dir = Path(cache_dir).expanduser().resolve()
        self.mode = mode
        self.resource = resource
        super().__init__(
            IRODSMetaStore(session, collection, mode=mode),
            DatasourceFactory(IRODSDatasource, new_kwargs={"store": self}, load_kwargs={"store": self}),
        )

    def _require_write(self) -> None:
        if self.mode != "rw":
            raise PermissionError("This iRODS object store was opened read-only.")

    def search(self, metadata: dict[str, Any] | None = None, **kwargs: Any) -> list[dict[str, Any]]:
        """Search authoritative catalog metadata without downloading payloads."""
        return self.metastore.search(**self._search_params(metadata, **kwargs))

    def retrieve(self, metadata: dict[str, Any] | None = None, **kwargs: Any) -> list[IRODSDatasource]:
        """Return catalog references matching search; dataset bytes remain remote."""
        return [self.get_datasource(record["uuid"]) for record in self.search(metadata, **kwargs)]

    def get_uuids(self, metadata: dict[str, Any] | None = None) -> list[str]:
        """Return UUIDs matching catalog metadata."""
        return [record["uuid"] for record in self.search(metadata)]

    def create(self, metadata: dict[str, Any], data: xr.Dataset, **kwargs: Any) -> str:
        """Publish a new immutable snapshot, returning its UUID.

        Metadata must be JSON-compatible and uniquely identify the datasource;
        include a revision descriptor when publishing a replacement. Dataset
        serialization and metadata validation precede upload. Publication uses
        one atomic AVU operation after upload and checksum verification. Failed
        publication attempts remove their new object when the server permits it.
        Extra storage kwargs are not supported by this prototype.
        """
        from irods import keywords as kw

        self._require_write()
        if kwargs:
            raise TypeError(f"Unsupported iRODS snapshot options: {', '.join(kwargs)}")
        record = json.loads(encode_metadata(metadata))
        if "uuid" in record:
            raise ValueError("UUIDs are assigned by the object store.")
        if self.search(record):
            raise ObjectStoreError(
                "This metadata already identifies a datasource; use distinct revision metadata."
            )
        uuid = str(uuid4())
        record["uuid"] = uuid
        encode_metadata(record)
        path = self.metastore.path(uuid)
        options: dict[str, Any] = {kw.FORCE_FLAG_KW: False}
        if self.resource is not None:
            options[kw.DEST_RESC_NAME_KW] = self.resource
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "data.nc"
            data.to_netcdf(local, engine="h5netcdf")
            # A random UUID plus no-force upload protects other catalog objects.
            if self.session.data_objects.exists(path):
                raise ObjectStoreError("The generated UUID already exists in the catalog.")
            uploaded = False
            try:
                self.session.data_objects.put(str(local), path, num_threads=1, **options)
                uploaded = True
                self.session.data_objects.chksum(path)
                identity = _snapshot(self.session, path)
                if (
                    identity["size"] != local.stat().st_size
                    or _digest(local, identity["checksum"]) != identity["checksum"]
                ):
                    raise ObjectStoreError("Uploaded snapshot failed checksum verification.")
                self.metastore.insert(record)
            except BaseException:
                try:
                    # A failed transfer may be ambiguous (including a collision).
                    # Only remove objects whose upload this call completed.
                    if uploaded and self.session.data_objects.exists(path):
                        self.session.data_objects.unlink(path, force=True)
                except Exception:
                    logger.exception("Could not remove unpublished snapshot %s; inspect the catalog.", path)
                raise
        return uuid

    def update(
        self,
        uuid: str,
        metadata: dict[str, Any] | None = None,
        data: xr.Dataset | None = None,
        keys_to_delete: str | list[str] | None = None,
        extend_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Update catalog metadata; reject payload updates before any mutation."""
        self._require_write()
        if data is not None:
            raise NotImplementedError(
                "iRODS snapshots are immutable; create a new datasource with revision metadata."
            )
        if kwargs:
            raise TypeError(f"Unsupported iRODS metadata options: {', '.join(kwargs)}")
        super().update(
            uuid,
            metadata=dict(metadata) if metadata is not None else None,
            keys_to_delete=keys_to_delete,
            extend_keys=extend_keys,
        )

    def delete(self, uuid: str) -> None:
        """Move a published object and its AVUs to trash; do not delete local caches."""
        self._require_write()
        self.metastore.record(uuid)
        self.session.data_objects.unlink(self.metastore.path(uuid), force=False)

    def replicate(self, uuid: str, resource: str) -> None:
        """Ask iRODS for a replica on an existing server-managed storage resource.

        This is distinct from downloading into ``cache_dir``. Resource creation,
        reachability and permissions must be arranged with the zone administrator.
        """
        self._require_write()
        self.metastore.record(uuid)
        self.session.data_objects.replicate(self.metastore.path(uuid), resource=resource)
