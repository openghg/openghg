"""iRODS transport for Zarr keys and catalog-resident JSON documents.

Session factories return context managers. A configured store can therefore
close while lazy arrays retain the ability to open their own short-lived
sessions. Callers using a borrowed session may supply ``lambda: nullcontext(s)``.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterator, MutableMapping
from contextlib import AbstractContextManager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import tempfile
from typing import Any

from filelock import FileLock
from zarr.storage import KVStore

from openghg.types import ObjectStoreError

SessionFactory = Callable[[], AbstractContextManager[Any]]
DOCUMENT_PREFIX = "openghg:document:"
_CHUNK_SIZE = 2500


def validate_collection(collection: str) -> str:
    """Require an absolute logical path that PRC GenQuery can represent safely."""
    if not isinstance(collection, str) or not collection.startswith("/"):
        raise ValueError("An absolute iRODS collection path is required.")
    _validate_parts(collection[1:])
    return collection


def validate_ordinary_collection(session: Any, collection: str) -> str:
    """Reject linked or mounted catalog collections at the root or its ancestors.

    A writer mutex relies on ordinary catalog collection creation returning a
    conflict for an existing name. Special collections have different creation
    semantics. PRC 3.3 omits COL_COLL_TYPE, so register that native query column.
    """
    from irods.column import Column, In, String
    from irods.models import Collection, Model, ModelBase

    collection = validate_collection(collection)
    session.collections.get(collection)
    collection_type = ModelBase.columns().get(510)
    if collection_type is None:

        class CollectionType(Model):
            kind = Column(String, "COLL_TYPE", 510)

        collection_type = CollectionType.kind
        # PRC caches this registry before dynamically added Model subclasses.
        ModelBase.columns()[510] = collection_type
    path = PurePosixPath(collection)
    ancestors = [str(p) for p in [path, *path.parents] if str(p) != "/"]
    found = set()
    for row in session.query(Collection.name, collection_type).filter(In(Collection.name, ancestors)):
        name, kind = row[Collection.name], row[collection_type]
        found.add(name)
        if kind not in (None, "", "0"):
            raise ObjectStoreError(f"iRODS ObjectStore requires ordinary collections; {name} is special.")
    if missing := set(ancestors) - found:
        raise ObjectStoreError(
            "The iRODS collection type could not be verified for: "
            + ", ".join(sorted(missing))
            + ". Read access to the store root and its ancestors is required."
        )
    return collection


def _validate_parts(path: str) -> None:
    if (
        not isinstance(path, str)
        or any(part in ("", ".", "..") for part in path.split("/"))
        or any(ord(c) < 32 or c in "'\\" for c in path)
    ):
        raise ValueError("iRODS paths must have safe, nonempty components without traversal or quoting.")


def document_attribute(key: str) -> str:
    """Return the exact AVU attribute identifying one catalog JSON document."""
    if (
        not isinstance(key, str)
        or not key
        or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for c in key)
    ):
        raise ValueError("Document keys must contain lowercase letters, digits, underscores or hyphens.")
    if len(key) > 128:
        raise ValueError("Document keys must be at most 128 characters.")
    return DOCUMENT_PREFIX + key


def read_document(session_factory: SessionFactory, collection: str, key: str) -> dict[str, Any]:
    """Read one complete JSON document; raise KeyError when it is absent."""
    from irods.exception import CollectionDoesNotExist

    name = document_attribute(key)
    with session_factory() as session:
        try:
            avus = session.collections.get(validate_collection(collection)).metadata.get_all(name)
        except CollectionDoesNotExist as exc:
            raise KeyError(key) from exc
    if not avus:
        raise KeyError(key)
    try:
        chunks = {avu.units: avu.value for avu in avus}
        manifest = json.loads(chunks.pop("manifest"))
        if len(chunks) + 1 != len(avus) or manifest["schema_version"] != 1:
            raise ValueError("Duplicate chunks or unsupported schema")
        expected = {str(i) for i in range(manifest["chunks"])}
        if set(chunks) != expected:
            raise ValueError("Missing or unexpected chunks")
        payload = "".join(chunks[str(i)] for i in range(manifest["chunks"]))
        if hashlib.sha256(payload.encode()).hexdigest() != manifest["sha256"]:
            raise ValueError("Checksum mismatch")
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("Expected a JSON object")
        return value
    except (KeyError, TypeError, ValueError) as exc:
        raise ObjectStoreError(f"Invalid iRODS catalog document: {key}") from exc


def write_document(session_factory: SessionFactory, collection: str, key: str, value: dict[str, Any]) -> None:
    """Atomically replace a chunked JSON document, preserving unrelated AVUs.

    This is a single catalog transaction, not a compare-and-swap operation.
    The enclosing ObjectStore writer context must serialize concurrent updates.
    """
    from irods.meta import AVUOperation, iRODSMeta

    name = document_attribute(key)
    if not isinstance(value, dict):
        raise TypeError("Catalog documents must be dictionaries.")
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    chunks = [payload[i : i + _CHUNK_SIZE] for i in range(0, len(payload), _CHUNK_SIZE)]
    manifest = json.dumps(
        {"schema_version": 1, "chunks": len(chunks), "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    )
    replacements = [iRODSMeta(name, chunk, str(i)) for i, chunk in enumerate(chunks)]
    replacements.append(iRODSMeta(name, manifest, "manifest"))
    with session_factory() as session:
        metadata = session.collections.get(validate_collection(collection)).metadata
        operations = [AVUOperation("remove", avu) for avu in metadata.get_all(name)]
        operations.extend(AVUOperation("add", avu) for avu in replacements)
        metadata.apply_atomic_operations(*operations)


def delete_document(session_factory: SessionFactory, collection: str, key: str) -> None:
    """Remove only this document's AVUs; an absent document is a no-op."""
    from irods.meta import AVUOperation

    name = document_attribute(key)
    with session_factory() as session:
        metadata = session.collections.get(validate_collection(collection)).metadata
        operations = [AVUOperation("remove", avu) for avu in metadata.get_all(name)]
        if operations:
            metadata.apply_atomic_operations(*operations)


def list_collections(session_factory: SessionFactory, collection: str) -> list[str]:
    """List direct child collections without scanning descendants or data bytes."""
    from irods.exception import CollectionDoesNotExist

    with session_factory() as session:
        try:
            parent = session.collections.get(validate_collection(collection))
        except CollectionDoesNotExist:
            return []
        return sorted(c.path for c in parent.subcollections)


def _digest(payload: Path | bytes, checksum: str) -> str:
    if checksum.startswith("sha2:"):
        hasher = hashlib.sha256()
    elif len(checksum) == 32 and all(c in "0123456789abcdef" for c in checksum.lower()):
        hasher = hashlib.md5()  # noqa: S324 - compatibility with iRODS catalogs
    else:
        raise ObjectStoreError("A supported catalog checksum (SHA-256 or MD5) is required.")
    if isinstance(payload, bytes):
        hasher.update(payload)
    else:
        with payload.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                hasher.update(chunk)
    return (
        "sha2:" + base64.b64encode(hasher.digest()).decode("ascii")
        if checksum.startswith("sha2:")
        else hasher.hexdigest()
    )


def _snapshot(session: Any, path: str) -> dict[str, Any]:
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


class IRODSZarrMapping(MutableMapping[str, bytes]):
    """Store each Zarr key as an iRODS object with checksum-verified reads.

    Reads fetch bytes into memory unless ``cache_dir`` explicitly enables a
    persistent cache. Reads check the live catalog even on cache hits. Cached
    objects and receipts are immutable per catalog identity; deleting remote
    data does not evict cached bytes. Cache entries are not iRODS replicas.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        collection: str,
        cache_dir: str | Path | None = None,
        *,
        read_only: bool = False,
        resource: str | None = None,
        write_guard: Callable[[], None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.collection = validate_collection(collection)
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir is not None else None
        self.read_only = read_only
        self.resource = resource
        self.write_guard = write_guard

    def _path(self, key: str) -> str:
        _validate_parts(key)
        return self.collection + "/" + key

    def _writable(self) -> None:
        if self.read_only:
            raise PermissionError("The iRODS Zarr mapping is read-only.")
        if self.write_guard is not None:
            self.write_guard()

    def local_path(self, key: str) -> Path:
        """Return a verified cached file; raise ValueError if caching is disabled."""
        from irods import keywords as kw
        from irods.exception import DataObjectDoesNotExist

        if self.cache_dir is None:
            raise ValueError("Persistent caching is disabled; set cache_dir to enable it.")
        path = self._path(key)
        with self.session_factory() as session:
            try:
                identity = _snapshot(session, path)
            except DataObjectDoesNotExist as exc:
                raise KeyError(key) from exc
            digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            directory = self.cache_dir / digest
            directory.mkdir(parents=True, exist_ok=True)
            target, receipt = directory / "data", directory / "provenance.json"
            with FileLock(str(directory / "download.lock")):
                if target.exists() and receipt.exists():
                    try:
                        provenance = json.loads(receipt.read_text())
                        if (
                            provenance["schema_version"] == 1
                            and provenance["kind"] == "verified_local_cache"
                            and provenance["source"] == identity
                            and isinstance(provenance["downloaded_at"], str)
                            and target.stat().st_size == identity["size"]
                            and _digest(target, identity["checksum"]) == identity["checksum"]
                        ):
                            return target
                    except (KeyError, ValueError, TypeError):
                        pass
                with tempfile.TemporaryDirectory(dir=directory) as temporary:
                    staged = Path(temporary) / "data"
                    session.data_objects.get(
                        path, str(staged), num_threads=1, **{kw.REPL_NUM_KW: str(identity["replica_number"])}
                    )
                    if (
                        staged.stat().st_size != identity["size"]
                        or _digest(staged, identity["checksum"]) != identity["checksum"]
                    ):
                        raise ObjectStoreError("Downloaded bytes do not match the catalog checksum and size.")
                    if _snapshot(session, path) != identity:
                        raise ObjectStoreError("Remote object changed during download; retry the read.")
                    provenance = {
                        "schema_version": 1,
                        "kind": "verified_local_cache",
                        "source": identity,
                        "downloaded_at": datetime.now(timezone.utc).isoformat(),
                    }
                    staged_receipt = Path(temporary) / "provenance.json"
                    staged_receipt.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
                    staged.replace(target)
                    staged_receipt.replace(receipt)
            return target

    def provenance(self, key: str) -> dict[str, Any]:
        """Read a verified cache receipt; raise ValueError if caching is disabled."""
        return json.loads(self.local_path(key).with_name("provenance.json").read_text())

    def __getitem__(self, key: str) -> bytes:
        from irods import keywords as kw
        from irods.exception import DataObjectDoesNotExist

        if self.cache_dir is not None:
            return self.local_path(key).read_bytes()
        path = self._path(key)
        with self.session_factory() as session:
            try:
                identity = _snapshot(session, path)
                with session.data_objects.open(
                    path, "r", **{kw.REPL_NUM_KW: str(identity["replica_number"])}
                ) as stream:
                    payload: bytes = stream.read(identity["size"] + 1)
            except DataObjectDoesNotExist as exc:
                raise KeyError(key) from exc
            if (
                len(payload) != identity["size"]
                or _digest(payload, identity["checksum"]) != identity["checksum"]
            ):
                raise ObjectStoreError("Downloaded bytes do not match the catalog checksum and size.")
            if _snapshot(session, path) != identity:
                raise ObjectStoreError("Remote object changed during download; retry the read.")
            return payload

    def __setitem__(self, key: str, value: bytes) -> None:
        from irods import keywords as kw

        self._writable()
        path = self._path(key)
        with self.session_factory() as session, tempfile.TemporaryDirectory() as temporary:
            staged = Path(temporary) / "data"
            staged.write_bytes(bytes(value))
            session.collections.create(str(PurePosixPath(path).parent), recurse=True)
            options = {kw.FORCE_FLAG_KW: True}
            if self.resource:
                options[kw.DEST_RESC_NAME_KW] = self.resource
            session.data_objects.put(str(staged), path, num_threads=1, **options)
            session.data_objects.chksum(path, **{kw.FORCE_CHKSUM_KW: ""})
            identity = _snapshot(session, path)
            if (
                identity["size"] != staged.stat().st_size
                or _digest(staged, identity["checksum"]) != identity["checksum"]
            ):
                raise ObjectStoreError("Uploaded bytes do not match the catalog checksum and size.")

    def __delitem__(self, key: str) -> None:
        from irods.exception import DataObjectDoesNotExist

        self._writable()
        with self.session_factory() as session:
            path = self._path(key)
            if not session.data_objects.exists(path):
                raise KeyError(key)
            try:
                session.data_objects.unlink(path, force=False)
            except DataObjectDoesNotExist as exc:
                raise KeyError(key) from exc

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        path = self._path(key)
        with self.session_factory() as session:
            return session.data_objects.exists(path)

    def __iter__(self) -> Iterator[str]:
        from irods.exception import CollectionDoesNotExist

        with self.session_factory() as session:
            try:
                collection = session.collections.get(self.collection)
            except CollectionDoesNotExist:
                return iter(())
            prefix = self.collection + "/"
            keys = [obj.path[len(prefix) :] for _, _, objects in collection.walk() for obj in objects]
        return iter(sorted(keys))

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def rmdir(self, path: str | None = None) -> None:
        """Move a Zarr directory subtree to iRODS trash, retaining local caches."""
        self._writable()
        target = self._path(path) if path else self.collection
        with self.session_factory() as session:
            if session.collections.exists(target):
                session.collections.remove(target, recurse=True, force=False)

    def getsize(self, path: str | None = None) -> int:
        """Return object size, or the sum of direct children for a directory."""
        target = self._path(path) if path else self.collection
        with self.session_factory() as session:
            if session.data_objects.exists(target):
                return int(session.data_objects.get(target).size)
            if session.collections.exists(target):
                return sum(int(obj.size) for obj in session.collections.get(target).data_objects)
        return 0


class IRODSKVStore(KVStore):
    """Adapt an iRODS mapping to Zarr with catalog-only sizing and subtree removal."""

    def __init__(self, mapping: IRODSZarrMapping) -> None:
        super().__init__(mapping)
        self._writeable = self._erasable = not mapping.read_only

    def rmdir(self, path: str | None = None) -> None:
        self._mutable_mapping.rmdir(path)

    def getsize(self, path: str | None = None) -> int:
        return self._mutable_mapping.getsize(path)
