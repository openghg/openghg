"""Searchable metadata stored as chunked JSON documents in the iRODS catalog.

A ``record`` document publishes its UUID collection to searches. Datasource
state and Zarr payloads are independent: searching and editing records never
downloads scientific data. Writers are serialized by the ObjectStore context.
"""

from __future__ import annotations

from collections.abc import Callable
import json
from typing import Any, Literal
from uuid import UUID

from openghg.objectstore._irods_storage import (
    SessionFactory,
    delete_document,
    list_collections,
    read_document,
    validate_collection,
    write_document,
)
from openghg.objectstore.metastore import MetaStore
from openghg.types import MetastoreError, ObjectStoreError


def encode_metadata(metadata: dict[str, Any]) -> str:
    """Encode JSON metadata with lowercase top-level keys and stable ordering.

    Args:
        metadata: JSON-compatible metadata. Nested mapping keys retain their case.

    Returns:
        Canonical JSON with no whole-record size limit. The transport splits
        records into catalog AVUs small enough for the server.

    Raises:
        TypeError: If mapping keys are not strings or values cannot be encoded.
        ValueError: If keys collide after lowercasing, keys contain control
            characters, or values contain non-finite numbers.
    """

    def check_keys(value: Any) -> None:
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise TypeError("iRODS metadata mapping keys must be strings")
            for item in value.values():
                check_keys(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                check_keys(item)

    if not isinstance(metadata, dict):
        raise TypeError("iRODS metadata must be a dictionary")
    check_keys(metadata)
    normalized = {key.lower(): value for key, value in metadata.items()}
    if len(normalized) != len(metadata):
        raise ValueError("iRODS metadata keys must be unique after lowercasing")
    if any(ord(character) < 32 or ord(character) == 127 for key in normalized for character in key):
        raise ValueError("iRODS metadata keys must not contain control characters")
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class IRODSMetaStore(MetaStore):
    """Store one catalog record on each direct UUID child collection.

    Args:
        session_factory: Callable returning a context manager for an iRODS session.
        collection: Absolute collection for one OpenGHG data type.
        mode: ``"r"`` for searches or ``"rw"`` to permit mutations.
        write_guard: Optional callback enforcing the enclosing store's writer lock.

    Notes:
        Publication and updates replace one chunked catalog document atomically.
        The enclosing ObjectStore must serialize writers; this is not a
        compare-and-swap transaction. Removing a record leaves its collection,
        datasource state, and payloads intact.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        collection: str,
        mode: Literal["r", "rw"] = "r",
        *,
        write_guard: Callable[[], None] | None = None,
    ) -> None:
        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")
        self.session_factory = session_factory
        self.collection = validate_collection(collection)
        self.mode = mode
        self.write_guard = write_guard

    def path(self, uuid: str) -> str:
        """Return the child collection for a canonical lowercase UUID.

        Raises:
            ValueError: If the UUID is invalid or not in canonical form.
        """
        if not isinstance(uuid, str) or str(UUID(uuid)) != uuid:
            raise ValueError("Datasource UUID must be a canonical lowercase UUID")
        return f"{self.collection}/{uuid}"

    def record(self, uuid: str) -> dict[str, Any]:
        """Read one published record without reading datasource state or data.

        Raises:
            ObjectStoreError: If the record is absent.
            MetastoreError: If its contents are invalid or identify another UUID.
        """
        try:
            record = read_document(self.session_factory, self.path(uuid), "record")
        except KeyError as exc:
            raise ObjectStoreError(f"No published iRODS datasource with UUID {uuid}") from exc
        except ObjectStoreError as exc:
            raise MetastoreError(f"Invalid catalog record for iRODS datasource {uuid}") from exc
        try:
            if not isinstance(record, dict) or record.get("uuid") != uuid:
                raise ValueError("Datasource UUID does not match catalog record")
            encode_metadata(record)
        except (TypeError, ValueError) as exc:
            raise MetastoreError(f"Invalid catalog record for iRODS datasource {uuid}") from exc
        return record

    def search(
        self,
        search_terms: dict[str, Any] | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
        search_list_keys: dict | None = None,
    ) -> list[dict[str, Any]]:
        """Search published catalog records with the existing metastore semantics.

        Args:
            search_terms: Exact values to match, with case-insensitive key names.
            search_functions: Predicates applied to values under their keys.
            negative_lookup_keys: Keys that must be absent.
            search_list_keys: Items that must be present in list-valued metadata.

        Returns:
            Matching records. Only catalog metadata is read, including when
            predicates require scanning all published records.
        """
        from openghg.objectstore._objectstore import _memory_metastore

        # ponytail: scan per-type catalog records; add AVU candidate indexes when scale requires them.
        records = []
        for path in list_collections(self.session_factory, self.collection):
            uuid = path.rsplit("/", 1)[-1]
            try:
                if path != self.path(uuid):
                    continue
            except ValueError:
                continue
            try:
                records.append(self.record(uuid))
            except ObjectStoreError:
                # Unpublished collection or a concurrently removed record.
                continue
        with _memory_metastore(records) as metastore:
            return metastore.search(search_terms, search_functions, negative_lookup_keys, search_list_keys)

    def _require_write(self) -> None:
        if self.mode != "rw":
            raise PermissionError("Cannot modify a read-only iRODS metastore")
        if self.write_guard is not None:
            self.write_guard()

    def insert(self, metadata: dict[str, Any]) -> None:
        """Publish metadata on its UUID collection, creating the collection if needed.

        Args:
            metadata: JSON-compatible metadata including a canonical UUID.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If the UUID already has a published record.
        """
        self._require_write()
        record = json.loads(encode_metadata(metadata))
        path = self.path(record["uuid"])
        try:
            self.record(record["uuid"])
        except ObjectStoreError:
            pass
        else:
            raise MetastoreError(f"iRODS datasource {record['uuid']} is already published")
        with self.session_factory() as session:
            session.collections.create(path, recurse=True)
        write_document(self.session_factory, path, "record", record)

    def update(
        self,
        where: dict[str, Any],
        to_update: dict[str, Any] | None = None,
        to_delete: str | list[str] | None = None,
        to_extend: dict | None = None,
    ) -> None:
        """Update exactly one record using the existing metastore merge semantics.

        Args:
            where: Metadata identifying exactly one record.
            to_update: Values to add or replace.
            to_delete: Names of keys to remove.
            to_extend: Values to append or merge with existing metadata.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If the selection does not identify exactly one record.
            ValueError: If the update changes or deletes the UUID.
        """
        from openghg.objectstore._objectstore import _memory_metastore

        self._require_write()
        records = self.search(where)
        with _memory_metastore(records) as metastore:
            metastore.update(where, to_update, to_delete, to_extend)
            updated = metastore.search()[0]
        if updated.get("uuid") != records[0]["uuid"]:
            raise ValueError("Cannot change or delete a datasource UUID")
        encode_metadata(updated)
        write_document(self.session_factory, self.path(updated["uuid"]), "record", updated)

    def delete(self, metadata: dict[str, Any], delete_one: bool = True) -> None:
        """Unpublish matching records while retaining their collections and payloads.

        Args:
            metadata: Exact metadata selecting records.
            delete_one: Require exactly one matching record when true.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If ``delete_one`` is true and the selection is not unique.
        """
        self._require_write()
        records = self.search(metadata)
        if delete_one and len(records) != 1:
            raise MetastoreError("Metadata must identify exactly one iRODS datasource")
        for record in records:
            delete_document(self.session_factory, self.path(record["uuid"]), "record")

    def close(self) -> None:
        """Do nothing; each transport operation manages its own session context."""
