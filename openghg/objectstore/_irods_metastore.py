"""Experimental iRODS catalog metadata for immutable OpenGHG snapshots.

The ``openghg:record`` AVU publishes a data object as a searchable snapshot.
Per-field AVUs provide exact-match catalog indexes; the JSON record remains
authoritative. Atomic AVU replacement protects individual changes, but this
prototype requires a single metadata writer because it has no compare-and-swap.
"""

from __future__ import annotations

from collections.abc import Callable
import json
from typing import Any, Literal
from uuid import UUID

from openghg.objectstore.metastore import MetaStore
from openghg.types import MetastoreError, ObjectStoreError

RECORD_ATTRIBUTE = "openghg:record"
FIELD_PREFIX = "openghg:field:"
MAX_AVU_BYTES = 2700


def encode_metadata(metadata: dict[str, Any]) -> str:
    """Encode JSON metadata with lowercase keys and deterministic AVU values.

    Args:
        metadata: JSON-compatible metadata. Top-level keys are lowercased,
            matching the existing metastore's key normalization.

    Returns:
        Canonical JSON suitable for a catalog AVU.

    Raises:
        TypeError: If metadata contains non-string mapping keys or values that
            JSON cannot represent.
        ValueError: If keys collide after lowercasing, values are non-finite,
            keys contain control characters, or the record or an indexed name
            exceeds the prototype's 2700-byte AVU limit.
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
    for key in normalized:
        if any(ord(character) < 32 or ord(character) == 127 for character in key):
            raise ValueError("iRODS metadata keys must not contain control characters")
        if len((FIELD_PREFIX + key).encode("utf-8")) > MAX_AVU_BYTES:
            raise ValueError("Indexed iRODS metadata names must not exceed 2700 UTF-8 bytes")
    encoded = json.dumps(
        normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    if len(encoded.encode("utf-8")) > MAX_AVU_BYTES:
        raise ValueError("iRODS metadata records must not exceed 2700 UTF-8 bytes")
    return encoded


class IRODSMetaStore(MetaStore):
    """Store metadata on direct child data objects in an existing collection.

    Args:
        session: Borrowed python-irodsclient session; closing this metastore
            leaves the session open.
        collection: Absolute, normalized iRODS collection path, validated by
            the object store factory.
        mode: ``"r"`` for read-only catalog access or ``"rw"`` for mutations.

    Notes:
        Payloads must exist before insertion. This class only manages catalog
        metadata; its ``delete`` method does not unlink data objects.
    """

    def __init__(self, session: Any, collection: str, mode: Literal["r", "rw"] = "r") -> None:
        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")
        self.session = session
        self.collection = collection
        self.mode = mode

    def path(self, uuid: str) -> str:
        """Return the logical snapshot path after validating a canonical UUID.

        Args:
            uuid: Canonical lowercase UUID string identifying a snapshot.

        Returns:
            Absolute iRODS logical path for the snapshot's NetCDF object.

        Raises:
            ValueError: If the identifier is not a canonical UUID string.
        """
        if not isinstance(uuid, str) or str(UUID(uuid)) != uuid:
            raise ValueError("Snapshot UUID must be a canonical lowercase UUID")
        return f"{self.collection}/{uuid}.nc"

    def record(self, uuid: str) -> dict[str, Any]:
        """Read one published snapshot's authoritative metadata.

        Args:
            uuid: Canonical UUID of the snapshot to inspect.

        Returns:
            A new dictionary containing the published metadata.

        Raises:
            ObjectStoreError: If no object or publication marker exists.
            MetastoreError: If the marker is ambiguous, malformed, or identifies
                a different snapshot.
        """
        from irods.exception import DataObjectDoesNotExist

        try:
            obj = self.session.data_objects.get(self.path(uuid))
        except DataObjectDoesNotExist as exc:
            raise ObjectStoreError(f"No iRODS snapshot with UUID {uuid}") from exc
        markers = obj.metadata.get_all(RECORD_ATTRIBUTE)
        if not markers:
            raise ObjectStoreError(f"No published iRODS snapshot with UUID {uuid}")
        if len(markers) != 1:
            raise MetastoreError(f"Multiple publication markers for iRODS snapshot {uuid}")
        try:
            record = json.loads(markers[0].value)
            if not isinstance(record, dict) or record.get("uuid") != uuid:
                raise ValueError("Snapshot UUID does not match catalog record")
            encode_metadata(record)
        except (TypeError, ValueError) as exc:
            raise MetastoreError(f"Invalid catalog record for iRODS snapshot {uuid}") from exc
        return record

    def search(
        self,
        search_terms: dict[str, Any] | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
        search_list_keys: dict | None = None,
    ) -> list[dict[str, Any]]:
        """Search published records with the existing metastore semantics.

        Args:
            search_terms: Exact metadata values to match.
            search_functions: Metadata keys and predicates to apply.
            negative_lookup_keys: Keys that must be absent.
            search_list_keys: Keys and items that must appear in metadata lists.

        Returns:
            Matching records, without downloading scientific data.
        """
        from irods import keywords
        from irods.models import Collection, DataObject, DataObjectMeta
        from openghg.objectstore._objectstore import _memory_metastore

        terms = json.loads(encode_metadata(search_terms or {}))
        query = self.session.query(DataObject.name).filter(Collection.name == self.collection)
        # JSON distinguishes 1, 1.0 and True while TinyDB's Python equality
        # does not. PRC's GenQuery serializer also does not escape quotes.
        indexed_term = None
        for key, value in terms.items():
            if value is not None and not isinstance(value, str):
                continue
            encoded = json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
            if any(character in key + encoded for character in ("'", "\\")):
                continue
            indexed_term = key, encoded
            break
        if indexed_term is not None:
            key, encoded = indexed_term
            query = query.filter(DataObjectMeta.name == FIELD_PREFIX + key, DataObjectMeta.value == encoded)
        else:
            query = query.filter(DataObjectMeta.name == RECORD_ATTRIBUTE)
        query = query.add_keyword(keywords.ZONE_KW, self.collection.split("/")[1])

        records = []
        names = {row[DataObject.name] for row in query}
        for name in sorted(names):
            if not name.endswith(".nc"):
                continue
            uuid = name[:-3]
            try:
                self.path(uuid)
            except ValueError:
                continue
            try:
                records.append(self.record(uuid))
            except ObjectStoreError:
                # An unpublished upload or a concurrently removed snapshot.
                continue
        with _memory_metastore(records) as metastore:
            return metastore.search(terms, search_functions, negative_lookup_keys, search_list_keys)

    def _require_write(self) -> None:
        if self.mode != "rw":
            raise PermissionError("Cannot modify a read-only iRODS metastore")

    def _publish(self, metadata: dict[str, Any]) -> None:
        """Replace managed AVUs atomically while preserving unrelated metadata."""
        from irods.meta import AVUOperation, iRODSMeta

        encoded = encode_metadata(metadata)
        record = json.loads(encoded)
        obj = self.session.data_objects.get(self.path(record["uuid"]))
        avus = obj.metadata
        operations = [
            AVUOperation("remove", avu)
            for avu in avus.items()
            if avu.name == RECORD_ATTRIBUTE or avu.name.startswith(FIELD_PREFIX)
        ]
        for key, value in record.items():
            field = json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
            operations.append(AVUOperation("add", iRODSMeta(FIELD_PREFIX + key, field)))
        operations.append(AVUOperation("add", iRODSMeta(RECORD_ATTRIBUTE, encoded)))
        avus.apply_atomic_operations(*operations)

    def insert(self, metadata: dict[str, Any]) -> None:
        """Publish metadata for an uploaded, previously unpublished snapshot.

        Args:
            metadata: JSON-compatible metadata including the snapshot UUID.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If the snapshot is already published.
        """
        self._require_write()
        record = json.loads(encode_metadata(metadata))
        obj = self.session.data_objects.get(self.path(record["uuid"]))
        if obj.metadata.get_all(RECORD_ATTRIBUTE):
            raise MetastoreError(f"iRODS snapshot {record['uuid']} is already published")
        self._publish(record)

    def update(
        self,
        where: dict[str, Any],
        to_update: dict[str, Any] | None = None,
        to_delete: str | list[str] | None = None,
        to_extend: dict | None = None,
    ) -> None:
        """Update exactly one record using TinyDB's existing merge semantics.

        Args:
            where: Metadata identifying exactly one snapshot.
            to_update: Keys and replacement values.
            to_delete: Key or keys to remove, excluding the immutable UUID.
            to_extend: Values to merge or append using existing metastore rules.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If the selection does not identify exactly one record.
            ValueError: If the update attempts to change or delete its UUID.
        """
        from openghg.objectstore._objectstore import _memory_metastore

        self._require_write()
        records = self.search(where)
        with _memory_metastore(records) as metastore:
            metastore.update(where, to_update, to_delete, to_extend)
            updated = metastore.search()[0]
        if updated.get("uuid") != records[0]["uuid"]:
            raise ValueError("Cannot change or delete a snapshot UUID")
        self._publish(updated)

    def delete(self, metadata: dict[str, Any], delete_one: bool = True) -> None:
        """Unpublish matching snapshots without deleting their payloads.

        Args:
            metadata: Exact metadata identifying records to unpublish.
            delete_one: Require exactly one match when true.

        Raises:
            PermissionError: If this metastore is read-only.
            MetastoreError: If ``delete_one`` is true and the match is not unique.
        """
        from irods.meta import AVUOperation

        self._require_write()
        records = self.search(metadata)
        if delete_one and len(records) != 1:
            raise MetastoreError("Metadata must identify exactly one iRODS snapshot")
        for record in records:
            avus = self.session.data_objects.get(self.path(record["uuid"])).metadata
            operations = [
                AVUOperation("remove", avu)
                for avu in avus.items()
                if avu.name == RECORD_ATTRIBUTE or avu.name.startswith(FIELD_PREFIX)
            ]
            if operations:
                avus.apply_atomic_operations(*operations)

    def close(self) -> None:
        """Leave the borrowed iRODS session open."""
