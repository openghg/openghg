"""Small JSON documents stored alongside scientific datasources."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal, Protocol

from ._local_store import rlock


class DocumentStore(Protocol):
    """Persistence needed for store state and metadata-key configuration."""

    def read(self, key: str) -> dict[str, Any] | None:
        """Read a document, returning None when it does not exist."""
        ...

    def write(self, key: str, value: dict[str, Any]) -> None:
        """Persist a document or raise if the store is read-only."""
        ...


class LocalDocuments:
    """Use existing relative JSON filenames in a local object store.

    Args:
        bucket: Local store directory.
        mode: Whether document writes are permitted.
    """

    def __init__(self, bucket: str, mode: Literal["r", "rw"] = "rw") -> None:
        self.bucket = Path(bucket)
        self.mode = mode

    def _path(self, key: str) -> Path:
        path = Path(key)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("Document keys must be relative to the object store.")
        return self.bucket / path

    def read(self, key: str) -> dict[str, Any] | None:
        """Read a JSON document without creating files or directories."""
        with rlock:
            try:
                result = json.loads(self._path(key).read_text())
            except FileNotFoundError:
                return None
        if not isinstance(result, dict):
            raise ValueError(f"Object store document {key!r} must contain a dictionary.")
        return result

    def write(self, key: str, value: dict[str, Any]) -> None:
        """Write a JSON document using the existing local filename."""
        if self.mode != "rw":
            raise PermissionError("Cannot write documents in a read-only object store.")
        encoded = json.dumps(value)
        path = self._path(key)
        with rlock:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(encoded)
