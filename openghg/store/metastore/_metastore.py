"""
This module defines an interface for metastores.

Metastores store metadata and associated uuid's.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import tinydb


class MetaStore(ABC):
    def __init__(self, bucket: str) -> None:
        self._bucket = bucket

    @abstractmethod
    def search(self, search_terms: dict[str, Any]) -> list[Any]:
        """Search for data using a dictionary of search terms.

        TODO: need to specify output format.
        """
        pass

    @abstractmethod
    def add(self, metadata: dict[str, Any]) -> None:
        """Add new metadata to the object store."""
        pass


class TinyDBMetaStore(MetaStore):
    def __init__(self, bucket: str, session: tinydb.TinyDB) -> None:
        super().__init__(bucket=bucket)
        self._metastore = session

    def search(self, search_terms: dict[str, Any] = dict()) -> list[Any]:
        search_terms = {k.lower(): v for k, v in search_terms.items()}
        query = tinydb.Query().fragment(search_terms)
        return list(self._metastore.search(query))  # TODO: find better way to deal with mypy than casting...

    def add(self, metadata: dict[str, Any]) -> None:
        metadata = {k.lower(): v for k, v in metadata.items()}
        self._metastore.insert(metadata)
