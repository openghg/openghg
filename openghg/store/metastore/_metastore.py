"""
This module defines an interface for metastores.

Metastores store metadata that is used to search an
object store for data.

A MetaStore managed by an ObjectStore object will store
UUIDs, which are used to locate data stored in the object store.

UUIDs are managed by ObjectStore objects, so they are not part of
the MetaStore interface.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import tinydb

from openghg.types import MetastoreError

MetaData = dict[str, Any]
QueryResults = list[Any]  # ...to avoid clashes with `SearchResults` object


class MetaStore(ABC):
    def __init__(self, bucket: str) -> None:
        self._bucket = bucket

    @abstractmethod
    def search(self, search_terms: MetaData) -> QueryResults:
        """Search for data using a dictionary of search terms.

        Args:
            search_terms: dictionary of key-value pairs to search by.

        Returns:
            list of records in the metastore matching the given search terms.
        """
        pass

    @abstractmethod
    def add(self, metadata: MetaData) -> None:
        """Add new metadata to the metastore."""
        pass


    @abstractmethod
    def delete(self, metadata: MetaData) -> None:
        """Delete metadata from the metastore.

        Note: this will delete *all* records matching the given metadata.
        To see what will be deleted, search the metastore using the same
        metadata.

        Args:
            metadata: metadata to search for records to delete.

        Returns:
            None
        """
        pass

    def update(self) -> None:
        """Method for updating existing records.

        TODO: figure out appropriate signature for this function.

        Can this be implemented in the ABC by combining
        `delete` and `add`? (Probably yes, if we don't care about preserving
        document IDs.)
        """
        raise NotImplementedError


class TinyDBMetaStore(MetaStore):
    def __init__(self, bucket: str, session: tinydb.TinyDB) -> None:
        super().__init__(bucket=bucket)
        self._metastore = session

    def _format_metadata(self, metadata: MetaData) -> MetaData:
        """Convert all keys to lowercase.

        Args:
            metadata: metadata to format.

        Returns:
            formatted metadata.
        """
        return {k.lower(): v for k, v in metadata.items()}

    def search(self, search_terms: MetaData = dict()) -> QueryResults:
        """Search metastore using a dictionary of search terms.

        Args:
            search_terms: dictionary of key-value pairs to search by.
        For instance search_terms = {'site': 'TAC'} will find all results
        whose site is 'TAC'.

        Returns:
            list of records in the metastore matching the given search terms.
        """
        query = tinydb.Query().fragment(self._format_metadata(search_terms))
        return list(self._metastore.search(query))  # TODO: find better way to deal with mypy than casting...

    def add(self, metadata: MetaData) -> None:
        """Add new metadata to the metastore.

        Args:
            metadata: metdata to add to the metastore.

        Returns:
            None
        """
        self._metastore.insert(self._format_metadata(metadata))

    def delete(self, metadata: MetaData, delete_one: bool = True) -> None:
        """Delete metadata from the metastore.

        Note: this will delete *all* records matching the given metadata.
        To see what will be deleted, search the metastore using the same
        metadata.

        By default, an error will be thrown if more than one record will
        be deleted.

        Args:
            metadata: metadata to search for records to delete.
            delete_one: if True, throw error if more than one record will
        be deleted.

        Returns:
            None
        """
        if delete_one:
            results = self.search(metadata)
            if len(results) > 1:
                raise MetastoreError("Multiple records found matching metadata. Pass `delete_one=False` to delete multiple records.")

        query = tinydb.Query().fragment(self._format_metadata(metadata))
        self._metastore.remove(query)
