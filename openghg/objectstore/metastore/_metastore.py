"""
This module defines an interface for metastores.

Metastores store metadata that is used to search an
object store for data.

The documentation in this module refers to metadata entries stored
in the metastore as "records". These correspond to "documents" in
TinyDB and Mongodb.

A MetaStore managed by an ObjectStore object will store
UUIDs, which are used to locate data stored in the object store.

UUIDs are managed by ObjectStore objects, so they are not part of
the MetaStore interface.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

import tinydb

from openghg.types import MetastoreError

MetaData = Dict[str, Any]
QueryResults = List[Any]  # ...to avoid clashes with `SearchResults` object
Bucket = str


class MetaStore(ABC):

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

    @abstractmethod
    def update(self, record_to_update: MetaData, metadata_to_add: MetaData) -> None:
        """Update a single record with given metadata.

        Args:
            record_to_update: metadata identifying the record to update. This must uniquely
        identify the record.
            metadata_to_add: metadata to overwrite or add to the record.

        Returns:
            None

        Raises:
            MetastoreError if more than one record matches the metadata in `record_to_update`.
        """
        pass


class TinyDBMetaStore(MetaStore):
    def __init__(self, session: tinydb.TinyDB) -> None:
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

    def _uniquely_identifies(self, metadata: MetaData) -> bool:
        """Return true if the given metadata identifies a single record
        in the metastore.

        Args:
            metadata: metadata to test to see if it identifies a single record.

        Returns:
            True if a search for the given metadata returns a single result; False otherwise.
        """
        result = self.search(search_terms=metadata)
        return len(result) == 1

    def add(self, metadata: MetaData) -> None:
        """Add new metadata to the metastore.

        Args:
            metadata: metdata to add to the metastore.

        Returns:
            None
        """
        self._metastore.insert(self._format_metadata(metadata))

    def update(self, record_to_update: MetaData, metadata_to_add: MetaData) -> None:
        """Update a single record with given metadata.

        Args:
            record_to_update: metadata identifying the record to update. This must uniquely
        identify the record.
            metadata_to_add: metadata to overwrite or add to the record.

        Returns:
            None

        Raises:
            MetastoreError if more than one record matches the metadata in `record_to_update`.
        """
        if not self._uniquely_identifies(self._format_metadata(record_to_update)):
            raise MetastoreError(
                "Multiple records found matching metadata. `record_to_update` must identify a single record."
            )
        query = tinydb.Query().fragment(self._format_metadata(record_to_update))
        self._metastore.update(metadata_to_add, query)

    def delete(self, metadata: MetaData, delete_one: bool = True) -> None:
        """Delete metadata from the metastore.

        By default, an error will be thrown if more than one record will
        be deleted.

        If `delete_one` is False, then this will delete *all* records matching
        the given metadata. To see what will be deleted, search the metastore using
        the same metadata.

        Args:
            metadata: metadata to search for records to delete.
            delete_one: if True, throw error if more than one record will
        be deleted.

        Returns:
            None
        """
        if delete_one:
            if not self._uniquely_identifies(metadata):
                raise MetastoreError(
                    "Multiple records found matching metadata. Pass `delete_one=False` to delete multiple records."
                )

        query = tinydb.Query().fragment(self._format_metadata(metadata))
        self._metastore.remove(query)
