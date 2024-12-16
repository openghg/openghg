"""This module defines an interface for metastores.

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
from functools import reduce
from typing import Any
from collections.abc import Callable

import tinydb
from tinydb.operations import delete as tinydb_delete

from openghg.types import MetastoreError

MetaData = dict[str, Any]
QueryResults = list[dict[str, Any]]  # ...to avoid clashes with `SearchResults` object
Bucket = str


class MetaStore(ABC):
    """Interface for MetaStore.

    All classes implementing this interface must provide the following methods:
        search: to search for records in the metastore matching given key-value pairs
        add: to add metadata to a new record in the metastore
        delete: to delete records matching the given key-value pairs
        update: update the metadata of a record matching the `where` key-value pairs;
            keys may be added or updated (using `to_update`) or deleted (using `to_delete`)
    """

    @abstractmethod
    def search(self, search_terms: MetaData | None = None) -> QueryResults:
        """Search for data using a dictionary of search terms.

        Args:
            search_terms: dictionary of key-value pairs to search by.

        Returns:
            list of records in the metastore matching the given search terms.
            If search_terms is None, return all records.
        """
        pass

    @abstractmethod
    def insert(self, metadata: MetaData) -> None:
        """Insert new metadata into the metastore."""
        pass

    @abstractmethod
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

        Raises:
            MetastoreError if delete_one=True and multiple records will
                be deleted.
        """
        if delete_one and not self._uniquely_identifies(metadata):
            raise MetastoreError(
                "Multiple records found matching metadata. "
                "Pass `delete_one=False` to delete multiple records."
            )

    @abstractmethod
    def update(
        self,
        where: MetaData,
        to_update: MetaData | None = None,
        to_delete: str | list[str] | None = None,
    ) -> None:
        """Update a single record with given metadata.

        Args:
            where: metadata identifying the record to update. This must uniquely
                identify the record.
            to_update: metadata to overwrite or add to the record.
            to_delete: key or list of keys to delete from record.

        Returns:
            None

        Raises:
            MetastoreError if more than one record matches the metadata in `where`.
        """
        if not self._uniquely_identifies(where):
            raise MetastoreError(
                "Multiple records found matching metadata. `where` must identify a single record."
            )

    def select(self, key: str) -> list[Any]:
        """Select the values stored in all records for given key.

        Args:
            key: key to select.

        Returns:
            list: list of values stored at that key, over all records in the metastore.
        """
        return [result[key] for result in self.search()]

    def _uniquely_identifies(self, metadata: MetaData) -> bool:
        """Return true if the given metadata identifies a single record in the metastore.

        Args:
            metadata: metadata to test to see if it identifies a single record.

        Returns:
            True if a search for the given metadata returns a single result; False otherwise.
        """
        result = self.search(search_terms=metadata)
        return len(result) == 1


class TinyDBMetaStore(MetaStore):
    """MetaStore using a TinyDB database backend."""

    def __init__(self, database: tinydb.TinyDB) -> None:
        """Create TinyDBMetaStore object.

        Args:
            database: a TinyDB database.

        Returns:
            None
        """
        self._db = database

    @staticmethod
    def _format_key(key: str) -> str:
        """Format metadata keys by making them lowercase."""
        return key.lower()

    def _format_metadata(self, metadata: MetaData) -> MetaData:
        """Convert all keys to lowercase.

        Args:
            metadata: metadata to format.

        Returns:
            formatted metadata.
        """
        return {self._format_key(k): v for k, v in metadata.items()}

    def _get_query(self, metadata: MetaData) -> tinydb.queries.QueryInstance:
        """Return a TinyDB query for all records that contain the given metadata.

        Args:
            metadata: key-value pairs to search by

        Returns:
            TinyDB QueryInstance that can be used to search via the given metadata.
        """
        return tinydb.Query().fragment(self._format_metadata(metadata))

    def _get_function_query(self, search_functions: dict[str, Callable]) -> tinydb.queries.QueryInstance:
        """Return a TinyDB query for all records whose values that pass the "test function" for that key.

        For instance, if search_functions = {"inlet": (lambda x: float(x[:-1]) > 100.0)}, then the query returned
        will search for data where the inlet height is above 100 meters. Notice that the function includes
        formatting for the values stored in the metastore.

        Args:
            search_functions: key-value pairs to search by; the values are functions, which
                are applied to the metadata records in the metastore: `v(metadata_record[k])`,
                and the query tests if the result is `True`.

        Returns:
            TinyDB QueryInstance that requires all of the functions to evaluate to true
        """
        search_functions = self._format_metadata(search_functions)
        queries = [tinydb.Query()[k].test(v) for k, v in search_functions.items()]
        return reduce(lambda x, y: (x & y), queries)

    def _get_negative_lookup_query(self, negative_lookup_keys: list[str]) -> tinydb.queries.QueryInstance:
        """Return a TinyDB query that searches for all records that do not contain the given keys.

        Args:
            negative_lookup_keys: list of keys that will will be tested to see if they are *not* present
                in a metadata record.

        Returns:
            TinyDB QueryInstance that requires all of the given keys to *not* be present
        """
        negative_lookup_keys = [self._format_key(k) for k in negative_lookup_keys]
        queries = [~(tinydb.Query()[k].exists()) for k in negative_lookup_keys]
        return reduce(lambda x, y: (x & y), queries)

    def search(
        self,
        search_terms: MetaData | None = None,
        search_functions: dict[str, Callable] | None = None,
        negative_lookup_keys: list[str] | None = None,
    ) -> QueryResults:
        """Search metastore using a dictionary of search terms.

        Args:
            search_terms: dictionary of key-value pairs to search by.
                For instance search_terms = {'site': 'TAC'} will find all results
                whose site is 'TAC'.
            search_functions: dictionary of key-function pairs to search by.
                See `_get_function_query` docstring for examples.
            negative_lookup_keys: list of keys that should *not* be present in the
                results.

        Returns:
            list: list of records in the metastore matching the given search terms.
        """
        if not search_terms:
            search_terms = {}
        _query = self._get_query(search_terms)

        if search_functions:
            _function_query = self._get_function_query(search_functions)
            _query &= _function_query

        if negative_lookup_keys:
            _neg_query = self._get_negative_lookup_query(negative_lookup_keys)
            _query &= _neg_query

        return list(self._db.search(_query))

    def insert(self, metadata: MetaData) -> None:
        """Add new metadata to the metastore.

        Args:
            metadata: metdata to add to the metastore.

        Returns:
            None
        """
        self._db.insert(self._format_metadata(metadata))

    def update(
        self,
        where: MetaData,
        to_update: MetaData | None = None,
        to_delete: str | list[str] | None = None,
    ) -> None:
        """Update a single record with given metadata.

        Args:
            where: metadata identifying the record to update. This must uniquely
                identify the record.
            to_update: metadata to overwrite or add to the record.
            to_delete: key or list of keys to delete from record.

        Returns:
            None

        Raises:
            MetastoreError if more than one record matches the metadata in `where`.
        """
        super().update(where, to_update, to_delete)  # Error handling
        _query = self._get_query(where)
        if to_update:
            self._db.update(to_update, _query)
        if to_delete:
            if not isinstance(to_delete, list):
                to_delete = [to_delete]
            for key in to_delete:
                self._db.update(tinydb_delete(key), _query)

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

        Raises:
            MetastoreError if delete_one=True and multiple records will
                be deleted.
        """
        super().delete(metadata, delete_one)  # Error handling
        _query = self._get_query(metadata)
        self._db.remove(_query)
