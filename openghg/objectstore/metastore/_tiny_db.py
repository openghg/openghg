"""TinyDB customisation code."""

from collections.abc import Callable
import contextlib
import json
from pathlib import Path
from typing import Any, cast, Literal

import tinydb
from tinydb.middlewares import Middleware

from openghg.objectstore import exists, get_object, set_object_from_json
from openghg.types import MetastoreError
from openghg.util import hash_string


class BucketKeyStorage(tinydb.Storage):
    """Custom TinyDB storage class.

    Uses methods in `_local_store` module to read/write files via bucket and key.
    """

    def __init__(self, bucket: str, key: str, mode: Literal["r", "rw"]) -> None:
        """Create BucketKeyStorage object.

        Args:
            bucket: path to object store bucket (as string)
            key: metastore key
            mode: "r" for read-only, "rw" for read/write

        Returns:
            None
        """
        valid_modes = ("r", "rw")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, please choose one of {valid_modes}.")

        self._key = key
        self._bucket = bucket
        self._mode = mode

    def read(self) -> dict | None:
        """Read data from database.

        Returns:
            Dictionary version of JSON database, or None if database has
            not been initialised. (Returning None is required by the TinyDB
            interface.)
        """
        key = self._key

        if not exists(bucket=self._bucket, key=key):
            return None

        data = get_object(bucket=self._bucket, key=self._key)

        try:
            json_data: dict = json.loads(data)
            return json_data
        except json.JSONDecodeError:
            return None

    def write(self, data: dict) -> None:
        """Write data to metastore.

        Args:
            data: dictonary of data to add

        Returns:
            None

        Raises:
            MetastoreError if metastore opened in read-only mode.
        """
        if self._mode == "r":
            raise MetastoreError("Cannot write to metastore in read-only mode.")

        key = self._key
        set_object_from_json(bucket=self._bucket, key=key, data=data)

    def close(self) -> None:
        pass


class SafetyCachingMiddleware(Middleware):
    """Middleware that caches changes to the database, and writes
    these changes when the database is closed. Changes are only written
    if the underlying file has not changed. (The underlying file is the
    persistent record of the database that is read in by the storage class.)

    This differs from CachingMiddleware in two ways:
    1) changes are never written before closing the database, whereas
    CachingMiddleware will write after 1000 changes by default.

    2) CachingMiddleware does not check if the underlying file
    has changed since it was first accessed by the storage class.
    """

    def __init__(self, storage_cls: type[tinydb.Storage]) -> None:
        """Follows the standard pattern for middleware.

        Args:
            storage_cls: tinydb storage class to wrap with Middleware
        Returns:
            None
        """
        super().__init__(storage_cls)
        self.cache = None  # in-memory version of database
        self.database_hash = None  # hash taken when database first read
        self.writes_made = False  # flag to check if writes made

    def read(self):
        """Read the database from the cache, if present, otherwise load
        the database from the underlying storage and save a hash of the result.
        """
        if self.cache is None:
            self.cache = self.storage.read()
            self.database_hash = hash_string(str(self.cache))

        return self.cache

    def write(self, data):
        """Store data in the cache.

        Args:
            data: data to store (this is used internally by TinyDB)
        """
        self.cache = data
        self.writes_made = True

    def close(self):
        """Close the database. If writes have been made, and the underlying
        file has not changed, writes will be saved to disk at this point.

        Raises: MetaStoreError if writes have been made and the underlying file *has* been changed.
        """
        if self.writes_made:
            # we know that the cache is a dictionary of dictionaries
            self.cache = cast(dict[str, dict[str, Any]], self.cache)

            # check if stored hash matches current hash
            if self.database_hash == hash_string(str(self.storage.read())):
                # if underlying file not changed, write data
                self.storage.write(self.cache)
            else:
                raise MetastoreError(
                    "Could not write to object store: object store modified while write in progress."
                )

        # if close is called explicitly, rather than through a context manager,
        # then the cache should be empty, otherwise if the metastore instance is reused
        # it won't reflect the actual state of the metastore. (This is an edge case.)
        self.cache = None

        # let underlying storage clean up
        self.storage.close()


def try_to_open_json(info: str | bytes | Path) -> dict:
    """Open str/bytes or Path as json.

    Bytes are opened with `json.loads` and Paths are opened with `json.load`.
    Strings are first checked to see if they are valid paths; if not, they are
    loaded with `json.loads`.

    Args:
        info: str or bytes to load with `json.loads`, or Path (or
        string version of Path) to load with `json.load`.

    Returns:
        dict containing JSON info; empty if `json.load` and `json.loads` fail.

    """
    # suppress JSON decoding errors while we work through possible ways of opening
    with contextlib.suppress(json.JSONDecodeError):
        # try to treat the string as a path
        if isinstance(info, str | Path):
            path = Path(info)
            if path.exists():
                with path.open() as f:
                    return json.load(f)

        # if there was an error, try treating the string as a JSON string
        if isinstance(info, str | bytes):
            return json.loads(info)

    # return empty dict if opening didn't work
    return {}


class MultiJSONStorage(tinydb.Storage):
    """Read multiple JSON documents simultaneously.

    This storage is read-only.
    """

    def __init__(
        self, updater: Callable[[str, int, dict], None] | None = None, **json_info: str | bytes | Path
    ) -> None:
        super().__init__()
        self.json_info = json_info

        def default_updater(name: str, doc_id: int, doc: dict) -> None:
            doc["__name"] = name
            doc["__original_doc_id"] = doc_id

        self.updater = updater if updater is not None else default_updater

    def read(self) -> dict[str, dict[str, Any]] | None:
        db_info = {}
        for name, source in self.json_info.items():
            # open default table
            db_info[name] = try_to_open_json(source).get("_default", {})

        # combine all dicts, storing name and original doc id
        doc_id_count = 1  # assign new doc_ids for TinyDB internal use
        result = {}
        for name, docs in db_info.items():
            for doc_id, doc in docs.items():
                result[str(doc_id_count)] = doc
                self.updater(name, doc_id, result[str(doc_id_count)])
                doc_id_count += 1

        # return default table consisting of combined databases
        return {"_default": result}

    def write(self, data: dict[str, dict[str, Any]]) -> None:
        raise NotImplementedError("MultiJSONStorage is read-only.")
