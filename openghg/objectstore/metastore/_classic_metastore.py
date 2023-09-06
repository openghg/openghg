from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
import json
from typing import Literal, Optional

import tinydb
from tinydb.middlewares import CachingMiddleware

from openghg.objectstore import exists, get_object, set_object_from_json
from openghg.objectstore.metastore import TinyDBMetaStore
from openghg.types import MetastoreError


object_store_data_classes = {
    "surface": {"_root": "ObsSurface", "_uuid": "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"},
    "column": {"_root": "ObsColumn", "_uuid": "5c567168-0287-11ed-9d0f-e77f5194a415"},
    "emissions": {"_root": "Emissions", "_uuid": "c5c88168-0498-40ac-9ad3-949e91a30872"},
    "footprints": {"_root": "Footprints", "_uuid": "62db5bdf-c88d-4e56-97f4-40336d37f18c"},
    "boundary_conditions": {"_root": "BoundaryConditions", "_uuid": "4e787366-be91-4fc5-ad1b-4adcb213d478"},
    "eulerian_model": {"_root": "EulerianModel", "_uuid": "63ff2365-3ba2-452a-a53d-110140805d06"},
}


def get_key(data_type: str) -> str:
    """Return the metakey for a given data type."""
    try:
        result = object_store_data_classes[data_type]
    except KeyError:
        return ""
    else:
        return f"{result['_root']}/uuid/{result['_uuid']}"


def get_metakey(data_type: str) -> str:
    """Return the metakey for a given data type."""
    try:
        result = object_store_data_classes[data_type]
    except KeyError:
        return ""
    else:
        return f"{result['_root']}/uuid/{result['_uuid']}/metastore"


class ObjectStorage(tinydb.Storage):
    def __init__(self, bucket: str, key: str, mode: Literal["r", "rw"]) -> None:
        valid_modes = ("r", "rw")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, please choose one of {valid_modes}.")

        self._key = key
        self._bucket = bucket
        self._mode = mode

    def read(self) -> Optional[dict]:
        """Read data from DB.

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
        if self._mode == "r":
            raise MetastoreError("Cannot write to metastore in read-only mode.")

        key = self._key
        set_object_from_json(bucket=self._bucket, key=key, data=data)

    def close(self) -> None:
        pass


@contextmanager
def open_metastore(
    bucket: str, data_type: str, mode: Literal["r", "rw"] = "rw"
) -> Generator[ClassicMetaStore, None, None]:
    """Context manager for TinyDBMetaStore based on OpenGHG v<=6.2 set-up for keys
    and TinyDB.

    NOTE: This is a convenience function to help bring new MetaStore code into the
    existing code base.

    Args:
        bucket: path to object store
        data_type: data type of metastore to open
        mode: 'rw' for read/write, 'r' for read only

    Yields:
        ClassicMetaStore instance.
    """
    key = get_metakey(data_type)
    with tinydb.TinyDB(bucket, key, mode, storage=CachingMiddleware(ObjectStorage)) as session:
        metastore = ClassicMetaStore(bucket=bucket, session=session, data_type=data_type)
        yield metastore


class ClassicMetaStore(TinyDBMetaStore):
    """Class that provides additional methods previously available
    from `load_metastore`.
    """

    def __init__(self, bucket: str, session: tinydb.TinyDB, data_type: str) -> None:
        super().__init__(bucket=bucket, session=session)
        self.data_type = data_type

    @property
    def _datasource_uuids(self) -> dict:
        return {result["uuid"]: "" for result in self.search()}

    def datasources(self) -> list:
        return [result["uuid"] for result in self.search()]

    def remove_datasource(self, uuid: str) -> None:
        """List of datasource UUIDs is drawn from metastore, so this
        function isn't needed. (But remains in the code base because
        'Make Metastore source of truth for datasource UUIDs' PR is still
        open, as of writing this doc string.)
        """
        pass

    def key(self) -> str:
        """This is the only place where `get_key` is used.

        This method is only used by some tests in test_obssurface.py.

        TODO: remove dependency on this function.
        """
        return get_key(self.data_type)
