from __future__ import annotations

from contextlib import contextmanager
from typing import Literal

import tinydb

from openghg.store import load_metastore
from openghg.store.base import Datasource
from openghg.store.metastore._metastore import TinyDBMetaStore


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


@contextmanager
def open_metastore(bucket: str, data_type: str, mode: Literal['r', 'rw'] = 'rw'):
    """Context manager for TinyDBMetaStore based on OpenGHG v<=6.2 set-up for keys
    and TinyDB.

    NOTE: This is a convenience function to help bring new MetaStore code into the
    existing code base.

    Args:
        bucket: object store bucket containing metastore
        data_type: data type of metastore to open
        mode: specify read or read/write mode

    Yields:
        TinyDBMetaStore instance storing data as Datasource objects.
    """
    with load_metastore(bucket, get_metakey(data_type), mode=mode) as session:
        metastore = ClassicMetaStore(bucket=bucket, session=session, data_type=data_type)
        yield metastore


class ClassicMetaStore(TinyDBMetaStore[Datasource]):
    def __init__(self, bucket: str, session: tinydb.TinyDB, data_type: str) -> None:
        super().__init__(storage_object=Datasource, bucket=bucket, session=session)
        self.data_type = data_type

    @property
    def _datasource_uuids(self):
        return {result['uuid']: "" for result in self.search()}

    def datasources(self):
        return [result['uuid'] for result in self.search()]

    def remove_datasource(self, uuid: str) -> None:
        pass
    def key(self):
        return get_key(self.data_type)

    def delete(self, uuid: str) -> None:
        """Delete a Datasource with the given UUID.

        This deletes both the data and the record in
        the metastore.

        Args:
            uuid: UUID of the Datasource to delete.

        Returns:
            None
        """
        from openghg.objectstore import delete_object

        # Delete Datasource data
        Datasource.load(bucket=self._bucket, uuid=uuid).delete_all_data()

        # Delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=self._bucket, key=key)

        # Delete the UUID from the metastore
        self._metastore.remove(tinydb.where("uuid") == uuid)
