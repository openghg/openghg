from typing import Any, TypeVar
from uuid import uuid4

import tinydb

from openghg.store import load_metastore
from openghg.store.metastore._metastore import MetaStore, BucketUUIDLoadable


T = TypeVar('T', bound=BucketUUIDLoadable)


object_store_data_classes = {
    "surface": {"_root": "ObsSurface", "_uuid": "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"},
    "column": {"_root": "ObsColumn", "_uuid": "5c567168-0287-11ed-9d0f-e77f5194a415"},
    "emissions": {"_root": "Emissions", "_uuid": "c5c88168-0498-40ac-9ad3-949e91a30872"},
    "footprints": {"_root": "Footprints", "_uuid": "62db5bdf-c88d-4e56-97f4-40336d37f18c"},
    "boundary_conditions": {"_root": "BoundaryConditions", "_uuid": "4e787366-be91-4fc5-ad1b-4adcb213d478"},
    "eulerian_model": {"_root": "EulerianModel", "_uuid": "63ff2365-3ba2-452a-a53d-110140805d06"},
}


def get_metakey(data_type: str) -> str:
    """Return the metakey for a given data type."""
    try:
        result = object_store_data_classes[data_type]
    except KeyError:
        return ""
    else:
        return f"{result['_root']}/uuid/{result['_uuid']}/metastore"


def get_new_uuid() -> str:
    """Return a new uuid as a string."""
    return str(uuid4())


class TinyDBMetaStore(MetaStore[T]):
    def __init__(self, storage_object: type[T], bucket: str, data_type: str) -> None:
        super().__init__(storage_object=storage_object, bucket=bucket)
        self.data_type = data_type
        self._metastore = load_metastore(self._bucket, get_metakey(self.data_type))

    def search(self, search_terms: dict[str, Any] = dict()) -> list[Any]:
        search_terms = {k.lower(): v for k, v in search_terms.items()}
        query = tinydb.Query().fragment(search_terms)
        return self._metastore.search(query)

    def add(self, metadata: dict[str, Any]) -> str:
        uuid = get_new_uuid()
        metadata = {k.lower(): v for k, v in metadata.items()}
        metadata['uuid'] = uuid
        self._metastore.insert(metadata)
        return uuid
