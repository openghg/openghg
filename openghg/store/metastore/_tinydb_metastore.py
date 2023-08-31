from typing import Any, TypeVar
from uuid import uuid4

import tinydb

from openghg.store import load_metastore
from openghg.store.metastore._metastore import MetaStore, BucketUUIDLoadable


T = TypeVar('T', bound=BucketUUIDLoadable)


def get_metakey(data_type: str) -> str:
    """Return the metakey for a given data type."""
    return ""  # TODO: return metakeys given data types...


def get_new_uuid() -> str:
    """Return a new uuid as a string."""
    return str(uuid4())


class TinyDBMetaStore(MetaStore[T]):
    def __init__(self, bucket: str, data_type: str) -> None:
        super().__init__(bucket)
        self.data_type = data_type
        self._metastore = load_metastore(self.bucket, get_metakey(self.data_type))

    def search(self, search_terms: dict[str, Any]) -> list[Any]:
        search_terms = {k.lower(): v for k, v in search_terms.items()}
        query = tinydb.Query().fragment(search_terms)
        return self._metastore.search(query)

    def add(self, metadata: dict[str, Any]) -> str:
        uuid = get_new_uuid()
        metadata = {k.lower(): v for k, v in metadata.items()}
        metadata['uuid'] = uuid
        self._metastore.insert(metadata)
        return uuid
