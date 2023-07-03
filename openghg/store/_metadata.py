import json
from typing import Dict, Optional
from openghg.objectstore import exists, get_object, set_object_from_json, get_writable_buckets


from openghg.types import ObjectStoreError
from tinydb import Storage, TinyDB
from tinydb.middlewares import CachingMiddleware


class ObjectStorage(Storage):
    def __init__(self, bucket: str, key: str) -> None:
        self._key = key
        self._bucket = bucket

    def read(self) -> Optional[Dict]:
        key = self._key

        if not exists(bucket=self._bucket, key=key):
            return None

        data = get_object(bucket=self._bucket, key=self._key)

        try:
            json_data: Dict = json.loads(data)
            return json_data
        except json.JSONDecodeError:
            return None

    def write(self, data: Dict) -> None:
        key = self._key

        set_object_from_json(bucket=self._bucket, key=key, data=data)

    def close(self) -> None:
        pass


def load_metastore(bucket: str, key: str) -> TinyDB:
    """Load the metastore. This can be used as a context manager
    otherwise the database must be closed using the close method
    otherwise records are not written to file.

    Args:
        key: Key to metadata store
    Returns:
        TinyDB: instance of metadata database
    """
    return TinyDB(bucket, key, storage=CachingMiddleware(ObjectStorage))


def data_manager(data_type: str, store: str, **kwargs: Dict):  # type: ignore
    """Lookup the data / metadata you'd like to modify.

    Args:
        data_type: Type of data, for example surface, flux, footprint
        store: Name of store
        kwargs: Any pair of keyword arguments for searching
    Returns:
        DataManager: A handler object to help modify the metadata
    """
    from openghg.dataobjects import DataManager
    from openghg.retrieve import search

    writable_stores = get_writable_buckets()

    if store not in writable_stores:
        raise ObjectStoreError(f"You do not have permission to write to the {store} store.")

    res = search(data_type=data_type, **kwargs)
    metadata = res.metadata
    return DataManager(metadata=metadata, store=store)
