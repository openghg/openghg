import contextlib
import json
from typing import Dict, Iterator, Optional

from openghg.objectstore import exists, get_local_bucket, get_object, set_object_from_json
from tinydb import Storage, TinyDB
from tinydb.middlewares import CachingMiddleware


@contextlib.contextmanager
def metastore_manager(key: str) -> Iterator:
    """A context manager to be used to open the metadata database
    at the given key.

    Args:
        key: Key for database in the object store
    """
    db = TinyDB(key, storage=CachingMiddleware(ObjectStorage))

    yield db

    db.close()


def load_metastore(key: str) -> TinyDB:
    """Load the metastore.

    Note: the database must be closed with db.close()
    to ensure correct writing of new values.

    Args:
        key: Key to metadata store
    Returns:
        TinyDB: instance of metadata database
    """
    return TinyDB(key, storage=CachingMiddleware(ObjectStorage))


class ObjectStorage(Storage):
    def __init__(self, key: str) -> None:
        self._key = key

    def read(self) -> Optional[Dict]:
        bucket = get_local_bucket()
        key = self._key

        if not exists(bucket=bucket, key=key):
            return None

        data = get_object(bucket=bucket, key=self._key)

        try:
            data = json.loads(data)
            return data
        except json.JSONDecodeError:
            return None

    def write(self, data: Dict) -> None:
        bucket = get_local_bucket()
        key = self._key

        set_object_from_json(bucket=bucket, key=key, data=data)

    def close(self) -> None:
        pass
