import contextlib
import json
from typing import Dict, Iterator, Optional, Sequence

from openghg.objectstore import exists, get_bucket, get_object, set_object_from_json
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
        bucket = get_bucket()
        key = self._key

        if not exists(bucket=bucket, key=key):
            return None

        data = get_object(bucket=bucket, key=self._key)

        try:
            json_data: Dict = json.loads(data)
            return json_data
        except json.JSONDecodeError:
            return None

    def write(self, data: Dict) -> None:
        bucket = get_bucket()
        key = self._key

        set_object_from_json(bucket=bucket, key=key, data=data)

    def close(self) -> None:
        pass


def datasource_lookup(
    metastore: TinyDB, data: Dict, required_keys: Sequence[str], min_keys: Optional[int] = None
) -> Dict:
    """Search the metadata store for a Datasource UUID using the metadata in data. We expect the required_keys
    to be present and will require at leas min_keys of these to be present when searching.

    As some metadata value might change (such as data owners etc) we don't want to do an exact
    search on *all* the metadata so we extract a subset (the required keys) and search for these.

    Args:
        metastore: Metadata database
        data: Combined data dictionary of form {key: {data: Dataset, metadata: Dict}}
        required_keys: Iterable of keys to extract from metadata
        min_keys: The minimum number of required keys, if not given it will be set
        to the length of required_keys
    Return:
        dict: Dictionary of datasource information
    """
    from openghg.retrieve import metadata_lookup

    if min_keys is None:
        min_keys = len(required_keys)

    results = {}
    for key, _data in data.items():
        metadata = _data["metadata"]
        required_metadata = {k: v for k, v in metadata.items() if k in required_keys}

        if len(required_metadata) < min_keys:
            raise ValueError(
                f"The given metadata doesn't contain enough information, we need: {required_keys}"
            )

        results[key] = metadata_lookup(metadata=required_metadata, database=metastore)

    return results
