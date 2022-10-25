import json
from typing import Dict, List, Optional, Sequence, Union

from openghg.objectstore import exists, get_bucket, get_object, set_object_from_json
from openghg.retrieve import search
from openghg.store.base import Datasource
from openghg.store.spec import define_data_type_classes
from tinydb import Storage, TinyDB, where
from tinydb.middlewares import CachingMiddleware


def load_metastore(key: str) -> TinyDB:
    """Load the metastore. This can be used as a context manager
    otherwise the database must be closed using the close method
    otherwise records are not written to file.

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
        required_metadata = {k.lower(): str(v).lower() for k, v in metadata.items() if k in required_keys}

        if len(required_metadata) < min_keys:
            raise ValueError(
                f"The given metadata doesn't contain enough information, we need: {required_keys}"
            )

        results[key] = metadata_lookup(metadata=required_metadata, database=metastore)

    return results


#  These handle the modification of metadata
def find_metadata(data_type: str, **kwargs):
    """Lookup the metadata you'd like to modify

    Args:
        data_type: Type of data, for example surface, flux, footprint
        kwargs: Any pair of keyword arguments for searching
    Returns:
        MetadataHandler: A handler object to help modify the metadata
    """
    res = search(data_type=data_type, **kwargs)
    metadata = res.metadata
    return MetadataHandler(metadata=metadata)


class MetadataHandler:
    def __init__(self, metadata: Optional[Dict[str, Dict]] = None):
        self._metadata = metadata if metadata is not None else {}

    def update_metadata(self, uid: Union[List, str], updated_metadata: Dict):
        if uid not in self._metadata:
            raise ValueError("Invalid UUID, please check metadata.")

        if not isinstance(uid, list):
            uid = [uid]

        # We should only have one data type
        data_types = {self._metadata[i]["data_type"] for i in uid}
        if len(data_types) > 1:
            raise ValueError(
                f"We can only modify Datasources of a single data type at once. We currently have {data_types}"
            )

        dtype = data_types.pop()

        data_objs = define_data_type_classes()
        metakey = data_objs[dtype]._metakey

        with load_metastore(key=metakey) as store:
            for u in uid:
                #         res = store.search(where("uuid") == uid)
                response = store.update(updated_metadata, where("uuid") == u)
                if not response:
                    raise ValueError("Unable to update metadata, possibly metadata sync error.")

                d = Datasource.load(uuid=u, shallow=True)
                d._metadata = updated_metadata
                d.save()

        print(f"Modified metadata for {uid}.")
