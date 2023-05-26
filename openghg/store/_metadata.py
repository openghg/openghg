import json
from typing import DefaultDict, Dict, List, Optional, Sequence, Union, cast
from xarray import Dataset
import logging
from openghg.objectstore import exists, get_bucket, get_object, set_object_from_json
from openghg.store.base import Datasource

# from openghg.dataobjects import DataHandler
# DHType = TypeVar('U', bound=DataHandler)

from tinydb import Storage, TinyDB
from tinydb.middlewares import CachingMiddleware

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

DataDictType = DefaultDict[str, Dict[str, Union[Dict, Dataset]]]


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


def data_handler_lookup(data_type: str, **kwargs: Dict):  # type: ignore
    """Lookup the data / metadata you'd like to modify.

    Args:
        data_type: Type of data, for example surface, flux, footprint
        kwargs: Any pair of keyword arguments for searching
    Returns:
        DataHandler: A handler object to help modify the metadata
    """
    from openghg.retrieve import search
    from openghg.dataobjects import DataHandler

    res = search(data_type=data_type, **kwargs)
    metadata = res.metadata
    return DataHandler(metadata=metadata)


def _update_meta_from_datasource(metadata: Dict,
                                 datasource: Datasource,
                                 update_keys: Optional[List] = None) -> Dict:
    """Update the metadata entries based on internal metadata from the Datasource.

    Args:
        metadata : Dictionary of metadata
        datasource: Datasource object
        update_keys: List of keys to update within metadata using Datasource object.
    Returns:
        dict: Updated metadata dictionary
    """

    meta_copy = metadata.copy()
    d_meta = datasource._metadata

    if update_keys is not None:
        for key in update_keys:
            if key in d_meta:
                try:
                    meta_value = metadata[key]
                except KeyError:
                    meta_copy[key] = d_meta[key]
                else:
                    d_value = d_meta[key]
                    if d_value != meta_value:
                        meta_copy[key] = d_meta[key]
            else:
                logger.warning(f"Unable to update '{key}' key in metastore."
                               " Not present on Datasource.")

    return meta_copy


def update_metadata(data_dict: DataDictType,
                    uuid_dict: Dict,
                    update_keys: Optional[List] = None) -> DataDictType:
    """Update metadata (to be saved to metastore) for each source using
    details from the Datasource. See openghg.store.base.Datasource object
    for details of metadata stored on this object.

    For example this could include:
     - ["start_date", "end_date", "latest_version"]

    Args:
        data_dict : Dictionary containing data and metadata for species
        uuid_dict : Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
        update_keys : List of keys to update within metadata using Datasource object.
    Returns:
        dict: data_dict with metadata updated where appropriate.
    """

    for key in data_dict:

        uuid = uuid_dict[key]["uuid"]
        datasource = Datasource.load(uuid=uuid, shallow=True)

        if isinstance(data_dict[key]["metadata"], Dict):
            metadata = cast(Dict, data_dict[key]["metadata"])
            metadata = _update_meta_from_datasource(metadata,
                                                    datasource,
                                                    update_keys=update_keys)
            data_dict[key]["metadata"] = metadata
        else:
            logger.warning(f"Unable to update keys: {update_keys} within metadata")

    return data_dict
