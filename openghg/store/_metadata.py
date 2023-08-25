from __future__ import annotations
import json
from openghg.objectstore import exists, get_object, set_object_from_json, get_writable_buckets
from typing import DefaultDict, Dict, Literal, Optional, Union, TYPE_CHECKING
from xarray import Dataset
import logging
from openghg.types import ObjectStoreError, MetastoreError
from tinydb import Storage, TinyDB
from tinydb.middlewares import CachingMiddleware

if TYPE_CHECKING:
    from openghg.dataobjects import DataManager


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

DataDictType = DefaultDict[str, Dict[str, Union[Dict, Dataset]]]


class ObjectStorage(Storage):
    def __init__(self, bucket: str, key: str, mode: Literal["r", "rw"]) -> None:
        valid_modes = ("r", "rw")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, please choose one of {valid_modes}.")

        self._key = key
        self._bucket = bucket
        self._mode = mode

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
        if self._mode == "r":
            raise MetastoreError("Cannot write to metastore in read-only mode.")

        key = self._key
        set_object_from_json(bucket=self._bucket, key=key, data=data)

    def close(self) -> None:
        pass


def load_metastore(bucket: str, key: str, mode: Literal["r", "rw"] = "rw") -> TinyDB:
    """Load the metastore. This can be used as a context manager
    otherwise the database must be closed using the close method
    otherwise records are not written to file.

    Args:
        bucket: Path to object store
        key: Key to metadata store
        mode: rw for read-write or r for read-only
    Returns:
        TinyDB: instance of metadata database
    """
    return TinyDB(bucket, key, mode, storage=CachingMiddleware(ObjectStorage))


def data_manager(data_type: str, store: str, **kwargs: Dict) -> DataManager:
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


# def _update_meta_from_datasource(metadata: Dict,
#                                  datasource: Datasource,
#                                  update_keys: Optional[List] = None) -> Dict:
#     """Update the metadata entries based on internal metadata from the Datasource.

#     Args:
#         metadata : Dictionary of metadata
#         datasource: Datasource object
#         update_keys: List of keys to update within metadata using Datasource object.
#     Returns:
#         dict: Updated metadata dictionary
#     """

#     meta_copy = metadata.copy()
#     d_meta = datasource._metadata

#     if update_keys is not None:
#         for key in update_keys:
#             if key in d_meta:
#                 try:
#                     meta_value = metadata[key]
#                 except KeyError:
#                     meta_copy[key] = d_meta[key]
#                 else:
#                     d_value = d_meta[key]
#                     if d_value != meta_value:
#                         meta_copy[key] = d_meta[key]
#             else:
#                 logger.warning(f"Unable to update '{key}' key in metastore."
#                                " Not present on Datasource.")

#     return meta_copy


# def update_metadata(data_dict: DataDictType,
#                     uuid_dict: Dict,
#                     update_keys: Optional[List] = None) -> DataDictType:
#     """Update metadata (to be saved to metastore) for each source using
#     details from the Datasource. See openghg.store.base.Datasource object
#     for details of metadata stored on this object.

#     For example this could include:
#      - ["start_date", "end_date", "latest_version"]

#     Args:
#         data_dict : Dictionary containing data and metadata for species
#         uuid_dict : Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
#         update_keys : List of keys to update within metadata using Datasource object.
#     Returns:
#         dict: data_dict with metadata updated where appropriate.
#     """
#     for key in data_dict:
#         uuid = uuid_dict[key]["uuid"]
#         datasource = Datasource.load(uuid=uuid, shallow=True)

#         if isinstance(data_dict[key]["metadata"], Dict):
#             metadata = cast(Dict, data_dict[key]["metadata"])
#             metadata = _update_meta_from_datasource(metadata,
#                                                     datasource,
#                                                     update_keys=update_keys)
#             data_dict[key]["metadata"] = metadata
#         else:
#             logger.warning(f"Unable to update keys: {update_keys} within metadata")

#     return data_dict
