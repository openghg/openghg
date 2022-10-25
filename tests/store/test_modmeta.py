from openghg.retrieve import search
from openghg.store.base import Datasource
from openghg.store import load_metastore
from openghg.store.spec import define_data_type_classes
from tinydb import Query, where
from typing import Dict, Optional


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

    uuids = res.uuids()
    uid = uuids[0]
    data_type = metadata[uid]["data_type"]


class MetadataHandler:
    def __init__(self, metadata: Optional[Dict] = None):
        self.metadata = metadata if metadata is not None else {}

    def update_metadata(data_type: str, **kwargs: Dict):
        res = search(data_type=data_type, **kwargs)

        metadata = res.metadata

        uuids = res.uuids()
        uid = uuids[0]
        data_type = metadata[uid]["data_type"]

        data_objs = define_data_type_classes()
        metakey = data_objs[data_type]._metakey

        with load_metastore(key=metakey) as store:
            #         res = store.search(where("uuid") == uid)
            response = store.update(updated_metadata, where("uuid") == uid)
            if not response:
                raise ValueError("Unable to update metadata, possibly metadata sync error.")

        d = Datasource.load(uuid=uid, shallow=True)
        d._metadata = updated_metadata
        d.save()
