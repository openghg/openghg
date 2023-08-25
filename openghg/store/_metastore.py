from __future__ import annotations
from openghg.store.base import BaseStore, get_data_class


def open_metastore(bucket: str, data_type: str) -> BaseStore:
    """Metastore factory function.

    args:
        bucket: object store bucket containing metastore
        data_type: data type of metastore to open

    returns:
        Child of BaseStore corresponding to given data type, connected
    to given bucket.
    """
    data_class = get_data_class(data_type)
    return data_class(bucket=bucket)
