from openghg.store.spec import define_data_type_classes
from openghg.objectstore import get_readable_buckets
from openghg.types import ObjectStoreError
from openghg.store._connection import get_object_store_connection

def integrity_check() -> None:
    """Check the integrity of object stores.

    Returns:
        None
    """
    from openghg.store.base import Datasource

    # For now loop over each of the object stores, can we somehow lock the object store?
    readable_buckets = get_readable_buckets()
    data_types = list(define_data_type_classes().keys())

    for bucket in readable_buckets.values():
        for data_type in data_types:
            # Now load the object
            with get_object_store_connection(data_type=data_type, bucket=bucket) as sc:
                # Get all the Datasources
                datasource_uuids = sc._datasources()
                # Check they all exist
                for uid in datasource_uuids:
                    Datasource.load(bucket=bucket, uuid=uid, shallow=True).integrity_check()

                metastore_uuids = [r["uuid"] for r in sc._metastore]

                if datasource_uuids != metastore_uuids:
                    raise ObjectStoreError(
                        f"{data_type} - Mismatch between metastore Datasource UUIDs and {data_type} Datasource UUIDs."
                    )
