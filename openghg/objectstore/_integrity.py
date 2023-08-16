from openghg.store.spec import define_data_type_classes
from openghg.objectstore import get_readable_buckets
from openghg.types import ObjectStoreError


def integrity_check() -> None:
    """Check the integrity of object stores.

    Returns:
        None
    """
    from openghg.store.base import Datasource

    # For now loop over each of the object stores, can we somehow lock the object store?
    readable_buckets = get_readable_buckets()
    datastore_classes = define_data_type_classes().values()

    for bucket in readable_buckets.values():
        for storage_class in datastore_classes:
            # Now load the object
            with storage_class(bucket=bucket) as sc:
                # Get all the Datasources
                datasource_uuids = sc.datasources()
                # Check they all exist
                for uid in datasource_uuids:
                    Datasource.load(bucket=bucket, uuid=uid, shallow=True).integrity_check()

                metastore_uuids = [r["uuid"] for r in sc._metastore]

                if datasource_uuids != metastore_uuids:
                    only_in_class = list(set(datasource_uuids) - set(metastore_uuids))
                    only_in_metastore = list(set(metastore_uuids) - set(datasource_uuids))
                    class_name = storage_class.__class__.__name__
                    raise ObjectStoreError(
                        f"{class_name} - mismatch between metastore Datasource UUIDs and class Datasource UUIDs."
                        + f"\nOnly in class: {only_in_class}"
                        + f"\nOnly in metastore: {only_in_metastore}\n"
                    )
