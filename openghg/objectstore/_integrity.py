from openghg.objectstore import get_readable_buckets
from openghg.store.spec import define_data_types
from openghg.types import ObjectStoreError


def integrity_check() -> None:  # TODO: update since metastore source UUID truth
    """Check the integrity of object stores.

    Returns:
        None
    """
    # NOTE: these imports would be circular if they were at the top of the file
    # this maybe suggests that _integrity.py belongs outside of objectstore
    from openghg.store.base import Datasource
    from openghg.objectstore.metastore import open_metastore

    # For now loop over each of the object stores, can we somehow lock the object store?
    readable_buckets = get_readable_buckets()
    data_types = define_data_types()

    for bucket in readable_buckets.values():
        for data_type in data_types:
            # Now load the object
            with open_metastore(bucket=bucket, data_type=data_type) as metastore:
                # Get all the Datasources
                datasource_uuids = metastore.select("uuid")
                # Check they all exist
                for uid in datasource_uuids:
                    Datasource.load(bucket=bucket, uuid=uid, shallow=True).integrity_check()

                metastore_uuids = metastore.select("uuid")

                if datasource_uuids != metastore_uuids:
                    raise ObjectStoreError(
                        f"Mismatch between metastore Datasource UUIDs and {metastore} Datasource UUIDs."
                    )
