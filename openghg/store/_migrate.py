from openghg.objectstore import get_writable_bucket
from openghg.store.spec import define_data_type_classes
from openghg.store.base import Datasource
import tinydb


def migrate_metadata(store_name: str) -> None:
    """Update metadata to contain the object store each Datasource is
    saved in and synchronises the metadata of each datasource so

    Args:
        store_name: Object store name
    Returns:
        None
    """
    storage_classes = define_data_type_classes()
    bucket = get_writable_bucket(name=store_name)

    # Load in the metadata for each Datasource and add in the object store
    # Load in each Datasource's record from the metastore and add in the object store it comes from

    for storage_class in storage_classes.values():
        with storage_class(bucket=bucket) as sc:
            for uuid in sc._datasource_uuids:
                # Update the Datasource's metadata
                ds = Datasource.load(bucket=bucket, uuid=uuid, shallow=True)
                ds._metadata["object_store"] = bucket
                ds.save(bucket=bucket)
                # Sync the metastore
                sc._metastore.update(ds._metadata, tinydb.where("uuid") == uuid)
