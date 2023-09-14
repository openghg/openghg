import logging
from collections import defaultdict
from openghg.store.spec import define_data_type_classes
from openghg.objectstore import get_readable_buckets
from openghg.types import ObjectStoreError
from typing import List, Optional

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(level=logging.DEBUG)


def integrity_check(raise_error: bool = True) -> Optional[List]:
    """Check the integrity of object stores.

    Args:
        on_failure: On integrity check failure either raise or return
        If return a list of Datasources that failed the integrity check are returned.
    Returns:
        None
    """
    from openghg.store.base import Datasource

    # For now loop over each of the object stores, can we somehow lock the object store?
    readable_buckets = get_readable_buckets()
    datastore_classes = define_data_type_classes().values()

    failed_datasources = defaultdict(dict)
    for bucket in readable_buckets.values():
        for storage_class in datastore_classes:
            # Now load the object
            with storage_class(bucket=bucket) as sc:
                # Get all the Datasources
                datasource_uuids = sc.datasources()
                # Check they all exist
                failures = []
                for uid in datasource_uuids:
                    try:
                        Datasource.load(bucket=bucket, uuid=uid, shallow=True).integrity_check()
                    except ObjectStoreError:
                        failures.append(uid)

                if failures:
                    sc_name = sc.__class__.__name__
                    failed_datasources[bucket][sc_name] = failures

    if failed_datasources:
        if raise_error:
            raise ObjectStoreError(
                "The following Datasources failed their integriy checks:"
                + f"\n{failed_datasources}"
                + "\nYour object store is corrupt. Please remove these Datasources."
            )
        else:
            return failed_datasources
