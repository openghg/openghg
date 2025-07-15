from __future__ import annotations

from collections import defaultdict
import logging

from openghg.objectstore import get_readable_buckets
from openghg.objectstore._objectstore import open_object_store
from openghg.store.spec import define_data_types
from openghg.types import ObjectStoreError


logger = logging.getLogger("openghg.objectstore")
logger.setLevel(level=logging.DEBUG)


def integrity_check(raise_error: bool = True) -> dict[str, dict[str, str]] | None:
    """Check the integrity of object stores.

    Args:
        raise_error: if True, raise ObjectStoreError if integrity check fails.
        Otherwise, return a list of Datasources that failed the integrity check are returned.

    Returns:
        Nested dictionaries of failed datasource UUIDs, keyed by bucket and data type, if failures
        found, otherwise None.
    """
    # For now loop over each of the object stores, can we somehow lock the object store?
    readable_buckets = get_readable_buckets()
    data_types = define_data_types()

    failed_datasources = defaultdict(dict)
    for bucket in readable_buckets.values():
        for data_type in data_types:
            # Now load the object
            with open_object_store(bucket=bucket, data_type=data_type) as objstore:
                # Get all the Datasources
                datasource_uuids = objstore.uuids
                # Check they all exist
                failures = []
                for uid in datasource_uuids:
                    try:
                        objstore.get_datasource(uuid=uid).integrity_check()
                    except ObjectStoreError:
                        failures.append(uid)

                if failures:
                    failed_datasources[bucket][data_type] = failures

    if failed_datasources:
        if raise_error:
            raise ObjectStoreError(
                "The following Datasources failed their integrity checks:"
                + f"\n{dict(failed_datasources)}"
                + "\nYour object store is corrupt. Please remove these Datasources."
            )
        return failed_datasources
    return None
