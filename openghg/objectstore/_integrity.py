import logging
import os
from pathlib import Path
from collections import defaultdict
from openghg.store.spec import define_data_type_classes
from openghg.objectstore import get_readable_buckets
from openghg.types import ObjectStoreError
from typing import List, Optional

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(level=logging.DEBUG)


def integrity_check(raise_error: bool = True, check_datasource_integrity=False) -> Optional[List]:
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
        datasource_path = Path(bucket) / "datasource/uuid"
        datasource_uuids = [uuid[:-6] for uuid in os.listdir(datasource_path)]
        for storage_class in datastore_classes:
            # Now load the object
            with storage_class(bucket=bucket) as sc:
                # Get all the Datasources
                metastore_datasource_uuids = sc.datasources()
                # Check they all exist
                failures = []
                for uuid in metastore_datasource_uuids:
                    if uuid not in datasource_uuids:
                        failures.append(uuid)
                    if check_datasource_integrity:
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
                "The following Datasources failed their integrity checks:"
                + f"\n{failed_datasources}"
                + "\nYour object store is corrupt. Please remove these Datasources."
            )
        else:
            return failed_datasources
