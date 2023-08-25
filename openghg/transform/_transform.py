"""
This module contains the user interface for adding
data to the object store via transformations.
"""
from pathlib import Path
from typing import Optional, Union, Any

from openghg.objectstore import get_writable_bucket
from openghg.store.base import get_data_class


def transform_emissions_data(
    datapath: Union[str, Path],
    database: str,
    overwrite: bool = False,
    bucket: Optional[str] = None,
    store: Optional[str] = None,
    **kwargs: Any
) -> Optional[dict]:
    """Read and transform an emissions database. This will find the appropriate
    parser function to use for the database specified. The necessary inputs
    are determined by which database is being used.

    The underlying parser functions will be of the form:
        - openghg.transform.emissions.parse_{database.lower()}
        - e.g. openghg.transform.emissions.parse_edgar()

    Args:
        datapath: Path to local copy of database archive (for now)
        database: Name of database
        overwrite: Should this data overwrite currently stored data
            which matches.
        bucket: object store bucket to write data to.
        store: name of object store to write data to.
        **kwargs: Inputs for underlying parser function for the database.
            Necessary inputs will depend on the database being parsed.
    """
    if bucket is None:
        bucket = get_writable_bucket(name=store)

    dclass = get_data_class("emissions")
    with dclass(bucket) as dc:
        result = dc.transform_data(datapath=datapath, database=database, overwrite=overwrite, **kwargs)
    return result
