"""
This module contains the user interface for adding
data to the object store via transformations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from openghg.objectstore import get_writable_bucket
from openghg.store import get_data_class


def transform_flux_data(
    datapath: Union[str, Path],
    database: str,
    overwrite: bool = False,
    store: Optional[str] = None,
    info: Optional[dict] = None,
    **kwargs,
) -> dict:
    """Read and transform a flux / emissions database. This will find the appropriate
    parser function to use for the database specified. The necessary inputs
    are determined by which database is being used.

    The underlying parser functions will be of the form:
        - openghg.transform.flux.parse_{database.lower()}
        - e.g. openghg.transform.flux.parse_edgar()

    Args:
        datapath: Path to local copy of database archive (for now)
        database: Name of database
        overwrite: Should this data overwrite currently stored data
            which matches.
        store: name of object store to write data to.
        info: dictionary of tags for searching or other additional information. This info is *not* used to distinguish between datasources.
            e.g. {"project": "paris", "comments": "Flat prior based on EDGAR totals by country"}
        **kwargs: Inputs for underlying parser function for the database.
            Necessary inputs will depend on the database being parsed. Any

    Returns:
        dict containing datasource UUIDs data is assigned to
    """
    bucket = get_writable_bucket(name=store)
    dclass = get_data_class("flux")

    with dclass(bucket) as dc:
        result = dc.transform_data(datapath=datapath, database=database, overwrite=overwrite, info=info, **kwargs)
    return result
