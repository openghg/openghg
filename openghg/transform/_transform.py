"""Public interfaces for transforming and storing external data products.

The functions in this module validate transformation requests before opening
an object store, then delegate parsing, schema validation, and persistence to
the appropriate store class. Each request must name a supported database and
provide exactly one input source: a filesystem path or an in-memory dataset.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import xarray as xr

from openghg.objectstore import get_writable_bucket
from openghg.store import get_data_class
from openghg.store.spec import define_transform_parsers


def _validate_transform_request(
    data_type: str,
    datapath: str | Path | None,
    database: str | None,
    data: xr.Dataset | None,
) -> str:
    """Validate common transform inputs before object-store access.

    Args:
        data_type: OpenGHG data type whose transform parsers should be checked.
        datapath: Optional path containing raw input data.
        database: Name of the database-specific transform parser.
        data: Optional in-memory raw dataset.

    Returns:
        The validated database name.

    Raises:
        ValueError: If the database is missing or unsupported, or if the
            request does not provide exactly one of ``datapath`` and ``data``.
    """
    transform_parsers = define_transform_parsers()[data_type]
    if not isinstance(database, str) or database.upper() not in transform_parsers.__members__:
        supported = ", ".join(parser.value for parser in transform_parsers)
        raise ValueError(f"Unable to transform '{database}' selected. Choose one of: {supported}.")

    if (datapath is None) == (data is None):
        raise ValueError("Please specify exactly one of `datapath` or `data`.")

    return database


def transform_flux_data(
    datapath: str | Path | None = None,
    database: str | None = None,
    overwrite: bool = False,
    store: str | None = None,
    data: xr.Dataset | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Transform raw flux data and write the result to an object store.

    The database name selects a parser such as
    :func:`openghg.transform.flux.parse_edgar`. Exactly one of ``datapath``
    and ``data`` must be supplied. Inputs are validated before the writable
    object store is acquired.

    Args:
        datapath: Path to a local database directory or archive.
        database: Name of the supported flux database to transform.
        overwrite: Whether matching stored data should be overwritten.
        store: Name of the object store to write data to.
        data: Raw in-memory dataset to transform instead of reading a path.
        **kwargs: Inputs for underlying parser function for the database.
            Necessary inputs will depend on the database being parsed.

    Returns:
        Metadata dictionaries identifying the stored datasources.

    Raises:
        ValueError: If ``database`` is unsupported or exactly one of
            ``datapath`` and ``data`` is not supplied.
    """
    database = _validate_transform_request("flux", datapath, database, data)
    bucket = get_writable_bucket(name=store)
    dclass = get_data_class("flux")

    with dclass(bucket) as dc:
        result = dc.transform_data(
            datapath=datapath, database=database, data=data, overwrite=overwrite, **kwargs
        )
    return result


def transform_bc_data(
    datapath: str | Path | None = None,
    database: str | None = None,
    overwrite: bool = False,
    store: str | None = None,
    data: xr.Dataset | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Transform raw boundary-condition data and store the result.

    The database name selects a parser such as
    :func:`openghg.transform.boundary_conditions.parse_cams`. Exactly one of
    ``datapath`` and ``data`` must be supplied. Inputs are validated before
    the writable object store is acquired.

    Args:
        datapath: Path to local raw boundary-condition data.
        database: Name of the supported boundary-condition database.
        overwrite: Whether matching stored data should be overwritten.
        store: Name of the object store to write data to.
        data: Raw in-memory dataset to transform instead of reading a path.
        **kwargs: Inputs for underlying parser function for the database.
            Necessary inputs will depend on the database being parsed.

    Returns:
        Metadata dictionaries identifying the stored datasources.

    Raises:
        ValueError: If ``database`` is unsupported or exactly one of
            ``datapath`` and ``data`` is not supplied.
    """
    database = _validate_transform_request("boundary_conditions", datapath, database, data)
    bucket = get_writable_bucket(name=store)
    dclass = get_data_class("boundary_conditions")

    with dclass(bucket) as dc:
        result = dc.transform_data(
            datapath=datapath, database=database, data=data, overwrite=overwrite, **kwargs
        )
    return result
