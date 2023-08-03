from pathlib import Path
from typing import Dict, Literal, Optional, Union


def parse_openghg(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    data_type: str,
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
) -> Dict:
    """
    Read and parse input emissions data already in OpenGHG format.

    Args:
        filepath: Path to data file
        chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
        Passing "auto" will ask xarray to calculate a chunk size.
    Returns:
        dict: Dictionary of data
    """
    from openghg.standardise.meta import assign_flux_attributes
    from openghg.store import infer_date_range, update_zero_dim
    from openghg.util import timestamp_now
    from xarray import open_dataset

    em_data = open_dataset(filepath, chunks=chunks)

    # Some attributes are numpy types we can't serialise to JSON so convert them
    # to their native types here
    attrs = {}
    for key, value in em_data.attrs.items():
        try:
            attrs[key] = value.item()
        except AttributeError:
            attrs[key] = value

    author_name = "OpenGHG Cloud"
    em_data.attrs["author"] = author_name
    print("parse_openghg attrs database_version:", attrs.get("database_version", None))
    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source

    optional_keywords = {"database": database,
                         "database_version": database_version,
                         "model": model}
    print("Optional keywords in parse_openghg:", optional_keywords)
    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = "emissions"
    metadata["source_format"] = "openghg"

    # As emissions files handle things slightly differently we need to check the time values
    # more carefully.
    # e.g. a flux / emissions file could contain e.g. monthly data and be labelled as 2012 but
    # contain 12 time points labelled as 2012-01-01, 2012-02-01, etc.

    # Check if time has 0-dimensions and, if so, expand this so time is 1D
    if "time" in em_data.coords:
        em_data = update_zero_dim(em_data, dim="time")

    em_time = em_data["time"]

    start_date, end_date, period_str = infer_date_range(
        em_time, filepath=filepath, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

    metadata["time_resolution"] = "high" if high_time_resolution else "standard"
    metadata["time_period"] = period_str

    key = "_".join((species, source, domain))

    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata
    emissions_data = assign_flux_attributes(emissions_data)
    return emissions_data
