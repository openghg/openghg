from pathlib import Path
from typing import Dict, Optional, Union


def parse_openghg(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    date: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
) -> Dict:
    """
    Read and parse input emissions data already in OpenGHG format.

    Args:
        filepath: Path to data file
    Returns:
        dict: Dictionary of data
    """
    from xarray import open_dataset
    from openghg.util import timestamp_now
    from openghg.store import infer_date_range
    from openghg.standardise.meta import assign_flux_attributes

    em_data = open_dataset(filepath)

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

    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source
    metadata["date"] = date
    metadata["author"] = author_name
    metadata["processed"] = str(timestamp_now())

    # As emissions files handle things slightly differently we need to check the time values
    # more carefully.
    # e.g. a flux / emissions file could contain e.g. monthly data and be labelled as 2012 but
    # contain 12 time points labelled as 2012-01-01, 2012-02-01, etc.

    em_time = em_data.time

    start_date, end_date, period_str = infer_date_range(
        em_time, filepath=filepath, period=period, continuous=continuous
    )

    if date is None:
        # Check for how granular we should make the date label
        if "year" in period_str:
            date = f"{start_date.year}"
        elif "month" in period_str:
            date = f"{start_date.year}{start_date.month:02}"
        else:
            date = start_date.astype("datetime64[s]").astype(str)

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

    metadata["time_resolution"] = "high" if high_time_resolution else "standard"
    metadata["time_period"] = period_str

    key = "_".join((species, source, domain, date))

    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata

    emissions_data = assign_flux_attributes(emissions_data)

    return emissions_data
