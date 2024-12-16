from pathlib import Path
import warnings


def parse_openghg(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    data_type: str,
    database: str | None = None,
    database_version: str | None = None,
    model: str | None = None,
    time_resolved: bool = False,
    high_time_resolution: bool = False,
    period: str | tuple | None = None,
    chunks: dict | None = None,
    continuous: bool = True,
) -> dict:
    """
    Read and parse input flux / emissions data already in OpenGHG format.

    Args:
        filepath: Path to the flux file.
        species: Name of species
        source: Source of the emissions data
        domain: Geographic domain
        data_type: Type of data
        database: Name of the database
        database_version: Version of the database
        model: Model name if applicable.
        time_resolved: If this is a high resolution file.
        high_time_resolution:  This argument is deprecated and will be replaced in future versions with time_resolved.
        period: The time period for which data is to be parsed.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass in an empty dictionary.
        continuous: Flag indicating whether the data is continuous or not
    Returns:
        dict: Dictionary of data
    """
    from openghg.standardise.meta import assign_flux_attributes
    from openghg.store import infer_date_range, update_zero_dim
    from openghg.util import timestamp_now
    from xarray import open_dataset

    if high_time_resolution:
        warnings.warn(
            "This argument is deprecated and will be replaced in future versions with time_resolved.",
            DeprecationWarning,
        )
        time_resolved = high_time_resolution

    em_data = open_dataset(filepath).chunk(chunks)

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

    optional_keywords = {"database": database, "database_version": database_version, "model": model}
    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = "flux"
    metadata["source_format"] = "openghg"

    # As flux / emissions files handle things slightly differently we need to check the time values
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

    metadata["time_resolution"] = "high" if time_resolved else "standard"
    metadata["time_period"] = period_str

    key = "_".join((species, source, domain))

    flux_data: dict[str, dict] = {}
    flux_data[key] = {}
    flux_data[key]["data"] = em_data
    flux_data[key]["metadata"] = metadata

    flux_data = assign_flux_attributes(flux_data)

    return flux_data
