from pathlib import Path
import warnings


def parse_intem(
    filepath: Path,
    species: str,
    source: str,
    chunks: dict,
    data_type: str = "emissions",
    domain: str = "europe",
    model: str = "intem",
    period: str | tuple | None = None,
    time_resolved: bool = False,
    high_time_resolution: bool = False,
    continuous: bool = True,
) -> dict:
    """
    Parse INTEM emissions data from the specified file.

    Args:
        filepath: Path to the INTEM emissions data file.
        species: Name of species
        source: Source of the emissions data
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass in an empty dictionary.
        data_type: Type of data, default is 'emissions'.
        domain: Geographic domain, default is 'europe'.
        model: Model name if applicable.
        period: The time period for which data is to be parsed.
        time_resolved: If this is a high resolution file.
        high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
        continuous: Flag indicating whether the data is continuous or not
    Returns:
        Dict: Parsed emissions data in dictionary format.
    """
    from openghg.util import timestamp_now
    from openghg.store import infer_date_range
    from xarray import open_dataset

    if high_time_resolution:
        warnings.warn(
            "This argument is deprecated and will be replaced in future versions with time_resolved.",
            DeprecationWarning,
        )
        time_resolved = high_time_resolution

    emissions_dataset = open_dataset(filepath).chunk(chunks)

    author_name = "OpenGHG Cloud"
    emissions_dataset.attrs["author"] = author_name
    attrs = {}
    for key, value in emissions_dataset.attrs.items():
        try:
            attrs[key] = value.item()
        except AttributeError:
            attrs[key] = value

    # Creation of metadata dictionary
    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source

    optional_keywords = {"model": model}

    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    metadata["author"] = author_name
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = data_type
    metadata["source_format"] = "openghg"
    metadata["time_resolution"] = "high" if time_resolved else "standard"
    dataset_time = emissions_dataset["time"]

    # Fetching start_date and end_date from dataset time dimension
    start_date, end_date, period_str = infer_date_range(
        dataset_time, filepath=filepath, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)
    metadata["max_longitude"] = round(float(emissions_dataset["lon"].max()), 5)
    metadata["min_longitude"] = round(float(emissions_dataset["lon"].min()), 5)
    metadata["max_latitude"] = round(float(emissions_dataset["lat"].max()), 5)
    metadata["min_latitude"] = round(float(emissions_dataset["lat"].min()), 5)

    key = "_".join((species, source, domain))

    emissions_dataset = emissions_dataset.rename_vars({"flux_mean": "flux"})

    # Creation of final dictionary with data and metadata as key
    emissions_data: dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = emissions_dataset
    emissions_data[key]["metadata"] = metadata

    return emissions_data
