from pathlib import Path
from typing import Dict, Literal, Optional, Union


def parse_intem(
    filepath: Path,
    species: str,
    source: str = "intem",
    domain: str = "europe",
    data_type: str = "emissions",
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
) -> Dict:
    """
    Parse INTEM emissions data from the specified file.

    Args:
        filepath (Path): Path to the '.nc' file containing INTEM emissions data.
        species (str): Name of species
        source (str): Source of the emissions data, default is 'intem'.
        domain (str): Geographic domain, default is 'europe'.
        data_type (str): Type of data, default is 'emissions'.
        database (Optional[str]): Database name if applicable.
        database_version (Optional[str]): Version of the database if applicable.
        model (Optional[str]): Model name if applicable.
        period "needs explainer"
        chunks (Union[int, Dict, Literal["auto"], None]): Chunking configuration.
        continuous (bool): "needs explainer"

    Returns:
        Dict: Parsed emissions data in dictionary format.
    """
    from openghg.util import timestamp_now
    from openghg.store import infer_date_range
    from openghg.store._emissions import Emissions
    from xarray import open_dataset

    emissions_dataset = open_dataset(filepath, chunks=chunks)

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

    optional_keywords = {"database": database, "database_version": database_version, "model": model}

    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = "emissions"
    metadata["source_format"] = "openghg"

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

    # Dataset validation
    Emissions.validate_data(emissions_dataset)

    # Creation of final dictionary with data and metadata as key
    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = emissions_dataset
    emissions_data[key]["metadata"] = metadata

    return emissions_data
