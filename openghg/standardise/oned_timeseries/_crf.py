import numpy as np
from pathlib import Path
from typing import Dict, Optional


def parse_crf(
    filepath: Path,
    species: str,
    source: str = "crf",
    domain: str = "europe",
    data_type: str = "flux",
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    chunks: Optional[Dict] = None,
) -> Dict:
    """
    Parse INTEM emissions data from the specified file.

    Args:
        filepath (Path): Path to the '.xlsx' file containing INTEM emissions data.
        species (str): Name of species
        source (str): Source of the emissions data, default is 'intem'.
        domain (str): Geographic domain, default is 'europe'.
        data_type (str): Type of data, default is 'flux'.
        database (Optional[str]): Database name if applicable.
        database_version (Optional[str]): Version of the database if applicable.
        model (Optional[str]): Model name if applicable.

    Returns:
        Dict: Parsed emissions data in dictionary format.
    """
    import pandas as pd
    from openghg.util import timestamp_now

    # Dictionary of species corresponding to sheet names
    sheet_selector = {"ch4": "Table10s3", "co2": "Table10s1", "n2o": "Table10s4", "hfc": "Table10s5"}

    # Creating dataframe based on species name
    if species.lower() in sheet_selector:
        dataframe = pd.read_excel(filepath, sheet_name=sheet_selector[species.lower()], skiprows=4)

    if species.lower() == "co2" or species.lower() == "hfc":
        dataframe = dataframe.iloc[1]
    else:
        dataframe = dataframe.iloc[49]

    dataframe = pd.DataFrame(dataframe).iloc[2:-1]
    dataframe = dataframe.rename(columns={dataframe.columns[0]: "oneD"}).astype(np.floating)
    dataframe.index = pd.to_datetime(dataframe.index, format="%Y")

    metadata = {}
    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source

    optional_keywords = {"database": database, "database_version": database_version, "model": model}

    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    author_name = "OpenGHG Cloud"
    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["source_format"] = "openghg"

    dataframe = dataframe.rename_axis("time")
    dataarray = dataframe.to_xarray()
    dataarray = dataarray.assign_coords(time=dataarray.time)

    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = dataarray
    emissions_data[key]["metadata"] = metadata
    print(emissions_data)

    return emissions_data
