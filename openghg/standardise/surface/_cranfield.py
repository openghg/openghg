from typing import Dict, List, Optional, Union
from pathlib import Path


def parse_cranfield(
    data_filepath: Union[str, Path],
    site: Optional[str] = None,
    network: Optional[str] = None,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Creates a CRDS object holding data stored within Datasources

    Args:
        filepath: Path of file to load
        data_filepath : Filepath of data to be read
        site: Name of site
        network: Name of network
    Returns:
        dict: Dictionary of gas data
    """
    from pandas import read_csv
    from openghg.util import clean_string

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)
    data = read_csv(data_filepath, parse_dates=["Date"], index_col="Date")

    data = data.rename(
        columns={
            "Methane/ppm": "ch4",
            "Methane stdev/ppm": "ch4 variability",
            "CO2/ppm": "co2",
            "CO2 stdev/ppm": "co2 variability",
            "CO/ppm": "co",
            "CO stdev/ppm": "co variability",
        }
    )
    data.index.name = "time"

    # Convert CH4 and CO to ppb
    data["ch4"] = data["ch4"] * 1e3
    data["ch4 variability"] = data["ch4 variability"] * 1e3
    data["co"] = data["co"] * 1e3
    data["co variability"] = data["co variability"] * 1e3

    metadata = {}
    metadata["site"] = "THB"
    metadata["instrument"] = "CRDS"
    metadata["sampling_period"] = str(sampling_period)
    metadata["height"] = "10magl"
    metadata["inlet"] = "10magl"
    metadata["network"] = "CRANFIELD"

    # TODO - this feels fragile
    species: List[str] = [col for col in data.columns if " " not in col]

    combined_data = {}
    # Number of columns of data for each species
    n_cols = 2

    for n, sp in enumerate(species):
        # for sp in species:
        # Create a copy of the metadata dict
        species_metadata = metadata.copy()
        species_metadata["species"] = str(clean_string(sp))

        # Here we don't want to match the co in co2
        # For now we'll just have 2 columns for each species
        # cols = [col for col in data.columns if sp in col]
        gas_data = data.iloc[:, n * n_cols : (n + 1) * n_cols]

        # Convert from a pandas DataFrame to an xarray Dataset
        gas_data = gas_data.to_xarray()

        combined_data[sp] = {"metadata": species_metadata, "data": gas_data}

    return combined_data
