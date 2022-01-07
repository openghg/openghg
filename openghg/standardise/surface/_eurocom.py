from typing import Dict, Optional, Union
from pathlib import Path


def parse_eurocom(
    data_filepath: Union[str, Path],
    site: str,
    sampling_period: str,
    network: Optional[str] = None,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
) -> Dict:
    """Parses EUROCOM data files into a format expected by OpenGHG

    Args:
        data_filepath: Path of file to read
        site: Site code
        sampling_period: Sampling period in seconds
        network: Network name
        Inlet: Inlet height in metres
        Instrument: Instrument name
    Returns:
        dict: Dictionary of measurement data
    """
    from openghg.standardise.meta import assign_attributes, get_attributes
    from pandas import read_csv, Timestamp
    from openghg.util import read_header, load_json

    data_filepath = Path(data_filepath)

    if site is None:
        site = data_filepath.stem.split("_")[0]

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)

    filename = data_filepath.name
    inlet_height = filename.split("_")[1]

    if "m" not in inlet_height:
        inlet_height = "NA"

    # This dictionary is used to store the gas data and its associated metadata
    combined_data = {}

    # Read the header as lines starting with #
    header = read_header(data_filepath, comment_char="#")
    n_skip = len(header) - 1
    species = "co2"

    def date_parser(year: str, month: str, day: str, hour: str, minute: str) -> Timestamp:
        return Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)

    datetime_columns = {"time": ["Year", "Month", "Day", "Hour", "Minute"]}
    use_cols = [
        "Day",
        "Month",
        "Year",
        "Hour",
        "Minute",
        str(species.lower()),
        "SamplingHeight",
        "Stdev",
        "NbPoints",
    ]

    dtypes = {
        "Day": int,
        "Month": int,
        "Year": int,
        "Hour": int,
        "Minute": int,
        species.lower(): float,
        "Stdev": float,
        "SamplingHeight": float,
        "NbPoints": int,
    }

    data = read_csv(
        data_filepath,
        skiprows=n_skip,
        parse_dates=datetime_columns,
        date_parser=date_parser,
        index_col="time",
        sep=";",
        usecols=use_cols,
        dtype=dtypes,
        na_values="-999.99",
    )

    data = data[data[species.lower()] >= 0.0]
    data = data.dropna(axis="rows", how="any")
    # Drop duplicate indices
    data = data.loc[~data.index.duplicated(keep="first")]
    # Convert to xarray Dataset
    data = data.to_xarray()

    attributes_data = load_json(filename="attributes.json")
    eurocom_attributes = attributes_data["EUROCOM"]
    global_attributes = eurocom_attributes["global_attributes"]

    if inlet_height == "NA":
        try:
            inlet = eurocom_attributes["intake_height"][site]
            global_attributes["inlet_height_m"] = inlet
            calibration_scale = eurocom_attributes["calibration"][site]
        except KeyError:
            calibration_scale = {}
            raise ValueError(f"Unable to find inlet from filename or attributes file for {site}")

    gas_data = get_attributes(
        ds=data,
        species=species,
        site=site,
        global_attributes=global_attributes,
        units="ppm",
    )

    # Create a copy of the metadata dict
    metadata = {}
    metadata["site"] = site
    metadata["species"] = species
    metadata["inlet_height"] = global_attributes["inlet_height_m"]
    metadata["calibration_scale"] = calibration_scale
    metadata["network"] = "EUROCOM"
    metadata["sampling_period"] = str(sampling_period)

    combined_data[species] = {
        "metadata": metadata,
        "data": gas_data,
        "attributes": global_attributes,
    }

    combined_data = assign_attributes(data=combined_data, site=site, sampling_period=sampling_period)

    return combined_data
