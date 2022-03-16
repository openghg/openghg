from typing import Dict, Optional, Union
from pathlib import Path


def parse_icos(
    data_filepath: Union[str, Path],
    species: str,
    site: str,
    inlet: str,
    instrument: str,
    network: str = "ICOS",
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Parses an ICOS data file and creates a dictionary of xarray Datasets
    ready for storage in the object store.

    Args:
        data_filepath: Path to file
        site: Three letter site code
        network: Network name
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period e.g. 2 hour: 2H, 2 minute: 2m
        measurement_type: Measurement type e.g. insitu, flask
    Returns:
        dict: Dictionary of gas data
    """
    from pathlib import Path
    from openghg.standardise.meta import assign_attributes

    if not isinstance(data_filepath, Path):
        data_filepath = Path(data_filepath)

    gas_data = _read_data(
        data_filepath=data_filepath,
        species=species,
        site=site,
        inlet=inlet,
        network=network,
        instrument=instrument,
        sampling_period=sampling_period,
        measurement_type=measurement_type,
    )

    # Ensure the data is CF compliant
    gas_data = assign_attributes(data=gas_data, site=site, sampling_period=sampling_period)

    return gas_data


def _read_data(
    data_filepath: Path,
    species: str,
    site: str,
    inlet: str,
    network: str,
    instrument: str,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Separates the gases stored in the dataframe in
    separate dataframes and returns a dictionary of gases
    with an assigned UUID as gas:UUID and a list of the processed
    dataframes

    TODO - update this to process multiple species here?

    Args:
        data_filepath : Path of datafile
        species: Species to process
    Returns:
        dict: Dictionary containing attributes, data and metadata keys
    """
    from pandas import read_csv, Timestamp
    from openghg.util import read_header, clean_string

    # metadata = read_metadata(filepath=data_filepath, data=data, data_type="ICOS")
    header = read_header(filepath=data_filepath)
    n_skip = len(header) - 1

    species = clean_string(species)
    site = clean_string(site)
    inlet = clean_string(inlet)
    instrument = clean_string(instrument)
    network = clean_string(network)
    sampling_period = clean_string(sampling_period)
    measurement_type = clean_string(measurement_type)

    def date_parser(year: str, month: str, day: str, hour: str, minute: str) -> Timestamp:
        return Timestamp(year, month, day, hour, minute)

    datetime_columns = {"time": ["Year", "Month", "Day", "Hour", "Minute"]}

    use_cols = [
        "Year",
        "Month",
        "Day",
        "Hour",
        "Minute",
        str(species.lower()),
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
        index_col="time",
        sep=" ",
        usecols=use_cols,
        dtype=dtypes,
        na_values="-999.99",
        date_parser=date_parser,
    )

    data = data[data[species.lower()] >= 0.0]

    # Drop duplicate indices
    data = data.loc[~data.index.duplicated(keep="first")]

    # Check if the index is sorted
    if not data.index.is_monotonic_increasing:
        data = data.sort_index()

    rename_dict = {
        "Stdev": species + " variability",
        "NbPoints": species + " number_of_observations",
    }

    data = data.rename(columns=rename_dict)

    # Conver to xarray Dataset
    data = data.to_xarray()

    combined_data = {}

    # Read some metadata from the filename
    split_filename = data_filepath.name.split(".")

    try:
        site_fname = split_filename[0]

        if site_fname.lower() != site:
            raise ValueError("Site mismatch between site argument passed and filename")

        file_sampling_period = split_filename[2]
        inlet_height = split_filename[3]

        if inlet_height.lower() != inlet:
            raise ValueError("Mismatch between inlet height passed and in filename")
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as tta.co2.1minute.222m.dat"
        )

    if file_sampling_period == "1minute":
        file_sampling_period = "60"
    elif file_sampling_period == "1hour":
        file_sampling_period = "3600"

    if sampling_period is not None:
        if file_sampling_period != sampling_period:
            raise ValueError("Mismatch between sampling period read from filename and that passed in.")
    else:
        sampling_period = file_sampling_period

    metadata = {
        "site": site,
        "species": species,
        "inlet": inlet_height,
        "sampling_period": sampling_period,
        "network": network,
        "instrument": instrument,
    }

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    combined_data[species] = {
        "metadata": metadata,
        "data": data,
    }

    return combined_data
