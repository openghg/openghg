from typing import Dict, Optional, Union
from pathlib import Path
from openghg.util import load_json


def parse_icos(
    data_filepath: Union[str, Path],
    species: str,
    site: str,
    inlet: str,
    instrument: str,
    network: str = "ICOS",
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    header_type: str = "large",
) -> Dict:
    """Parses an ICOS data file and creates a dictionary containing the Dataset and metadata

    Args:
        data_filepath: Path to file
        site: Three letter site code
        network: Network name
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period e.g. 2 hour: 2H, 2 minute: 2m
        measurement_type: Measurement type e.g. insitu, flask
        header_type: ICOS data file with large (40 line) header or shorter single line header
            Options: large, small
    Returns:
        dict: Dictionary of gas data
    """
    from pathlib import Path
    from openghg.standardise.meta import assign_attributes

    if not isinstance(data_filepath, Path):
        data_filepath = Path(data_filepath)

    if header_type == "large":
        gas_data = _read_data_large_header(
            data_filepath=data_filepath,
            species=species,
            site=site,
            inlet=inlet,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            measurement_type=measurement_type,
        )
    else:
        gas_data = _read_data_small_header(
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


def _read_data_large_header(
    data_filepath: Path,
    species: str,
    site: str,
    inlet: str,
    network: str,
    instrument: str,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Parses ICOS files with the larger (~40) line header

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
    from pandas import read_csv, to_datetime
    from openghg.util import read_header, clean_string

    species = clean_string(species)
    site = clean_string(site)
    inlet = clean_string(inlet)
    instrument = clean_string(instrument)
    network = clean_string(network)
    sampling_period = clean_string(sampling_period)
    measurement_type = clean_string(measurement_type)

    # Read the header and check its length
    header = read_header(filepath=data_filepath)
    len_header = len(header)

    dtypes = {
        "#Site": "string",
        "SamplingHeight": "string",
        "DecimalDate": "float",
        "Stdev": "float",
        "NbPoints": "int",
        "Flag": "string",
        "InstrumentId": "int",
        "QualityId": "string",
        "InternalFlag": "string",
        "AutoDescriptiveFlag": "string",
        "ManualDescriptiveFlag": "string",
    }

    df = read_csv(
        data_filepath,
        header=len_header - 1,
        sep=";",
        parse_dates={"time": [2, 3, 4, 5, 6]},
        index_col="time",
        na_values=["-9.990", "-999.990"],
        dtype=dtypes,
    )

    df.index = to_datetime(df.index, format="%Y %m %d %H %M")

    # Lowercase all the column titles
    df.columns = [str(c).lower() for c in df.columns]

    # Read some metadata before dropping the columns
    # sampling_height_data = df["samplingheight"][0]
    site_name_data = df["#site"][0]
    species_name_data = df.columns[3]

    # Drop the columns we don't want
    cols_to_keep = [species_name_data, "stdev", "nbpoints", "flag", "instrumentid"]
    df = df[cols_to_keep]

    # Remove rows with NaNs in the species or stdev columns
    df = df.dropna(axis="rows", subset=[species_name_data, "stdev"])

    # Drop duplicate indices
    df = df.loc[~df.index.duplicated(keep="first")]

    # Check if the index is sorted
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    rename_dict = {
        "stdev": species + " variability",
        "nbpoints": species + " number_of_observations",
    }

    df = df.rename(columns=rename_dict)

    # Convert to xarray Dataset
    data = df.to_xarray()

    # Read metadata from the filename and cross check to make sure the passed
    # arguments match
    split_filename = data_filepath.name.split(".")

    try:
        site_fname = split_filename[0]
        species_fname = split_filename[1]
        file_sampling_period = split_filename[2]
        instrument_fname = split_filename[3]
        inlet_height_fname = split_filename[4]
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as tta.co2.1minute.222m.dat"
        )

    if site_fname.lower() != site != site_name_data.lower():
        raise ValueError("Site mismatch between site argument passed and filename")

    if species_name_data.lower() != species != species_fname.lower():
        raise ValueError("Mismatch in species between data, argument and filename")

    if inlet_height_fname.lower() != inlet:
        raise ValueError("Mismatch between inlet height passed and in filename")

    if instrument_fname.lower() != instrument:
        raise ValueError("Mismatch between instrument passed and that in filename")

    if file_sampling_period == "1minute":
        file_sampling_period = "60"
    elif file_sampling_period == "hourly":
        file_sampling_period = "3600"

    if sampling_period is not None:
        if file_sampling_period != sampling_period:
            raise ValueError("Mismatch between sampling period read from filename and that passed.")
    else:
        sampling_period = file_sampling_period

    metadata = {
        "site": site,
        "species": species,
        "inlet": inlet,
        "sampling_period": sampling_period,
        "network": network,
        "instrument": instrument,
    }

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    unit_line = header[22]
    if "MEASUREMENT UNIT" in unit_line:
        units = unit_line.split(":")[1].lower().strip()
        metadata["units"] = units

    scale_line = header[26]
    if "MEASUREMENT SCALE" in scale_line:
        calibration_scale = scale_line.split(":")[1].lower().lstrip(" ").replace(" ", "_").strip()
        metadata["calibration_scale"] = calibration_scale

    attrs = _retrieve_site_attrs(site=site, network=network)

    species_data = {
        species: {
            "metadata": metadata,
            "data": data,
            "attributes": attrs,
        }
    }

    return species_data


def _read_data_small_header(
    data_filepath: Path,
    species: str,
    site: str,
    inlet: str,
    network: str,
    instrument: str,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Parses ICOS files with a single line header

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
    from pandas import read_csv, Timestamp
    from openghg.util import read_header

    # metadata = read_metadata(filepath=data_filepath, data=data, data_type="ICOS")
    header = read_header(filepath=data_filepath)
    n_skip = len(header) - 1

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

    # Convert to xarray Dataset
    data = data.to_xarray()

    # Read some metadata from the filename
    split_filename = data_filepath.name.split(".")

    try:
        site_fname = split_filename[0]
        file_sampling_period = split_filename[2]
        inlet_height = split_filename[3]
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as tta.co2.1minute.222m.dat"
        )

    if site_fname.lower() != site:
        raise ValueError("Site mismatch between site argument passed and filename")

    if inlet_height.lower() != inlet:
        raise ValueError("Mismatch between inlet height passed and in filename")

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

    attrs = _retrieve_site_attrs(site=site, network=network)

    species_data = {
        species: {
            "metadata": metadata,
            "data": data,
            "attributes": attrs,
        }
    }

    return species_data


def _retrieve_site_attrs(site: str, network: str = "ICOS") -> Dict:
    """Retrieve site attributes from metadata file

    Args:
        site: Site code
        network: Network name, defaults to ICOS
    Returns:
        dict: Dictionary of site metadata
    """
    site_metadata = load_json(filename="acrg_site_info.json")

    site = site.upper()
    network = network.upper()

    site_attrs = site_metadata[site][network]

    return site_attrs
