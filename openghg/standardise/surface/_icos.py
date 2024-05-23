from pathlib import Path
from typing import Dict, Optional, Union
import logging

from openghg.types import optionalPathType

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_icos(
    data_filepath: Union[str, Path],
    site: str,
    inlet: str,
    instrument: str,
    network: str = "ICOS",
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    header_type: str = "large",
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    **kwargs: Dict,
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
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        dict: Dictionary of gas data
    """
    from pathlib import Path

    from openghg.standardise.meta import assign_attributes
    from openghg.util import clean_string, format_inlet

    site = clean_string(site)
    instrument = clean_string(instrument)
    network = clean_string(network)
    sampling_period = clean_string(sampling_period)
    measurement_type = clean_string(measurement_type)

    inlet = clean_string(inlet)
    inlet = format_inlet(inlet)

    if not isinstance(data_filepath, Path):
        data_filepath = Path(data_filepath)

    if header_type == "large":
        gas_data = _read_data_large_header(
            data_filepath=data_filepath,
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
            site=site,
            inlet=inlet,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            measurement_type=measurement_type,
        )

    # Ensure the data is CF compliant
    gas_data = assign_attributes(
        data=gas_data,
        site=site,
        sampling_period=sampling_period,
        update_mismatch=update_mismatch,
        site_filepath=site_filepath,
    )

    return gas_data


def _read_data_large_header(
    data_filepath: Path,
    site: str,
    inlet: str,
    network: str,
    instrument: str,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    **kwargs: Dict,
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
    from openghg.util import read_header, format_inlet
    from pandas import read_csv

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
            "Unable to read metadata from filename. We expect a filename such as mhd.ch4.hourly.g2401.15m.dat"
        )

    if site_fname.lower() != site:
        raise ValueError("Site mismatch between site argument passed and filename.")

    inlet_height_fname = format_inlet(inlet_height_fname)
    if inlet is not None and inlet_height_fname.lower() != inlet:
        raise ValueError("Mismatch between inlet height passed and in filename.")

    if instrument is not None and instrument_fname.lower() != instrument:
        raise ValueError("Mismatch between instrument passed and that in filename.")

    # Read the header and check its length
    header = read_header(filepath=data_filepath)
    len_header = len(header)

    if len_header != 40:
        logger.warning(
            f"We expect a header length of 40 but got {len_header}, \
            note that some metadata may not be collected, \
            please raise an issue on GitHub if this file format is to be expected."
        )

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
        date_format="%Y %m %d %H %M",
        index_col="time",
        na_values=["-9.990", "-999.990"],
        dtype=dtypes,
    )

    # Lowercase all the column titles
    df.columns = [str(c).lower() for c in df.columns]

    # Read some metadata before dropping the columns
    # sampling_height_data = df["samplingheight"][0]
    site_name_data = df["#site"][0]
    species_name_data = df.columns[3]

    if site != site_name_data.lower():
        raise ValueError("Site mismatch between site argument passed and site in data.")

    if species_fname != species_name_data.lower():
        raise ValueError("Speices mismatch between site passed and species in data.")

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
        "stdev": species_name_data + " variability",
        "nbpoints": species_name_data + " number_of_observations",
    }

    df = df.rename(columns=rename_dict)

    # Convert to xarray Dataset
    data = df.to_xarray()

    data["flag"] = data["flag"].astype(str)

    if file_sampling_period == "1minute":
        file_sampling_period = "60.0"
    elif file_sampling_period == "hourly":
        file_sampling_period = "3600.0"

    if sampling_period is not None:
        if file_sampling_period != sampling_period:
            raise ValueError("Mismatch between sampling period read from filename and that passed.")
    else:
        sampling_period = file_sampling_period

    metadata = {
        "site": site,
        "species": species_name_data,
        "inlet": inlet_height_fname,
        "sampling_period": file_sampling_period,
        "network": network,
        "instrument": instrument_fname,
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

    data_owner_line = header[18]
    if "CONTACT POINT" in data_owner_line:
        data_owner_email = data_owner_line.split(":")[1].split(",")[1].strip()
        metadata["data_owner_email"] = data_owner_email

    species_data = {
        species_name_data: {
            "metadata": metadata,
            "data": data,
        }
    }

    return species_data


def _read_data_small_header(
    data_filepath: Path,
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
    from openghg.util import read_header, format_inlet
    from pandas import read_csv

    # Read some metadata from the filename
    split_filename = data_filepath.name.split(".")

    try:
        site_fname = split_filename[0]
        species_fname = split_filename[1]
        file_sampling_period = split_filename[2]
        inlet_height = split_filename[3]
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as tta.co2.1minute.222m.dat"
        )

    # metadata = read_metadata(filepath=data_filepath, data=data, data_type="ICOS")
    header = read_header(filepath=data_filepath)
    n_skip = len(header) - 1

    datetime_columns = ["Year", "Month", "Day", "Hour", "Minute"]

    use_cols = [
        "Year",
        "Month",
        "Day",
        "Hour",
        "Minute",
        str(species_fname.lower()),
        "Stdev",
        "NbPoints",
    ]

    dtypes = {
        species_fname.lower(): float,
        "Stdev": float,
        "SamplingHeight": float,
        "NbPoints": int,
    }

    data = read_csv(
        data_filepath,
        skiprows=n_skip,
        sep=" ",
        usecols=use_cols,
        dtype=dtypes,
        na_values="-999.99",
        parse_dates={"time": datetime_columns},
        date_format="%Y %m %d %H %M",
        index_col="time",
    )

    data = data[data[species_fname.lower()] >= 0.0]

    # Drop duplicate indices
    data = data.loc[~data.index.duplicated(keep="first")]

    # Check if the index is sorted
    if not data.index.is_monotonic_increasing:
        data = data.sort_index()

    rename_dict = {
        "Stdev": species_fname + " variability",
        "NbPoints": species_fname + " number_of_observations",
    }

    data = data.rename(columns=rename_dict)

    # Convert to xarray Dataset
    data = data.to_xarray()

    if site_fname.lower() != site:
        raise ValueError("Site mismatch between site argument passed and filename")

    inlet_height = format_inlet(inlet_height)
    if inlet_height.lower() != inlet:
        raise ValueError("Mismatch between inlet height passed and in filename")

    if file_sampling_period == "1minute":
        file_sampling_period = "60.0"
    elif file_sampling_period == "1hour":
        file_sampling_period = "3600.0"

    if sampling_period is not None:
        if file_sampling_period != sampling_period:
            raise ValueError("Mismatch between sampling period read from filename and that passed in.")
    else:
        sampling_period = file_sampling_period

    metadata = {
        "site": site,
        "species": species_fname,
        "inlet": inlet_height,
        "sampling_period": sampling_period,
        "network": network,
        "instrument": instrument,
        "data_type": "surface",
        "source_format": "icos",
    }

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    species_data = {
        species_fname: {
            "metadata": metadata,
            "data": data,
        }
    }

    return species_data
