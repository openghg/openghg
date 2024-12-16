from pathlib import Path
import logging

from openghg.standardise.meta import dataset_formatter
from openghg.types import optionalPathType

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_icos(
    filepath: str | Path,
    site: str,
    instrument: str,
    inlet: str | None = None,
    network: str = "ICOS",
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    header_type: str = "large",
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    **kwargs: dict,
) -> dict:
    """Parses an ICOS data file and creates a dictionary containing the Dataset and metadata

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        instrument: Instrument name
        inlet: Optionally specify inlet height to check against filename
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

    if not isinstance(filepath, Path):
        filepath = Path(filepath)

    if header_type == "large":
        gas_data = _read_data_large_header(
            filepath=filepath,
            site=site,
            inlet=inlet,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            measurement_type=measurement_type,
        )
    else:
        gas_data = _read_data_small_header(
            filepath=filepath,
            site=site,
            inlet=inlet,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            measurement_type=measurement_type,
        )

    gas_data = dataset_formatter(data=gas_data)

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
    filepath: Path,
    site: str,
    network: str,
    instrument: str,
    inlet: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    **kwargs: dict,
) -> dict:
    """Parses ICOS files with the larger (~40) line header

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        instrument: Instrument name
        inlet: Optionally specify inlet height to check against filename
        sampling_period: Sampling period e.g. 2 hour: 2H, 2 minute: 2m
        measurement_type: Measurement type e.g. insitu, flask
    Returns:
        dict: Dictionary of gas data
    """
    from openghg.util import read_header, format_inlet
    from pandas import read_csv, to_datetime

    # Read metadata from the filename
    try:
        species_fname = filepath.name.split(".")[-1]
        site_fname = filepath.name.split("_")[-3]
        inlet_height_fname = filepath.name.split("_")[-2]
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as \
            ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4"
        )

    if site_fname.lower() != site:
        raise ValueError("Site mismatch between site argument passed and filename.")

    inlet_height_fname = format_inlet(inlet_height_fname)
    if inlet is not None and inlet_height_fname.lower() != inlet:
        raise ValueError("Mismatch between inlet height passed and in filename.")
    inlet_height_magl = format_inlet(inlet_height_fname, key_name="inlet_height_magl")

    # Read the header and check its length
    header = read_header(filepath=filepath)
    len_header = len(header)

    if len_header < 40:
        logger.warning(f"We expect a header length of 40 or more but got {len_header}.")

    df = read_csv(
        filepath,
        header=len_header - 1,
        sep=";",
        date_format="%Y %m %d %H %M",
        na_values=["-9.990", "-999.990"],
    )

    df["time"] = to_datetime(df[["Year", "Month", "Day", "Hour", "Minute"]])
    df.index = df["time"]

    # Lowercase all the column titles
    df.columns = [str(c).lower() for c in df.columns]

    # Read some metadata before dropping the columns
    # sampling_height_data = df["samplingheight"][0]
    site_name_data = df["#site"][0]

    if site != site_name_data.lower():
        raise ValueError("Site mismatch between site argument passed and site in data.")

    # Drop the columns we don't want
    if "unc_" + species_fname.lower() in df.columns:
        cols_to_keep = [species_fname.lower(), "stdev", "nbpoints", "flag", "unc_" + species_fname.lower()]
    else:
        cols_to_keep = [species_fname.lower(), "stdev", "nbpoints", "flag"]
    df = df[cols_to_keep]

    # Remove rows with NaNs in the species or stdev columns
    # JP edit 2024-09-09. Some of the non-icos data is missing stdev but we still use it
    # df = df.dropna(axis="rows", subset=[species_fname.lower(), "stdev"])
    df = df.dropna(axis="rows", subset=species_fname.lower())

    # Drop duplicate indices
    df = df.loc[~df.index.duplicated(keep="first")]

    # Check if the index is sorted
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    if "unc_" + species_fname.lower() in df.columns:
        rename_dict = {
            "stdev": species_fname.lower() + "_variability",
            "nbpoints": species_fname.lower() + "_number_of_observations",
            "unc_" + species_fname.lower(): species_fname.lower() + "_repeatability",
        }
    else:
        rename_dict = {
            "stdev": species_fname.lower() + "_variability",
            "nbpoints": species_fname.lower() + "_number_of_observations",
        }

    df = df.rename(columns=rename_dict)

    # Convert to xarray Dataset
    data = df.to_xarray()

    data["flag"] = data["flag"].astype(str)

    metadata = {
        "site": site,
        "species": species_fname.lower(),
        "inlet": inlet_height_fname,
        "inlet_height_magl": inlet_height_magl,
        "sampling_period": sampling_period,
        "network": network,
        "instrument": instrument,
    }
    attributes = {"inlet_height_magl": metadata["inlet_height_magl"], "data_owner": "See data_owner_email"}

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    f_header = [s for s in header if "MEASUREMENT UNIT" in s]
    if len(f_header) == 1:
        units = f_header[0].split(":")[1].lower().strip()
        metadata["units"] = units
    else:
        raise ValueError("No unique MEASUREMENT UNIT in file header")

    f_header = [s for s in header if "CONTACT POINT" in s]
    if len(f_header) == 1:
        data_owner_email = f_header[0].split(":")[1].strip()
        metadata["data_owner_email"] = data_owner_email
    else:
        f_header = [s for s in header if "CONTACT POINT EMAIL" in s]
        if len(f_header) == 1:
            data_owner_email = f_header[0].split(":")[1].strip()
            metadata["data_owner_email"] = data_owner_email
        else:
            raise ValueError("Couldn't identify data owner email")

    if sampling_period is None:
        f_header = [s for s in header if "TIME INTERVAL" in s]
        interval_str = f_header[0].split(":")[1].strip()
        if interval_str == "hourly":
            metadata["sampling_period"] = "3600.0"

    species_data = {species_fname.lower(): {"metadata": metadata, "data": data, "attributes": attributes}}

    return species_data


def _read_data_small_header(
    filepath: Path,
    site: str,
    network: str,
    instrument: str,
    inlet: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
) -> dict:
    """Parses ICOS files with a single line header

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        instrument: Instrument name
        inlet: Optionally specify inlet height to check against filename
        sampling_period: Sampling period e.g. 2 hour: 2H, 2 minute: 2m
        measurement_type: Measurement type e.g. insitu, flask
    Returns:
        dict: Dictionary of gas data
    """
    from openghg.util import read_header, format_inlet
    from pandas import read_csv

    # Read some metadata from the filename
    split_filename = filepath.name.split(".")

    try:
        site_fname = split_filename[0]
        species_fname = split_filename[1]
        file_sampling_period = split_filename[2]
        inlet_height = split_filename[3]
    except IndexError:
        raise ValueError(
            "Unable to read metadata from filename. We expect a filename such as tta.co2.1minute.222m.dat"
        )

    # metadata = read_metadata(filepath=filepath, data=data, data_type="ICOS")
    header = read_header(filepath=filepath)
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
        filepath,
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
        "Stdev": species_fname + "_variability",
        "NbPoints": species_fname + "_number_of_observations",
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
    attributes = {"inlet_height_magl": metadata["inlet"], "data_owner": "NOT_SET"}

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    species_data = {species_fname: {"metadata": metadata, "data": data, "attributes": attributes}}

    return species_data
