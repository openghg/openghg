from pathlib import Path

from openghg.standardise.meta import dataset_formatter
from openghg.types import pathType
from openghg.util import (
    clean_string,
    find_duplicate_timestamps,
    format_inlet,
    get_site_info,
    load_internal_json,
)

import pandas as pd
from pandas import Timedelta


def parse_crds(
    filepath: pathType,
    site: str,
    network: str,
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | float | int | None = None,
    drop_duplicates: bool = True,
    update_mismatch: str = "never",
    site_filepath: pathType | None = None,
    **kwargs: dict,
) -> dict:
    """Parses a CRDS data file and creates a dictionary of xarray Datasets
    ready for storage in the object store.

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period in seconds
        drop_duplicates: Drop measurements at duplicate timestamps, keeping the first.
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
    from openghg.standardise.meta import assign_attributes

    if not isinstance(filepath, Path):
        filepath = Path(filepath)

    inlet = format_inlet(inlet)

    # This may seem like an almost pointless function as this is all we do
    # but it makes it a lot easier to test assign_attributes
    gas_data = _read_data(
        filepath=filepath,
        site=site,
        network=network,
        inlet=inlet,
        instrument=instrument,
        sampling_period=sampling_period,
        drop_duplicates=drop_duplicates,
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


def _get_raw_dataframe(filepath: Path, drop_duplicates: bool = True) -> pd.DataFrame:
    """Read raw CRDS data into dataframe with combined data for all gases.

    Args:
        filepath: path to raw data
        drop_duplicates: if True, drop duplicate times.

    Returns:
        dict mapping species to pd.DataFrame containing data for that gas

    Raises:
        ValueError: if duplicate times found and `drop_duplicates` is False.

    """
    # parse with multiindex for columns
    combined_df = pd.read_csv(filepath, header=[0, 1], skiprows=1, sep=r"\s+")

    # parse dates
    datetime_series = (
        combined_df.pop(("-", "date")).astype(str)
        + " "
        + combined_df.pop(("-", "time")).astype(str).str.zfill(6)
    )
    combined_df["time"] = pd.to_datetime(datetime_series, format="%y%m%d %H%M%S")
    combined_df = combined_df.set_index("time")

    # drop duplicate times
    dupes = find_duplicate_timestamps(data=combined_df)

    if dupes:
        if not drop_duplicates:
            raise ValueError(f"Duplicate dates detected: {dupes}")
        combined_df = combined_df.loc[~combined_df.index.duplicated(keep="first")]

    return combined_df


def _read_data(
    filepath: Path,
    site: str,
    network: str,
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | float | int | None = None,
    site_filepath: pathType | None = None,
    drop_duplicates: bool = True,
) -> dict:
    """Read the datafile passed in and extract the data we require.

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period in seconds
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        drop_duplicates: Drop measurements at duplicate timestamps, keeping the first.

    Returns:
        dict: Dictionary of gas data
    """
    split_fname = filepath.stem.split(".")
    site = site.lower()

    try:
        site_fname = clean_string(split_fname[0])
        inlet_fname = clean_string(split_fname[3])
    except IndexError:
        raise ValueError(
            "Error reading metadata from filename, we expect a form hfd.picarro.1minute.100m.dat"
        )

    if site_fname != site:
        raise ValueError("Site mismatch between passed site code and that read from filename.")

    if "m" not in inlet_fname:
        raise ValueError("No inlet found, we expect filenames such as: bsd.picarro.1minute.108m.dat")

    if inlet is not None and inlet != inlet_fname:
        raise ValueError("Inlet mismatch between passed inlet and that read from filename.")
    else:
        inlet = inlet_fname

    # get data
    data = _get_raw_dataframe(filepath, drop_duplicates=drop_duplicates)

    # get metadata
    metadata = _read_metadata(filepath)

    # add port and type from raw data, then drop these columns
    metadata["type"] = data[("-", "type")].iloc[0]
    metadata["port"] = data[("-", "port")].iloc[0]
    drop_cols = [col for col in data.columns if col[0] == "-"]
    data = data.drop(columns=drop_cols)

    if network is not None:
        metadata["network"] = network

    if sampling_period is not None:
        sampling_period = float(sampling_period)
        # Compare against value extracted from the file name
        file_sampling_period = Timedelta(seconds=float(metadata["sampling_period"]))
        given_sampling_period = Timedelta(seconds=sampling_period)

        comparison_seconds = abs(given_sampling_period - file_sampling_period).total_seconds()
        tolerance_seconds = 1

        if comparison_seconds > tolerance_seconds:
            raise ValueError(
                f"Input sampling period {sampling_period} does not match to value "
                f"extracted from the file name of {metadata['sampling_period']} seconds."
            )

    # Read the scale from JSON
    # I'll leave this here for the possible future movement from class to functions
    network_metadata = load_internal_json(filename="process_gcwerks_parameters.json")
    crds_metadata = network_metadata["CRDS"]

    # This dictionary is used to store the gas data and its associated metadata
    combined_data = {}

    # get gases from first level of column multi-index
    gases = set([col[0] for col in data.columns])

    for gas in gases:
        gas_data = data.loc[:, gas]
        species = gas.lower()
        gas_data.columns = [species, f"{species}_variability", f"{species}_number_of_observations"]
        gas_data = gas_data.dropna(axis="rows", how="any")
        gas_data[f"{species}_number_of_observations"] = gas_data[f"{species}_number_of_observations"].astype(
            int
        )

        # Here we can convert the Dataframe to a Dataset and then write the attributes
        gas_data = gas_data.to_xarray()

        site_attributes = _get_site_attributes(
            site=site, inlet=inlet, crds_metadata=crds_metadata, site_filepath=site_filepath
        )

        scale = crds_metadata["default_scales"].get(species.upper(), "NA")

        # Create a copy of the metadata dict
        species_metadata = metadata.copy()
        species_metadata["species"] = clean_string(species)
        species_metadata["inlet"] = format_inlet(inlet, key_name="inlet")
        species_metadata["calibration_scale"] = scale
        species_metadata["long_name"] = site_attributes["long_name"]
        species_metadata["data_type"] = "surface"

        # Make sure metadata keys are included in attributes
        site_attributes.update(species_metadata)

        combined_data[species] = {
            "metadata": species_metadata,
            "data": gas_data,
            "attributes": site_attributes,
        }

    return combined_data


def _read_metadata(filepath: Path) -> dict:
    """Parse CRDS files and create a metadata dict.

    Args:
        filepath: Data filepath

    Returns:
        dict: Dictionary containing metadata
    """
    # Split the filename to get the site and resolution
    split_filename = str(filepath.name).split(".")

    if len(split_filename) < 4:
        raise ValueError(
            "Error reading metadata from filename. The expected format is \
            {site}.{instrument}.{sampling period}.{height}.dat"
        )

    site = split_filename[0]
    instrument = split_filename[1]
    sampling_period_str = split_filename[2]
    inlet = split_filename[3]

    if sampling_period_str == "1minute":
        sampling_period = "60.0"
    elif sampling_period_str == "hourly":
        sampling_period = "3600.0"
    else:
        raise ValueError("Unable to read sampling period from filename.")

    metadata = {}
    metadata["site"] = site
    metadata["instrument"] = instrument
    metadata["sampling_period"] = str(sampling_period)
    metadata["inlet"] = format_inlet(inlet, key_name="inlet")

    return metadata


def _get_site_attributes(
    site: str,
    inlet: str,
    crds_metadata: dict,
    site_filepath: pathType | None = None,
) -> dict:
    """Get site specific attributes for writing to Datasets.

    Args:
        site: Site name
        inlet: Inlet height, example: 108m
        crds_metadata: General CRDS metadata
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.

    Returns:
        dict: Dictionary of attributes
    """
    try:
        site_attributes: dict = crds_metadata["sites"][site.upper()]
        global_attributes: dict = site_attributes["global_attributes"]
    except KeyError:
        raise ValueError(f"Unable to read attributes for site: {site}")

    # TODO - we need to combine the metadata
    full_site_metadata = get_site_info(site_filepath)

    attributes = global_attributes.copy()

    try:
        metadata = full_site_metadata[site.upper()]
    except KeyError:
        pass
    else:
        network_key = next(iter(metadata))
        site_metadata = metadata[network_key]
        attributes["station_latitude"] = str(site_metadata["latitude"])
        attributes["station_longitude"] = str(site_metadata["longitude"])
        attributes["station_long_name"] = site_metadata["long_name"]
        attributes["station_height_masl"] = site_metadata["height_station_masl"]

    attributes["inlet_height_magl"] = format_inlet(inlet, key_name="inlet_height_magl")
    attributes["comment"] = crds_metadata["comment"]
    attributes["long_name"] = site_attributes["gcwerks_site_name"]

    return attributes
