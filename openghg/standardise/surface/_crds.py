from openghg.util import load_json
from pandas import DataFrame, Timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, Union


def parse_crds(
    data_filepath: Union[str, Path],
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Creates a CRDS object holding data stored within Datasources

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

    # This may seem like an almost pointless function as this is all we do
    # but it makes it a lot easier to test assign_attributes
    gas_data = _read_data(
        data_filepath=data_filepath,
        site=site,
        network=network,
        inlet=inlet,
        instrument=instrument,
        sampling_period=sampling_period,
        measurement_type=measurement_type,
    )

    # Ensure the data is CF compliant
    gas_data = assign_attributes(data=gas_data, site=site, sampling_period=sampling_period)

    return gas_data


def _read_data(
    data_filepath: Path,
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Read the datafile passed in and extract the data we require.

    Args:
        data_filepath: Path to file
        site: Three letter site code
        network: Network name
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period including the unit (using pandas frequency aliases like '1H' or '1min')
        measurement_type: Measurement type e.g. insitu, flask
    Returns:
        dict: Dictionary of gas data
    """
    from pandas import RangeIndex, read_csv, to_datetime
    import warnings
    from openghg.util import clean_string

    split_fname = data_filepath.stem.split(".")
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

    # Catch dtype warnings
    # TODO - look at setting dtypes - read header and data separately?
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        data = read_csv(
            data_filepath,
            header=None,
            skiprows=1,
            sep=r"\s+",
            parse_dates={"time": [0, 1]},
            index_col="time",
        )

    # Drop any rows with NaNs
    # This is now done before creating metadata
    data = data.dropna(axis="rows", how="any")

    # Get the number of gases in dataframe and number of columns of data present for each gas
    n_gases, n_cols = _gas_info(data=data)

    header = data.head(2)
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    metadata = _read_metadata(filepath=data_filepath, data=data)

    if network is not None:
        metadata["network"] = network

    if sampling_period is not None:
        # Compare against value extracted from the file name
        file_sampling_period = Timedelta(seconds=metadata["sampling_period"])

        comparison_seconds = abs(sampling_period - file_sampling_period).total_seconds()
        tolerance_seconds = 1

        if comparison_seconds > tolerance_seconds:
            raise ValueError(
                f"Input sampling period {sampling_period} does not match to value "
                f"extracted from the file name of {metadata['sampling_period']} seconds."
            )

    # Read the scale from JSON
    # I'll leave this here for the possible future movement from class to functions
    network_metadata = load_json(filename="process_gcwerks_parameters.json")
    crds_metadata = network_metadata["CRDS"]

    # This dictionary is used to store the gas data and its associated metadata
    combined_data = {}

    for n in range(n_gases):
        # Slice the columns
        gas_data = data.iloc[:, skip_cols + n * n_cols : skip_cols + (n + 1) * n_cols]

        # Reset the column numbers
        gas_data.columns = RangeIndex(gas_data.columns.size)
        species = gas_data[0][0]
        species = species.lower()

        column_labels = [
            species,
            f"{species}_variability",
            f"{species}_number_of_observations",
        ]

        # Name columns
        gas_data = gas_data.set_axis(column_labels, axis="columns", inplace=False)

        header_rows = 2
        # Drop the first two rows now we have the name
        gas_data = gas_data.drop(index=gas_data.head(header_rows).index, inplace=False)
        gas_data.index = to_datetime(gas_data.index, format="%y%m%d %H%M%S")
        # Cast data to float64 / double
        gas_data = gas_data.astype("float64")

        # Here we can convert the Dataframe to a Dataset and then write the attributes
        gas_data = gas_data.to_xarray()

        site_attributes = _get_site_attributes(site=site, inlet=inlet, crds_metadata=crds_metadata)

        scale = crds_metadata["default_scales"].get(species.upper(), "NA")

        # Create a copy of the metadata dict
        species_metadata = metadata.copy()
        species_metadata["species"] = clean_string(species)
        species_metadata["inlet"] = inlet
        species_metadata["calibration_scale"] = scale
        species_metadata["long_name"] = site_attributes["long_name"]

        # Make sure metadata keys are included in attributes
        site_attributes.update(species_metadata)

        combined_data[species] = {
            "metadata": species_metadata,
            "data": gas_data,
            "attributes": site_attributes,
        }

    return combined_data


def _read_metadata(filepath: Path, data: DataFrame) -> Dict:
    """Parse CRDS files and create a metadata dict

    Args:
        filepath: Data filepath
        data: Raw pandas DataFrame
    Returns:
        dict: Dictionary containing metadata
    """
    # Find gas measured and port used
    type_meas = data[2][2]
    port = data[3][2]

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
        # sampling_period = "1min"
        sampling_period = 60
    elif sampling_period_str == "hourly":
        # sampling_period = "1H"
        sampling_period = 60 * 60
    else:
        raise ValueError("Unable to read sampling period from filename.")

    metadata = {}
    metadata["site"] = site
    metadata["instrument"] = instrument
    metadata["sampling_period"] = str(sampling_period)
    metadata["inlet"] = inlet
    metadata["port"] = port
    metadata["type"] = type_meas

    return metadata


def _get_site_attributes(site: str, inlet: str, crds_metadata: Dict) -> Dict:
    """Gets the site specific attributes for writing to Datsets

    Args:
        site: Site name
        inlet: Inlet height, example: 108m
        crds_metadata: General CRDS metadata
    Returns:
        dict: Dictionary of attributes
    """
    try:
        site_attributes: Dict = crds_metadata["sites"][site.upper()]
        global_attributes: Dict = site_attributes["global_attributes"]
    except KeyError:
        raise ValueError(f"Unable to read attributes for site: {site}")

    attributes = global_attributes.copy()

    attributes["inlet_height_magl"] = inlet
    attributes["comment"] = crds_metadata["comment"]
    attributes["long_name"] = site_attributes["gcwerks_site_name"]

    return attributes


def _gas_info(data: DataFrame) -> Tuple[int, int]:
    """Returns the number of columns of data for each gas
    that is present in the dataframe

    Args:
        data: Measurement data
    Returns:
        tuple (int, int): Number of gases, number of
        columns of data for each gas
    """
    from openghg.util import unanimous

    # Slice the dataframe
    head_row = data.head(1)

    gases: Dict[str, int] = {}
    # Loop over the gases and find each unique value
    for column in head_row.columns:
        s = head_row[column][0]
        if s != "-":
            gases[s] = gases.get(s, 0) + 1

    # Check that we have the same number of columns for each gas
    if not unanimous(gases):
        raise ValueError(
            "Each gas does not have the same number of columns. Please ensure data"
            "is of the CRDS type expected by this module"
        )

    return len(gases), list(gases.values())[0]
