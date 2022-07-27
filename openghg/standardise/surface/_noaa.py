from pathlib import Path
from typing import Dict, Optional, Union
import xarray as xr
import numpy as np


def parse_noaa(
    data_filepath: Union[str, Path],
    site: str,
    measurement_type: str,
    inlet: Optional[str] = None,
    network: str = "NOAA",
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
) -> Dict:
    """Read NOAA data from raw text file or ObsPack NetCDF

    Args:
        data_filepath: Data filepath
        site: Three letter site code
        inlet: Inlet height, if no height use measurement type e.g. flask
        measurement_type: One of ("flask", "insitu", "pfp")
        network: Network, defaults to NOAA
        instrument: Instrument name
        sampling_period: Sampling period
    Returns:
        dict: Dictionary of data and metadata
    """
    if sampling_period is None:
        sampling_period = "NOT_SET"

    sampling_period = str(sampling_period)

    file_extension = Path(data_filepath).suffix

    if file_extension == ".nc":
        return _read_obspack(
            data_filepath=data_filepath,
            site=site,
            inlet=inlet,
            measurement_type=measurement_type,
            instrument=instrument,
            sampling_period=sampling_period,
        )
    else:
        return _read_raw_file(
            data_filepath=data_filepath,
            site=site,
            inlet=inlet,
            measurement_type=measurement_type,
            instrument=instrument,
            sampling_period=sampling_period,
        )


def _format_inlet(inlet: Union[str, float, int]) -> str:
    """
    Output inlet name in expected format. This will include 1 decimal place for floats with a fractional component
    and 0 decimal places otherwise.

    Args:
        inlet : Inlet value. This can be a string as long as this can be converted directly to a float

    Returns:
        str : Formatted string of inlet name

    Examples:
        >>> _format_inlet(40)
        "40m"
        >>> _format_inlet("10.0")
        "10m"
        >>> _format_inlet(2.511432)
        "2.5m"
    """
    if isinstance(inlet, str):
        try:
            inlet_value: Union[float, np.floating] = float(inlet)
        except ValueError:
            inlet_values_str = inlet.split("-")
            try:
                inlet_values = [float(value) for value in inlet_values_str]
            except ValueError:
                raise ValueError("Unable to convert inlet {inlet} to a float")
            else:
                inlet_value = np.mean(inlet_values)
    else:
        inlet_value = inlet

    if inlet_value % 1 == 0:
        inlet_str = f"{inlet_value:.0f}m"
    else:
        inlet_str = f"{inlet_value:.1f}m"

    return inlet_str


def _standarise_variables(obspack_ds: xr.Dataset, species: str) -> xr.Dataset:
    """
    Converts data from NOAA ObsPack dataset into our standardised variables to be stored within the object store.
    The species is also needed so this name can be used to label the variables in the new dataset.

    Expects inputs with: "value", "value_std_dev" or "value_unc", "nvalue" as per NOAA ObsPack standard.

    Args:
        obspack_ds : Dataset derived from a netcdf file within the NOAA obspack
        species : Standardised species name (e.g. "ch4")

    Returns:
        Dataset : Standardised dataset with variables extracted and renamed

    Example output:
        For species = "ch4":
            xarray.Dataset("ch4":[...]
                           "ch4_variability":[...]
                           "ch4_number_of_observations": [...])
    """

    processed_ds = obspack_ds.copy()

    # Rename variables to match our internal standard
    # "value_std_dev" --> f"{species}_variability"
    # "value_unc" --> ??
    # TODO: Clarify what "value_unc" should be renamed to

    variable_names = {
        "value": species,
        "value_std_dev": f"{species}_variability",
        "value_unc": f"{species}_variability",  # May need to be updated
        "nvalue": f"{species}_number_of_observations",
    }

    to_extract = [name for name in variable_names.keys() if name in obspack_ds]

    # For the error variables we only want to take one set of values from the
    # obspack dataset but multiple variables may be available.
    # If multiple are present, combine these together and only extract one
    error_names = ["value_std_dev", "value_unc"]
    error_variables = [name for name in error_names if name in to_extract]

    if len(error_variables) > 1:
        main_ev = error_variables[0]  # Treat first item in the list at the one to keep

        history_attr = "history"
        processed_ds[main_ev].attrs[history_attr] = f"Merged {main_ev} variable from original file with "

        for ev in error_variables[1:]:
            # Combine details from additional additional error variable with main variable
            variable = processed_ds[main_ev]
            new_variable = processed_ds[ev]

            # Update Dataset and add details within attributes
            updated_variable = variable.combine_first(new_variable)
            processed_ds[main_ev] = updated_variable
            processed_ds[main_ev].attrs[history_attr] += f"{ev}, "

            # Remove this extra variables from the list of variables to extract from the dataset
            to_extract.remove(ev)

    # Create dictionary of names to convert obspack ds to our format
    name_dict = {name: key for name, key in variable_names.items() if name in to_extract}

    if not to_extract:
        wanted = variable_names.keys()
        raise ValueError(
            f"No valid data variables columns found in obspack dataset. We expect the following data variables in the passed NetCDF: {wanted}"
        )

    # Grab only the variables we want to keep and rename these
    processed_ds = processed_ds[to_extract]
    processed_ds = processed_ds.rename(name_dict)
    processed_ds = processed_ds.sortby("time")

    return processed_ds


def _split_inlets(
    obspack_ds: xr.Dataset, attributes: Dict, metadata: Dict, inlet: Optional[str] = None
) -> Dict:
    """
    Splits the overall dataset by different inlet values, if present. The expected dataset input should be from the NOAA ObsPack.

    Args:
        obspack_ds : Dataset derived from a netcdf file within the NOAA obspack
        attributes: Attributes extracted from the NOAA obspack. Should contain at least "species" and "measurement_type"
        metadata: Metadata to store alongside standardised data

    Returns:
        Dict: gas data containing "data", "metadata", "attributes" for each inlet

    Example output:
        {"ch4": {"data": xr.Dataset(...), "attributes": {...}, "metadata": {...}}}
        or
        {"ch4_40m": {"data": xr.Dataset(...), "attributes": {...}, "metadata": {...}}, "ch4_60m": {...}, ...}

    """

    orig_attrs = obspack_ds.attrs
    species = attributes["species"]
    measurement_type = attributes["measurement_type"]

    height_var = "intake_height"

    # Check whether the input data contains different inlet height values for each data point ("intake_height" data variable)
    # If so we need to select the data for each inlet and indicate this is a separate Datasource
    # Each data key is labelled based on the species and the inlet (if needed)

    gas_data: Dict[str, Dict] = {}
    if height_var in obspack_ds.data_vars:

        if inlet is not None:
            # TODO: Add to logging?
            print(
                f"WARNING: Ignoring inlet value of {inlet} since file has each data point has an associated height (contains 'intake_height' variable)"
            )

        # Group dataset by the height values
        # Note: could use ds.groupby_bins(...) if necessary if there are lots of small height differences to group these
        obspack_ds_grouped = obspack_ds.groupby(height_var)
        num_groups = len(obspack_ds_grouped.groups)

        # For each group standardise and store with id based on species and inlet height
        for ht, obspack_ds_ht in obspack_ds_grouped:

            # Creating id keys of the form "<species>_<inlet>" e.g. "ch4_40m" or "co_12.5m"
            inlet_str = _format_inlet(ht)
            inlet_num_str = inlet_str.strip("m")

            if num_groups > 1:
                id_key = f"{species}_{inlet_str}"
            else:
                id_key = f"{species}"

            # Extract wanted variables and convert to standardised names
            standarised_ds = _standarise_variables(obspack_ds_ht, species)

            gas_data[id_key] = {}
            gas_data[id_key]["data"] = standarised_ds

            # Add inlet details to attributes and metadata
            attrs_copy = attributes.copy()
            meta_copy = metadata.copy()

            attrs_copy["inlet"] = inlet_str
            attrs_copy["inlet_height_magl"] = inlet_num_str
            meta_copy["inlet"] = inlet_str
            meta_copy["inlet_height_magl"] = inlet_num_str

            gas_data[id_key]["metadata"] = meta_copy
            gas_data[id_key]["attributes"] = attrs_copy

    else:
        try:
            inlet_value = orig_attrs["dataset_intake_ht"]
        except KeyError:
            inlet_from_file = None
        else:
            inlet_from_file = _format_inlet(inlet_value)

        if measurement_type == "flask":
            inlet_from_file = "flask"

        # Check inlet from file against any provided inlet
        if inlet is None and inlet_from_file:
            inlet = inlet_from_file
        elif inlet is not None and inlet_from_file:
            if inlet != inlet_from_file:
                print(
                    f"WARNING: Provided inlet {inlet} does not match inlet derived from the input file: {inlet_from_file}"
                )
        else:
            raise ValueError(
                "Unable to derive inlet from NOAA file. Please pass as an input. If flask data pass 'flask' as inlet."
            )

        id_key = f"{species}"

        if inlet != "flask":
            inlet_num_str = inlet.strip("m")
        else:
            inlet_num_str = ""

        metadata["inlet"] = inlet
        metadata["inlet_height_magl"] = inlet_num_str
        attributes["inlet"] = inlet
        attributes["inlet_height_magl"] = inlet_num_str

        standardised_ds = _standarise_variables(obspack_ds, species)

        gas_data[id_key] = {"data": standardised_ds, "metadata": metadata, "attributes": attributes}

    return gas_data


def _read_obspack(
    data_filepath: Union[str, Path],
    site: str,
    sampling_period: str,
    measurement_type: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
) -> Dict[str, Dict]:
    """Read NOAA ObsPack NetCDF files

    Args:
        data_filepath: Path to file
        site: Three letter site code
        sampling_period: Sampling period
        measurement_type: One of flask, insitu or pfp
        inlet: Inlet height, if no height use measurement type e.g. flask
        instrument: Instrument name

    Returns:
        dict: Dictionary of results
    """
    from openghg.util import clean_string
    from openghg.standardise.meta import assign_attributes

    valid_types = ("flask", "insitu", "pfp")

    if measurement_type not in valid_types:
        raise ValueError(f"measurement_type must be one of {valid_types}")

    with xr.open_dataset(data_filepath) as temp:
        obspack_ds = temp
        orig_attrs = temp.attrs

    # Want to find and drop any duplicate time values for the original dataset
    # Using xarray directly we have to do in a slightly convoluted way as this is not well built
    # into the xarray workflow yet - https://github.com/pydata/xarray/pull/5239
    # - can use da.drop_duplicates() but only on one variable at a time and not on the whole Dataset
    # This method keeps attributes for each of the variables including units

    # The dimension within the original dataset is called "obs" and has no associated coordinates
    # Extract time from original Dataset (dimension is "obs")
    time = obspack_ds.time

    # To keep associated "obs" dimension, need to assign coordinate values to this (just 0, len(obs))
    time = time.assign_coords(obs=obspack_ds.obs)

    # Make "time" the primary dimension (while retaining "obs") and add "time" values as coordinates
    time = time.swap_dims(dims_dict={"obs": "time"})
    time = time.assign_coords(time=time)

    # Drop any duplicate time values and extract the associated "obs" values
    # TODO: Work out what to do with duplicates - may be genuine multiple measurements
    time_unique = time.drop_duplicates(dim="time", keep="first")
    obs_unique = time_unique.obs

    # Use these obs values to filter the original dataset to remove any repeated times
    processed_ds = obspack_ds.sel(obs=obs_unique)
    processed_ds = processed_ds.set_coords(["time"])

    # Estimate sampling period using metadata and midpoint time
    if sampling_period == "NOT_SET":
        sampling_period_estimate = _estimate_sampling_period(obspack_ds)
    else:
        sampling_period_estimate = -1.0

    species = clean_string(obspack_ds.attrs["dataset_parameter"])
    network = "NOAA"

    try:
        # Extract units attribute from value data variable
        units = processed_ds["value"].units
    except (KeyError, AttributeError):
        print("Unable to extract units from 'value' within input dataset")

    metadata = {}
    metadata["site"] = site
    metadata["network"] = network
    metadata["measurement_type"] = measurement_type
    metadata["species"] = species
    metadata["units"] = units
    metadata["sampling_period"] = sampling_period

    # Add additional sampling_period_estimate if sampling_period is not set
    if sampling_period_estimate >= 0.0:
        metadata["sampling_period_estimate"] = str(
            sampling_period_estimate
        )  # convert to string to keep consistent with "sampling_period"

    # Add instrument if present
    if instrument is not None:
        metadata["instrument"] = instrument
    else:
        metadata["instrument"] = orig_attrs.get("instrument", "NOT_SET")

    # Add data owner details, station position and calibration scale, if present
    metadata["data_owner"] = orig_attrs.get("provider_1_name", "NOT_SET")
    metadata["data_owner_email"] = orig_attrs.get("provider_1_email", "NOT_SET")
    metadata["station_longitude"] = orig_attrs.get("site_longitude", "NOT_SET")
    metadata["station_latitude"] = orig_attrs.get("site_latitude", "NOT_SET")
    metadata["calibration_scale"] = orig_attrs.get("dataset_calibration_scale", "NOT_SET")

    # Create attributes with copy of metadata values
    attributes = metadata.copy()

    # TODO: At the moment all attributes from the NOAA ObsPack are being copied
    # plus any variables we're adding - decide if we want to reduce this
    attributes.update(orig_attrs)

    # expected_keys = {
    #     "site",
    #     "species",
    #     "inlet",
    #     "instrument",
    #     "sampling_period",
    #     "calibration_scale",
    #     "station_longitude",
    #     "station_latitude",
    # }

    gas_data = _split_inlets(processed_ds, attributes, metadata, inlet=inlet)

    gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data


def _read_raw_file(
    data_filepath: Union[str, Path],
    site: str,
    sampling_period: str,
    measurement_type: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
) -> Dict:
    """Reads NOAA data files and returns a dictionary of processed
    data and metadata.

    Args:
        data_filepath: Path of file to load
        site: Site name
        sampling_period: Sampling period
        measurement_type: One of flask, insitu or pfp
        inlet: Inlet height, if no height use measurement type e.g. flask
        instrument: Instrument name

    Returns:
        list: UUIDs of Datasources data has been assigned to
    """
    from openghg.standardise.meta import assign_attributes

    # TODO: Added this for now to make sure inlet is specified but may be able to remove
    # if this can be derived from the data format.
    if inlet is None:
        raise ValueError("Inlet must be specified to derive data from NOAA raw (txt) files.")

    data_filepath = Path(data_filepath)
    filename = data_filepath.name

    species = filename.split("_")[0].lower()

    source_name = data_filepath.stem
    source_name = source_name.split("-")[0]

    gas_data = _read_raw_data(
        data_filepath=data_filepath,
        inlet=inlet,
        species=species,
        measurement_type=measurement_type,
        sampling_period=sampling_period,
    )

    gas_data = assign_attributes(data=gas_data, site=site, network="NOAA")

    return gas_data


def _read_raw_data(
    data_filepath: Path,
    species: str,
    inlet: str,
    sampling_period: str,
    measurement_type: str = "flask",
) -> Dict:
    """Separates the gases stored in the dataframe in
    separate dataframes and returns a dictionary of gases
    with an assigned UUID as gas:UUID and a list of the processed
    dataframes

    Args:
        data_filepath: Path of datafile
        species: Species string such as CH4, CO
        measurement_type: Type of measurements e.g. flask
    Returns:
        dict: Dictionary containing attributes, data and metadata keys
    """
    from openghg.util import clean_string, read_header, load_json
    from pandas import read_csv, Timestamp

    header = read_header(filepath=data_filepath)

    column_names = header[-1][14:].split()

    def date_parser(year: str, month: str, day: str, hour: str, minute: str, second: str) -> Timestamp:
        return Timestamp(year, month, day, hour, minute, second)

    date_parsing = {
        "time": [
            "sample_year",
            "sample_month",
            "sample_day",
            "sample_hour",
            "sample_minute",
            "sample_seconds",
        ]
    }

    data_types = {
        "sample_year": int,
        "sample_month": int,
        "sample_day": int,
        "sample_hour": int,
        "sample_minute": int,
        "sample_seconds": int,
    }

    # Number of header lines to skip
    n_skip = len(header)

    data = read_csv(
        data_filepath,
        skiprows=n_skip,
        names=column_names,
        sep=r"\s+",
        dtype=data_types,
        parse_dates=date_parsing,
        date_parser=date_parser,
        index_col="time",
        skipinitialspace=True,
    )

    # Drop duplicates
    data = data.loc[~data.index.duplicated(keep="first")]

    # Check if the index is sorted
    if not data.index.is_monotonic_increasing:
        data = data.sort_index()

    # Read the site code from the Dataframe
    site = str(data["sample_site_code"][0]).upper()

    site_data = load_json("acrg_site_info.json")
    # If this isn't a site we recognize try and read it from the filename
    if site not in site_data:
        site = str(data_filepath.name).split("_")[1].upper()

        if site not in site_data:
            raise ValueError(f"The site {site} is not recognized.")

    if species is not None:
        # If we're passed a species ensure that it is in fact the correct species
        data_species = str(data["parameter_formula"].values[0]).lower()

        passed_species = species.lower()
        if data_species != passed_species:
            raise ValueError(
                f"Mismatch between passed species ({passed_species}) and species read from data ({data_species})"
            )

    species = species.upper()

    flag = []
    selection_flag = []
    for flag_str in data.analysis_flag:
        flag.append(flag_str[0] == ".")
        selection_flag.append(int(flag_str[1] != "."))

    combined_data = {}

    data[species + "_status_flag"] = flag
    data[species + "_selection_flag"] = selection_flag

    data = data[data[species + "_status_flag"]]

    data = data[
        [
            "sample_latitude",
            "sample_longitude",
            "sample_altitude",
            "analysis_value",
            "analysis_uncertainty",
            species + "_selection_flag",
        ]
    ]

    rename_dict = {
        "analysis_value": species,
        "analysis_uncertainty": species + "_repeatability",
        "sample_longitude": "longitude",
        "sample_latitude": "latitude",
        "sample_altitude": "altitude",
    }

    data = data.rename(columns=rename_dict, inplace=False)
    data = data.to_xarray()

    # TODO  - this could do with a better name
    noaa_params = load_json("attributes.json")["NOAA"]

    site_attributes = noaa_params["global_attributes"]
    site_attributes["inlet_height_magl"] = "NA"
    site_attributes["instrument"] = noaa_params["instrument"][species.upper()]
    site_attributes["sampling_period"] = sampling_period

    metadata = {}
    metadata["species"] = clean_string(species)
    metadata["site"] = site
    metadata["measurement_type"] = measurement_type
    metadata["network"] = "NOAA"
    metadata["inlet"] = inlet
    metadata["sampling_period"] = sampling_period
    metadata["instrument"] = noaa_params["instrument"][species.upper()]

    combined_data[species.lower()] = {
        "metadata": metadata,
        "data": data,
        "attributes": site_attributes,
    }

    return combined_data


def _estimate_sampling_period(obspack_ds: xr.Dataset, min_estimate: float = 10.0) -> float:
    """
    Estimate the sampling period for the NOAA data using either the "data_selection_tag"
    attribute (this sometimes contains useful information such as "HourlyData") or by using
    the midpoint_time within the data itself.

    Note: midpoint_time often seems to match start_time implying instantaneous measurement
    or that this value has not been correctly included.

    If the estimate is less than `min_estimate` the estimate sampling period will be set to
    this value.

    Args:
        obspack_ds : Dataset of raw obs pack file opened as an xarray Dataset
        min_estimate : Minimum sampling period estimate to use in seconds.

    Returns:
        int: Seconds for the estimated sampling period.
    """
    # Check useful attributes
    data_selection = obspack_ds.attrs["dataset_selection_tag"]

    hourly_s = 60 * 60
    daily_s = hourly_s * 24
    weekly_s = daily_s * 7
    monthly_s = weekly_s * 28  # approx
    yearly_s = daily_s * 365  # approx

    sampling_period_estimate = 0.0  # seconds

    frequency_keywords = {
        "hourly": hourly_s,
        "daily": daily_s,
        "weekly": weekly_s,
        "monthly": monthly_s,
        "yearly": yearly_s,
    }
    for freq, time_s in frequency_keywords.items():
        if freq in data_selection.lower():
            sampling_period_estimate = time_s

    if not sampling_period_estimate:
        if "start_time" in obspack_ds and "midpoint_time" in obspack_ds:
            half_sample_time = (obspack_ds["midpoint_time"] - obspack_ds["start_time"]).values
            half_sample_time_s = half_sample_time.astype("timedelta64[s]").mean().astype(float)
            sampling_period_estimate = round(half_sample_time_s * 2, 1)

    if sampling_period_estimate < min_estimate:
        sampling_period_estimate = min_estimate

    return sampling_period_estimate
