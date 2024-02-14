from pathlib import Path
from typing import Dict, List, Optional, Union
from pandas import DataFrame
import xarray as xr

from openghg.types import optionalPathType


def find_files(
    data_path: Union[str, Path], skip_str: Union[str, List[str]] = "sf6"
) -> List[Path]:
    """A helper file to find new format GCWERKS .nc files in a given folder.
    The files are of the format AGAGE-GCMS-Medusa_SITE_species.nc, replacing the two .C data and precision files. 

    Please note the limited scope of this function, it will only work with
    files that are named in the correct pattern.

    Args:
        data_path: Folder path to search
        skip_str: String or list of strings, if found in filename these files are skipped
    Returns:
        list: Sorted list of filepaths
    """
    import re
    from pathlib import Path

    data_path = Path(data_path)

    files = data_path.glob("*.nc")

    if not isinstance(skip_str, list):
        skip_str = [skip_str]

    # Set the regex to match standard AGAGE data formats

    data_regex = re.compile(r"AGAGE-GCMS-Medusa+\_+[\w]+\_+[\w-]+.nc")

    data_nc_files = []

    for file in files:
        data_match = data_regex.match(file.name)

        if data_match:
            data_filepath = data_path / data_match.group()

            if any(s in data_match.group() for s in skip_str):
                continue

            if data_filepath.exists():
                data_nc_files.append(data_filepath)

    data_nc_files.sort()

    return data_nc_files


def parse_gcwerks_nc(
    data_filepath: Union[str, Path],
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
) -> Dict:
    """Reads a GC data file by creating a GC object and associated datasources

    Args:
        data_filepath: Path of .nc data file
        site: Three letter code or name for site
        instrument: Instrument name
        network: Network name
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        dict: Dictionary of source_name : UUIDs
    """
    from pathlib import Path

    from openghg.standardise.meta import assign_attributes
    from openghg.util import clean_string, load_internal_json

    data_filepath = Path(data_filepath)

    # Do some setup for processing
    # Load site data
    gcwerks_data = load_internal_json(filename="process_gcwerks_parameters.json")
    gc_params = gcwerks_data["GCWERKS"]

    network = clean_string(network)
    # We don't currently do anything with inlet here as it's always read from data
    # or taken from process_gcwerks_parameters.json
    if inlet is not None:
        inlet = clean_string(inlet)
    if instrument is not None:
        instrument = clean_string(instrument)

    # If we're not passed the instrument name and we can't find it raise an error
    if instrument is None:
        instrument = _check_instrument(filepath=data_filepath, gc_params=gc_params, should_raise=True)
    else:
        fname_instrument = _check_instrument(filepath=data_filepath, gc_params=gc_params, should_raise=False)

        if fname_instrument is not None and instrument != fname_instrument:
            raise ValueError(
                f"Mismatch between instrument passed as argument {instrument} and instrument read from filename {fname_instrument}"
            )

    instrument = str(instrument)

    gas_data = _read_data(
        data_filepath=data_filepath,
        site=site,
        instrument=instrument,
        network=network,
        sampling_period=sampling_period,
        gc_params=gc_params,
    )

    # Assign attributes to the data for CF compliant NetCDFs
    gas_data = assign_attributes(
        data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
    )

    return gas_data


def _check_instrument(filepath: Path, gc_params: Dict, should_raise: bool = False) -> Union[str, None]:
    """Ensure we have the correct instrument or translate an instrument
    suffix to an instrument name.

    Args:
        instrument_suffix: Instrument suffix such as md
        should_raise: Should we raise if we can't find a valid instrument
        gc_params: GCWERKS parameters
    Returns:
        str: Instrument name
    """

    instrument: str = filepath.name.split("_")[0].split("-", 1)[1].lower()
    try:
        if instrument in gc_params["instruments"]:
            return instrument
        else:
            try:
                instrument = gc_params["suffix_to_instrument"][instrument]
            except KeyError:
                if "medusa" in instrument:
                    instrument = "medusa"
                else:
                    raise KeyError(f"Invalid instrument {instrument}")
    except KeyError:
        if should_raise:
            raise
        else:
            return None

    return instrument


def _read_data(
    data_filepath: Path,
    site: str,
    instrument: str,
    network: str,
    gc_params: Dict,
    sampling_period: Optional[str] = None,
) -> Dict:
    """Read data from the .nc data files

    Args:
        data_filepath: Path of data file
        site: Name of site
        instrument: Instrument name
        network: Network name
        gc_params: GCWERKS parameters
        sampling_period: Period over which the measurement was samplied.
    Returns:
        dict: Dictionary of gas data keyed by species
    """
    from pandas import Timedelta as pd_Timedelta
    from pandas import to_timedelta as pd_to_timedelta
    from openghg.standardise.meta import define_species_label
    from openghg.util import load_internal_json
    from numpy import floor

    # Extract the species name from the filename, which has format {instrument}_{site}_{species}_{version}.nc as of Feb 2024 code retreat

    species = str(data_filepath).split(sep="_")[-2]
    species = define_species_label(species)[0]

    dataset = xr.load_dataset(data_filepath)

    data = dataset.to_dataframe()

    if data.empty:
        raise ValueError("Cannot process empty file.")

    # The .nc files have the DateTime object as an index already, just needs renaming

    data.index.name = "Datetime"

    # This metadata will be added to when species are split and attributes are written
    metadata: Dict[str, str] = {
        "instrument": instrument,
        "site": site,
        "network": network,
    }

    extracted_sampling_period = _get_sampling_period(instrument=instrument, gc_params=gc_params)
    metadata["sampling_period"] = extracted_sampling_period

    if sampling_period is not None:
        # Compare input to definition within json file
        file_sampling_period_td = pd_Timedelta(seconds=float(extracted_sampling_period))
        sampling_period_td = pd_Timedelta(seconds=float(sampling_period))
        comparison_seconds = abs(sampling_period_td - file_sampling_period_td).total_seconds()
        tolerance_seconds = 1

        if comparison_seconds > tolerance_seconds:
            raise ValueError(
                f"Input sampling period {sampling_period} does not match to value "
                f"extracted from the file name of {metadata['sampling_period']} seconds."
            )

    units = {}
    scale = {}

    # this is a horrible bit of code but it should work. But it can't pick out
    # if there are multiple names for the units (e.g. ppt vs pmol mol-1). Currently
    # just picks out the first one.

    species_attributes = load_internal_json(filename="attributes.json")
    if dataset.units in species_attributes["unit_interpret"].values():
        for key, value in species_attributes["unit_interpret"].items():
            if dataset.units == value:
                units[species] = key
                break
    else:
        units[species] = dataset.units

    # this line just copies over Matt's units, which are 1e-12-format.

    # units[species] = dataset.units

    scale[species] = dataset.calibration_scale

    # These .nc files do not have flags attached to them.

    # The precisions are a variable in the xarray dataset, and so a column in the dataframe. Note that there is only one species per netCDF file here as well.

    data["mf_repeatability"] = data["mf_repeatability"].astype(float)

    # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
    # Do this based on the sampling_period recording in the file (can be time-varying)
    # For GC-MD data the sampling_period is recorded as 1 second, but this is really instantaneous
    # so use floor to leave these timestamps unchanged
    data["new_time"] = data.index - pd_to_timedelta(floor(data.sampling_period / 2), unit="s")

    data = data.set_index("new_time", inplace=False, drop=True)
    data.index.name = "time"

    gas_data = _format_species(
        data=data,
        site=site,
        species=species,
        instrument=instrument,
        metadata=metadata,
        units=units,
        scale=scale,
        gc_params=gc_params,
    )
    return gas_data


def _format_species(
    data: DataFrame,
    site: str,
    instrument: str,
    species: str,
    metadata: Dict,
    units: Dict,
    scale: Dict,
    gc_params: Dict,
) -> Dict:
    """Formats the dataframes and splits up by species_inlet combination to be stored within individual Datasources.
    Note that because .nc files contain only a single species, this function is no longer called _split_species

    Args:
        data: DataFrame of raw data
        site: Name of site from which this data originates
        instrument: Name of instrument
        species: species in data
        metadata: Dictionary of metadata
        units: Dictionary of units for each species
        scale: Dictionary of scales for each species
        gc_params: GCWERKS parameter dictionary
    Returns:
        dict: Dictionary of gas data and metadata, paired by species_inlet combination (so for a single inlet this is just a single entry)
    """
    from fnmatch import fnmatch

    from addict import Dict as aDict
    from openghg.util import format_inlet
    from openghg.standardise.meta import define_species_label

    # Read inlets from the parameters
    expected_inlets = _get_inlets(site_code=site, gc_params=gc_params)

    # data_inlets is a list of unique inlets for this species
    try:
        data_inlets = data["inlet_height"].unique().tolist()
    except KeyError:
        raise KeyError(
            "Unable to read inlets from data, please ensure this data is of the GC type expected by this standardise module"
        )

    # inlet heights are just the numbers here in Matt's files, rather than having the units attached.

    data_inlets = [format_inlet(i) for i in data_inlets]

    # Skip this species if the data is all NaNs
    if data["mf"].isnull().all():
        raise ValueError(f"All values for this species {species} is null")

    combined_data = aDict()

    # Here inlet is the inlet in the data and inlet_label is the label we want to use as metadata
    for inlet, inlet_label in expected_inlets.items():  # iterates through the two pairs above
        inlet_label = format_inlet(inlet_label)

        # Create a copy of metadata for local modification and give it the species-specific metadata

        species_metadata = metadata.copy()
        species_metadata["units"] = units[species]

        species_metadata["calibration_scale"] = scale[species]

        # If we've only got a single inlet, pick out the mf and mf_repeatability
        if inlet == "any" or inlet == "air":
            species_data = data[["mf", "mf_repeatability"]]
            species_data = species_data.dropna(axis="index", how="any")
            species_metadata["inlet"] = inlet_label

        elif "date" in inlet:
            dates = inlet.split("_")[1:]  # this is the two dates in the string; only true for Shangdianzi
            data_sliced = data.loc[
                dates[0] : dates[1]
            ]  # this slices up the dataframe between these two dates
            species_data = data_sliced[["mf", "mf_repeatability"]]
            species_data = species_data.dropna(axis="index", how="any")
            species_metadata["inlet"] = inlet_label

        else:  # this is when there are multiple inlets;
            # Find the inlet(s) corresponding to inlet

            matching_inlets = [i for i in data_inlets if fnmatch(i, inlet)]
            if not matching_inlets:
                continue
            # Only set the label in metadata when we have the correct label
            species_metadata["inlet"] = inlet_label
            # Take only data for this inlet from the dataframe
            inlet_data = data.loc[data["inlet_height"].apply(format_inlet).isin(matching_inlets)]

            species_data = inlet_data[["mf", "mf_repeatability"]]
            species_data = species_data.dropna(axis="index", how="any")

        # Check that the Dataframe has something in it
        if species_data.empty:
            continue

        attributes = _get_site_attributes(
            site=site, inlet=inlet_label, instrument=instrument, gc_params=gc_params
        )
        attributes = attributes.copy()

        # Create a standardised / cleaned species label
        comp_species = define_species_label(species)[0]

        # change the column names to {species} and {species} repeatability, which is what the get_obs_surface function expects
        species_data = species_data.rename(
            columns={"mf": f"{comp_species}", "mf_repeatability": f"{comp_species} repeatability"}
        )

        # We want an xarray Dataset
        species_data = species_data.to_xarray()

        # Add the cleaned species name to the metadata and alternative name if present
        species_metadata["species"] = comp_species
        species_metadata["data_type"] = "surface"

        if comp_species != species.lower() and comp_species != species.upper():
            species_metadata["species_alt"] = species

        # Reformat variables so they have lowercase and alphanumeric names
        to_rename = {}
        for var in species_data.variables:
            if species in var:
                new_name = var.replace(species, comp_species)
                new_name = new_name.replace("-", "")
                to_rename[var] = new_name

        species_data = species_data.rename(name_dict=to_rename)

        # As a single species may have measurements from multiple inlets we
        # use the species and inlet as a key
        data_key = f"{comp_species}_{inlet_label}"

        combined_data[data_key]["metadata"] = species_metadata
        combined_data[data_key]["data"] = species_data
        combined_data[data_key]["attributes"] = attributes

    to_return: Dict = combined_data.to_dict()

    return to_return


def _get_sampling_period(instrument: str, gc_params: Dict) -> str:
    """Process the suffix from the filename to get the correct instrument name
    then retrieve the sampling period of that instrument from metadata.

    Args:
        instrument: Instrument name
        gc_params: GCWERKS parameter dictionary
    Returns:
        str: Precision of instrument in seconds
    """
    instrument = instrument.lower()
    try:
        sampling_period = str(gc_params["sampling_period"][instrument])
    except KeyError:
        raise ValueError(
            f"Invalid instrument: {instrument}\nPlease select one of {gc_params['sampling_period'].keys()}\n"
        )

    return sampling_period


def _get_inlets(site_code: str, gc_params: Dict) -> Dict:
    """Get the inlets we expect to be at this site and create a
    mapping dictionary so we get consistent labelling.

    Args:
        site: Site code
        gc_params: GCWERKS parameters
    Returns:
        dict: Mapping dictionary of inlet and required inlet label
    """
    site = site_code.upper()
    site_params = gc_params["sites"]

    # Create a mapping of inlet to match to the inlet label
    inlets = site_params[site]["inlets"]
    try:
        inlet_labels = site_params[site]["inlet_label"]
    except KeyError:
        inlet_labels = inlets

    mapping_dict = {k: v for k, v in zip(inlets, inlet_labels)}

    return mapping_dict


def _get_site_attributes(site: str, inlet: str, instrument: str, gc_params: Dict) -> Dict[str, str]:
    """Gets the site specific attributes for writing to Datsets

    Args:
        site: Site code
        inlet: Inlet height in metres
        instrument: Instrument name
        gc_params: GCWERKS parameters
    Returns:
        dict: Dictionary of attributes
    """
    from openghg.util import format_inlet

    site = site.upper()
    instrument = instrument.lower()

    attributes: Dict[str, str] = gc_params["sites"][site]["global_attributes"]

    attributes["inlet_height_magl"] = format_inlet(inlet, key_name="inlet_height_magl")
    try:
        attributes["comment"] = gc_params["comment"][instrument]
    except KeyError:
        valid_instruments = list(gc_params["comment"].keys())
        raise KeyError(f"Invalid instrument {instrument} passed, valid instruments : {valid_instruments}")

    return attributes
