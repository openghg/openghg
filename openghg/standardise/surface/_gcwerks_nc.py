from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import xarray as xr
from addict import Dict as aDict
from numpy import floor
from openghg.standardise.meta import assign_attributes, define_species_label, metadata_default_keys
from openghg.types import optionalPathType
from openghg.util import clean_string, format_inlet, load_internal_json


def find_files(data_path: Union[str, Path], skip_str: Union[str, List[str]] = "sf6") -> List[Path]:
    """A helper file to find new format GCWERKS .nc files in a given folder.
    The files are of the format agage_SITE_SPECIES_version.nc, replacing the two .C data and precision files.

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

    data_regex = re.compile(r"agage+\_+[\w]+\_+[\w-]+\_+[\w]+.nc")

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
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    update_mismatch: str = "from_source",
    site_filepath: optionalPathType = None,
) -> Dict:
    """Reads a GC data file by creating a GC object and associated datasources

    Args:
        data_filepath: Path of .nc data file
        site: Three letter code or name for site
        instrument: Instrument name
        network: Network name
        sampling_period: sampling period for this instrument. If not supplied, will be read from the file. 
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
    data_filepath = Path(data_filepath)

    network = clean_string(network)
    instrument = clean_string(instrument)

    # get the parameters from the file metadata, as opposed to from the .json file

    with xr.load_dataset(data_filepath) as ds:
        file_params = ds.attrs

    # if we're not passed the instrument name, get it from the file:

    if instrument is None:
        if "instrument" in file_params.keys():
            instrument = file_params["instrument"]
        else:
            raise ValueError(
                f"No instrument found in file metadata. Please pass explicity as argument."
            )

    instrument = str(instrument)

    species = str(data_filepath).split(sep="_")[-2]
    species = define_species_label(species)[0]

    with xr.open_dataset(data_filepath) as dataset:
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


        # sampling period should be in the metadata of the openghg datasource as a single value. 

        extracted_sampling_periods = data['sampling_period'].unique()
        if len(extracted_sampling_periods) == 1:
            extracted_sampling_period = extracted_sampling_periods[0]
            single_sampling_period = True
        else:
            extracted_sampling_period = "multiple"

        metadata["sampling_period"] = str(extracted_sampling_period)

        if sampling_period is not None:
            if single_sampling_period:
                # Compare input to file contents
                file_sampling_period_td = pd.Timedelta(seconds=float(extracted_sampling_period))
                sampling_period_td = pd.Timedelta(seconds=float(sampling_period))
                comparison_seconds = abs(sampling_period_td - file_sampling_period_td).total_seconds()
                tolerance_seconds = 1

                if comparison_seconds > tolerance_seconds:
                    raise ValueError(
                        f"Input sampling period {sampling_period} does not match to value "
                        f"extracted from the file name of {metadata['sampling_period']} seconds."
                    )

        units = dataset.mf.units
        scale = dataset.calibration_scale

        # These .nc files do not have flags attached to them.
        # The precisions are a variable in the xarray dataset, and so a column in the dataframe.
        # Note that there is only one species per netCDF file here as well.
        data["mf_repeatability"] = data["mf_repeatability"].astype(float)

        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        # Do this based on the sampling_period recording in the file (can be time-varying)
        # For GC-MD data the sampling_period is recorded as 1 second, but this is really instantaneous
        # so use floor to leave these timestamps unchanged
        data["new_time"] = data.index - pd.to_timedelta(floor(data.sampling_period / 2), unit="s")

        data = data.set_index("new_time", inplace=False, drop=True)
        data.index.name = "time"

        gas_data = _format_species(
            data=data,
            instrument=instrument,
            metadata=metadata,
            units=units,
            scale=scale,
            file_params=file_params,
        )

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = assign_attributes(
            data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
        )

        return gas_data


def _format_species(
    data: pd.DataFrame,
    species: str,
    metadata: Dict,
    units: str,
    scale: str,
    file_params: Dict,
) -> Dict:
    """Formats the dataframes and splits up by species_inlet combination to be stored within individual Datasources.
    Note that because .nc files contain only a single species, this function is no longer called _split_species

    Args:
        data: DataFrame of raw data
        species: species in data
        metadata: Dictionary of metadata
        units: units (e.g. 1e-12)
        scale: calibration scale used
        file_params: dictionary of metadata/attributes
    Returns:
        dict: Dictionary of gas data and metadata, paired by species_inlet combination (so for a single inlet this is just a single entry)
    """

    # data_inlets is a list of unique inlets for this species
    try:
        data_inlets = data["inlet_height"].unique().tolist()
    except KeyError:
        raise KeyError(
            "Unable to read inlets from data, please ensure this data is of the GC type expected by this standardise module"
        )

    # inlet heights are just the numbers here in Matt's files, rather than having the units attached.
    data_inlets = {i:format_inlet(i) for i in data_inlets}

    # Skip this species if the data is all NaNs
    if data["mf"].isnull().all():
        raise ValueError(f"All values for this species {species} is null")

    combined_data = aDict()

    for inlet, inlet_label in data_inlets.items():  # iterates through the two pairs above

        # Create a copy of metadata for local modification and give it the species-specific metadata
        species_metadata = metadata.copy()

        species_metadata["units"] = units
        species_metadata["calibration_scale"] = scale
        species_metadata["inlet"] = inlet_label

        # want to select the data corresponding to each inlet

        inlet_data = data.loc[data["inlet_height"] == inlet]
        species_data = inlet_data[["mf", "mf_repeatability"]]
        species_data = species_data.dropna(axis="index", how="any")

        # Check that the Dataframe has something in it
        if species_data.empty:
            continue

        attributes = file_params
        
        # need to rename the inlet height attribute:

        attributes["inlet_height_magl"] = file_params["inlet_base_elevation_masl"]

        metadata_keys = metadata_default_keys()

        for k, v in attributes.items():
            if k in metadata_keys:
                metadata[k] = v

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
