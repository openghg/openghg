from pathlib import Path

import pandas as pd
import re
import xarray as xr
from addict import Dict as aDict
from openghg.standardise.meta import (
    assign_attributes,
    define_species_label,
    attributes_default_keys,
    dataset_formatter,
)
from openghg.types import optionalPathType
from openghg.util import clean_string, format_inlet


def parse_agage(
    filepath: str | Path,
    site: str,
    network: str,
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    update_mismatch: str = "from_source",
    site_filepath: optionalPathType = None,
) -> dict:
    """Reads a GC data file by creating a GC object and associated datasources

    Args:
        filepath: Path of .nc data file
        site: Three letter code or name for site
        instrument: Instrument name
        network: Network name
        inlet: inlet name (optional)
        sampling_period: sampling period for this instrument. If not supplied, will be read from the file.
        measurement_type: measurement type
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
    filepath = Path(filepath)

    network = clean_string(network)
    instrument = clean_string(instrument)

    # get the parameters from the file metadata, as opposed to from the .json file

    with xr.load_dataset(filepath) as ds:
        file_params = ds.attrs

    # if we're not passed the instrument name, get it from the file:

    file_instrument = None

    if "instrument_type" in file_params.keys():
        # For multiple values in instrument_type of file updating the instrument metadata to multiple
        instrument_number = len(file_params["instrument_type"].split("/"))
        if instrument_number > 1:
            file_instrument = "multiple"
            instrument = file_instrument
        else:
            file_instrument = clean_string(file_params["instrument_type"])
            if instrument is None:
                instrument = file_instrument

    elif instrument is None:
        raise ValueError("No instrument found in file metadata. Please pass explicity as argument.")

    if instrument != "multiple":
        if file_instrument and instrument:
            if file_instrument != instrument:
                raise ValueError(
                    f"Instrument {instrument} passed does not match instrument {file_instrument} in file."
                )

    instrument = str(instrument)

    species = str(filepath).split(sep="_")[-2]
    species = define_species_label(species)[0]

    with xr.open_dataset(filepath) as dataset:
        data = dataset.to_dataframe()

        if data.empty:
            raise ValueError("Cannot process empty file.")

        # This metadata will be added to when species are split and attributes are written
        metadata: dict[str, str] = {
            "instrument": instrument,
            "site": site,
            "network": network,
        }

        metadata["instrument_name_0"] = clean_string(file_params["instrument"])

        # fetching all instrument_n values from the file
        pattern = re.compile(r"^instrument_(\d+)$")

        for key in file_params:
            match = pattern.match(key)
            if match:
                number = match.group(1)
                new_key = f"instrument_name_{number}"
                metadata[new_key] = file_params[key]

        # sampling period should be in the metadata of the openghg datasource as a single value.

        extracted_sampling_periods = data["sampling_period"].unique()
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

        gas_data = _format_species(
            data=data,
            species=species,
            metadata=metadata,
            units=units,
            scale=scale,
            file_params=file_params,
        )

        gas_data = dataset_formatter(data=gas_data)

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = assign_attributes(
            data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
        )

        return gas_data


def _format_species(
    data: pd.DataFrame,
    species: str,
    metadata: dict,
    units: str,
    scale: str,
    file_params: dict,
) -> dict:
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
    data_inlets = {i: format_inlet(i) for i in data_inlets}

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
        species_metadata["inlet_height_magl"] = inlet

        # want to select the data corresponding to each inlet

        inlet_data = data.loc[data["inlet_height"] == inlet]
        species_data = inlet_data[["mf", "mf_repeatability"]]
        species_data = species_data.dropna(axis="index", how="any")

        # Check that the Dataframe has something in it
        if species_data.empty:
            continue

        attributes = file_params.copy()
        # Need to stop the "instrument" attr from being overwritten in combined files
        if "instrument" in attributes.keys():
            attributes["instrument_name"] = attributes.pop("instrument")

        attribute_keys = attributes_default_keys()

        # JP hack to stop instrument getting overwritten for multi-instrument files
        # instrument = metadata["instrument"]

        for k, v in attributes.items():
            if k in attribute_keys:
                metadata[k] = v

        attributes["inlet_height_magl"] = species_metadata["inlet_height_magl"]

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

    to_return: dict = combined_data.to_dict()

    return to_return
