from pathlib import Path
from typing import cast
import logging
import xarray as xr

from openghg.types import optionalPathType

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_openghg(
    filepath: str | Path,
    site: str | None = None,
    species: str | None = None,
    network: str | None = None,
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | None = None,
    calibration_scale: str | None = None,
    data_owner: str | None = None,
    data_owner_email: str | None = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    **kwargs: str,
) -> dict:
    """
    Parse and extract data from pre-formatted netcdf file which already
    matches expected OpenGHG format.

    At the moment this must also be for a site known to OpenGHG.
    See "site_info.json" file within the openghg_defs repository.

    The arguments specified below are the metadata needed to store this
    surface observation file within the object store. If these keywords are
    not included within the attributes of the netcdf file being passed then
    these arguments must be specified.

    Args:
        site: Site code/name e.g. "TAC" (for Tacolneston)
        species: Species name or synonym e.g. "ch4"
        network: Network name. e.g. "DECC"
        inlet: Height of inlet for observation. e.g. "10m"
        instrument: Instrument name used for measurement e.g. "gcmd".
            Can be set to "NA" if this is an unknown detail.
        sampling_period: Sampling period in pandas style
            (e.g. 2H for 2 hour period, 2m for 2 minute period).
        calibration_scale: Calibration scale used for measurements
            e.g. "WMOX2007"
        data_owner: Name of data owner.
        data_owner_email: Email address for data owner.
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        kwargs: Any additional attributes to be associated with the data.
    Returns:
        Dict: Dictionary of source_name : data, metadata, attributes
    """
    from openghg.util import clean_string, format_inlet, load_internal_json, get_site_info
    from openghg.standardise.meta import (
        attributes_default_keys,
        define_species_label,
        assign_attributes,
        dataset_formatter,
    )

    filepath = Path(filepath)

    try:
        data = xr.open_dataset(filepath)  # Change this to with statement?
    except ValueError as e:
        raise ValueError(f"Input file {filepath.name} could not be opened by xarray.") from e

    # Extract current attributes from input data
    attributes = data.attrs

    # Define metadata based on input arguments.
    metadata_initial = {
        "site": site,
        "species": species,
        "network": network,
        "instrument": instrument,
        "sampling_period": sampling_period,
        "calibration_scale": calibration_scale,
        "data_owner": data_owner,
        "data_owner_email": data_owner_email,
    }

    # Run some checks on the
    data_attrs = {k.lower().replace(" ", "_"): v for k, v in data.attrs.items()}

    # Populate metadata with values from attributes if inputs have not been passed
    for key, value in metadata_initial.items():
        if value is None:
            try:
                metadata_initial[key] = data_attrs[key]
            except KeyError:
                raise ValueError(f"Input '{key}' must be specified if not included in data attributes.")
        else:
            # If attributes are present, check these match to inputs passed
            if key in attributes:
                attributes_value = attributes[key]
                if str(value).lower() != str(attributes_value).lower():
                    try:
                        # As we may have things like 1200 != 1200.0
                        # we'll check if the floats are equal
                        if float(value) == float(attributes_value):
                            continue
                    except ValueError:
                        # If inputs do not match attribute values, raise a ValueError
                        raise ValueError(
                            f"Input for '{key}': {value} does not match value in file attributes: {attributes_value}"
                        )

    # Read the inlet
    if inlet is None:
        inlet_val = [v for k, v in data_attrs.items() if "inlet" in k]

        if not inlet_val:
            raise ValueError("Cannot read inlet from attributes, please pass as argument.")
        if len(set(inlet_val)) > 1:
            raise ValueError("More than one inlet value found in attributes, please pass as argument.")

        inlet = inlet_val[0]
        inlet = format_inlet(str(inlet))

    metadata_initial["inlet"] = inlet

    metadata = cast(dict[str, str], metadata_initial)

    metadata["inlet_height_magl"] = format_inlet(str(metadata["inlet"]), key_name="inlet_height_magl")
    metadata["data_type"] = "surface"

    # Define remaining keys needed for metadata
    attributes_needed = attributes_default_keys()
    attributes_needed = [param for param in attributes_needed if param not in metadata]

    metadata["site"] = clean_string(metadata["site"])
    metadata["species"] = define_species_label(metadata["species"])[0]

    # Update attributes to match metadata after cleaning
    attributes["site"] = metadata["site"]
    attributes["species"] = metadata["species"]

    if "inlet" in attributes:
        attributes["inlet_height_magl"] = str(attributes["inlet"]).strip("m")

    site = metadata["site"]
    network = metadata["network"]
    species = metadata["species"]

    # Allow site and network data to be treated in a case insensitive way
    site_case_options = [site, site.upper(), site.lower()]
    network_case_options = [network, network.upper(), network.lower()]

    # Extract centralised data for site (if present)
    site_data = get_site_info(site_filepath=site_filepath)
    for site_value in site_case_options:
        if site_value in site_data:
            site_info_all = site_data[site_value]
            break
    else:
        logger.info(
            "Unknown site. Will attempt to extract metadata from dataset attributes or input keywords"
        )
        site_info_all = {}

    for network_value in network_case_options:
        if network_value in site_info_all:
            site_info = site_info_all[network_value]
            break
    else:
        logger.info(
            "Network {network} does not match with site {site}. Will attempt to extract metadata from dataset attributes or input keywords"
        )
        site_info = {}

    if site_info:
        # Ensure keywords match to metadata names for station values
        # e.g. "station_longitude" derived from "longitude"
        for key in attributes_needed:
            prefix = "station_"
            if key.startswith(prefix):
                split_key = key.split("_")[1:]
                short_key_option1 = "_".join(split_key)

                split_key.insert(1, "station")  # to catch "height_station_masl"
                short_key_option2 = "_".join(split_key)

                short_key_options = [short_key_option1, short_key_option2]
                for short_key in short_key_options:
                    if short_key in site_info:
                        site_info[key] = site_info[short_key]
                        break

    # Load attributes data for network if present
    param_data = load_internal_json(filename="attributes.json")
    for network_value in network_case_options:
        if network_value in param_data:
            network_params = param_data[network_value]
            site_attributes = network_params["global_attributes"]
            break
    else:
        site_attributes = {}

    # Define sources of attributes to use when defining metadata
    # The order here influences the hierarchy if keys appear multiple times.
    # kwargs allow additional variables such as "station_longitude" to be included if needed.
    # 2023-04: Re-ordered to be - values explicitly passed, stored data (2), attributes from dataset
    # # attribute_sources = [attributes, kwargs, site_info, site_attributes]
    attribute_sources = [kwargs, site_info, site_attributes, attributes]

    # Search attributes sources (in order) and populate metadata
    for param in attributes_needed:
        for source in attribute_sources:
            if param in source:
                metadata[param] = source[param]
                break
        else:
            raise ValueError(
                f"Cannot extract or infer '{param}' parameter needed for metadata from stored data, attributes or keywords"
            )

    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    gas_data = dataset_formatter(data=gas_data)

    gas_data = assign_attributes(
        data=gas_data,
        site=site,
        network=network,
        update_mismatch=update_mismatch,
        site_filepath=site_filepath,
    )

    return gas_data
