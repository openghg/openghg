from typing import Union, Optional, Dict, cast
from pathlib import Path
import xarray as xr


def parse_openghg(
    data_filepath: Union[str, Path],
    site: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    calibration_scale: Optional[str] = None,
    data_owner: Optional[str] = None,
    data_owner_email: Optional[str] = None,
    **kwargs: str,
) -> Dict:
    """
    Parse and extract data from pre-formatted netcdf file which already
    matches expected OpenGHG format.

    At the moment this must also be for a site known to OpenGHG. See
    'acrg_site_info.json' file.

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
        kwargs: Any additional attributes to be associated with the data.

    Returns:
        Dict : Dictionary of source_name : data, metadata, attributes
    """
    from openghg.util import clean_string, load_json, synonyms
    from openghg.standardise.meta import metadata_default_keys, assign_attributes

    data_filepath = Path(data_filepath)

    if data_filepath.suffix != ".nc":
        raise ValueError("Input file must be a .nc (netcdf) file.")

    data = xr.open_dataset(data_filepath)  # Change this to with statement?

    # Extract current attributes from input data
    attributes = data.attrs

    # Define metadata based on input arguments.
    metadata_initial = {
        "site": site,
        "species": species,
        "network": network,
        "inlet": inlet,
        "instrument": instrument,
        "sampling_period": sampling_period,
        "calibration_scale": calibration_scale,
        "data_owner": data_owner,
        "data_owner_email": data_owner_email,
    }

    # TODO: Decide if to allow any of these to be missed.

    # Populate metadata with values from attributes if inputs have not been passed
    for key, value in metadata_initial.items():
        if value is None:
            try:
                metadata_initial[key] = attributes[key]
            except KeyError:
                raise ValueError(f"Input '{key}' must be specified if not included in data attributes.")
        else:
            # If attributes are present, check these match to inputs passed
            if key in attributes:
                attributes_value = attributes[key]
                if value != attributes_value:
                    # If inputs do not match attribute values, raise a ValueError
                    raise ValueError(
                        f"Input for '{key}': {value} does not match value in file attributes: {attributes_value}"
                    )

    metadata = cast(Dict[str, str], metadata_initial)

    metadata["inlet_height_magl"] = metadata["inlet"]

    # Define remaining keys needed for metadata
    metadata_needed = metadata_default_keys()
    metadata_needed = [param for param in metadata_needed if param not in metadata]

    metadata["site"] = clean_string(metadata["site"])
    metadata["species"] = synonyms(metadata["species"]).lower()
    # May want to remove the .lower() here and centralise this

    # Update attributes to match metadata after cleaning
    attributes["site"] = metadata["site"]
    attributes["species"] = metadata["species"]

    site = metadata["site"]
    network = metadata["network"]
    species = metadata["species"]

    # Allow site and network data to be treated in a case insensitive way
    site_case_options = [site, site.upper(), site.lower()]
    network_case_options = [network, network.upper(), network.lower()]

    # Extract centralised data for site (if present)
    site_data = load_json(filename="acrg_site_info.json")
    for site_value in site_case_options:
        if site_value in site_data:
            site_info_all = site_data[site_value]
            break
    else:
        print("Unknown site. Will attempt to extract metadata from dataset attributes or input keywords")
        site_info_all = {}

    for network_value in network_case_options:
        if network_value in site_info_all:
            site_info = site_info_all[network_value]
            break
    else:
        print(
            "Network {network} does not match with site {site}. Will attempt to extract metadata from dataset attributes or input keywords"
        )
        site_info = {}

    if site_info:
        # Ensure keywords match to metadata names for station values
        # e.g. "station_longitude" derived from "longitude"
        for key in metadata_needed:
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
    param_data = load_json(filename="attributes.json")
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
    attribute_sources = [attributes, kwargs, site_info, site_attributes]

    # Search attributes sources (in order) and populate metadata
    for param in metadata_needed:
        for source in attribute_sources:
            if param in source:
                metadata[param] = source[param]
                break
        else:
            raise ValueError(
                f"Cannot extract or infer '{param}' parameter needed for metadata from stored data, attributes or keywords"
            )

    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data
