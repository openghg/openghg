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
    **kwargs: str,
    ) -> Dict:
    """
    Parse and extract data from pre-formatted netcdf file which already matches expected
    OpenGHG format.

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
    metadata = {"site": site,
                "species": species,
                "network": network,
                "inlet": inlet,
                "instrument": instrument,
                "sampling_period": sampling_period}

    # Populate metadata with values from attributes if inputs have not been passed
    for key, value in metadata.items():
        if value is None:
            try:
                metadata[key] = attributes[key]
            except KeyError:
                raise ValueError(f"Input '{key}' must be specified if not included in data attributes.")
        else:
            # If attributes are present, check these match to inputs passed
            if key in attributes:
                attributes_value = attributes[key]
                if value != attributes_value:
                    # If inputs do not match attribute values, raise a ValueError
                    raise ValueError(f"Input for '{key}': {value} does not match value in file attributes: {attributes_value}")

    metadata["inlet_height_magl"] = metadata["inlet"]

    # Define remaining keys needed for metadata
    metadata_needed = metadata_default_keys()
    metadata_needed = [param for param in metadata_needed if param not in metadata]

    metadata["site"] = clean_string(metadata["site"])
    metadata["species"] = synonyms(metadata["species"]).lower()  # May want to remove the .lower() here and centralise this

    # Update attributes to match metadata after cleaning
    attributes["site"] = metadata["site"]
    attributes["species"] = metadata["species"]
    
    site = metadata["site"]
    network = metadata["network"]
    species = metadata["species"]

    site_upper = site.upper()
    network_upper = network.upper()

    # Extract centralised data for site (if present)
    site_data = load_json(filename="acrg_site_info.json")
    try:
        site_info_all = site_data[site_upper]
    except KeyError:
        print("Unknown site. Will attempt to extract metadata from dataset attributes or input keywords")
        site_info = {}
    else:
        if network in site_info_all:
            site_info = site_info_all[network]
        elif network_upper in site_info_all:
            site_info = site_info_all[network_upper]
        else:
            site_info = {}
        
        # Ensure keywords match to metadata names for station values
        # e.g. "station_longitude" derived from "longitude"
        for key in metadata_needed:
            prefix = "station_"
            if key.startswith(prefix):
                short_key = '_'.join(key.split('_')[1:])
                if short_key in site_info:
                    site_info[key] = site_info[short_key]

    # Load attributes data for network if present
    try:
        param_data = load_json(filename="attributes.json")
        network_params = param_data[network_upper]
        site_attributes = network_params["global_attributes"]
    except KeyError:
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
            raise ValueError(f"Cannot extract or infer '{param}' parameter needed for metadata from stored data, attributes or keywords")

    gas_data = {species: {"metadata": metadata,
                          "data": data,
                          "attributes": attributes}}

    gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data
