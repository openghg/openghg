from pathlib import Path
from typing import cast
from collections.abc import MutableMapping

import xarray as xr


def parse_openghg(
    filepath: str | Path,
    satellite: str | None = None,
    domain: str | None = None,
    selection: str | None = None,
    site: str | None = None,
    species: str | None = None,
    network: str | None = None,
    instrument: str | None = None,
    platform: str = "satellite",
    chunks: dict | None = None,
    **kwargs: str,
) -> dict:
    """
    Parse and extract data from pre-formatted netcdf file which already
    matches expected OpenGHG format.

    The arguments specified below are the metadata needed to store this
    surface observation file within the object store. If these keywords are
    not included within the attributes of the netcdf file being passed then
    these arguments must be specified.

    For column data this can either be a satellite (e.g. satellite="GOSAT") or a
    site (site="RUN", network="TCCON"). Either can be specified or this function
    will attempt to extract this from the data file.

    Args:
        filepath: Path of observation file
        satellite: Name of satellite (if relevant)
        domain: For satellite only. If data has been selected on an area include the
            identifier name for domain covered. This can map to previously defined domains
            (see openghg_defs "domain_info.json" file) or a newly defined domain.
        selection: For satellite only, identifier for any data selection which has been
            performed on satellite data. This can be based on any form of filtering, binning etc.
            but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
            If not specified, domain will be used.
        site : Site code/name (if relevant). Can include satellite OR site.
        species: Species name or synonym e.g. "ch4"
        instrument: Instrument name e.g. "TANSO-FTS"
        network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"
        platform: Type of platform. Should be one of:
            - "satellite"
            - "site"
            Note: this will be superceded if site or satellite keywords are specified.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass in an empty dictionary.
        kwargs: Any additional attributes to be associated with the data.
    Returns:
        Dict : Dictionary of source_name : data, metadata, attributes
    """
    from openghg.standardise.meta import define_species_label
    from openghg.util import clean_string

    # from openghg.standardise.meta import attributes_default_keys, assign_attributes

    filepath = Path(filepath)

    if filepath.suffix.lower() != ".nc":
        raise ValueError("Input file must be a .nc (netcdf) file.")

    data = xr.open_dataset(filepath).chunk(chunks)

    # Extract current attributes from input data
    attributes = cast(MutableMapping, data.attrs)

    if satellite is not None or platform == "satellite":
        metadata_required = metadata_default_satellite_column()
        metadata_required.remove("selection")
        platform = "satellite"
    elif site is not None or platform == "site":
        metadata_required = metadata_default_site_column()
        platform = "site"

    if platform == "satellite":
        if domain is None:
            raise ValueError(
                "For satellite data, please specify selected domain."
                "This can be 'global' if no selection has been made."
            )

    # Define metadata based on input arguments.
    metadata_initial = {
        "site": site,
        "satellite": satellite,
        "instrument": instrument,
        "species": species,
        "domain": domain,
        "network": network,
        "platform": platform,
        "data_type": "column",
        "source_format": "openghg",
    }

    # TODO: Tidy this up a bit (maybe split some into a separate function?)
    # and incorporate kwargs

    metadata = {}
    key_translation = satellite_attribute_translation()
    # Populate metadata with values from attributes if inputs have not been passed
    for key, value in metadata_initial.items():
        if key in metadata_required:
            # Extract equivalent key from passed file if present using translation
            try:
                attr_keys = key_translation[key]
            except KeyError:
                attr_keys = key

            # Make sure this is a list for cases with multiple options
            if isinstance(attr_keys, str):
                attr_keys = [attr_keys]

            if value is None:
                # Extract value from attributes if this has not been specified
                for attr_key in attr_keys:
                    try:
                        metadata[key] = attributes[attr_key]
                    except KeyError:
                        continue
                    else:
                        break
                else:
                    raise ValueError(f"Input '{key}' must be specified if not included in data attributes.")
            else:
                # If attributes are present, check these match to inputs passed
                for attr_key in attr_keys:
                    if attr_key in key_translation and attr_key in attributes:
                        attributes_value = attributes[attr_key]
                        if value != attributes_value:
                            # If inputs do not match attribute values, raise a ValueError
                            raise ValueError(
                                f"Input for '{key}': {value} does not match value in file attributes: {attributes_value}"
                            )
                metadata[key] = value

    # metadata = cast(Dict[str, str], metadata_initial)

    if selection is not None:
        metadata["selection"] = selection
    elif platform == "satellite":
        metadata["selection"] = domain

    # TODO: Add loose domain checking? If known domain is specified, make sure points
    # are within this for example.

    species = define_species_label(metadata["species"])[0]
    metadata["species"] = species

    if "site" in metadata:
        metadata["site"] = clean_string(metadata["site"])

    # Add data type to metadata
    metadata["data_type"] = "column"

    # Define remaining keys needed for metadata
    metadata_needed = [param for param in metadata_required if param not in metadata]

    for key in metadata_needed:
        try:
            attr_keys = key_translation[key]
        except KeyError:
            attr_keys = key

        if isinstance(attr_keys, str):
            attr_keys = [attr_keys]

        for attr_key in attr_keys:
            for attr_key in attr_keys:
                try:
                    metadata[key] = attributes[attr_key]
                except KeyError:
                    continue
                else:
                    break
            else:
                raise ValueError(f"Input '{key}' must be specified if not included in data attributes.")

    # In GOSAT UoL files (and copied into our files)
    #  - platform = "GOSAT"; sensor = "TANSO-FTS"
    # In TROPOMI S5P_OFFL_...nc files
    #  - platform = 'S5P'; sensor = "TROPOMI"
    # Could "platform" --> "satellite" perhaps?

    # site = metadata["site"]
    # network = metadata["network"]
    # species = metadata["species"]

    # # Allow site and network data to be treated in a case insensitive way
    # site_case_options = [site, site.upper(), site.lower()]
    # network_case_options = [network, network.upper(), network.lower()]

    # # Extract centralised data for site (if present)
    # site_data = load_json(filename="site_info.json")
    # for site_value in site_case_options:
    #     if site_value in site_data:
    #         site_info_all = site_data[site_value]
    #         break
    # else:
    #     print("Unknown site. Will attempt to extract metadata from dataset attributes or input keywords")
    #     site_info_all = {}

    # for network_value in network_case_options:
    #     if network_value in site_info_all:
    #         site_info = site_info_all[network_value]
    #         break
    # else:
    #     print(
    #         "Network {network} does not match with site {site}. Will attempt to extract metadata from dataset attributes or input keywords"
    #     )
    #     site_info = {}

    # if site_info:
    #     # Ensure keywords match to metadata names for station values
    #     # e.g. "station_longitude" derived from "longitude"
    #     for key in metadata_needed:
    #         prefix = "station_"
    #         if key.startswith(prefix):
    #             split_key = key.split("_")[1:]
    #             short_key_option1 = "_".join(split_key)

    #             split_key.insert(1, "station")  # to catch "height_station_masl"
    #             short_key_option2 = "_".join(split_key)

    #             short_key_options = [short_key_option1, short_key_option2]
    #             for short_key in short_key_options:
    #                 if short_key in site_info:
    #                     site_info[key] = site_info[short_key]
    #                     break

    # # Load attributes data for network if present
    # param_data = load_json(filename="attributes.json")
    # for network_value in network_case_options:
    #     if network_value in param_data:
    #         network_params = param_data[network_value]
    #         site_attributes = network_params["global_attributes"]
    #         break
    # else:
    #     site_attributes = {}

    # # Define sources of attributes to use when defining metadata
    # # The order here influences the hierarchy if keys appear multiple times.
    # # kwargs allow additional variables such as "station_longitude" to be included if needed.
    # attribute_sources = [attributes, kwargs, site_info, site_attributes]

    # # Search attributes sources (in order) and populate metadata
    # for param in metadata_needed:
    #     for source in attribute_sources:
    #         if param in source:
    #             metadata[param] = source[param]
    #             break
    #     else:
    #         raise ValueError(
    #             f"Cannot extract or infer '{param}' parameter needed for metadata from stored data, attributes or keywords"
    #         )

    # Update attributes to match metadata after cleaning
    attributes.update(metadata)

    # TODO: Decide if the key here should be more descriptive that just `species`
    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    # gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data


def metadata_default_satellite_column() -> list[str]:
    """
    Define default keys for satellite column data
    """
    default_keys = [
        "satellite",
        "species",
        "network",
        "instrument",
        "platform",
        "domain",
        "selection",
        "data_owner",
        "data_owner_email",
    ]

    return default_keys


def metadata_default_site_column() -> list[str]:
    """
    Define default keys for site column data
    """
    default_keys = [
        "site",
        "species",
        "network",
        "instrument",
        "platform",
        "data_owner",
        "data_owner_email",
    ]

    return default_keys


TranslationDict = dict[str, str | list[str]]


def satellite_attribute_translation() -> TranslationDict:
    """
    Define translation between openghg keyword and input files.
    Currently includes:
     - GOSAT (University of Leicester product)
     - TROPOMI
    """
    # Current values within (at least) GOSAT, TROPOMI files
    # - values can contain lists as well as single string values
    keywords: TranslationDict = {
        "instrument": "sensor",
        "satellite": "platform",
        "network": "platform",
        "data_owner": "creator_name",
        "data_owner_email": "creator_email",
    }

    return keywords
