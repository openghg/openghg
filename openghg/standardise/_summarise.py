from typing import Dict

import pandas as pd
from openghg.types import SurfaceTypes

# from openghg.types import DataTypes  # Would this be more appropriate?
# This does include Footprint as well as the input obs data types?


def _extract_site_param(source_format: str) -> Dict:
    """Extract site data for a given data type based on the available data
    file. If no file is available for source_format input it returns an empty dictionary.

    TODO: Create files of site details for other data types and incorporate here.

    For data types:
    - "GCWERKS", "CRDS"
       - "process_gcwerks_parameters.json"
    - "NPL", "BTT", "THAMESBARRIER"
       - "lghg_sites.json"
    - "BEACO2N"
       - "beaco2n_site_data.json"
    - Other
       - none specified

    Args:
        source_format: Accepted data type (defined in SurfaceTypes)
    Returns:
        dict: Details of site params for the data type extracted from json file
    """
    from openghg.util import load_json

    source_format = source_format.upper()

    lghg_sites = {"NPL": "NPL", "BTT": "BTT", "THAMESBARRIER": "TMB"}

    # TODO: Could create another json / csv which includes links
    # between GCWERKS and instruments (with a warning) if needed

    if source_format == "GCWERKS":
        # "process_gcwerks_parameters.json" - ["GCWERKS"]["sites"], sites are keys
        params_full = load_json(filename="process_gcwerks_parameters.json")
        site_params: Dict = params_full[source_format]["sites"]
    elif source_format == "CRDS":
        # "process_gcwerks_parameters.json" - ["CRDS"]["sites"], sites are keys
        params_full = load_json(filename="process_gcwerks_parameters.json")
        crds_params = params_full[source_format]["sites"]
        site_params = {key: value for key, value in crds_params.items() if len(key) == 3 and key.isupper()}
    elif source_format in lghg_sites.keys():
        # TODO: May want to update this now new sites have been added to lghg_data.json
        # Do these sites have their own data types?
        # "lghg_data.json" - ["sites"], sites are keys
        params_full = load_json(filename="lghg_data.json")
        site_name = lghg_sites[source_format]
        site_params = {}
        site_params[site_name] = params_full["sites"][site_name]
    elif source_format == "BEACO2N":
        # "beaco2n_site_data.json" - sites are keys
        params_full = load_json(filename="beaco2n_site_data.json")
        site_params = params_full
    else:
        site_params = {}

    return site_params


def _extract_site_names(site_params: Dict, source_format: str) -> Dict:
    """
    Extracts long names from site parameters - expects output to
    match format from `_extract_site_param()` function.

    Args:
        site_params (dict) : Dictionary of site data (extracted from
        relevant json file)
        source_format (str) : Associated data type for this data

    Returns:
        Dict: Long names for each site code
    """
    if source_format in ("GCWERKS", "CRDS"):
        name = "gcwerks_site_name"
    else:
        name = "long_name"

    site_names = {}
    for site, data in site_params.items():
        site_names[site] = data[name]

    return site_names


def summary_source_formats() -> pd.DataFrame:
    """
    Create summary DataFrame of accepted input source formats. This includes
    the site code, long name, platform and source format.

    Returns:
        pandas.DataFrame

    TODO: Add source_format details for mobile / column etc. when added
    """
    # Could include input for surface / mobile / column etc.?
    surface_source_formats = list(SurfaceTypes.__members__)

    collated_site_data = pd.DataFrame()

    for source_format in surface_source_formats:
        site_params = _extract_site_param(source_format)

        if site_params:
            site_codes = list(site_params.keys())
            site_names_dict = _extract_site_names(site_params, source_format)
            site_names = [site_names_dict[code] for code in site_codes]
        else:
            site_codes = [""]
            site_names = [""]

        site_data = pd.DataFrame({"Site code": site_codes, "Long name": site_names})
        site_data["Source format"] = source_format
        site_data["Platform"] = "surface site"

        collated_site_data = pd.concat([collated_site_data, site_data], ignore_index=True)

    # TODO: May want to sort by the site code but removed for now as nice
    # to keep GCWERKS and CRDS first.
    # collated_site_data = collated_site_data.sort_values(by="Site code")

    return collated_site_data


def summary_site_codes() -> pd.DataFrame:
    """
    Create summary DataFrame of site codes. This includes details of the network,
    longitude, latitude, height above sea level and stored heights.

    Note: there may be multiple entries for the same site code if this is
    associated with multiple networks.

    Returns:
        pandas.DataFrame

    TODO: Allow input for site json file to use. Must match to format within
    acrg_site_info.json file.
    """

    from openghg.util import load_json

    site_info = load_json(filename="acrg_site_info.json")

    site_dict: Dict[str, list] = {}
    site_dict["site"] = []
    site_dict["network"] = []

    expected_keys: list = ["long_name",
                           "latitude",
                           "longitude",
                           "height_station_masl",
                          ("heights", "height")]

    name_keys = [key[0] if isinstance(key, tuple) else key for key in expected_keys]
    for key in name_keys:
        site_dict[key] = []

    for site, network_data in site_info.items():
        for network, data in network_data.items():
            for key in expected_keys:
                if not isinstance(key, tuple):
                    search_keys = (key, )
                else:
                    search_keys = key
                
                name = search_keys[0]
                for key in search_keys:
                    if key in data:
                        site_dict[name].append(str(data[key]))
                        break
                else:
                    site_dict[name].append("")

            site_dict["network"].append(network)
            site_dict["site"].append(site)

    site_df = pd.DataFrame(site_dict)

    all_keys = name_keys.copy()
    all_keys.extend(["site", "network"])
    descriptive_names = {"site": "Site Code",
                         "long_name": "Long name",
                         "height_station_masl": "Station height (masl)",
                         "heights": "Inlet heights"}
    column_names = {name: (descriptive_names[name] if name in descriptive_names else name.capitalize()) for name in all_keys}

    site_df = site_df.rename(columns=column_names)
    site_df = site_df.set_index("Site Code")

    return site_df
