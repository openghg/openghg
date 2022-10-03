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


def summary_data_types() -> pd.DataFrame:
    """
    Create summary DataFrame of accepted input data types. This includes
    the site code, long name, platform and data type.

    Returns:
        pandas.DataFrame

    TODO: Add source_format details for mobile / column etc. when added
    """
    # Could include input for surface / mobile / column etc.?
    surface_data_types = list(SurfaceTypes.__members__)

    collated_site_data = pd.DataFrame()

    for source_format in surface_data_types:
        site_params = _extract_site_param(source_format)

        if site_params:
            site_codes = list(site_params.keys())
            site_names_dict = _extract_site_names(site_params, source_format)
            site_names = [site_names_dict[code] for code in site_codes]
        else:
            site_codes = [""]
            site_names = [""]

        site_data = pd.DataFrame({"Site code": site_codes, "Long name": site_names})
        site_data["Data type"] = source_format
        site_data["Platform"] = "surface site"

        collated_site_data = pd.concat([collated_site_data, site_data], ignore_index=True)

    # TODO: May want to sort by the site code but removed for now as nice
    # to keep GCWERKS and CRDS first.
    # collated_site_data = collated_site_data.sort_values(by="Site code")

    return collated_site_data
