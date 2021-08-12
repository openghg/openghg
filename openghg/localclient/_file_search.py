import glob
from openghg.util import load_json
from os.path import join
from typing import List, Dict, Tuple


__all__ = ["find_all_files"]


def find_gc_files(site: str, instrument: str, data_folder: str = None) -> List[Tuple[str, str]]:
    """
    Find files from GC instruments.

    Args:
        site (str) - three-letter site code e.g. "MHD"
        instrument (str) - one of "GCMD, "GCMS" or "medusa"
    Returns:
        list(tuple):
            List of tuple pairs for data file and associated
            GCWERKS precision data file.
    """
    params = load_json(filename="process_gcwerks_parameters.json")

    try:
        site_gcwerks = params["GC"][site]["gcwerks_site_name"]
        instrument_gcwerks = params["GC"]["instruments"][instrument]

        if data_folder is not None:
            data_folder = data_folder[instrument]
        else:
            data_folder = params["GC"]["directory"][instrument]

        suffixes = params["GC"]["instruments_suffix"][instrument]
    except KeyError:
        print("Unable to extract data files")
        print(f"Instrument {instrument} or site {site} not found within json parameters file")
        return []

    for suffix in suffixes:

        fname_search = f"{site_gcwerks}{instrument_gcwerks}{suffix}.??.C"
        search_string = join(data_folder, fname_search)

        data_files = sorted(glob.glob(search_string))

        if len(data_files) > 0:
            break

    precision_files = [data_file[0:-2] + ".precisions.C" for data_file in data_files]

    data_tuples = [
        (data_file, precision_file) for data_file, precision_file in zip(data_files, precision_files)
    ]

    return data_tuples


def find_crds_files(site: str, data_folder: str = None) -> List:
    """
    Find files from CRDS instruments.

    Args:
        site (str) - three-letter site code e.g. "MHD"
    Returns:
        list:
            List of data file names for that site
    """
    params = load_json(filename="process_gcwerks_parameters_bp1.json")

    try:
        # Get directories and site strings
        params_crds = params["CRDS"]
        site_string = params_crds[site]["gcwerks_site_name"]

        if data_folder is None:
            data_folder = params_crds["directory"].replace("%site", site_string)
    except KeyError:
        print("Unable to extract data files")
        print(f"site {site} not found within json parameters file for CRDS instrument")
        return []

    # Find files
    fname_search = f"{site.lower()}.*.1minute.*.dat"
    data_file_search = join(data_folder, fname_search)
    data_files = glob.glob(data_file_search)

    return data_files


def data_type_function() -> Dict:
    """
    Defines functions for finding files related to each data type.
    Includes "GCWERKS", "CRDS" and "ICOS" at present.

    Returns:
        dict :
            Dictionary of read file functions for each data type

    """
    return {"GCWERKS": find_gc_files, "CRDS": find_crds_files}


def site_all() -> Dict:
    """
    Defines inputs needed to find the files for sites within the AGAGE
    and DECC networks which are loaded as standard into our
    object store.

    This is split into two data types (based on the necessary processing):
        - GCWERKS
        - CRDS

    To find the data files and then to load the data, details are needed
    for:
        - site
        - network
        - instrument (for GCWERKS only)

    Returns:
        dict :
            Associated data definitions for each data type.
    """
    # GCWERKS needs both site and instrument to find the file name
    gc_werks_input = [
        # AGAGE Medusa
        {"site": "MHD", "instrument": "medusa", "network": "AGAGE"},
        {"site": "CGO", "instrument": "medusa", "network": "AGAGE"},
        {"site": "GSN", "instrument": "medusa", "network": "AGAGE"},
        {"site": "SDZ", "instrument": "medusa", "network": "AGAGE"},
        {"site": "THD", "instrument": "medusa", "network": "AGAGE"},
        {"site": "RPB", "instrument": "medusa", "network": "AGAGE"},
        {"site": "SMO", "instrument": "medusa", "network": "AGAGE"},
        {"site": "SIO", "instrument": "medusa", "network": "AGAGE"},
        {"site": "JFJ", "instrument": "medusa", "network": "AGAGE"},
        {"site": "CMN", "instrument": "medusa", "network": "AGAGE"},
        {"site": "ZEP", "instrument": "medusa", "network": "AGAGE"},
        # AGAGE GC data
        {"site": "RPB", "instrument": "GCMD", "network": "AGAGE"},
        {"site": "CGO", "instrument": "GCMD", "network": "AGAGE"},
        {"site": "MHD", "instrument": "GCMD", "network": "AGAGE"},
        {"site": "SMO", "instrument": "GCMD", "network": "AGAGE"},
        {"site": "THD", "instrument": "GCMD", "network": "AGAGE"},
        # AGAGE GCMS data
        {"site": "CGO", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "MHD", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "RPB", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "SMO", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "THD", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "JFJ", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "CMN", "instrument": "GCMS", "network": "AGAGE"},
        {"site": "ZEP", "instrument": "GCMS", "network": "AGAGE"},
        # GAUGE and DECC GC data
        {"site": "BSD", "instrument": "GCMD", "network": "DECC"},
        {"site": "HFD", "instrument": "GCMD", "network": "DECC"},
        {"site": "TAC", "instrument": "GCMD", "network": "DECC"},
        {"site": "RGL", "instrument": "GCMD", "network": "DECC"},
        # DECC Medusa
        {"site": "TAC", "instrument": "medusa", "network": "DECC"},
    ]

    crds_input = [
        # AGAGE CRDS data
        {"site": "RPB", "network": "AGAGE"},
        # GAUGE and DECC CRDS data
        {"site": "HFD", "network": "DECC"},
        {"site": "BSD", "network": "DECC"},
        {"site": "TTA", "network": "DECC"},
        {"site": "RGL", "network": "DECC"},
        {"site": "TAC", "network": "DECC"},
    ]

    instrument_details = {"GCWERKS": gc_werks_input, "CRDS": crds_input}

    return instrument_details


def find_all_files(data_folders: Dict) -> List[Dict]:
    """
    Finds all the filenames for sites within the AGAGE and
    DECC networks which are loaded as standard into our
    object store.
    See site_all() function for full list.

    Each input contains the keys needed for ObsSurface.read_file method:
     - "filepath"
     - "data_type"
     - "site"
     - "network"

    Returns:
        list (dict) : List of each input as a dictionary in the form
        appropriate to pass to the read_file() function.
    """
    all_instrument_details = site_all()
    find_functions = data_type_function()

    data_files = []
    for data_type in all_instrument_details:
        data_details = all_instrument_details[data_type]
        fn_find = find_functions[data_type]
        data_folder = data_folders[data_type]

        for data in data_details:
            # Find all expected parameters in function and extract matching
            # parameters from the inputs
            input_param = fn_find.__code__.co_varnames[: fn_find.__code__.co_argcount]
            param = {key: value for key, value in data.items() if key in input_param}

            files = fn_find(**param, data_folder=data_folder)

            read_input_dict = {
                "filepath": files,
                "data_type": data_type,
                "site": data["site"],
                "network": data["network"],
            }

            if "instrument" in param:
                read_input_dict["instrument"] = param["instrument"]

            if files:
                data_files.append(read_input_dict)

    return data_files
