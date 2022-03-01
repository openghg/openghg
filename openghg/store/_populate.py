from pathlib import Path
from typing import Dict, List, Optional, Union
from openghg.store import ObsSurface


def add_noaa_obspack(
    data_directory: Union[str, Path], project: Optional[str] = None, overwrite: bool = False
) -> Dict:
    """
    Function to detect and add files from the NOAA ObsPack to the object store.

    Args:
        data_directory: Top level directory for the downloaded NOAA ObsPack
        project (optional) : Can specify project or type to process only e.g. "surface"
        or "surface-flask"
        overwrite : Whether to overwrite existing entries in the object store
    Returns:
        Dict: Details of data which has been processed into the object store
    Examples:
        To add all NOAA ObsPack data (which can be processed) to the object store:
        >>> add_noaa_obspack(Path("/home/user/obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"))
        {"processed": {"ch4_esp_surface-flask_2_representative.nc":{"ch4": ...}, ...}}

        To add NOAA ObsPack data for one type e.g. "surface":
        >>> add_noaa_obspack(Path("/home/user/obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"), "surface")
        {"processed": {"ch4_esp_surface-flask_2_representative.nc":{"ch4": ...}, ...}}

        To add NOAA ObsPack data for one project type e.g. "surface-flask"
        >>> add_noaa_obspack(Path("/home/user/obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"), "surface-flask")
        {"processed": {"ch4_esp_surface-flask_2_representative.nc":{"ch4": ...}, ...}}

    TODO: At the moment this will exclude all types which we can't process
    yet e.g. aircraft, shipboard, aircorenoaa. These should be updated once
    this functionality has been added.
    """

    # Options which we can process at the moment (ObsSurface)
    project_options = {"surface": ["flask", "insitu", "pfp"], "tower": ["insitu"]}

    project_names = _create_project_names(project_options)

    # Options we can't process at the moment but may be encountered (ObsMobile, ...).
    # TODO: "tower-insitu" should be able to run through ObsSurface but
    # these contain multiple heights per file - not sure we are handling this yet
    project_options_not_implemented_yet = {
        "aircraft": ["pfp", "insitu"],
        "shipboard": ["flask"],
        "aircorenoaa": [""],
    }

    project_names_not_implemented = _create_project_names(project_options_not_implemented_yet)

    # If a specific project has been specified, extract file matching strings
    if project:
        if project in project_names:
            projects_to_read = [project]
        elif project in project_options:
            projects_to_read = [name for name in project_names if project in name]
        elif project in project_options_not_implemented_yet or project in project_names_not_implemented:
            raise ValueError(f"Functionality to process {project} data has not been implemented yet.")
        else:
            raise ValueError(f"Did not recognise input {project} for project")
    else:
        projects_to_read = project_names
        print(f"Reading subset of data from {','.join(projects_to_read)}")

    if isinstance(data_directory, str):
        data_directory = Path(data_directory)

    # ObsPack may contain nc or txt files
    # Try and extract one and then the other if possible
    files = _find_noaa_files(data_directory, ".nc")
    if not files:
        files = _find_noaa_files(data_directory, ".txt")

    # Find relevant details for each file and call parse_noaa() function
    processed_summary: Dict[str, Dict] = {}
    for filepath in files:
        param = _param_from_filename(filepath)
        site = param["site"]
        project = param["project"]
        measurement_type = param["measurement_type"]

        if project in projects_to_read:
            processed = ObsSurface.read_file(
                filepath,
                site=site,
                measurement_type=measurement_type,
                network="NOAA",
                data_type="NOAA",
                overwrite=overwrite,
            )
        elif project in project_names_not_implemented:
            print(f"Not processing {filepath.name} - no standardisation for {project} data implemented yet.")
            processed = {}
        else:
            processed = {}

        # Expect "processed" dictionary and/or "error" dictionary within `processed`
        for key, value in processed.items():
            if key not in processed_summary:
                processed_summary[key] = {}
            for key_in, value_in in value.items():
                processed_summary[key][key_in] = value_in

    return processed_summary


def _param_from_filename(filename: Union[str, Path]) -> Dict:
    """
    Extract parameter from the NOAA filename based on the agreed naming convention.
    See: https://gml.noaa.gov/ccgg/obspack/documentation.html

    <trace gas identifier>_<site code>_<project>_<lab number>_<selection tag>.<filetype extension>

    Args:
        filename: NOAA ObsPack filename with expected naming convention
    Returns:
        Dict : Extracted parameters from filename
    Examples:
        >>> _param_from_filename("ch4_esp_surface-flask_2_representative.nc")
        {"species": "ch4", "site" : "esp", "project": "surface-flask", "measurement_type": "flask"}
    """

    if isinstance(filename, str):
        extracted_param = filename.split("_")
    else:
        extracted_param = filename.name.split("_")
    param = {}
    param["species"] = extracted_param[0]
    param["site"] = extracted_param[1]
    param["project"] = extracted_param[2]
    param["measurement_type"] = param["project"].split("-")[-1]

    return param


def _create_project_names(input_dict: Dict) -> List:
    """
    Creates full project names as would be included in the NOAA filepath

    Expects input dictionary for each the type e.g. "surface" and the
    associated measurement types e.g. ["flask", "insitu", "pfp"]

    Args:
        input_dict: In format described above
    Returns:
        List: collated names for each project
    Examples:
        The input dictionary should be defined as follows, where each value is a list.
        >>> input_dict = {"surface": ["flask", "insitu", "pfp"]}
        >>> _create_project_names(input_dict)
           ["surface-flask", "surface-insitu", "surface-php"]
    """
    projects = []
    for key, values in input_dict.items():
        if not isinstance(values, list):
            values = [values]
        full_names = ["-".join([key, value]) for value in values]
        projects.extend(full_names)
    return projects


def _find_noaa_files(data_directory: Union[str, Path], ext: str) -> List:
    """
    Find obs files in NOAA ObsPack.

    Expected directory structure is:
     - <ObsPack>/data/<filetype>/
       - e.g. obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24/data/nc/

    Args:
        data_directory: Top level ObsPack data directory to search
        ext: Extension for files. Should be either ".txt" or ".nc"
    Returns:
        list: Filenames with appropriate extension
    Examples:
        Can include top level directory (str or Path):
        >>> _find_noaa_files(Path("/home/user/obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"), ".txt")
        ["co_pocn25_surface-flask_1_ccgg_event.txt", ...]

        Or optionally can include more direct subdirectory (for nc files):
        >>> _find_noaa_files("/home/user/obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24/data/nc", ".nc")
        ["ch4_esp_surface-flask_2_representative.nc", ...]
    """

    # ObsPack may contain nc or txt files:
    # - For nc files found, these should all the data files
    # - For txt files found, we need to make sure files are found in the correct
    # sub-directory as otherwise this may find README and summary files
    if ext == ".nc":
        subdirectories = ["data/nc", "nc", ""]
    elif ext == ".txt":
        subdirectories = ["data/txt", "txt"]
    else:
        raise ValueError("Did not recognise input for extension: {ext}. Should be one of '.txt' or '.nc'")

    data_directory = Path(data_directory).expanduser().resolve()

    # Allow user to specify various levels within ObsPack to e.g. just
    # extract nc files.
    for subdir in subdirectories:
        path_to_search = data_directory / subdir
        print(f"Searching for {ext} files within {path_to_search}")
        files = list(path_to_search.glob(f"*{ext}"))
        suffix_values = [file.suffix for file in files]
        if ext in suffix_values:
            break
    else:
        files = []

    return files
