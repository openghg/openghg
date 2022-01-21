from pathlib import Path
from typing import Dict, Optional, Union
from openghg.store import ObsSurface


def _param_from_filename(filename) -> Dict:

    # Naming convention for files:
    #  <trace gas identifier>_<site code>_<project>_<lab number>_<selection tag>.<filetype extension>

    extracted_param = filename.name.split("_")
    param = {}
    param["species"] = extracted_param[0]
    param["site"] = extracted_param[1]
    param["project"] = extracted_param[2]
    param["measurement_type"] = param["project"].split('-')[-1]

    return param


def add_noaa_obspack(
    data_directory: Union[str, Path],
    project: Optional[str] = None,
    ):

    # Project names are of the form surface-flask, tower-insitu
    def create_project_names(input_dict):
        ''' Creates full project names as would be included in the NOAA filepath'''
        projects = []
        for key, values in input_dict.items():
            full_names = ["-".join([key,value]) for value in values]
            projects.extend(full_names)
        return projects

    # Options which we can process at the moment (ObsSurface)
    project_options = {"surface": ["flask", "insitu", "pfp"],
                       "tower": ["insitu"]}

    project_names = create_project_names(project_options)

    # Options we can't process at the moment but may be encountered (ObsMobile, ...).
    project_options_not_implemented_yet = {"aircraft": ["pfp", "insitu"],
                                           "shipboard":["flask"],
                                           "aircorenoaa":[""]}
    
    project_names_not_implemented = create_project_names(project_options_not_implemented_yet)

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

    def find_files(data_directory, subdirectories, ext):
        '''
        Find obs files in appropriate subdirectories
        '''
        for dir in subdirectories:
            path_to_search = data_directory / dir
            print(f"Searching for nc files within {path_to_search}")
            files = list(path_to_search.glob(f"*{ext}"))
            suffix_values = [file.suffix for file in files]
            if ext in suffix_values:
                break
        else:
            files = []
        return files

    # ObsPack may contain nc or txt files, try and extract both if possible
    top_level_directories_nc = ["", "nc", "data/nc"]
    # top_level_directories_txt = ["data/txt", "txt"] #, ""]

    nc_files = find_files(data_directory, top_level_directories_nc, ".nc")
    # txt_files = find_files(data_directory, top_level_directories_txt, ".txt")
    # files = nc_files.extend(txt_files)
    files = nc_files

    # Find relevant details for each file and call parse_noaa() function
    for filepath in files:
        param = _param_from_filename(filepath)
        site = param["site"]
        project = param["project"]
        measurement_type = param["measurement_type"]
        if project in projects_to_read:
            # try:
            ObsSurface.read_file(filepath, site=site, measurement_type=measurement_type, network="NOAA", data_type="NOAA")
            # except (ValueError, KeyError):
                # print(f"Unable to process {filepath.name}, site: {site}")
        elif project in project_names_not_implemented:
            print(f"Not processing {filepath.name} - no standardisation for {project} data implemented yet.")

        # What would we want to pass back, if anything, here?

