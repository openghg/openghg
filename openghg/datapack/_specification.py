import logging

from openghg.retrieve import get_obs_surface, get_obs_column

logger = logging.getLogger("openghg.obspack")


def define_obs_types() -> list:
    """
    Define names of obs type folders to be included in an ObsPack.
    """
    obs_types = ["surface-insitu", "surface-flask", "column"]
    return obs_types


def define_data_type_for_obs_type(obs_type: str) -> str:
    """
    Define associated data_type within the object store for a given obs_type.
    """
    # TODO: Decide if we should update column to surface-column and satellite
    # etc. - do we want to link this to "platform"?
    data_types = {"surface-insitu": "surface", "surface-flask": "surface", "column": "column"}

    data_type = data_types[obs_type]

    return data_type


def define_get_functions() -> tuple[dict, dict]:
    """
    Define relationship between data type names (for folders) and get functions.

    Returns:
        dict, dict: Folder name to get function mapping and additional arguments
                    which need to be passed to that function.
    """
    get_functions = {
        "surface-insitu": get_obs_surface,
        "surface-flask": get_obs_surface,
        "column": get_obs_column,
    }

    get_fn_arguments = {
        get_obs_surface: {"rename_vars": False},
        get_obs_column: {"return_mf": False},
    }

    return get_functions, get_fn_arguments
