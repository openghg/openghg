""" Some helper functions for things we do in tests frequently
"""
import os
import shutil
from pathlib import Path
from typing import Dict, List, Union

__all__ = [
    "get_surface_datapath",
    "get_emissions_datapath",
    "get_bc_datapath",
    "get_footprint_datapath",
    "glob_files",
    "clear_test_store",
    "all_datasource_keys",
]


def clear_test_store():
    # Clears the testing object store
    path = os.getenv("OPENGHG_TEST")
    if path is not None:
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass


def get_surface_datapath(filename: str, source_format: str) -> Path:
    """Return the full path of a test data file. This function is
    widely used in test functions

    Args:
        filename: Filename
        source_format: Data type, folder with same name must exist in proc_test_data
    Returns:
        Path: Path to data file
    """
    source_format = source_format.upper()

    return (
        Path(__file__).parent.parent.joinpath(f"data/proc_test_data/{source_format}/{filename}")
    ).resolve()


def get_mobile_datapath(filename: str) -> Path:
    """Return the path to the emissions test data file"""
    return get_datapath_base(data_type="mobile", filename=filename)


def get_column_datapath(filename: str) -> Path:
    """Return the path to the emissions test data file"""
    return get_datapath_base(data_type="column", filename=filename)


def get_emissions_datapath(filename: str) -> Path:
    """Return the path to the emissions test data file"""
    return get_datapath_base(data_type="emissions", filename=filename)


def get_bc_datapath(filename: str) -> Path:
    """Return the path to the boundary conditions test data file"""
    return get_datapath_base(data_type="boundary_conditions", filename=filename)


def get_eulerian_datapath(filename: str) -> Path:
    """Return the path to the boundary conditions test data file"""
    return get_datapath_base(data_type="eulerian_model", filename=filename)


def get_footprint_datapath(filename: str) -> Path:
    """Return the path to the footprints test data file"""
    return get_datapath_base(data_type="footprints", filename=filename)


def get_datapath_base(data_type: str, filename: str) -> Path:
    """Return the path to the footprints test data file"""
    return Path(__file__).parent.parent.joinpath(f"data/{data_type}/{filename}").resolve()


def get_retrieval_datapath(filename: str):
    return Path(__file__).parent.parent.joinpath(f"data/retrieve/{filename}").resolve()


def get_info_datapath(filename: str):
    return Path(__file__).parent.parent.joinpath(f"data/info/{filename}").resolve()


def glob_files(search_str: str, data_type: str) -> List:
    """Returns the list of files

    Args:
        search_str: String to find at start of filename
        data_type: Data type, folder with same name must exist in proc_test_data
    Returns:
        list: List of files found
    """
    data_type = data_type.upper()
    globule = (
        Path(__file__)
        .resolve(strict=True)
        .parent.parent.joinpath(f"data/proc_test_data/{data_type}/")
        .glob(f"{search_str}*")
    )

    files = [str(g) for g in globule]

    return files


def call_function_packager(status: int, headers: Dict, content: Dict) -> Dict:
    """Packages some data to mock the return value of the openghg.cloud.call_function

    Args:
        data: Data to package
    Returns:
        bytes:
    """
    d = {}
    d["status"] = status
    d["headers"] = dict(headers)
    d["content"] = content

    return d


def key_to_local_filepath(key: Union[str, List]) -> List[Path]:
    from openghg.objectstore import get_bucket
    from pathlib import Path

    if not isinstance(key, list):
        key = [key]

    return [Path(get_bucket()).joinpath(f"{k}._data") for k in key]


def all_datasource_keys(keys: Dict) -> List[str]:
    ds_keys = []
    for key_data in keys.values():
        data_keys = list(key_data["keys"].values())
        ds_keys.extend(data_keys)

    return ds_keys
