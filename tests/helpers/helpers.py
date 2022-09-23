""" Some helper functions for things we do in tests frequently
"""
from pathlib import Path
from typing import Dict, List

__all__ = [
    "get_datapath",
    "get_emissions_datapath",
    "get_bc_datapath",
    "get_footprint_datapath",
    "glob_files",
    # "get_datapath_mobile",
]


def get_datapath(filename: str, data_type: str) -> Path:
    """Return the full path of a test data file. This function is
    widely used in test functions

    Args:
        filename: Filename
        data_type: Data type, folder with same name must exist in proc_test_data
    Returns:
        Path: Path to data file
    """
    data_type = data_type.upper()

    return (
        Path(__file__)
        .parent.parent.joinpath(f"data/proc_test_data/{data_type}/{filename}")
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


def get_footprint_datapath(filename: str) -> Path:
    """Return the path to the footprints test data file"""
    return get_datapath_base(data_type="footprints", filename=filename)


def get_datapath_base(data_type: str, filename: str) -> Path:
    """Return the path to the footprints test data file"""
    return Path(__file__).parent.parent.joinpath(f"data/{data_type}/{filename}").resolve()


def get_retrieval_data_file(filename: str):
    return Path(__file__).parent.parent.joinpath(f"data/retrieve/{filename}").resolve()


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
