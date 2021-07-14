""" Some helper functions for things we do in tests frequently
"""
from pathlib import Path
from typing import List

__all__ = ["get_datapath", "glob_files"]


def get_datapath(filename: str, data_type: str) -> Path:
    """ Return the full path of a test data file. This function is
    widely used in test functions

    Args:
        filename: Filename
        data_type: Data type, folder with same name must exist in proc_test_data
    Returns:
        Path: Path to data file
    """
    data_type = data_type.upper()

    return Path(__file__).resolve(strict=True).parent.parent.joinpath(f"data/proc_test_data/{data_type}/{filename}")


def glob_files(search_str: str, data_type: str) -> List:
    """ Returns the list of files 

    Args:
        search_str: String to find at start of filename
        data_type: Data type, folder with same name must exist in proc_test_data
    Returns:
        list: List of files found
    """
    data_type = data_type.upper()
    globule = Path(__file__).resolve(strict=True).parent.parent.joinpath(f"data/proc_test_data/{data_type}/").glob(f"{search_str}*")

    files = [str(g) for g in globule]

    return files

