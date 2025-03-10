""" Some helper functions for things we do in tests frequently
"""

import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Union


def temporary_store_paths() -> Dict[str, Path]:
    # Add some uppercasing and numbers here to enusure paths work
    # with other characters - see https://github.com/openghg/openghg/issues/701
    return {
        "user": Path(tempfile.gettempdir(), "openghg_testing-STORE_123"),
        "group": Path(tempfile.gettempdir(), "openghg_testing_group_store"),
        "shared": Path(tempfile.gettempdir(), "openghg_testing_shared_store"),
    }


def clear_test_store(name: str) -> None:
    """Clear one of the testing object stores

    Args:
        name: Name of store to clear
    Returns:
        None
    """
    tmp_stores = temporary_store_paths()
    path = tmp_stores[name]
    shutil.rmtree(path=path, ignore_errors=True)


def clear_test_stores() -> None:
    """Clears the testing object stores"""
    # Clears the testing object stores
    tmp_stores = temporary_store_paths()
    for path in tmp_stores.values():
        shutil.rmtree(path=path, ignore_errors=True)


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
    """Return the path to the mobile test data file"""
    return get_datapath_base(data_type="mobile", filename=filename)


def get_column_datapath(filename: str) -> Path:
    """Return the path to the column test data file"""
    return get_datapath_base(data_type="column", filename=filename)


def get_flux_datapath(filename: str) -> Path:
    """Return the path to the flux test data file"""
    return get_datapath_base(data_type="flux", filename=filename)


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


def get_obspack_datapath(filename: str):
    return Path(__file__).parent.parent.joinpath(f"data/obspack/{filename}").resolve()


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
    from pathlib import Path

    from openghg.objectstore import get_bucket

    if not isinstance(key, list):
        key = [key]

    return [Path(get_bucket()).joinpath(f"{k}._data") for k in key]


def all_datasource_keys(keys: Dict) -> List[str]:
    ds_keys = []
    for key_data in keys.values():
        ds_keys.extend(key_data["keys"])

    return ds_keys


def get_flux_timeseries_datapath(filename: str) -> Path:
    """Return the path to the flux_timeseries test data file"""
    return get_datapath_base(data_type="flux_timeseries", filename=filename)


def print_dict_diff(dict1: dict, dict2: dict, skip_missing: bool = False) -> None:
    all_keys = set(dict1.keys()).union(set(dict2.keys()))

    print("\nDiff of dicts:\n")
    print(f"{'key':<20}{'dict1':<20}{'dict2':<20}")

    for key in all_keys:
        val1 = dict1.get(key, "Missing")
        val2 = dict2.get(key, "Missing")

        if skip_missing and "Missing" in (val1, val2):
            continue

        if val1 != val2:
            print(f"{key:<20}{val1:<20}{val2:<20}")
