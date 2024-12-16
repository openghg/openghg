import bz2
import json
import os
import xarray as xr
from pathlib import Path
from functools import partial
from typing import Any
from collections.abc import Callable

from openghg.types import pathType, multiPathType
from openghg.util import align_lat_lon

__all__ = [
    "load_parser",
    "load_standardise_parser",
    "load_transform_parser",
    "get_datapath",
    "get_logfile_path",
    "load_json",
    "read_header",
    "compress",
    "decompress",
    "compress_str",
    "decompress_str",
    "compress_json",
    "decompress_json",
]


def load_parser(data_name: str, module_name: str) -> Callable:
    """Load parse function from within module.

    This expects a function of the form to be:
        - parse_{data_name}()
    and for this to have been imported with an appropriate __init__.py module.

    Args:
        data_name: Name of data type / database / data source for the
        parse function.
        module_name: Full module name to be imported e.g. "openghg.standardise.surface"
    Returns:
        Callable : parse function
    """
    from importlib import import_module

    module = import_module(name=module_name)

    function_name = f"parse_{data_name.lower()}"
    fn: Callable = getattr(module, function_name)

    return fn


def load_standardise_parser(data_type: str, source_format: str) -> Callable:
    """
    Load a standardise parsing function associated with a given data_type.
    This will look for a parser function with a sub-module of `openghg.standardise`
    depending on the specified data_type and source_format.

    For example for inputs of data_type="surface" and source_format="openghg"
    this will look for a function called:
     - `openghg.standardise.surface.parse_openghg`

    Args:
        data_type: Data types for objects within OpenGHG
            see openghg.store.specification.define_data_types() for full list.
        source_format: Name given to the format of the input data e.g AGAGE
    Returns:
        callable: parser_function
    """
    standardise_module_name = "openghg.standardise"
    data_type_st_module_name = f"{standardise_module_name}.{data_type}"
    fn = load_parser(data_name=source_format, module_name=data_type_st_module_name)

    return fn


def load_transform_parser(data_type: str, source_format: str) -> Callable:
    """
    Load a transform parsing function associated with a given data_type.
    This will look for a parser function with a sub-module of `openghg.transform`
    depending on the specified data_type and source_format.

    For example for inputs of data_type="flux" and source_format="edgar"
    this will look for a function called:
     - `openghg.transform.surface.parse_edgar`

    Args:
        data_type: Data types for objects within OpenGHG
            see openghg.store.specification.define_data_types() for full list.
        source_format: Name given to the input data. Could be a database or
            a format e.g EDGAR
    Returns:
        callable: parser_function
    """
    transform_module_name = "openghg.transform"
    data_type_st_module_name = f"{transform_module_name}.{data_type}"
    fn = load_parser(data_name=source_format, module_name=data_type_st_module_name)

    return fn


def get_datapath(filename: pathType, directory: str | None = None) -> Path:
    """Returns the correct path to data files used for assigning attributes

    Args:
        filename: Name of file to be accessed
    Returns:
        pathlib.Path: Path of file
    """
    from pathlib import Path

    filename = str(filename)

    if directory is None:
        return Path(__file__).resolve().parent.parent.joinpath(f"data/{filename}")
    else:
        return Path(__file__).resolve().parent.parent.joinpath(f"data/{directory}/{filename}")


def load_json(path: str | Path) -> dict:
    """Returns a dictionary deserialised from JSON.

    Args:
        path: Path to file, can be any filepath
    Returns:
        dict: Dictionary created from JSON
    """
    with open(path) as f:
        data: dict[str, Any] = json.load(f)

    return data


def load_internal_json(filename: str) -> dict:
    """Returns a dictionary deserialised from JSON. Pass filename to load data from JSON files in the
    openghg/data directory or pass a full filepath to path to load from any file.

    Args:
        filename: Name of JSON file. Must be located in openghg/data
        path: Path to file, can be any filepath
    Returns:
        dict: Dictionary created from JSON
    """
    file_path = get_datapath(filename=filename)
    return load_json(path=file_path)


def read_header(filepath: pathType, comment_char: str = "#") -> list:
    """Reads the header lines denoted by the comment_char

    Args:
        filepath: Path to file
        comment_char: Character that denotes a comment line
        at the start of a file
    Returns:
        list: List of lines in the header
    """
    comment_char = str(comment_char)

    header = []
    # Get the number of header lines
    with open(filepath) as f:
        for line in f:
            if line.startswith(comment_char):
                header.append(line)
            else:
                break

    return header


def compress(data: bytes) -> bytes:
    """Compress the given data

    Args:
        data: Binary data
    Returns:
        bytes: Compressed data
    """
    return bz2.compress(data=data)


def decompress(data: bytes) -> bytes:
    """Decompress the given data

    Args:
        data: Compressed data
    Returns:
        bytes: Decompressed data
    """
    return bz2.decompress(data=data)


def compress_str(s: str) -> bytes:
    """Compress a string

    Args:
        s: String
    Retruns:
        bytes: Compressed data
    """
    return compress(data=s.encode(encoding="utf-8"))


def decompress_str(data: bytes) -> str:
    """Decompress a string from bytes

    Args:
        data: Compressed data
    Returns:
        str: Decompressed str
    """
    return decompress(data=data).decode(encoding="utf-8")


def decompress_json(data: bytes) -> Any:
    """Decompress a string and load to JSON

    Args:
        data: Compressed binary data
    Returns:
        Object loaded from JSON
    """
    decompressed = decompress_str(data=data)
    return json.loads(decompressed)


def compress_json(data: Any) -> bytes:
    """Convert object to JSON string and compress

    Args:
        data: Object to pass to json.dumps
    Returns:
        bytes: Compressed binary data
    """
    json_str = json.dumps(data)
    return compress_str(json_str)


def get_logfile_path() -> Path:
    """Get the logfile path

    Returns:
        Path: Path to logfile
    """
    return Path("/tmp/openghg.log")


def check_function_open_nc(
    filepath: multiPathType, realign_on_domain: str | None = None
) -> tuple[Callable, multiPathType]:
    """
    Check the filepath input to choose which xarray open function to use:
     - Path or single item List - use open_dataset
     - multiple item List - use open_mfdataset

    Args:
        filepath: Path or list of filepaths
        realign_on_domain: When present, realign the data on the given domain. Option usable
        when opening footprints or flux data but not observations and boundary conditions.
    Returns:
        Callable, Union[Path, List[Path]]: function and suitable filepath
            to use with the function.
    """
    if isinstance(filepath, list):
        if len(filepath) > 1:
            if realign_on_domain:
                xr_open_fn: Callable = partial(
                    xr.open_mfdataset, preprocess=lambda x: align_lat_lon(x, realign_on_domain)
                )
            else:
                xr_open_fn = xr.open_mfdataset
            return xr_open_fn, filepath

        else:
            filepath = filepath[0]

    if realign_on_domain:

        def xr_open_fn(x: pathType) -> xr.DataArray | xr.Dataset:
            return align_lat_lon(xr.open_dataset(x), realign_on_domain)

    else:
        xr_open_fn = xr.open_dataset

    return xr_open_fn, filepath


def permissions(file_path: str | Path) -> tuple[str, str, str]:
    """Return r, w, and/or x permissions for user, group, and other."""
    perms = oct(os.stat(file_path).st_mode)
    user, group, other = perms[-3], perms[-2], perms[-1]

    def bits_to_perms(bit_str: str) -> str:
        bits = [int(b) for b in bin(int(bit_str))[-3:]]
        perms = "r" * bits[0] + "w" * bits[1] + "x" * bits[2]
        return perms

    return bits_to_perms(user), bits_to_perms(group), bits_to_perms(other)
