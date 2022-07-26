from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Callable
import gzip


def load_surface_parser(data_type: str) -> Callable:
    """Load a parsing object of type class_name

    Args:
        data_type: Name of data type such as CRDS
    Returns:
        callable: class_name object
    """
    from importlib import import_module

    module_name = "openghg.standardise.surface"
    surface_module = import_module(name=module_name)

    function_name = f"parse_{data_type.lower()}"
    fn: Callable = getattr(surface_module, function_name)

    return fn


def load_emissions_parser(data_type: str) -> Callable:
    """Load a parsing object of type class_name

    Args:
        data_type: Name of data type such as EDGAR
    Returns:
        callable: class_name object
    """
    from importlib import import_module

    module_name = "openghg.standardise.emissions"
    surface_module = import_module(name=module_name)

    function_name = f"parse_{data_type.lower()}"
    fn: Callable = getattr(surface_module, function_name)

    return fn


def load_emissions_database_parser(database: str) -> Callable:
    """Load a parsing object of type class_name

    Args:
        data_type: Name of data type such as EDGAR
    Returns:
        callable: class_name object
    """
    from importlib import import_module

    module_name = "openghg.transform.emissions"
    emissions_module = import_module(name=module_name)

    function_name = f"parse_{database.lower()}"
    fn: Callable = getattr(emissions_module, function_name)

    return fn


def get_datapath(filename: str, directory: Optional[str] = None) -> Path:
    """Returns the correct path to JSON files used for assigning attributes

    Args:
        filename (str): Name of JSON file
    Returns:
        pathlib.Path: Path of file
    """
    from pathlib import Path

    filename = str(filename)

    if directory is None:
        return Path(__file__).resolve().parent.parent.joinpath(f"data/{filename}")
    else:
        return Path(__file__).resolve().parent.parent.joinpath(f"data/{directory}/{filename}")


def load_json(filename: str) -> Dict:
    """Returns a dictionary deserialised from JSON. This function only
    works for JSON files in the openghg/data directory.

    Args:
        filename (str): Name of JSON file
    Returns:
        dict: Dictionary created from JSON
    """
    from json import load

    path = get_datapath(filename)

    with open(path, "r") as f:
        data: Dict[str, Any] = load(f)

    return data


def read_header(filepath: Union[str, Path], comment_char: str = "#") -> List:
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
    with open(filepath, "r") as f:
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
    return gzip.compress(data=data)


def decompress(data: bytes) -> bytes:
    """Decompress the given data

    Args:
        data: Compressed data
    Returns:
        bytes: Decompressed data
    """
    return gzip.decompress(data=data)


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
