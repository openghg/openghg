import bz2
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Optional, Union

from openghg.types import pathType, multiPathType

__all__ = [
    "load_parser",
    "load_surface_parser",
    "load_column_parser",
    "load_column_source_parser",
    "load_flux_parser",
    "load_flux_database_parser",
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


def load_surface_parser(source_format: str) -> Callable:
    """Load parsing object for the obssurface data type.
    Used with `openghg.standardise.surface` sub-module

    Args:
        source_format: Name of data type such as CRDS
    Returns:
        callable: class_name object
    """
    surface_module_name = "openghg.standardise.surface"
    fn = load_parser(data_name=source_format, module_name=surface_module_name)

    return fn


def load_column_parser(source_format: str) -> Callable:
    """Load a parsing object for the obscolumn data type.
    Used with `openghg.standardise.column` sub-module

    Args:
        source_format: Name of data type e.g. OPENGHG
    Returns:
        callable: parser function
    """
    column_st_module = "openghg.standardise.column"
    fn = load_parser(data_name=source_format, module_name=column_st_module)

    return fn


def load_column_source_parser(source_format: str) -> Callable:
    """Load a parsing object for the source of column data.
    Used with `openghg.transform.column` sub-module

    Args:
        source_format: Name of data source e.g. GOSAT
    Returns:
        callable: parser function
    """
    column_tr_module = "openghg.transform.column"
    fn = load_parser(data_name=source_format, module_name=column_tr_module)

    return fn


def load_flux_parser(source_format: str) -> Callable:
    """Load a parsing object for the "flux" data type.
    Used with `openghg.standardise.flux` sub-module

    Args:
        source_format: Name of data type e.g. OPENGHG
    Returns:
        callable: parser function
    """
    flux_st_module_name = "openghg.standardise.flux"
    fn = load_parser(data_name=source_format, module_name=flux_st_module_name)

    return fn


def load_flux_database_parser(database: str) -> Callable:
    """Load a parsing object for the source of column data.
    Used with `openghg.transform.flux` sub-module

    Args:
        database: Name of data source e.g. EDGAR
    Returns:
        callable: parser function
    """
    flux_tr_module_name = "openghg.transform.flux"
    fn = load_parser(data_name=database, module_name=flux_tr_module_name)

    return fn


def load_footprint_parser(source_format: str) -> Callable:
    """Load a parsing object for the footprint data type.
    Used with `openghg.standardise.footprint` sub-module

    Args:
        source_format: Name of data type e.g. OPENGHG, ACRG_ORG
    Returns:
        callable: parser function
    """
    footprint_st_module_name = "openghg.standardise.footprint"
    fn = load_parser(data_name=source_format, module_name=footprint_st_module_name)

    return fn


def load_flux_timeseries_parser(source_format: str) -> Callable:
    """
    Load a parsing object for the one dimensional timeseries data type.
    Used with `openghg.standardise.flux_timeseries` sub-module

    Args:
        source_format: Name of the data type e.g CRF
    Returns:
        callable: parser_function
    """
    flux_timeseries_st_module_name = "openghg.standardise.flux_timeseries"
    fn = load_parser(data_name=source_format, module_name=flux_timeseries_st_module_name)

    return fn


def get_datapath(filename: pathType, directory: Optional[str] = None) -> Path:
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


def load_json(path: Union[str, Path]) -> Dict:
    """Returns a dictionary deserialised from JSON.

    Args:
        path: Path to file, can be any filepath
    Returns:
        dict: Dictionary created from JSON
    """
    with open(path, "r") as f:
        data: Dict[str, Any] = json.load(f)

    return data


def load_internal_json(filename: str) -> Dict:
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


def read_header(filepath: pathType, comment_char: str = "#") -> List:
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
    from openghg.util import running_locally

    if running_locally():
        return Path.home().joinpath("openghg.log")
    else:
        return Path("/tmp/openghg.log")


def check_function_open_nc(filepath: multiPathType) -> Tuple[Callable, multiPathType]:
    """
    Check the filepath input to choose which xarray open function to use:
     - Path or single item List - use open_dataset
     - multiple item List - use open_mfdataset

    Args:
        filepath: Path or list of filepaths
    Returns:
        Callable, Union[Path, List[Path]]: function and suitable filepath
            to use with the function.
    """
    import xarray as xr

    if isinstance(filepath, list):
        if len(filepath) > 1:
            xr_open_fn: Callable = xr.open_mfdataset
        else:
            xr_open_fn = xr.open_dataset
            filepath = filepath[0]
    else:
        xr_open_fn = xr.open_dataset

    return xr_open_fn, filepath
