import logging
import bz2
from functools import partial, wraps
import json
from pathlib import Path
from typing import Any
from collections.abc import Callable
import numpy as np
import xarray as xr

from openghg.types import pathType, multiPathType, convert_to_list_of_metadata_and_data, XrDataLikeMatch
from openghg.util import align_lat_lon

logger = logging.getLogger("openghg.util.file")

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
    "open_nc_fn",
    "open_time_nc_fn",
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

    @wraps(fn)
    def wrapped_fn(*args, **kwargs):  # type: ignore
        return convert_to_list_of_metadata_and_data(fn(*args, **kwargs))

    return wrapped_fn


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


def check_filepath(filepath: multiPathType, source_format: str) -> list[str] | list[Path]:
    """
    Check that filepath is of the correct format - this assumes the filepath
    should be a str, Path or list but not a tuple (this is only needed for gcwerks).

    Args:
        filepath: Input filepath details
        source_format: Name of source format to use when standardising this data.
    Returns:
        list: List of filepaths
    """
    if not isinstance(filepath, list):
        filepath = [filepath]

    for fp in filepath:
        if isinstance(fp, tuple):
            if source_format.lower() == "gcwerks":
                msg = "The openghg.standardise.surface._check_gcwerks_input() function must be used to extract filepath and precision_filepath"
                logger.exception(msg)
                raise ValueError(msg)
            else:
                msg = f"Do not expect tuple for filepath for source_format={source_format}. Please provide a str/Path or list of Paths."
                logger.exception(msg)
                raise ValueError(msg)

    return filepath


def _select_time(x: xr.Dataset) -> xr.Dataset:
    """
    WARNING : designed for a specific case where a day from another month was present
    in a monthly file (concerns file from an old NAME_processing version).
    This is not designed for a general case.
    Args:
        x: xarray data to be checked
    Returns:
        xarray.Dataset: Updated dataset
    """
    month = x.time.resample(time="M").count().idxmax().values.astype("datetime64[M]")
    start_date = month.astype("datetime64[D]")
    end_date = (month + np.timedelta64(1, "M")).astype("datetime64[D]")
    return x.sel(time=slice(start_date, end_date))


def check_coords_nc(data: XrDataLikeMatch, coords: str | list | None = None) -> XrDataLikeMatch:
    """
    Check coordinates are present and registering correctly as 1D dimensions within an xarray Dataset. This is to account for cases
    where a single value is present for a coordinate but this has been squeezed to zero dimensions meaning this is not registering as a dimension.
    Args:
        data: xarray data to be checked
        coords: Name of coordinates to check. This can be a str, list or None.
    Returns:
        xr.Dataset / xr.DataArray: data with coordinates registered correctly
    """

    if coords is None:
        return data
    elif isinstance(coords, str):
        coords = [coords]

    for c in coords:
        if c in data.dims:
            continue
        elif c in data:
            data = data.expand_dims(c)
        else:
            msg = f"Expected coordinate: '{c}' is not present in the input dataset"

            raise ValueError(msg)

    return data


def open_nc_fn(
    filepath: str | Path | list[str] | list[Path],
    realign_on_domain: str | None = None,
    sel_month: bool = False,
    check_coords: str | None = None,
) -> tuple[Callable, str | Path | list[str] | list[Path]]:
    """
    Check the filepath input to choose which xarray open function to use:
     - Path or single item List - use open_dataset
     - multiple item List - use open_mfdataset

    Args:
        filepath: Path or list of filepaths
        realign_on_domain: When present, realign the data on the given domain. Option usable
            when opening footprints, flux or boundary conditions data but not observations or flux_timeseries.
        sel_month : when present keep only one month of data
        check_coords: Check whether expected coordinates are present and registered correctly as 1D dimensions in the netcdf files.
            Default = None.
    Returns:
        Callable, Union[Path, List[Path]]: function and suitable filepath
            to use with the function.
    """

    def process(x: xr.Dataset) -> xr.Dataset:
        """
        Apply appropriate process functions for the provided dataset.

        Returns:
            xarray.Dataset: updated Dataset with appropriate pre-processing applied.
        """

        if check_coords:
            x = check_coords_nc(x, coords=check_coords)

        if realign_on_domain and sel_month:
            return align_lat_lon(_select_time(x), realign_on_domain)
        elif realign_on_domain:
            return align_lat_lon(x, realign_on_domain)
        elif sel_month:
            return _select_time(x)
        else:
            return x

    if isinstance(filepath, list):
        if len(filepath) > 1:
            xr_open_fn_1: Callable = partial(xr.open_mfdataset, preprocess=process)
            return xr_open_fn_1, filepath

        else:
            filepath = filepath[0]

    def xr_open_fn_2(x: pathType) -> xr.DataArray | xr.Dataset:
        return process(xr.open_dataset(x))

    return xr_open_fn_2, filepath


def open_time_nc_fn(
    filepath: str | Path | list[str] | list[Path],
    realign_on_domain: str | None = None,
    sel_month: bool = False,
    check_coords: str | None = "time",
) -> tuple[Callable, str | Path | list[str] | list[Path]]:
    """
    Check the filepath input to choose which xarray open function to use:
     - Path or single item List - use open_dataset
     - multiple item List - use open_mfdataset

    This function is a wrapper for open_nc_fn() to include appropriate default checks for data which contains a time axis. See open_nc_fn() for full details.
    """
    return open_nc_fn(
        filepath=filepath,
        realign_on_domain=realign_on_domain,
        sel_month=sel_month,
        check_coords=check_coords,
    )
