from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Callable

# import urllib.request
# from tqdm import tqdm


# class _TqdmUpTo(tqdm):
#     """Provides `update_to(n)` which uses `tqdm.update(delta_n)`.

#     Modified from https://github.com/tqdm/tqdm#hooks-and-callbacks
#     """

#     def update_to(self, b: int = 1, bsize: int = 1, tsize: Optional[int] = None):
#         """
#         b: Number of blocks transferred so far [default: 1].
#         bsize: Size of each block (in tqdm units) [default: 1].
#         tsize: Total size (in tqdm units). If [default: None] remains unchanged.
#         """
#         if tsize is not None:
#             self.total = tsize

#         return self.update(b * bsize - self.n)  # also sets self.n = b * bsize

# def download_file(url: str, download_path: Union[str, Path]) -> Path:
#     """Downloads a file from the given URL and shows a process bar during download.

#     Args:
#         url: URL
#         download_folder: Folder to save file
#     Returns:
#         Path: Path to downloaded file
#     """
#     filename = url.split("/")[-1]

#     with _TqdmUpTo(
#         unit="B", unit_scale=True, unit_divisor=1024, miniters=1, desc=filename
#     ) as t:  # all optional kwargs
#         _ = urllib.request.urlretrieve(url, filename=download_path, reporthook=t.update_to, data=None)

#     return download_path


def load_parser(data_name:str, module_name: str) -> Callable:
    """
    Load parse function from within module.
    
    This expects a function of the form to be :
        - parse_{data_name}()
    and for this to have been imported with an appropriate __init__.py module.

    Args:
        data_name: Name of data type / database / data source for the
            parse function.
        module_name: Full module name to be imported e.g.
            "openghg.standardise.surface"
    
    Returns:
        Callable : parse function
    """
    from importlib import import_module

    module = import_module(name=module_name)

    function_name = f"parse_{data_name.lower()}"
    fn: Callable = getattr(module, function_name)

    return fn


def load_surface_parser(data_type: str) -> Callable:
    """
    Load parsing object for the obssurface data type.
    Used with `openghg.standardise.surface` sub-module

    Args:
        data_type: Name of data type such as CRDS
    Returns:
        callable: class_name object
    """
    surface_module_name = "openghg.standardise.surface"
    fn = load_parser(data_type, surface_module_name)

    return fn


def load_column_parser(data_type) -> Callable:
    """
    Load a parsing object for the obscolumn data type.
    Used with `openghg.standardise.column` sub-module

    Args:
        data_type: Name of data type e.g. OPENGHG
    Returns:
        callable: parser function
    """
    column_st_module = "openghg.standardise.column"
    fn = load_parser(data_type, column_st_module)

    return fn


def load_column_source_parser(data_source) -> Callable:
    """
    Load a parsing object for the source of column data.
    Used with `openghg.transform.column` sub-module

    Args:
        data_type: Name of data source e.g. GOSAT
    Returns:
        callable: parser function
    """
    column_tr_module = "openghg.transform.column"
    fn = load_parser(data_source, column_tr_module)

    return fn


def load_emissions_parser(data_type: str) -> Callable:
    """
    Load a parsing object for the emissions data type.
    Used with `openghg.standardise.emissions` sub-module

    Args:
        data_type: Name of data type e.g. OPENGHG
    Returns:
        callable: parser function
    """
    emissions_st_module_name = "openghg.standardise.emissions"
    fn = load_parser(data_type, emissions_st_module_name)

    return fn


def load_emissions_database_parser(database: str) -> Callable:
    """
    Load a parsing object for the source of column data.
    Used with `openghg.transform.emissions` sub-module

    Args:
        data_type: Name of data source e.g. EDGAR
    Returns:
        callable: parser function
    """
    emissions_tr_module_name = "openghg.transform.emissions"
    fn = load_parser(database, emissions_tr_module_name)

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
