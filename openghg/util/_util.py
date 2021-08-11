""" Utility functions that are used by multiple modules

"""
from typing import Any, Dict, Set, List, Union, Tuple, Optional, Iterator, overload
from collections.abc import Iterable
from pathlib import Path


__all__ = [
    "unanimous",
    "load_object",
    "get_datapath",
    "read_header",
    "load_json",
    "valid_site",
    "is_number",
    "to_lowercase",
    "pairwise",
    "multiple_inlets",
]


def unanimous(seq: Dict) -> bool:
    """Checks that all values in an iterable object
    are the same

    Args:
        seq: Iterable object
    Returns
        bool: True if all values are the same

    """
    it = iter(seq.values())
    try:
        first = next(it)
    except StopIteration:
        return True
    else:
        return all(i == first for i in it)


def load_object(class_name: str) -> Any:
    """Load an object of type class_name

    Args:
        class_name (str): Name of class to load
    Returns:
        class_name: class_name object
    """
    module_path = "openghg.modules"
    class_name = str(class_name).upper()

    # Here we try upper and lowercase for the module
    try:
        # Although __import__ is not usually recommended, here we want to use the
        # fromlist argument that import_module doesn't support
        module_object = __import__(name=module_path, fromlist=class_name)
        target_class = getattr(module_object, class_name)
    except AttributeError:
        class_name = class_name.lower().capitalize()
        module_object = __import__(name=module_path, fromlist=class_name)
        target_class = getattr(module_object, class_name)
    except ModuleNotFoundError as err:
        raise ModuleNotFoundError(f"{class_name} is not a valid module {err}")

    return target_class()


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


def read_header(filepath: Union[str, Path], comment_char: Optional[str] = "#") -> List:
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


def valid_site(site: str) -> bool:
    """Check if the passed site is a valid one

    Args:
        site: Three letter site code
    Returns:
        bool: True if site is valid
    """
    site_data = load_json("acrg_site_info.json")

    return site.upper() in site_data


def multiple_inlets(site: str) -> bool:
    """Check if the passed site has more than one inlet

    Args:
        site: Three letter site code
    Returns:
        bool: True if multiple inlets
    """
    site_data = load_json("acrg_site_info.json")

    site = site.upper()
    network = next(iter(site_data[site]))

    try:
        heights = set(site_data[network]["height"])
    except KeyError:
        try:
            heights = set(site_data[network]["height_name"])
        except KeyError:
            return True

    return len(heights) > 1


def is_number(s: str) -> bool:
    """Is it a number?

    Args:
        s: String which may be a number
    Returns:
        bool
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


@overload
def to_lowercase(d: Dict) -> Dict:
    ...


@overload
def to_lowercase(d: List) -> List:
    ...


@overload
def to_lowercase(d: Tuple) -> Tuple:
    ...


@overload
def to_lowercase(d: Set) -> Set:
    ...


@overload
def to_lowercase(d: str) -> str:
    ...


def to_lowercase(d: Union[Dict, List, Tuple, Set, str]) -> Union[Dict, List, Tuple, Set, str]:
    """Convert an object to lowercase. All keys and values in a dictionary will be converted
    to lowercase as will all objects in a list, tuple or set.

    Based on the answer https://stackoverflow.com/a/40789531/1303032

    Args:
        d: Object to lower case
    Returns:
        dict: Dictionary of lower case keys and values
    """
    if isinstance(d, dict):
        return {k.lower(): to_lowercase(v) for k, v in d.items()}
    elif isinstance(d, (list, set, tuple)):
        t = type(d)
        return t(to_lowercase(o) for o in d)
    elif isinstance(d, str):
        return d.lower()
    else:
        return d


def pairwise(iterable: Iterable) -> Iterator[Tuple[str, str]]:
    """Return a zip of an iterable where a is the iterable
    and b is the iterable advanced one step.

    Args:
        iterable: Any iterable type
    Returns:
        tuple: Tuple of iterables
    """
    from itertools import tee

    a, b = tee(iterable)
    next(b, None)

    return zip(a, b)
