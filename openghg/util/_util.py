""" Utility functions that are used by multiple modules

"""
from typing import Dict

__all__ = ["create_uuid", "unanimous", "load_object", "get_datapath", "read_header", "load_json", "valid_site", "is_number", "to_lowercase"]


def create_uuid():
    from uuid import uuid4

    return uuid4()


def unanimous(seq):
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


def load_object(class_name):
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


def get_datapath(filename, directory=None):
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


def load_json(filename):
    """Returns a dictionary deserialised from JSON. This function only works
        for JSON files in the openghg/data directory.

    Args:
        filename (str): Name of JSON file
    Returns:
        dict: Dictionary created from JSON
    """
    from json import load

    path = get_datapath(filename)

    with open(path, "r") as f:
        data = load(f)

    return data


def read_header(filepath, comment_char="#"):
    """Reads the header lines denoted by the comment_char

    Args:
        filepath (str or Path): Path to file
        comment_char (str, default="#"): Character that denotes a comment line
        at the start of a file
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


def valid_site(site):
    """Check if the passed site is a valid one

    Args:
        site (str): Three letter site code
    Returns:
        bool: True if site is valid
    """
    site_data = load_json("acrg_site_info.json")

    site = site.upper()

    if site not in site_data:
        site = site.lower()
        site_name_code = load_json("site_codes.json")
        return site in site_name_code["name_code"]

    return True


def is_number(s):
    """Is it a number?

    Args:
        s (str): String which may be a number
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def to_lowercase(d: Dict) -> Dict:
    """Convert all keys and values in a dictionary to lowercase

    Args:
        d: Dictionary to lower case
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
