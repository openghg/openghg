from pathlib import Path
from typing import Any, Dict, List, Union, Optional

__all__ = ["load_object", "get_datapath", "read_header", "load_json"]


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
        return (
            Path(__file__)
            .resolve()
            .parent.parent.joinpath(f"data/{directory}/{filename}")
        )


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
