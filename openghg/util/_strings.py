from typing import Any, Dict, Set, List, Union, Tuple, Optional, overload

__all__ = ["clean_string", "to_lowercase"]


@overload
def clean_string(to_clean: str) -> str:
    ...


@overload
def clean_string(to_clean: None) -> None:
    ...


def clean_string(to_clean: Optional[str]) -> Union[str, None]:
    """Returns a lowercase string with only alphanumeric
    characters and underscores.

    Args:
        to_clean: String to clean
    Returns:
        str or None: Clean string
    """
    import re

    if to_clean is None:
        return None

    try:
        # This might be used with numbers
        if is_number(to_clean):
            return str(to_clean)

        # Removes all whitespace
        cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
        # Removes non-alphanumeric characters but keep underscores
        cleanest = re.sub(r"\W+", "", cleaner)
    except TypeError:
        return to_clean

    return cleanest


@overload
def to_lowercase(d: Dict, skip_keys: Optional[List] = None) -> Dict:
    ...


@overload
def to_lowercase(d: List, skip_keys: Optional[List] = None) -> List:
    ...


@overload
def to_lowercase(d: Tuple, skip_keys: Optional[List] = None) -> Tuple:
    ...


@overload
def to_lowercase(d: Set, skip_keys: Optional[List] = None) -> Set:
    ...


@overload
def to_lowercase(d: str, skip_keys: Optional[List] = None) -> str:
    ...


def to_lowercase(
    d: Union[Dict, List, Tuple, Set, str], skip_keys: Optional[List] = None
) -> Union[Dict, List, Tuple, Set, str]:
    """Convert an object to lowercase. All keys and values in a dictionary will be converted
    to lowercase as will all objects in a list, tuple or set. You can optionally pass in a list of keys to
    skip when lowercasing a dictionary.

    Based on the answer https://stackoverflow.com/a/40789531/1303032

    Args:
        d: Object to lower case
        skip_keys: List of keys to skip when lowercasing.
    Returns:
        dict: Dictionary of lower case keys and values
    """
    if skip_keys is None:
        skip_keys = []

    if isinstance(d, dict):
        lowercased = {k.lower(): to_lowercase(v) for k, v in d.items() if k not in skip_keys}

        if skip_keys:
            missing = {k: v for k, v in d.items() if k not in lowercased}
            lowercased.update(missing)

        return lowercased
    elif isinstance(d, (list, set, tuple)):
        t = type(d)
        return t(to_lowercase(o) for o in d)
    elif isinstance(d, str):
        return d.lower()
    else:
        return d


def is_number(s: Any) -> bool:
    """Is it a number?

    https://stackoverflow.com/q/354038

    Args:
        s: String which may be a number
    Returns:
        bool
    """
    if isinstance(s, bool):
        return False

    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def remove_punctuation(s: str) -> str:
    """Removes punctuation and converts the passed string
    to lowercase

    Args:
        s: String to convert
    Returns:
        str: Unpunctuated, lowercased string
    """
    import re

    s = s.lower()
    return re.sub(r"[^\w\s]", "", s)
