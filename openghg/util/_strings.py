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
