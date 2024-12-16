import re
from typing import Any, overload
from collections.abc import Iterable

from openghg.util import not_set_metadata_values, null_metadata_values

__all__ = ["clean_string", "to_lowercase"]


@overload
def clean_string(to_clean: str) -> str: ...


@overload
def clean_string(to_clean: None) -> None: ...


def clean_string(to_clean: str | None) -> str | None:
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

    if isinstance(to_clean, bool):
        return str(to_clean).lower()

    try:
        # This might be used with numbers
        if is_number(to_clean):
            return str(to_clean)

        # Removes all whitespace
        cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
        # Removes non-alphanumeric characters but keep underscores
        # cleanest = re.sub(r"\W+", "", cleaner)
        cleanest = re.sub(r"[^\w-]+", "", cleaner)
    except TypeError:
        return to_clean

    return cleanest


@overload
def to_lowercase(d: dict, skip_keys: list | None = None) -> dict: ...


@overload
def to_lowercase(d: list, skip_keys: list | None = None) -> list: ...


@overload
def to_lowercase(d: tuple, skip_keys: list | None = None) -> tuple: ...


@overload
def to_lowercase(d: set, skip_keys: list | None = None) -> set: ...


@overload
def to_lowercase(d: str, skip_keys: list | None = None) -> str: ...


def to_lowercase(
    d: dict | list | tuple | set | str, skip_keys: list | None = None
) -> dict | list | tuple | set | str:
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


def extract_float(s: str) -> float:
    """Extract float from string.

    This extends the built-in `float` function to find floats within a larger string.

    Args:
        s: string to extract float from

    Returns:
        first float value found in given string

    Raises:
        ValueError if no floats found
    """
    # construct regex for float following Python's float grammar:
    # https://docs.python.org/3/library/functions.html#grammar-token-float-floatvalue
    sign = r"[+-]?"  # optional sign
    letter_neg_lookbehind = r"(?<![a-zA-Z])"  # negative lookbehind assertion for letters
    letter_neg_lookahead = r"(?![a-zA-Z])"  # negative lookahead assertion for letters
    infinity = letter_neg_lookbehind + r"(Infinity|inf)" + letter_neg_lookahead
    nan = letter_neg_lookbehind + "nan" + letter_neg_lookahead
    digit_part = r"(\d(_?\d)*)"  # underscores ignored
    number = (
        rf"({digit_part}?\.{digit_part}|{digit_part}\.?)"  # at least 1 number before or after decimal place
    )
    exponent = r"([eE]?[+-]?\d+)?"  # optional exponent
    float_number = number + exponent
    abs_float_value = "(" + "|".join([float_number, infinity, nan]) + ")"

    float_pat = re.compile(sign + abs_float_value, re.IGNORECASE)

    if m := float_pat.search(s):
        return float(m.group(0))

    raise ValueError(f"No float values found in '{s}'")


def check_and_set_null_variable(
    param: str | None,
    not_set_value: str | None = None,
    null_values: Iterable = null_metadata_values(),
) -> str | None:
    """
    Check whether a variable is set to a null value (e.g. None) and if so replace this with
    a defined string used to indicate the variable has not been set.
    This is typically: "not_set"

    Args:
        param: variable to check
        not_set_value: Optional value to replace if None. By default details
            from openghg.util.not_set_metadata_values() will be used.
        null_values: Values to identify as null. By default details
            from openghg.util.null_metadata_values() will be used.
    Returns:
        str: Original string or value to indicate this is not set
        None: Only returned if value is None and None is not included as one of the null_values.
    """

    if not_set_value is None:
        not_set_values = not_set_metadata_values()
        not_set_value = not_set_values[0]

    if param in null_values:
        param = not_set_value

    return param
