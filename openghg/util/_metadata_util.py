from typing import Any, Union, Iterable
import logging
import math

__all__ = ["check_number_match", "check_str_match", "check_value_match", "check_overlap_keys", "remove_keys_null"]


logger = logging.getLogger("openghg.util")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


def check_number_match(
    number1: Union[str, int, float], number2: Union[str, int, float], relative_tolerance: float = 1e-3
) -> bool:
    """
    Check values that can be identified as numbers match within a specified tolerance.

    Args:
        number1, number2: Input values for comparison - number-like.
        relative_tolerance: Allowed relative difference between numbers.
    Returns:
        bool: True/False if numbers match within relative tolerance.
    """
    if math.isclose(float(number1), float(number2), rel_tol=relative_tolerance):
        return True
    else:
        return False


def check_str_match(value1: Any, value2: Any, lower: bool = True) -> bool:
    """
    Check values which can be converted to strings match. Can specify whether lower
    case should be applied when checking match (case-insensitive).

    Args:
        value1, value2: Input values for comparison - will convert to strings.
        lower: Apply lower case to the two string to make this case-insensitive.
    Returns:
        bool: True/False if values match depending on case-sensitivity.
    """
    value1 = str(value1)
    value2 = str(value2)

    if lower:
        value1 = value1.lower()
        value2 = value2.lower()

    if value1 == value2:
        return True
    else:
        return False


def check_value_match(value1: Any, value2: Any, relative_tolerance: float = 1e-3, lower: bool = True) -> bool:
    """
    Check input values match.
    - For values which can be identified as numbers, a float comparison will be made
      using the relative_tolerance.
    - For other values a string comparison will be made, with lower case applied by default.

    Args:
        value1, value2: Input values for comparison
        relative_tolerance: Tolerance between two numbers.
            Only used if values are identified as number-like
        lower: Whether to apply lower case to the two input values as strings.
            Used when making a string comparison if values are not identified
            as number-like.
    Returns:
        bool: True/False if it is determined that values do/don't match
    """
    from openghg.util._strings import is_number

    if is_number(value1) and is_number(value2):
        return check_number_match(value1, value2, relative_tolerance)
    else:
        return check_str_match(value1, value2, lower)


def check_overlap_keys(
    dict1: dict, dict2: dict, check_value: bool = True, relative_tolerance: float = 1e-3, lower: bool = True
) -> list:
    """
    Check for overlapping keys between two dictionaries. Raise an error if overlapping
    keys are found and values do not match (within tolerance / case matching).

    Args:
        dict1, dict2: Input dictionaries for comparison
        check_value: Whether to check the value itself when comparing.
            If True: for overlapping keys check the value and don't raise an error
            If False: raise an error for any overlapping keys.
        relative_tolerance: If value is being checked, use this as check for
            number-like values.
        lower: If value is being checked, compare lower case strings.
    Returns:
        list: List of overlapping keys if values match

        raises ValueError: if overlapping keys and values do not match
    """

    overlapping_keys: Union[set, list] = set(dict1.keys()) & set(dict2.keys())

    overlap_values_not_matching = {}
    if overlapping_keys:
        for key in overlapping_keys:
            value1 = dict1[key]
            value2 = dict2[key]
            if check_value and check_value_match(value1, value2, relative_tolerance, lower):
                logger.warning(
                    f"Same key '{key}' supplied from different sources. Error not raised because values match: '{value1}' (1), '{value2}' (2)."
                )
            else:
                overlap_values_not_matching[key] = (value1, value2)

    if overlap_values_not_matching:
        mismatch_details = [
            f" - '{key}', metadata: {values[0]}, attributes: {values[1]}"
            for key, values in overlap_values_not_matching.items()
        ]
        mismatch_str = "\n".join(mismatch_details)
        msg = f"Same key(s) supplied from different sources:\n{mismatch_str}"
        logger.error(msg)
        raise ValueError(msg)

    return list(overlapping_keys)


def remove_keys_null(dictionary: dict, null_values: Iterable = (None)) -> dict:
    """
    Remove keys from a dictionary which are indicated to be null. By default,
    null_values is None.

    Returns:
        dict: Copy of dictionary with key: null value pairs removed.    
    """
    dictionary = {key: value for key, value in dictionary.items() if value not in null_values}
    return dictionary
