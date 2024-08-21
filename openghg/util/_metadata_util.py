from typing import Any, Union, Iterable, Optional
import logging
import math

__all__ = [
    "check_number_match",
    "check_str_match",
    "check_value_match",
    "get_overlap_keys",
    "merge_dict",
    "remove_keys_null",
]


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


def get_overlap_keys(dict1: dict, dict2: dict) -> list:
    """
    Find the keys which match between two dictionaries. Return list of these keys.
    """
    overlapping_keys_check = set(dict1.keys()) & set(dict2.keys())
    overlapping_keys = list(overlapping_keys_check)
    return overlapping_keys


def merge_dict(
    dict1: dict,
    dict2: dict,
    keys: Optional[Iterable] = None,
    keys_dict1: Optional[Iterable] = None,
    keys_dict2: Optional[Iterable] = None,
    check_value: bool = True,
    relative_tolerance: float = 1e-3,
    lower: bool = True,
    resolve_mismatch: bool = False,
) -> dict:
    """
    The merge_dict function merges the key:value pairs of two dictionaries checking for
    overlap between them.

    Depending on the choice of inputs, if the same keys are present in both dictionaries
    the value from dict1 gets preference in the merged dictionary.

    Args:
        dict1, dict2 : Dictionaries to compare and merge
        keys: Only include specific keys across both dictionaries in merged output
        keys_dict1, keys_dict1: Select specific keys from dict1/dict2 when merging
        check_value: If keys overlap, check values match.
            See check_value_match() function for rules of matching.
        relative_tolerance: Tolerance between two numbers when checking values.
        lower: Whether to apply lower case to the two input values as strings when checking values.
        resolve_mismatch: If keys overlap and values do not match, use value from
            dict1 and raise a warning. This will raise an error if set to False.
    Returns:
        dict: Merged dictionary

        if check_value is False:
            raises ValueError if any keys overlap
        if resolve_mismatch is False:
            raises ValueError if values for overlapping keys don't match
    """
    # Filter dictionaries based on any input key selections
    if keys is not None:
        dict1 = {key: value for key, value in dict1.items() if key in keys}
        dict2 = {key: value for key, value in dict2.items() if key in keys}
    else:
        if keys_dict1 is not None:
            dict1 = {key: value for key, value in dict1.items() if key in keys_dict1}
        if keys_dict2 is not None:
            dict2 = {key: value for key, value in dict2.items() if key in keys_dict2}

    overlapping_keys = get_overlap_keys(dict1, dict2)

    dict1_non_overlap = {key: value for key, value in dict1.items() if key not in overlapping_keys}
    dict2_not_overlap = {key: value for key, value in dict2.items() if key not in overlapping_keys}

    # Merge the two dictionaries for the keys we know don't overlap
    merged_dict = dict1_non_overlap | dict2_not_overlap

    if check_value:
        overlap_values_not_matching = {}
        for key in overlapping_keys:
            value1 = dict1[key]
            value2 = dict2[key]
            if check_value_match(value1, value2, relative_tolerance, lower):
                logger.warning(
                    f"Same key '{key}' supplied from different sources. Error not raised because values match: '{value1}' (1), '{value2}' (2)."
                )
                merged_dict[key] = value1
            else:
                if resolve_mismatch:
                    merged_dict[key] = value1
                    logger.warning(
                        f"Values do not match between dictionaries. Updating to {key} = {value1} (not {value2})"
                    )
                else:
                    overlap_values_not_matching[key] = (value1, value2)
    else:
        raise ValueError(f"Unable to merge dictionaries with overlapping keys: {','.join(overlapping_keys)}")

    if overlap_values_not_matching:
        mismatch_details = [
            f" - '{key}', dict1: {values[0]}, dict2: {values[1]}"
            for key, values in overlap_values_not_matching.items()
        ]
        mismatch_str = "\n".join(mismatch_details)
        msg = f"Same key(s) supplied from different sources:\n{mismatch_str}"
        logger.error(msg)
        raise ValueError(msg)

    return merged_dict


def remove_keys_null(dictionary: dict, null_values: Iterable = (None,)) -> dict:
    """
    Remove keys from a dictionary which are indicated to be null. By default,
    null_values is None.

    Returns:
        dict: Copy of dictionary with key: null value pairs removed.
    """
    dictionary = {key: value for key, value in dictionary.items() if value not in null_values}
    return dictionary
