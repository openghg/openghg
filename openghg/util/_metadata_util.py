from typing import Any, Union, Iterable, List, Optional
import logging
import math


__all__ = [
    "null_metadata_values",
    "not_set_metadata_values",
    "remove_null_keys",
    "check_number_match",
    "check_str_match",
    "check_value_match",
    "check_not_set_value",
    "get_overlap_keys",
    "merge_dict",
]


logger = logging.getLogger("openghg.util")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


def null_metadata_values() -> List:
    """
    Defines values which indicate metadata key should be ignored and not retained within
    the metadata (or added to the metastore).

    Returns:
        list: values to be seen as null
    """
    null_values = [None]

    return null_values


def not_set_metadata_values() -> List:
    """
    Defines values which indicate metadata value has not been explicitly specified
    but that we still want to retain within the metadata (and metastore) so the key is present.

    Returns:
        list: values which indicate key has not been set
    """
    # TODO: Depending on how this is implemented, may want to update this to include np.nan values
    not_set_values = ["not_set", "NOT_SET"]

    return not_set_values


def remove_null_keys(dictionary: dict, null_values: Iterable = null_metadata_values()) -> dict:
    """
    Remove keys from a dictionary for values which we want to ignore. By default,
    ignore_values is (None, ).

    Returns:
        dict: Copy of dictionary with key: null value pairs removed.
    """
    dictionary = {key: value for key, value in dictionary.items() if value not in null_values}
    return dictionary


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


def check_not_set_value(
    value1: Any, value2: Any, not_set_values: Iterable = not_set_metadata_values()
) -> Any:
    """
    Check whether either value is in the list of not set values and if so return the other
    value in preference.
    Not set values are sometimes needed if we want to include a key but need a string to indicate
    this value has not been specified by the user.

    Args:
        value1, value2: Input values for comparison
        not_set_values: List of values which indicate value has not been explicitly set. (e.g. "not_set")
            By default this is defined by not_set_metadata_values() function
    Returns:
        value: if one value has not been set (value1 checked first) returns the other value
        None: if neither value has not been set.
    """
    if value1 in not_set_values:
        return value2
    elif value2 in not_set_values:
        return value1
    else:
        return None


def get_overlap_keys(dict1: dict, dict2: dict) -> list:
    """
    Find the keys which match between two dictionaries. Return list of these keys.
    """
    overlapping_keys_check = set(dict1.keys()) & set(dict2.keys())
    overlapping_keys = list(overlapping_keys_check)
    return overlapping_keys


def _filter_keys(dictionary: dict, keys: Optional[Iterable] = None, negate: bool = False):
    """
    Convenience function to filter a dictionary by a set of keys.
    """
    if keys is None:
        return dictionary  # probably should return a copy for consistency, but keys = None is mostly for convenience
    if negate is True:
        return {key: value for key, value in dictionary.items() if key not in keys}
    return {key: value for key, value in dictionary.items() if key in keys}


def merge_dict(
    dict1: dict,
    dict2: dict,
    keys: Optional[Iterable] = None,
    keys_dict1: Optional[Iterable] = None,
    keys_dict2: Optional[Iterable] = None,
    remove_null: bool = True,
    null_values: Iterable = null_metadata_values(),
    check_value: bool = True,
    relative_tolerance: float = 1e-3,
    lower: bool = True,
    not_set_values: Iterable = not_set_metadata_values(),
    resolve_mismatch: bool = False,
) -> dict:
    """
    The merge_dict function merges the key:value pairs of two dictionaries checking for
    overlap between them.

    Depending on the choice of inputs:
        - null values (e.g. None) will be removed from both dictionaries before comparison
        - if the same keys are present in both dictionaries:
            - if one of the two values is identified as a value which has not been explicitly set (e.g. "not_set") the
            other value will be used in preference.
            - otherwise, the value from dict1 gets preference in the merged dictionary.

    Args:
        dict1, dict2 : Dictionaries to compare and merge
        keys: Only include specific keys across both dictionaries in merged output
        keys_dict1, keys_dict1: Select specific keys from dict1/dict2 when merging
        remove_null: Before comparing and merging, remove keys from dict1 and dict2 which have null values.
        null_values: Values which are classed as null.
            See null_metadata_values() function for list of default values.
        check_value: If keys overlap, check values match.
            See check_value_match() function for rules of matching.
        relative_tolerance: Tolerance between two numbers when checking values.
        lower: Whether to apply lower case to the two input values as strings when checking values.
        not_set_values: Values which indicate this value has not been specified.
            See not_set_metadata_values() function for list of default values.
        resolve_mismatch: If keys overlap, values do not match and neither is null,
            use value from dict1 and raise a warning. This will raise an error if set to False.
    Returns:
        dict: Merged dictionary
    Raises:
        ValueError: if check_value is False and any keys overlap
        ValueError: if resolve_mismatch is False, values for overlapping keys don't match and neither matched null_values
    """
    # Remove null values
    if remove_null:
        dict1 = remove_null_keys(dict1, null_values)
        dict2 = remove_null_keys(dict2, null_values)

    # Filter dictionaries based on any input key selections
    if keys is not None:
        dict1 = _filter_keys(dict1, keys)
        dict2 = _filter_keys(dict2, keys)
    else:
        if keys_dict1 is not None:
            dict1 = _filter_keys(dict1, keys_dict1)
        if keys_dict2 is not None:
            dict2 = _filter_keys(dict2, keys_dict2)

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

            # Check whether either one of the values indicates this has not been explictly set.
            # - if so this prefer the other value if there is a difference
            value_present = check_not_set_value(value1, value2, not_set_values)
            if check_value_match(value1, value2, relative_tolerance, lower):
                if value_present is None:
                    logger.warning(
                        f"Same key '{key}' supplied from different sources. Error not raised because values match: '{value1}' (1), '{value2}' (2)."
                    )
                merged_dict[key] = value1
            elif value_present is not None:
                merged_dict[key] = value_present
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
