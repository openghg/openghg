from typing import Optional, overload

__all__ = ["format_inlet"]


@overload
def format_inlet(inlet: str, special_keywords: Optional[list] = None) -> str:
    ...


@overload
def format_inlet(inlet: None, special_keywords: Optional[list] = None) -> None:
    ...


def format_inlet(inlet: Optional[str],
                 units: Optional[str] = "m",
                 key_name: Optional[str] = None,
                 special_keywords: Optional[list] = None) -> Optional[str]:
    """
    Make sure inlet / height name conforms to standard. The standard
    imposed can depend on the associated key_name itself (can
    be supplied as an option to check).

    This standard is as follows:
     - number followed by unit
     - number alone if unit / derviative is specified at the end of key_name (e.g. station_height_masl)
     - unchanged if this is one of the special keywords (by default "multiple" or "various")

    Other considerations:
     - For units of "m", we will also look for "magl" and "masl" (metres above ground and sea level)
     - If the input string just contains numbers, it is assumed this is already within the correct unit.

    Args:
        inlet: Inlet / Height value in the specified units
        units: Units for the inlet value ("m" by default)
        key_name: Name of the associated key. This is optional but will be used to
            determine whether the unit value should be added to the output string.
        special_keywords: Specify special keywords inlet could be set to
            If so do not apply any formatting.
            If this is not set a special keyword of "multiple" and "column" will still be allowed.
    Returns:
        str: formatted inlet string / None

        >>> format_inlet("10")
            "10m"
        >>> format_inlet("10m")
            "10m"
        >>> format_inlet("10magl")
            "10m"
        >>> format_inlet("10.111")
            "10.1m"
        >>> format_inlet("multiple")
            "multiple"
        >>> format_inlet("10m", key_name="inlet")
            "10m"
        >>> format_inlet("10m", key_name="inlet_magl")
            "10"
        >>> format_inlet("10m", key_name="station_height_masl")
            "10"
    """
    if inlet is None:
        return None

    # By default the special keyword is "multiple" for data containing multiple inlets.
    # This will be included if data is a combined object from the object store.
    if special_keywords is None:
        special_keywords = ["multiple", "column"]

    # Check if inlet is set to a special keyword
    if inlet in special_keywords:
        return inlet

    # Define set of options associated with units. For "m" this include
    # "magl" and "masl" (metres above ground and sea level).
    if units == "m":
        unit_options = ["m", "magl", "masl"]
    else:
        unit_options = [units]

    # Check whether unit is needed in string output.
    # This is dependent on whether the key name itself contains the unit value
    # (or derivative). If so, the unit itself is not needed in the value.
    unit_needed = True
    if key_name is not None:
        for value in unit_options:
            if key_name.split("_")[-1] == value:
                unit_needed = False

    # Check if input inlet just contains numbers and no unit
    # If so assume the units are metres and add this to the end of the string
    try:
        inlet_float = float(inlet)
    except ValueError:
        pass
    else:
        if inlet_float.is_integer():
            if unit_needed:
                inlet = f"{inlet_float:.0f}{units}"
            else:
                inlet = f"{inlet_float:.0f}"
        else:
            if unit_needed:
                inlet = f"{inlet_float:.1f}{units}"
            else:
                inlet = f"{inlet_float:.1f}"

        return str(inlet)

    # If we were unable to cast inlet as a float
    # check if inlet ends with unit or unit derivative
    # e.g. "magl" and "masl" would need to replaced with "m" or be removed
    for value in unit_options:
        if inlet.endswith(value):
            if unit_needed:
                inlet = inlet.replace(value, units)
            else:
                inlet = inlet.rstrip(value)
            break
    # else:
    #     raise ValueError(f"Did not recognise input for inlet: {inlet}")

    return str(inlet)
