from typing import Optional, overload


@overload
def format_inlet(inlet: str, special_keywords: Optional[list] = None) -> str:
    ...


@overload
def format_inlet(inlet: None, special_keywords: Optional[list] = None) -> None:
    ...


def format_inlet(inlet: Optional[str], special_keywords: Optional[list] = None) -> Optional[str]:
    """Make sure inlet name conforms to standard of number followed by unit
    (or is one of the special keywords).
    If the string just contains numbers, it is assumed this is in metres and
    units of "m" will be added.

    e.g. "10m" not "10" or "10magl"

    Args:
        inlet: Height above ground level
        special_keywords: Specify special keywords inlet could be set to
            If so do not apply any formatting.
            If this is not set a special keyword of "multiple" and "column" will still be allowed.
    Returns:
        str: formatted inlet string / None
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

    # Check if input inlet just contains numbers and no unit
    # If so assume the units are metres and add this to the end of the string
    try:
        float(inlet)
    except ValueError:
        pass
    else:
        inlet = inlet + "m"

    # Check if inlet ends with "magl" rather than just "m"
    # If so remove the end "agl" to just include the "m" value
    if inlet.endswith("magl"):
        inlet = inlet.rstrip("lga")

    return str(inlet)
