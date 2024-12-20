from typing import cast, overload
import logging
from openghg.types import optionalPathType

__all__ = ["format_inlet", "extract_height_name"]

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


@overload
def format_inlet(
    inlet: str,
    units: str = "m",
    key_name: str | None = None,
    special_keywords: list | None = None,
) -> str: ...


@overload
def format_inlet(
    inlet: None,
    units: str = "m",
    key_name: str | None = None,
    special_keywords: list | None = None,
) -> None: ...


@overload
def format_inlet(
    inlet: slice,
    units: str = "m",
    key_name: str | None = None,
    special_keywords: list | None = None,
) -> slice: ...


@overload
def format_inlet(
    inlet: list[str | slice | None],
    units: str = "m",
    key_name: str | None = None,
    special_keywords: list | None = None,
) -> list[str | slice | None]: ...


def format_inlet(
    inlet: str | slice | None | list[str | slice | None],
    units: str = "m",
    key_name: str | None = None,
    special_keywords: list | None = None,
) -> str | slice | None | list[str | slice | None]:
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
        same type as input, with all strings formatted

    Usage:
        >>> format_inlet("10")
            "10m"
        >>> format_inlet("10m")
            "10m"
        >>> format_inlet("10magl")
            "10m"
        >>> format_inlet("10.111")
            "10.1m"
        >>> format_inlet(["10", 100])
            ["10m", "100m"]
        >>> format_inlet("multiple")
            "multiple"
        >>> format_inlet("10m", key_name="inlet")
            "10m"
        >>> format_inlet("10m", key_name="inlet_magl")
            "10"
        >>> format_inlet("10m", key_name="station_height_masl")
            "10"
    """
    # process list recursively
    if isinstance(inlet, list):
        return [format_inlet(x) for x in inlet]

    # pass through None and slice
    if inlet is None or isinstance(inlet, slice):
        return inlet

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
        unit_options = [cast(str, units)]

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


def extract_height_name(
    site: str,
    network: str | None = None,
    inlet: str | None = None,
    site_filepath: optionalPathType = None,
) -> str | list | None:
    """
    Extract the relevant height associated with NAME from the
    "height_name" variable, if present from site_info data.

    This expects the "height_name" variable to be one of:
      - list containing the same number of items as inlets for the site
      - dictionary containing the mapping between inlets and heights
        used in NAME.

    Args:
        site : Site code
        network: Name of the associated network for the site
        inlet: Observation inlet / height value in the specified units
        site_filepath: Alternative site info file. Defaults to openghg_defs input.
    Returns:
        str : appropriate height name value extracted from site_info
        list: multiple height name options extracted from site_info
        None: if value not found or ambiguous.
    """
    from openghg.util import get_site_info

    site_data = get_site_info(site_filepath=site_filepath)

    if site:
        site_upper = site.upper()

    if network is None:
        network = next(iter(site_data[site_upper]))
    else:
        network = network.upper()

    height_name_attr = "height_name"
    height_attr = "height"

    if site_upper in site_data:
        site_metadata = site_data[site_upper][network]
        if height_name_attr in site_metadata:
            # Extract height_name variable from the site_metadata
            height_name_extracted = site_metadata[height_name_attr]
            # If this is a list, check and try and extract appropriate value.
            if isinstance(height_name_extracted, list):
                # Check if multiple values for height_name_extracted are present (list > 1)
                if len(height_name_extracted) == 1:
                    height_name: str | None = height_name_extracted[0]
                else:
                    # If this is ambiguous, check "height" attr to match against site inlet value
                    # This assumes two lists of the same length map to each other with translating values
                    if (inlet is not None) and (height_attr in site_metadata):
                        height_values = site_metadata[height_attr]
                        if len(height_values) == len(height_name_extracted) and (inlet in height_values):
                            index = height_values.index(inlet)
                            height_name = height_name_extracted[index]
                        else:
                            logger.warning(
                                f"Ambiguous '{height_name_attr}' in site_info. "
                                f"Unable to extract from: height_name = {height_name_extracted} using height = {inlet}"
                            )
                            height_name = None
                    else:
                        logger.warning(
                            f"Ambiguous '{height_name_attr}' in site_info. "
                            f"Unable to extract from: height_name = {height_name_extracted}"
                        )
                        height_name = None
            elif isinstance(height_name_extracted, dict):
                if (inlet is not None) and (inlet in height_name_extracted):
                    height_name = height_name_extracted[inlet]
                else:
                    logger.warning(
                        f"Unable to interpret {height_name_extracted}. Please supply or check supplied inlet value: {inlet}"
                    )
                    height_name = None
        else:
            height_name = None
    else:
        height_name = None

    return height_name
