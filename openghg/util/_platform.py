from typing import overload
import logging
from openghg.util._site import get_site_info
from openghg.util._metadata_util import not_set_metadata_values
from openghg.types import pathType, MetadataFormatError

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def define_platform(data_type: str | list | None = None) -> list:
    """
    Define platform values which can be used. These describe the type of data and
    platform used to measure the data.

    Args:
        data_type: Specify the data_type to select the associated platforms.
            For "surface" this is currently:
                - "surface-insitu"
                - "surface-flask"
            For "column" this is currently:
                - "satellite"
                - "column-insitu"
                - "column"
            For "mobile" this is currently:
                - "aircraft-flask"
                - "balloon-flask"
                - "mobile-flask"
            If no data_type is specified, this will return all platform values.
    Returns:
        list: Platform values
    """

    platform_values = []

    platform_values_all = {
        "surface": [
            "surface-insitu",
            "surface-flask",
        ],
        "column": [
            "satellite",
            "column-insitu",
            "column",
        ],
        "mobile": [
            "aircraft-flask",
            "balloon-flask",
            "mobile-flask",
        ],
    }

    obs_types = ["surface", "column", "mobile"]

    if data_type is None:
        data_types = obs_types
    elif isinstance(data_type, str):
        data_types = [data_type]
    else:
        data_types = data_types

    for data_type in data_types:
        platform_values.extend(platform_values_all[data_type])

    return platform_values


@overload
def format_platform(platform: str, data_type: str | None = None) -> str: ...


@overload
def format_platform(platform: None, data_type: str | None = None) -> None: ...


def format_platform(platform: str | None, data_type: str | None = None) -> str | None:
    """
    Check platform is a suitable value based on the values within define_platform.
    This could change the case of the platform value to match to the define_platform values if the original case does not match.

    Args:
        platform: Platform name to check
        data_type: Type of data associated with the platform.
            This is generally associated with observation measurements.
            These are defined within openghg.store.spec.define_data_types function.
    Returns:
        str | None: checked platform value
    """

    platform_values = define_platform(data_type=data_type)
    not_set_values = not_set_metadata_values()

    if platform is None or platform in not_set_values:
        return platform
    elif platform in platform_values:
        return platform
    elif platform.lower() in platform_values:
        return platform.lower()
    else:
        msg = f"Platform currently set to '{platform}'. This must be one of: {platform_values}"
        logger.exception(msg)
        raise MetadataFormatError(msg)


def get_platform_from_info(site: str, site_filepath: pathType | None = None) -> str | None:
    """Find the platform for a site, if present.

    This will access the "site_info.json" file from openghg_defs dependency to
    find this information.

    Args:
        site: Site code (usually 3-letters).
    Returns:
        str | None: platform name from site_info.json if present
    """

    site_data = get_site_info(site_filepath=site_filepath)

    site_upper = site.upper()

    try:
        site_details = site_data[site_upper]
    except KeyError:
        return None
    else:
        platform: str = site_details.get("platform")
        return platform
