from openghg.util._site import get_site_info
from openghg.types import optionalPathType


def get_platform_from_info(site: str, site_filepath: optionalPathType = None) -> str | None:
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
