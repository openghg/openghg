import pandas as pd
from openghg.types import SurfaceTypes, optionalPathType
from openghg.util import get_datapath, get_site_info, sites_in_network

# from openghg.types import DataTypes  # Would this be more appropriate?
# This does include Footprint as well as the input obs data types?


def _extract_site_names(site_codes: list, site_filepath: optionalPathType = None) -> list:
    """
    Extracts long names for site codes.

    This uses the data stored within openghg_defs/site_info JSON file by default.

    Args:
        site_codes: List of site codes
        site_filepath: Alternative site info file.

    Returns:
        list: Long names for each site code
    """

    # Get data for site
    site_data = get_site_info(site_filepath)

    # Extracts long name from site data
    site_names = []
    for site in site_codes:
        site_details = site_data[site]
        data0 = list(site_details.values())[0]  # Uses first network entry
        try:
            site_name = data0["long_name"]
        except KeyError:
            site_name = ""
        site_names.append(site_name)

    return site_names


def summary_source_formats() -> pd.DataFrame:
    """
    Create summary DataFrame of accepted input source formats. This includes
    the site code, long name, platform and source format.

    Returns:
        pandas.DataFrame

    TODO: Add source_format details for mobile / column etc. when added
    TODO: Consider if we need to / how best to incorporate BEA2CON data
    """
    # Could include input for surface / mobile / column etc.?
    surface_source_formats = list(SurfaceTypes.__members__)

    collated_site_data = pd.DataFrame()

    # Get additional data about source formats
    # - expect details in here to be related to sites where source_format
    #   does not match to network name.
    source_format_file = get_datapath("source_format_data.csv")
    source_format_data = pd.read_csv(source_format_file)

    for source_format in surface_source_formats:
        source_format_site = source_format_data[source_format_data["source_format"] == source_format]
        site_codes = source_format_site["Site"].values

        # If no data in source format file, assume source_format may be
        # applicable to network sites.
        # TODO: May be a bad assumption, consider this and update as necessary.
        if len(site_codes) == 0:
            site_codes = sites_in_network(network=source_format)

        site_names = _extract_site_names(site_codes=site_codes)

        if len(source_format_site) > 0:
            site_data = source_format_site.copy()
        else:
            site_data = pd.DataFrame({"source_format": source_format, "Site": site_codes})
            site_data = site_data.reindex(source_format_site.columns, axis=1)

        site_data = site_data.rename(columns={"Site": "Site code", "source_format": "Source format"})
        site_data["Long name"] = site_names
        site_data["Platform"] = "surface site"

        collated_site_data = pd.concat([collated_site_data, site_data], ignore_index=True)

    # TODO: May want to sort by the site code but removed for now as nice
    # to keep GCWERKS and CRDS first.
    # collated_site_data = collated_site_data.sort_values(by="Site code")

    # Reorder columns to make "Source format" first
    columns = list(collated_site_data.columns)
    columns.remove("Source format")
    columns = ["Source format"] + columns
    collated_site_data = collated_site_data[columns]

    return collated_site_data


def summary_site_codes(site_filepath: optionalPathType = None) -> pd.DataFrame:
    """
    Create summary DataFrame of site codes. This includes details of the network,
    longitude, latitude, height above sea level and stored heights.

    Note: there may be multiple entries for the same site code if this is
    associated with multiple networks.

    Returns:
        pandas.DataFrame
    """

    # Get data for site
    site_data = get_site_info(site_filepath)

    site_dict: dict[str, list] = {}
    site_dict["site"] = []
    site_dict["network"] = []

    expected_keys: list = ["long_name", "latitude", "longitude", "height_station_masl", ["heights", "height"]]

    name_keys = [key[0] if isinstance(key, list) else key for key in expected_keys]
    for key in name_keys:
        site_dict[key] = []

    for site, network_data in site_data.items():
        for network, data in network_data.items():
            for key in expected_keys:
                if not isinstance(key, list):
                    search_keys = [
                        key,
                    ]
                else:
                    search_keys = key

                name = search_keys[0]
                for key in search_keys:
                    if key in data:
                        site_dict[name].append(str(data[key]))
                        break
                else:
                    site_dict[name].append("")

            site_dict["network"].append(network)
            site_dict["site"].append(site)

    site_df = pd.DataFrame(site_dict)

    all_keys = name_keys.copy()
    all_keys.extend(["site", "network"])
    descriptive_names = {
        "site": "Site Code",
        "long_name": "Long name",
        "height_station_masl": "Station height (masl)",
        "heights": "Inlet heights",
    }
    column_names = {
        name: (descriptive_names[name] if name in descriptive_names else name.capitalize())
        for name in all_keys
    }

    site_df = site_df.rename(columns=column_names)
    site_df = site_df.set_index("Site Code")

    return site_df
