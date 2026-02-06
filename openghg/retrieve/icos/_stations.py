import logging
import pandas as pd
from dataclasses import asdict
from icoscp_core.icos import meta, ATMO_STATION
from icoscp_core.queries.stationlist import StationLite
from icoscp_core.metaclient import Station


logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def retrieve_list_stations(atmospheric: bool = True, country_code: str | None = None) -> list[StationLite]:
    """
    Return list of Station objects available from ICOS.

    Args:
        atmospheric: Filter station list to only include atmospheric stations.
        country_code: Two-letter country code to filter by.
    Returns:
        list[StationLite]: All matching stations as StationLite ICOS objects
    """

    if atmospheric:
        all_stations = meta.list_stations(ATMO_STATION)
    else:
        all_stations = meta.list_stations()

    if country_code is not None:
        stations = [s for s in all_stations if s.country_code == country_code]
    else:
        stations = all_stations

    return stations


def retrieve_station(site: str, atmospheric: bool = True) -> StationLite:
    """
    Retrieve station details for a particular station.

    Args:
        site: ICOS site ID e.g. "BIK"
        atmospheric: Whether to initially filter station list to only include atmospheric stations.
    Returns:
        StationLite: ICOS StationLite object for this particular station.
        ValueError: if unable to find station
    """

    stations = retrieve_list_stations(atmospheric=atmospheric)
    print(stations)
    site_id = site.upper()
    for station in stations:
        if station.id == site_id:
            break
    else:
        msg = f"Unable to locate station with site id: {site}"
        logger.exception(msg)
        raise ValueError(msg)  # TODO: Update to better error type?

    return station


def retrieve_station_meta(site: str, atmospheric: bool = True) -> Station:
    """
    Retrieve metadata details for a particular station.

    Args:
        site: ICOS site ID e.g. "BIK"
        atmospheric: Whether to initially filter station list to only include atmospheric stations.
    Returns:
        Station: ICOS Station metadata object for this particular station.
    """

    station = retrieve_station(site, atmospheric)
    uri = station.uri
    station_meta = meta.get_station_meta(uri)

    return station_meta


def retrieve_station_staff(
    site: str | None = None,
    station_meta: Station | None = None,
    role: str | None = None,
    atmospheric: bool = True,
) -> pd.DataFrame:
    """
    Find details of staff associated with a particular station. This can be filtered by the role.
    Option to search by site or to supply the station meta (Station object) directly
    One of site or station_meta must be specified.

    Args:
        site: ICOS site ID e.g. "BIK". Either this is station_meta must be specified.
        station_meta: Station object (typically from retrieve_station_meta).
        role: Name of staff role to filter by. This (non-exhaustively) includes:
         - "Principal Investigator" (can use "PI" for short)
         - "Administrator"
         - "Engineer"
        atmospheric: Whether to initially filter station list to only include atmospheric stations.
    Returns:
        pandas.DataFrame: Summarised details for staff members associated with the site
            (extracted from meta.get_station_meta(uri))
    """

    if site and station_meta is None:
        station_meta = retrieve_station_meta(site, atmospheric)

    if station_meta is None:
        msg = "Either site or station_meta must be specified to retrieve staff details."
        logger.exception(msg)
        raise ValueError(msg)

    station_staff = station_meta.staff

    staff_list = []
    for staff in station_staff:
        # Want to combine details for both individual person and role for the site
        overall_dict = {}
        person_dict = asdict(staff.person)
        role_dict = asdict(staff.role.role)

        # Two 'uri' details (one for person and one for role) so ensure these are distinct
        role_dict["role_uri"] = role_dict.pop("uri")

        # Include just the 'uri' from the `staff.person.self` entry
        person_dict["staff_uri"] = staff.person.self.uri
        person_dict.pop("self")

        overall_dict.update(person_dict)
        overall_dict.update(role_dict)
        staff_list.append(overall_dict)

    staff_df = pd.DataFrame(staff_list)

    if role is not None:
        if role == "PI":
            role = "Principal Investigator"

        role_filter = staff_df["label"] == role
        staff_df = staff_df[role_filter]

    return staff_df
