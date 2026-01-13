import logging
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
