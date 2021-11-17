from typing import Dict, List, Optional, Union
from openghg.util import valid_site, create_daterange_str, InvalidSiteError

from Acquire.Client import Wallet

__all__ = ["RankSources"]


class RankSources:
    def __init__(self, service_url: Optional[str] = None):
        wallet = Wallet()

        if service_url is None:
            service_url = "https://fn.openghg.org/t"

        self._service = wallet.get_service(service_url=f"{service_url}/openghg")

    def get_sources(self, site: str, species: str) -> Dict:
        """Get the datasources for this site and species to allow a ranking to be set

        Args:
            site: Three letter site code
            species: Species name
        Returns:
            dict: Dictionary of datasource metadata
        """
        if not valid_site(site):
            raise InvalidSiteError(f"{site} is not a valid site code")

        args = {"site": site, "species": species}

        response = self._service.call_function(function="rank.get_sources", args=args)

        if not response:
            raise ValueError(f"No sources found for {species} at {site}")

        self._user_info: Dict = response["user_info"]
        self._key_lookup: Dict = response["key_lookup"]

        self._lookup_data = {"site": site, "species": species}
        self._needs_update = False

        return self._user_info

    def get_specific_source(self, key: str) -> str:
        """Return the ranking data of a specific key

        Args:
            key: Key
        Returns:
            dict: Dictionary of ranking data
        """
        if self._needs_update:
            site = self._lookup_data["site"]
            species = self._lookup_data["species"]
            _ = self.get_sources(site=site, species=species)

        rank_data: str = self._user_info[key]["rank_data"]

        return rank_data

    def set_rank(
        self,
        key: str,
        rank: Union[int, str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        overwrite: bool = False,
        dateranges: Optional[Union[List, str]] = None,
    ) -> None:
        """Set the rank data for the

        Args:
            key: Key of ranking data from the original dict
            return by get_sources.
            rank: Number between 1 and 9
            start_date: Start date
            end_date: End date
        Returns:
            None
        """
        if all((start_date, end_date, dateranges)):
            raise ValueError("Either a start and end date must be passed or a list of dateranges")

        uuid = self._key_lookup[key]

        if dateranges is None:
            dateranges = create_daterange_str(start=start_date, end=end_date)

        args: Dict[str, Union[str, int, List]] = {}
        args["rank"] = rank
        args["uuid"] = uuid
        args["dateranges"] = dateranges
        args["overwrite"] = overwrite

        self._service.call_function(function="rank.set_rank", args=args)
        self._needs_update = True

    def clear_rank(self, key: str) -> None:
        """Clear the ranking data for a Datasource

        Args:
            key: Key for specific source
        Returns:
            None
        """
        uuid = self._key_lookup[key]

        args = {"uuid": uuid}

        self._service.call_function(function="rank.clear_rank", args=args)
