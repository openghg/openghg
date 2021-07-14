from typing import Optional

from openghg.processing import search as search_fn
from openghg.processing import recombine_datasets

__all__ = ["Search"]


class Search:
    """Used to search and download data from the object store"""

    def __init__(self):
        raise NotImplementedError()

    def search(
        self,
        species: Optional[str] = None,
        locations: Optional[str] = None,
        inlet: Optional[str] = None,
        instrument: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """This is just a wrapper for the search function that allows easy access through LocalClient

        Args:
            species (str or list): Terms to search for in Datasources
            locations (str or list): Where to search for the terms in species
            inlet (str, default=None): Inlet height such as 100m
            instrument (str, default=None): Instrument name such as picarro
            find_all (bool, default=False): Require all search terms to be satisfied
            start_date (datetime, default=None): Start datetime for search
            If None a start datetime of UNIX epoch (1970-01-01) is set
            end_date (datetime, default=None): End datetime for search
            If None an end datetime of the current datetime is set
        Returns:
            dict: List of keys of Datasources matching the search parameters
        """
        # if not all(**kwargs):
        #     raise ValueError("One argument must be passed")

        results = search_fn()

        self._results = results

        return results

    def retrieve(self, selected_keys):
        """Downloads the selected keys and returns a dictionary of
        xarray Datasets

        Args:
            keys (str, list): Key(s) from search results to download
        Returns:
            defaultdict(dict): Dictionary of Datasets
        """
        if not isinstance(selected_keys, list):
            selected_keys = [selected_keys]

        results = {}

        for key in selected_keys:
            try:
                data_keys = self._results[key]["keys"]
                # Retrieve the data from the object store and combine into a NetCDF
                results[key] = recombine_datasets(data_keys, sort=True)
            except KeyError:
                raise KeyError(f"Invalid key {key} passed for retrieval. Please check it is correct and try again.")

        return results
