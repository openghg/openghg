from openghg.processing import search as search_fn

__all__ = ["Search"]

from openghg.processing import recombine_sections
from collections import defaultdict


class Search:
    """ Used to search and download data from the object store

    """
    def search(
        self,
        species,
        locations,
        inlet=None,
        instrument=None,
        start_date=None,
        end_date=None,
    ):
        """ This is just a wrapper for the search function that allows easy access through LocalClient

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
        results = search_fn(
            species=species,
            locations=locations,
            inlet=inlet,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
        )

        self._results = results

        return results

    def retrieve(self, selected_keys):
        """ Downloads the selected keys and returns a dictionary of
            xarray Datasets

            Args:
                keys (str, list): Key(s) from search results to download
            Returns:
                defaultdict(dict): Dictionary of Datasets
        """
        if not isinstance(selected_keys, list):
            selected_keys = [selected_keys]

        # Select the keys we want to download
        key_dict = {key: self._results[key]["keys"] for key in selected_keys}

        results = defaultdict(dict)
        for key, dateranges in key_dict.items():
            for daterange in dateranges:
                # Create a key for this range
                data_keys = key_dict[key][daterange]
                # Retrieve the data from the object store and combine into a NetCDF
                recombined_data = recombine_sections(data_keys)

                results[key][daterange] = recombined_data

        return results
