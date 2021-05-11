from dataclasses import dataclass
from typing import Dict, Optional, Union
from openghg.dataobjects import ObsData
from openghg.processing import recombine_datasets
from openghg.util import clean_string

__all__ = ["SearchResults"]


@dataclass(frozen=True)
class SearchResults():
    """ This class is used to return data from the search function

        Args:
            results: Search results
    """
    results: Dict

    def __str__(self):
        return f"Results: {self.results}"

    def retrieve(self, site: Optional[str] = None, species: Optional[str] = None) -> Union[Dict, ObsData]:
        """ Retrieve some or all of the data found in the object store.

            Args:
                site: Three letter site code
                species: Species name
            Returns:
                ObsData or dict
        """
        site = clean_string(site)
        species = clean_string(species)

        # if site is None and species is None:
        if site is not None and species is not None:
            try:
                _ = self.results[site][species]["keys"]
            except KeyError:
                raise KeyError(f"Unable to find data keys for {species} at {site}.")

            return self._create_obsdata(site=site, species=species)

        # Get the data for all the species at that site
        if site is not None and species is None:
            results = {}

            for sp in self.results[site]:
                key = "_".join((site, sp))
                results[key] = self._create_obsdata(site=site, species=sp)

            return results

        # Get the data for all the species at that site
        if site is None and species is not None:
            results = {}

            for a_site in self.results:
                key = "_".join((a_site, species))

                try:
                    results[key] = self._create_obsdata(site=a_site, species=species)
                except KeyError:
                    pass

            return results

    def _create_obsdata(self, site: str, species: str) -> ObsData:
        """ Creates an ObsData object for return to the user

            Args:
                site: Site code
                species: Species name
            Returns:
                ObsData: ObsData object
        """
        data_keys = self.results[site][species]["keys"]

        data = recombine_datasets(data_keys, sort=True)
        metadata = self.results[site][species]["metadata"]
        rank_data = self.results[site][species]["rank_metadata"]

        metadata["rank_metadata"] = rank_data

        return ObsData(data=data, metadata=metadata)
