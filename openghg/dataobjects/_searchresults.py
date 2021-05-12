from dataclasses import dataclass
from typing import Dict, List, Optional, Union
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
        print_strs = []
        for site, species in self.results.items():
            print_strs.append(f"Site: {site.upper()} \nSpecies found: {', '.join(self.results[site])}\n\n")

        return "\n".join(print_strs)

    def __bool__(self):
        return bool(self.results)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        yield from self.results

    def raw(self) -> Dict:
        """ Returns the raw results data

            Returns:
                dict: Dictionary of results returned from search function
        """
        return self.results

    def keys(self, site: str, species: str) -> List:
        """ Return the data keys for the specified site and species.
        This is intended mainly for use in the search function when filling 
        gaps of unranked dateranges.

            Args:
                site: Three letter site code
                species: Species name
            Returns:
                list: List of keys
        """
        site = site.lower()
        species = species.lower()

        try:
            keys = self.results[site][species]["keys"]
        except KeyError:
            raise ValueError(f"No keys found for {species} at {site}")

        return keys

    def metadata(self, site: str, species: str) -> List:
        """ Return the metadata for the specified site and species

            Args:
                site: Three letter site code
                species: Species name
            Returns:
                list: List of keys
        """
        site = site.lower()
        species = species.lower()

        try:
            metadata = self.results[site][species]["metadata"]
        except KeyError:
            raise KeyError(f"No metadata found for {species} at {site}")

        return metadata

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
