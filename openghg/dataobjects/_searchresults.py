from addict import Dict as aDict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Union, TypeVar, Type
from openghg.dataobjects import ObsData
from openghg.processing import recombine_datasets
from openghg.util import clean_string
from openghg.client import Retrieve

__all__ = ["SearchResults"]


@dataclass
class SearchResults:
    """This class is used to return data from the search function

    Args:
        results: Search results
        ranked_data: True if results are ranked, else False
    """

    T = TypeVar("T", bound="SearchResults")

    results: Dict
    ranked_data: bool
    # Local or cloud service to be used
    cloud: bool = False

    def __str__(self) -> str:
        if not self.results:
            return "No results"

        print_strs = []
        for site, species in self.results.items():
            if self.ranked_data:
                print_strs.append(
                    f"Site: {site.upper()} \nSpecies found: {', '.join(self.results[site].keys())}"
                )
            else:
                print_strs.append(f"Site: {site.upper()}")
                print_strs.append("---------")
                print_strs.extend([f"{sp} at {', '.join(self.results[site][sp].keys())}" for sp in species])
            print_strs.append("\n")

        return "\n".join(print_strs)

    def __repr__(self) -> str:
        return self.__str__()

    def __bool__(self) -> bool:
        return bool(self.results)

    def __len__(self) -> int:
        return len(self.results)

    def __iter__(self) -> Iterator:
        yield from self.results

    def to_data(self) -> Dict:
        """Convert this object to a dictionary for JSON serialisation

        Returns:
            dict: Dictionary of data
        """
        return {"results": self.results, "ranked_data": self.ranked_data, "cloud": self.cloud}

    @classmethod
    def from_data(cls: Type[T], data: Dict) -> T:
        """Create a SearchResults object from a dictionary

        Args:
            data: Dictionary created by SearchResults.to_data
        Returns:
            SearchResults: SearchResults object
        """
        return cls(results=data["results"], ranked_data=data["ranked_data"], cloud=data["cloud"])

    def raw(self) -> Dict:
        """Returns the raw results data

        Returns:
            dict: Dictionary of results returned from search function
        """
        return self.results

    def keys(self, site: str, species: str, inlet: Optional[str] = None) -> List[str]:
        """Return the data keys for the specified site and species.
        This is intended mainly for use in the search function when filling
        gaps of unranked dateranges.

            Args:
                site: Three letter site code
                species: Species name
                inlet: Inlet height, required for unranked data
            Returns:
                list: List of keys
        """
        site = site.lower()
        species = species.lower()

        if inlet is not None:
            inlet = inlet.lower()

        try:
            if self.ranked_data:
                keys: List = self.results[site][species]["keys"]
            else:
                keys = self.results[site][species][inlet]["keys"]
        except KeyError:
            raise ValueError(f"No keys found for {species} at {site}")

        return keys

    def metadata(self, site: str, species: str, inlet: Optional[str] = None) -> Dict:
        """Return the metadata for the specified site and species

        Args:
            site: Three letter site code
            species: Species name
            inlet: Inlet height, required for unranked data
        Returns:
            dict: Dictionary of metadata
        """
        site = site.lower()
        species = species.lower()

        if inlet is None and not self.ranked_data:
            raise ValueError("Please pass an inlet height.")

        if inlet is not None:
            inlet = inlet.lower()

        try:
            if self.ranked_data:
                metadata: Dict = self.results[site][species]["metadata"]
            else:
                metadata = self.results[site][species][inlet]["metadata"]
        except KeyError:
            raise KeyError(f"No metadata found for {species} at {site}")

        return metadata


    def retrieve_all(self) -> Dict:
        """ Retrieve all the data found during the serch

            Returns:
                dict: Dictionary of all data
        """
        data = aDict()

        # Can we just traverse the dict without looping?
        for site, species_data in self.results.items():
            for species, inlet_data in species_data.items():
                for inlet, keys in inlet_data.items():
                    data[site][species][inlet] = self._create_obsdata(site=site, species=species, inlet=inlet)

        return data

    def retrieve(self, site: str = None, species: str = None, inlet: str = None) -> Union[Dict[str, ObsData], ObsData]:
        """Retrieve some or all of the data found in the object store.

        Args:
            site: Three letter site code
            species: Species name
        Returns:
            ObsData or dict
        """
        site = clean_string(site)
        species = clean_string(species)
        inlet = clean_string(inlet)

        if self.ranked_data:
            if all((site, species, inlet)):
                # TODO - how to do this in a cleaner way?
                site = str(site)
                species = str(species)
                inlet = str(inlet)
                return self._create_obsdata(site=site, species=species, inlet=inlet)

            results = {}
            if site is not None and species is not None:
                try:
                    _ = self.results[site][species]["keys"]
                except KeyError:
                    raise KeyError(f"Unable to find data keys for {species} at {site}.")

                return self._create_obsdata(site=site, species=species)

            # Get the data for all the species at that site
            if site is not None and species is None:
                for sp in self.results[site]:
                    key = "_".join((site, sp))
                    results[key] = self._create_obsdata(site=site, species=sp)

                return results

            # Get the data for all the species at that site
            if site is None and species is not None:
                for a_site in self.results:
                    key = "_".join((a_site, species))

                    try:
                        results[key] = self._create_obsdata(site=a_site, species=species)
                    except KeyError:
                        pass

                return results

            for a_site, species_list in self.results.items():
                for sp in species_list:
                    key = "_".join((a_site, sp))
                    results[key] = self._create_obsdata(site=a_site, species=sp)

            return results
        else:
            # if len(self.results) == 1 and not all((species, inlet)):
            #     raise ValueError("Please pass species and inlet")
            if not all((species, site, inlet)):
                raise ValueError("Please pass site, species and inlet")

            # TODO - how to do this in a cleaner way for mypy?
            site = str(site)
            species = str(species)
            inlet = str(inlet)
            return self._create_obsdata(site=site, species=species, inlet=inlet)

    def _create_obsdata(self, site: str, species: str, inlet: str = None) -> ObsData:
        """Creates an ObsData object for return to the user

        Args:
            site: Site code
            species: Species name
        Returns:
            ObsData: ObsData object
        """
        if self.ranked_data:
            specific_source = self.results[site][species]
            rank_data = specific_source["rank_metadata"]
        else:
            specific_source = self.results[site][species][inlet]

        data_keys = specific_source["keys"]

        # If cloud use the Retrieve object
        if self.cloud:
            retrieve = Retrieve()
            # TODO - update this function to allow multi-site / species retrieval
            key = f"{site}_{species}_{inlet}"
            keys_to_retrieve = {key: data_keys}
            retrieved = retrieve.retrieve(keys=keys_to_retrieve)
            data = retrieved[key]
        else:
            data = recombine_datasets(data_keys, sort=True)

        metadata = specific_source["metadata"]

        if self.ranked_data:
            metadata["rank_metadata"] = rank_data

        return ObsData(data=data, metadata=metadata)
