from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Type, TypeVar, Union

from addict import Dict as aDict
from openghg.dataobjects import ObsData
from openghg.store import recombine_datasets
from openghg.util import (
    clean_string,
    find_daterange_gaps,
    first_last_dates,
    split_daterange_str,
    create_daterange_str,
)

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
        return {
            "results": self.results,
            "ranked_data": self.ranked_data,
            "cloud": self.cloud,
        }

    @classmethod
    def from_data(cls: Type[T], data: Dict) -> T:
        """Create a SearchResults object from a dictionary

        Args:
            data: Dictionary created by SearchResults.to_data
        Returns:
            SearchResults: SearchResults object
        """
        return cls(
            results=data["results"],
            ranked_data=data["ranked_data"],
            cloud=data["cloud"],
        )

    def rankings(self) -> Dict:
        if not self.ranked_data:
            print("No rank data")

        rank_result = aDict()

        for site, species_data in self.results.items():
            for species, data in species_data.items():
                rank_result[site][species] = data["rank_metadata"]

        to_return: Dict = rank_result.to_dict()

        return to_return

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
        """Retrieve all the data found during the serch

        Returns:
            dict: Dictionary of all data
        """
        data = aDict()

        # Can we just traverse the dict without looping?
        for site, species_data in self.results.items():
            for species, inlet_data in species_data.items():
                for inlet, keys in inlet_data.items():
                    data[site][species][inlet] = self._create_obsdata(site=site, species=species, inlet=inlet)

        # TODO - update this once addict is stubbed
        data_dict: Dict = data.to_dict()
        return data_dict

    def retrieve(
        self, site: str = None, species: str = None, inlet: str = None
    ) -> Union[Dict[str, ObsData], ObsData]:
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

        # If inlet is not specified, check if this is unambiguous
        # If so, set inlet to be the only value and continue.
        if inlet is None:
            try:
                potential_inlets = self.results[site][species].keys()
            except KeyError:
                pass
            else:
                if len(potential_inlets) == 1:
                    inlet = list(potential_inlets)[0]

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
        from xarray import concat

        if self.ranked_data:
            specific_source = self.results[site][species]
        else:
            specific_source = self.results[site][species][inlet]

        data_keys = specific_source["keys"]
        metadata = specific_source["metadata"]

        # If cloud use the Retrieve object
        if self.cloud:
            raise NotImplementedError
            # from Acquire.Client import Wallet
            # from xarray import open_dataset

            # wallet = Wallet()
            # self._service_url = "https://fn.openghg.org/t"
            # self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

            # key = f"{site}_{species}"
            # keys_to_retrieve = {key: data_keys}

            # args = {"keys": keys_to_retrieve}

            # response: Dict = self._service.call_function(function="retrieve.retrieve", args=args)

            # response_data = response["results"]

            # data = open_dataset(response_data[key])
        else:
            if not self.ranked_data:
                keys = data_keys["unranked"]
                final_dataset = recombine_datasets(keys, sort=True)
            else:
                dataset_slices = []

                inlet_ranges = specific_source["rank_metadata"]

                metadata["rank_metadata"] = {}

                ranked_keys = data_keys["ranked"]
                ranked_slices = []

                inlets = set()

                for daterange, keys in ranked_keys.items():
                    data_slice = recombine_datasets(keys=keys, sort=True, elevate_inlet=True)

                    slice_start, slice_end = split_daterange_str(daterange_str=daterange, date_only=True)

                    # We convert to str here as xarray has some weird behaviour that means
                    # "2018-01-01" - "2018-06-01"
                    # gets treated differently to
                    # datetime.date(2018, 1, 1) - datetime.date(2018, 6, 1)
                    ranked_slice = data_slice.sel(time=slice(str(slice_start), str(slice_end)))

                    if ranked_slice.time.size > 0:
                        inlet = inlet_ranges[daterange]
                        inlets.add(inlet)

                        ranked_slices.append(ranked_slice)

                    ranked_metadata = specific_source["rank_metadata"]
                    metadata["rank_metadata"]["ranked"] = ranked_metadata

                dataset_slices.extend(ranked_slices)

                unranked_keys = data_keys["unranked"]

                if unranked_keys:
                    unranked_data = recombine_datasets(keys=unranked_keys, sort=True, elevate_inlet=True)

                    first_date, last_date = first_last_dates(keys=unranked_keys)

                    ranked_dateranges = list(ranked_keys.keys())
                    unranked_dateranges = find_daterange_gaps(
                        start_search=first_date, end_search=last_date, dateranges=ranked_dateranges
                    )

                    unranked_metadata = {}
                    if unranked_dateranges:
                        unranked_slices = []
                        for dr in unranked_dateranges:
                            slice_start, slice_end = split_daterange_str(daterange_str=dr, date_only=True)
                            unranked_slice = unranked_data.sel(time=slice(str(slice_start), str(slice_end)))

                            if unranked_slice.time.size > 0:
                                inlet = unranked_slice["inlet"].values[0]
                                inlets.add(inlet)
                                unranked_metadata[dr] = inlet
                                unranked_slices.append(unranked_slice)

                        dataset_slices.extend(unranked_slices)
                    else:
                        daterange_str = create_daterange_str(start=first_date, end=last_date)
                        inlet = unranked_data["inlet"].values[0]
                        inlets.add(inlet)
                        unranked_metadata[daterange_str] = inlet

                        dataset_slices.extend(unranked_data)

                    metadata["rank_metadata"]["unranked"] = unranked_metadata

                final_dataset = concat(objs=dataset_slices, dim="time").sortby("time")

                if len(inlets) == 1:
                    inlet_tag = str(inlets.pop())
                else:
                    inlet_tag = "multiple"

                # Update the attributes for single / multiple inlet heights
                final_dataset.attrs["inlet"] = inlet_tag

        metadata = specific_source["metadata"]

        return ObsData(data=final_dataset, metadata=metadata)
