from typing import Dict, Iterator, List, Optional, Type, TypeVar, Union
from xarray import Dataset, open_dataset
from io import BytesIO

from addict import Dict as aDict
import json

from openghg.dataobjects import ObsData
from openghg.store import recombine_datasets

from openghg.util import (
    clean_string,
    create_daterange_str,
    find_daterange_gaps,
    first_last_dates,
    split_daterange_str,
    running_in_cloud,
)


__all__ = ["SearchResults"]

T = TypeVar("T", bound="SearchResults")


class SearchResults:
    """This class is used to return data from the search function. It
    has member functions to retrieve data from the object store.

    Args:
        results: Search results
        ranked_data: True if results are ranked, else False
        cloud: True if running in cloud
    """

    def __init__(self, results: Optional[Dict] = None, ranked_data: bool = False):
        self.results = results if results is not None else {}
        self.ranked_data = ranked_data
        self.cloud = running_in_cloud()

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

    def to_json(self) -> str:
        """Serialises the object to JSON

        Returns:
            str: JSON str
        """
        return json.dumps(self.to_data())

    @classmethod
    def from_json(cls: Type[T], data: Union[bytes, str]) -> T:
        """Create a SearchResults object from a dictionary

        Args:
            data: Serialised object
        Returns:
            SearchResults: SearchResults object
        """
        loaded = json.loads(data)

        return cls(results=loaded["results"], ranked_data=loaded["ranked_data"])

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

    def keys(self, site: str, species: str, inlet: Optional[str] = None) -> Optional[List[str]]:
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

            return keys
        except KeyError:
            print(f"No keys found for {species} at {site}")
            return None

    def metadata(self, site: str, species: str, inlet: Optional[str] = None) -> Optional[Dict]:
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
            print(f"No metadata found for {species} at {site}")
            return None
        else:
            return metadata

    def retrieve_all(self) -> Union[ObsData, List[ObsData], None]:
        """Retrieve all the data found during the serch

        Returns:
            list: List of ObsData objects
        """
        results = []

        if self.ranked_data:
            # Can we just traverse the dict without looping?
            for site, species_data in self.results.items():
                for species, inlet_data in species_data.items():
                    obsdata = self._create_obsdata(site=site, species=species)
                    results.append(obsdata)
        else:
            # Can we just traverse the dict without looping?
            for site, species_data in self.results.items():
                for species, inlet_data in species_data.items():
                    for inlet in inlet_data:
                        obsdata = self._create_obsdata(site=site, species=species, inlet=inlet)
                        results.append(obsdata)

        if not results:
            return None
        if len(results) == 1:
            return results[0]
        else:
            return results

    def retrieve(
        self,
        site: Optional[str] = None,
        species: Optional[str] = None,
        inlet: Optional[str] = None,
    ) -> Union[ObsData, List[ObsData], None]:
        """Retrieve some or all of the data found in the object store.

        Args:
            site: Three letter site code
            species: Species name
        Returns:
            ObsData or dictionary of ObsData objects
        """
        site = clean_string(site)
        species = clean_string(species)
        inlet = clean_string(inlet)

        results = []
        if not self.ranked_data:
            for _site, site_data in self.results.items():
                if site is not None and _site != site:
                    continue
                for _species, species_data in site_data.items():
                    if species is not None and _species != species:
                        continue
                    for _inlet in species_data:
                        if inlet is not None and _inlet != inlet:
                            continue

                        obsdata = self._create_obsdata(site=_site, species=_species, inlet=_inlet)

                        if obsdata is not None:
                            results.append(obsdata)
        else:
            if inlet is not None:
                from openghg.retrieve import search

                with_inlet = search(site=site, species=species, inlet=inlet)
                inlet_data: ObsData = with_inlet.retrieve(site=site, species=species, inlet=inlet)
                return inlet_data

            for _site, site_data in self.results.items():
                if site is not None and _site != site:
                    continue
                for _species, species_data in site_data.items():
                    if species is not None and _species != species:
                        continue

                    obsdata = self._create_obsdata(site=_site, species=_species)

                    if obsdata is not None:
                        results.append(obsdata)

        if not results:
            return None
        if len(results) == 1:
            return results[0]
        else:
            return results

    def _create_obsdata(self, site: str, species: str, inlet: Optional[str] = None) -> ObsData:
        """Creates an ObsData object for return to the user

        Args:
            site: Site code
            species: Species name
        Returns:
            ObsData: ObsData object
        """
        from xarray import concat

        try:
            if self.ranked_data:
                specific_source = self.results[site][species]
            else:
                specific_source = self.results[site][species][inlet]
        except KeyError:
            raise ValueError("Error: We can't create an ObsData object using these parameters.")

        data_keys = specific_source["keys"]
        metadata = specific_source["metadata"]

        if not self.ranked_data:
            keys = data_keys["unranked"]
            final_dataset = self._retrieve_dataset(keys, sort=True, elevate_inlet=False)
        else:
            dataset_slices = []

            inlet_ranges = specific_source["rank_metadata"]

            metadata["rank_metadata"] = {}

            ranked_keys = data_keys["ranked"]
            ranked_slices = []

            inlets = set()

            for daterange, keys in ranked_keys.items():
                data_slice = self._retrieve_dataset(keys=keys, sort=True, elevate_inlet=True)

                slice_start, slice_end = split_daterange_str(daterange_str=daterange, date_only=True)

                # We convert to str here as xarray has some weird behaviour that means
                # "2018-01-01" - "2018-06-01"
                # gets treated differently to
                # datetime.date(2018, 1, 1) - datetime.date(2018, 6, 1)
                ranked_slice = data_slice.sel(time=slice(str(slice_start), str(slice_end)))

                if ranked_slice.time.size > 0:
                    inlets.add(inlet_ranges[daterange])
                    ranked_slices.append(ranked_slice)

                ranked_metadata = specific_source["rank_metadata"]
                metadata["rank_metadata"]["ranked"] = ranked_metadata

            dataset_slices.extend(ranked_slices)

            unranked_keys = data_keys["unranked"]

            if unranked_keys:
                unranked_data = self._retrieve_dataset(keys=unranked_keys, sort=True, elevate_inlet=True)

                first_date, last_date = first_last_dates(keys=unranked_keys)

                ranked_dateranges = list(ranked_keys.keys())
                unranked_dateranges = find_daterange_gaps(
                    start_search=first_date,
                    end_search=last_date,
                    dateranges=ranked_dateranges,
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

    def _retrieve_dataset(
        self, keys: List, sort: bool, elevate_inlet: bool = True, attrs_to_check: Optional[Dict] = None
    ) -> Dataset:
        """Retrieves datasets from either cloud or local object store

        Args:
            keys: List of object store keys
            sort: Sort data on recombination
            elevate_inlet: Elevate inlet from attribute to variable
        Returns:
            Dataset:
        """
        from openghg.cloud import call_function

        if self.cloud:
            to_post: Dict[str, Union[Dict, List, bool, str]] = {}
            to_post["keys"] = keys
            to_post["sort"] = sort
            to_post["elevate_inlet"] = elevate_inlet
            to_post["function"] = "retrieve"

            if attrs_to_check is not None:
                to_post["attrs_to_check"] = attrs_to_check

            result = call_function(data=to_post)
            binary_netcdf = result["content"]["data"]
            buf = BytesIO(binary_netcdf)
            ds: Dataset = open_dataset(buf).load()
            return ds
        else:
            return recombine_datasets(
                keys=keys, sort=sort, elevate_inlet=elevate_inlet, attrs_to_check=attrs_to_check
            )
