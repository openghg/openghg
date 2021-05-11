from dataclasses import dataclass
from typing import Dict, Optional
from openghg.dataobjects import ObsData
from openghg.processing import recombine_datasets

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

    def retrieve(self, site: Optional[str] = None, species: Optional[str] = None, ):
        """ Retrieve some or all of the data found in the object store.
            


        """
        # if site is None and species is None:
        if site is not None and species is not None:
            try:
                data_keys = self.results[site][species]["keys"]
            except KeyError:
                raise KeyError("Unable to find keys.")

            # Retrieve the data from the object store and combine into a NetCDF
            data = recombine_datasets(data_keys, sort=True)
            metadata = self.results[site][species]["metadata"]
            rank_data = self.results[site][species]["rank_metadata"]

            metadata["rank_metadata"] = rank_data

        obs = ObsData(data=data, metadata=metadata)

        return obs

