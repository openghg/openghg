from xarray import Dataset
from typing import Dict, List
from dataclasses import dataclass

__all__ = ["ObsData"]


@dataclass(frozen=True)
class ObsData:
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata
            doi: DOI string
    """
    data: Dataset
    metadata: Dict
    doi: str = "NA"

    def __str__(self):
        return f"Data: {self.data}\nMetadata : {self.metadata}\nDOI: {self.doi}"
