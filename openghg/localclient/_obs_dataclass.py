from xarray import Dataset
from typing import Dict
from dataclasses import dataclass

__all__ = ["ObsData"]


@dataclass(frozen=True)
class ObsData:
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata
            keys: Dictionary of data keys
            doi: DOI string
    """
    name: str
    data: Dataset
    metadata: Dict
    doi: str = "NA"

    def __str__(self):
        return f"Name: {self.name}\nData: {self.data}\nMetadata : {self.metadata}\nDOI: {self.doi}"
