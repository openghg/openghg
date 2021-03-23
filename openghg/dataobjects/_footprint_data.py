from dataclasses import dataclass
from typing import Dict

from ._basedata import _BaseData

__all__ = ["FootprintData"]


@dataclass
class FootprintData(_BaseData):
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata including model run parameters
            flux: Dictionary of flux data
            bc: Boundary conditions dictionary
            species: Species name
            scales: Measurements scale
            units: Measurements units
    """
    flux: Dict
    bc: Dict
    species: str
    scales: str
    units: str

    def __str__(self):
        return f"Data: {self.data}\nMetadata : {self.metadata}"
