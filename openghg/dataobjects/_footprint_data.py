from dataclasses import dataclass
from typing import Dict

from ._basedata import _BaseData

__all__ = ["FootprintData"]


@dataclass(frozen=True)
class FootprintData(_BaseData):
    """This class is used to return observations data from the get_footprint function

    Args:
        data: xarray Dataframe
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

    def __str__(self) -> str:
        return (
            f"Data: {self.data}\nMetadata : {self.metadata}"
            f"\nFlux : {self.flux}\nBC: {self.bc}"
            f"\nSpecies : {self.species}\nScales: {self.scales}\nUnits: {self.units}"
        )
