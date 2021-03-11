from dataclasses import dataclass
from ._basedata import _BaseData

__all__ = ["FootprintData"]


@dataclass
class FootprintData(_BaseData):
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata including model run parameters
            doi: DOI string
    """
    pass
