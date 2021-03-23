from dataclasses import dataclass
from ._basedata import _BaseData

__all__ = ["ObsData"]


@dataclass
class ObsData(_BaseData):
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata
    """
    pass
