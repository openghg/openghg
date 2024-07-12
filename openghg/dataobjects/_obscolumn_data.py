from ._basedata import _BaseData

__all__ = ["ObsColumnData"]


class ObsColumnData(_BaseData):
    """This class is used to return observations data from the get_obs_column function

    Args:
        data: xarray Dataset
        metadata: Dictionary of metadata including model run parameters
    """

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
