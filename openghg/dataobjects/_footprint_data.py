from ._basedata import _BaseData

__all__ = ["FootprintData"]


class FootprintData(_BaseData):
    """This class is used to return observations data from the get_footprint function

    Args:
        data: xarray Dataset
        metadata: Dictionary of metadata including model run parameters
    """

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
