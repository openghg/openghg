from ._basedata import BaseData

__all__ = ["FluxData"]


class FluxData(BaseData):
    """This class is used to return flux/emissions data from the get_flux function

    Args:
        data: xarray Dataframe
        metadata: Dictionary of metadata including model run parameters
    """

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
