from ._basedata import _BaseData

__all__ = ["BoundaryConditionsData"]


class BoundaryConditionsData(_BaseData):
    """This class is used to return boundary conditions data from the get_bc function

    Args:
        data: xarray Dataframe
        metadata: Dictionary of metadata including model run parameters
    """

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
