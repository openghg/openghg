
from dataclasses import dataclass
from typing import Optional, Dict, List
from xarray import Dataset
import numpy as np

__all__ = ["DataSchema"]


@dataclass
class DataSchema:
    """
    Example inputs:
        DataSchema(
            data_vars = {"fp": ("time", "lat", "lon"), ...},
            dtypes = {"fp" : np.floating, ...},
            dims = ["time", "lat", "lon", ...]
        )
    """
    # TODO : Change or add additional checks as needed

    data_vars: Optional[Dict[str, tuple]] = None
    dtypes: Optional[Dict[str, type]] = None
    dims: Optional[List[str]] = None

    def _check_data_vars(self, data: Dataset) -> None:
        """
        Check data variables and their dimensions of data against the schema. 
        """
        data_vars = data.data_vars
        expected_dv = self.data_vars.keys()
        for edv in expected_dv:
            if edv in data_vars:
                dims = data[edv].dims
                expected_dv_dims = self.data_vars[edv]
                for edim in expected_dv_dims:
                    if edim not in dims:
                        raise ValueError(f"Missing dimension for data variable: {edv}, {edim}. Current dims: {dims}")
            else:
                raise ValueError(f"Expected data variable: {edv} not present in standardised data")

    def _check_dims(self, data: Dataset) -> None:
        """
        Check dimensions of data against the the schema.
        """
        dims = data.dims
        expected_dims = self.dims
        for edim in expected_dims:
            if edim not in dims:
                raise ValueError(f"Expected dimension: {edim} not present in standardised data")

    def _check_dtypes(self, data: Dataset) -> None:
        """
        Check dtypes of variables and coordinates of data against the schema.
        """
        expected_data_types = self.dtypes
        for variable, edata_type in expected_data_types.items():
            if variable in data:
                dtype = data[variable].dtype
                if not np.issubdtype(dtype, edata_type):
                    raise ValueError(f"Expected data type of variable {variable} to be: {edata_type}. Current {dtype}")

    def validate_data(self, 
                      data: Dataset, 
                      ) -> None:
        """
        Validate input data based on schema.
        """

        if self.data_vars is not None:
            self._check_data_vars(data)

        if self.dims is not None:
            self._check_dims(data)

        if self.dtypes is not None:
            self._check_dtypes(data)
