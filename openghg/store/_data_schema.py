from dataclasses import dataclass
import logging
import numpy as np
from xarray import Dataset

from openghg.types import ValidationError

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

__all__ = ["DataSchema"]


@dataclass
class DataSchema:
    """
    Create schema for a data type based on inputs for components of the data.
    Expected format is based against an xarray Dataset.

    Example inputs:
        DataSchema(
            data_vars = {"fp": ("time", "lat", "lon"), ...},
            dtypes = {"fp" : np.floating, ...},
            dims = ["time", "lat", "lon", ...]
        )
    """

    # TODO : Change or add additional checks as needed

    data_vars: dict[str, tuple[str, ...]] | None = None
    dtypes: dict[str, type] | None = None
    dims: list[str] | None = None
    units: dict[str, str | None] | None = None

    def assign_units(self, data: Dataset) -> None:
        """Assign missing units defined by this schema.

        A schema unit is a default for an internal OpenGHG variable, rather
        than a conversion request. Existing units are therefore retained so
        that source-specific scales, such as ``1e-9`` for mole fractions,
        remain correct. A value of ``None`` marks a variable as outside Pint
        handling and removes any unit attribute it may have acquired.

        Args:
            data: Dataset whose variables and coordinates should be updated.
        """
        if self.units is None:
            return

        for variable, unit in self.units.items():
            if variable not in data:
                continue

            if unit is None:
                data[variable].attrs.pop("units", None)
            elif data[variable].attrs.get("units") in (None, ""):
                data[variable].attrs["units"] = unit

    def _check_data_vars(self, data: Dataset) -> None:
        """
        Check data variables and their dimensions of data against the schema.

        Args:
            data : xarray Dataset to be checked
        Returns:
            None

            Raises a ValueError with details if expected data variable is
            not present or expected dimensions are not present for a data variable
            based on the DataSchema object.
        """
        expected_data_vars = self.data_vars
        if expected_data_vars is None:
            logger.debug("No data variables to check against schema")
            return None
        else:
            expected_dv = expected_data_vars.keys()

        data_vars = data.data_vars

        for edv in expected_dv:
            if edv in data_vars:
                dims = data[edv].dims
                expected_dv_dims = expected_data_vars[edv]
                for edim in expected_dv_dims:
                    if edim not in dims:
                        raise ValueError(
                            f"Missing dimension for data variable: {edv}, {edim}. Current dims: {dims}"
                        )
            else:
                raise ValidationError(f"Expected data variable: {edv} not present in standardised data")

    def _check_dims(self, data: Dataset) -> None:
        """
        Check dimensions of data against the the schema.

        Args:
            data : xarray Dataset to be checked
        Returns:
            None

            Raises a ValueError with details if expected dimensions
            are not present in data based on the DataSchema object.
        """
        expected_dims = self.dims
        if expected_dims is None:
            logger.debug("No dims to check against schema")
            return None

        dims = data.dims

        for edim in expected_dims:
            if edim not in dims:
                raise ValidationError(f"Expected dimension: {edim} not present in standardised data")

    def _check_dtypes(self, data: Dataset) -> None:
        """
        Check dtypes of variables and coordinates of data against the schema.

        Args:
            data : xarray Dataset to be checked
        Returns:
            None

            Raises a ValueError with details if data variables and coordinates
            are not of expected data types based on the DataSchema object.
        """
        expected_data_types = self.dtypes
        if expected_data_types is None:
            logger.debug("No data types to check against schema")
            return None

        for variable, edata_type in expected_data_types.items():
            if variable in data:
                dtype = data[variable].dtype
                if not np.issubdtype(dtype, edata_type):
                    raise ValidationError(
                        f"Expected data type of variable {variable} to be: {edata_type}. Current {dtype}"
                    )

    def validate_data(
        self,
        data: Dataset,
    ) -> None:
        """
        Validate input data based on schema.

        Currently check can include:
         - data variables are present with expected dimensions.
         - general dimensions are present
         - data types of data variables and coordinates match to expected values
         - missing units are assigned from the schema

        Args:
            data : xarray Dataset to be validated
        Returns:
            None

            Raises a ValidationError with details if the input data does not adhere
            to the defined DataSchema.
        """

        self.assign_units(data)

        if self.data_vars is not None:
            self._check_data_vars(data)

        if self.dims is not None:
            self._check_dims(data)

        if self.dtypes is not None:
            self._check_dtypes(data)

    # TODO: Add string method for pretty printing
