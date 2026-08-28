from dataclasses import dataclass, field
from functools import partial
import logging

import numpy as np
from xarray import DataArray, Dataset
import xarray_validate as xv  # type: ignore[import-untyped]

from openghg.types import ValidationError

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

__all__ = ["DataSchema"]


def _check_dims(expected: tuple[str, ...], data: DataArray) -> None:
    for dim in expected:
        if dim not in data.dims:
            raise ValueError(
                f"Missing dimension for data variable: {data.name}, {dim}. Current dims: {data.dims}"
            )


@dataclass
class DataSchema:
    """OpenGHG compatibility wrapper around :mod:`xarray_validate` schemas."""

    data_vars: dict[str, tuple[str, ...]] | None = None
    dtypes: dict[str, type] | None = None
    dims: list[str] | None = None
    _schema: xv.DatasetSchema = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        variables: dict[str, xv.DataArraySchema] = {}
        for name, dims in (self.data_vars or {}).items():
            variables[name] = xv.DataArraySchema(
                dtype=(self.dtypes or {}).get(name),
                checks=[partial(_check_dims, dims)],
            )

        self._schema = xv.DatasetSchema(
            data_vars=variables or None,
            allow_extra_keys=True,
            checks=[self._check_remaining_constraints],
        )

    def _check_remaining_constraints(self, data: Dataset) -> None:
        """Retain the old optional-coordinate dtype and dataset-dimension checks."""
        for dim in self.dims or []:
            if dim not in data.dims:
                raise xv.SchemaError(f"Expected dimension: {dim} not present in standardised data")

        required_vars = self.data_vars or {}
        for name, dtype in (self.dtypes or {}).items():
            if name in data and name not in required_vars and not np.issubdtype(data[name].dtype, dtype):
                raise xv.SchemaError(
                    f"Expected data type of variable {name} to be: {dtype}. Current {data[name].dtype}"
                )

    def validate_data(self, data: Dataset) -> None:
        """Validate an xarray Dataset, exposing OpenGHG's public error type."""
        try:
            self._schema.validate(data)
        except xv.SchemaError as err:
            raise ValidationError(str(err)) from err
