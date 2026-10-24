from dataclasses import dataclass, field
from functools import partial
import logging

import numpy as np
from xarray import DataArray, Dataset
import xarray_validate as xv  # type: ignore[import-untyped]
from xarray_validate import units as xv_units  # type: ignore[import-untyped]

from openghg.types import ValidationError
from openghg.util import cf_ureg

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

__all__ = ["DataSchema"]


xv_units.set_registry(cf_ureg)


def _unit_attrs(unit: str | None = None, compatible: str | None = None) -> xv.AttrsSchema:
    if unit is None and compatible is None:
        requirement = xv.AttrSchema(type=str, value=r"{.*\S.*}")
    elif compatible is not None:
        requirement = xv.AttrSchema(type=str, units_compatible=compatible)
    else:
        requirement = xv.AttrSchema(type=str, units=unit)

    return xv.AttrsSchema({"units": requirement})


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
    units: dict[str, str | None] | None = None
    units_compatible: dict[str, str] | None = None
    _schema: xv.DatasetSchema = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        variables: dict[str, xv.DataArraySchema] = {}
        for name, dims in (self.data_vars or {}).items():
            variables[name] = xv.DataArraySchema(
                dtype=(self.dtypes or {}).get(name),
                attrs=self._attrs_for(name),
                checks=[partial(_check_dims, dims)],
            )

        coord_names = (set(self.units or {}) | set(self.units_compatible or {})) - set(variables)
        coords = {
            name: xv.DataArraySchema(
                attrs=self._attrs_for(name),
            )
            for name in coord_names
        }

        self._schema = xv.DatasetSchema(
            data_vars=variables or None,
            allow_extra_keys=True,
            coords=xv.CoordsSchema(coords) if coords else None,
            checks=[self._check_remaining_constraints],
        )

    def _attrs_for(self, name: str) -> xv.AttrsSchema | None:
        compatible_units = self.units_compatible or {}
        units = self.units or {}
        if name in compatible_units:
            return _unit_attrs(compatible=compatible_units[name])
        if name in units:
            return _unit_attrs(unit=units[name])
        return None

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
