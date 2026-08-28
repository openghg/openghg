from dataclasses import dataclass, field
from functools import lru_cache, partial
from importlib import resources
import json
import logging
from typing import Any, cast, Iterable, Mapping

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


_DTYPES: dict[str, type] = {
    "datetime64": np.datetime64,
    "floating": np.floating,
    "integer": np.integer,
    "number": np.number,
}


@lru_cache(maxsize=1)
def _schema_configs() -> dict[str, Any]:
    schema_path = resources.files(__package__).joinpath("data_schemas.json")
    with schema_path.open(encoding="utf-8") as file:
        return cast(dict[str, Any], json.load(file))


def _format(value: str, substitutions: Mapping[str, str]) -> str:
    try:
        return value.format_map(substitutions)
    except KeyError as err:
        raise ValueError(f"Missing schema substitution: {err.args[0]}") from err


def _merge_configs(configs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for config in configs:
        for field_name, value in config.items():
            if isinstance(value, dict):
                merged.setdefault(field_name, {}).update(value)
            elif isinstance(value, list):
                merged.setdefault(field_name, []).extend(value)
            else:
                raise ValueError(f"Unsupported schema field: {field_name}")
    return merged


def _attrs_schema(
    required: set[str] | None = None,
    unit: str | None = None,
    compatible: str | None = None,
    require_units: bool = False,
) -> xv.AttrsSchema | None:
    attrs = {name: xv.AttrSchema(type=str, value=r"{.*\S.*}") for name in required or set()}
    if unit is None and compatible is None:
        if require_units:
            attrs["units"] = xv.AttrSchema(type=str, value=r"{.*\S.*}")
    elif compatible is not None:
        attrs["units"] = xv.AttrSchema(type=str, units_compatible=compatible)
    else:
        attrs["units"] = xv.AttrSchema(type=str, units=unit)

    return xv.AttrsSchema(attrs) if attrs else None


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
    required_attrs: dict[str, set[str]] | None = None
    dataset_attrs: set[str] | None = None
    _schema: xv.DatasetSchema = field(init=False, repr=False, compare=False)

    @classmethod
    def from_name(
        cls,
        name: str,
        *,
        fragments: Iterable[str] = (),
        substitutions: Mapping[str, str] | None = None,
    ) -> "DataSchema":
        """Load an OpenGHG schema declaration from the packaged JSON resource."""
        try:
            config = _schema_configs()[name]
        except KeyError as err:
            raise ValueError(f"Unknown data schema: {name}") from err

        if "base" in config:
            selected = [config["base"]]
            try:
                selected.extend(config["fragments"][fragment] for fragment in fragments)
            except KeyError as err:
                raise ValueError(f"Unknown {name} schema fragment: {err.args[0]}") from err
            config = _merge_configs(selected)
        elif tuple(fragments):
            raise ValueError(f"Schema {name} does not define fragments")

        return cls.from_dict(config, substitutions=substitutions)

    @classmethod
    def from_dict(
        cls, config: dict[str, Any], substitutions: Mapping[str, str] | None = None
    ) -> "DataSchema":
        """Create a schema from the JSON-compatible OpenGHG declaration format."""
        substitutions = substitutions or {}

        def name(value: str) -> str:
            return _format(value, substitutions)

        try:
            dtypes = {name(variable): _DTYPES[dtype] for variable, dtype in config.get("dtypes", {}).items()}
        except KeyError as err:
            raise ValueError(f"Unknown schema dtype: {err.args[0]}") from err

        data_vars = {
            name(variable): tuple(name(dim) for dim in dims)
            for variable, dims in config.get("data_vars", {}).items()
        }

        return cls(
            data_vars=data_vars if "data_vars" in config else None,
            dtypes=dtypes if "dtypes" in config else None,
            dims=[name(dim) for dim in config.get("dims", [])] or None,
            units={name(variable): unit for variable, unit in config.get("units", {}).items()} or None,
            units_compatible={
                name(variable): unit for variable, unit in config.get("units_compatible", {}).items()
            }
            or None,
            required_attrs={
                name(variable): set(attributes)
                for variable, attributes in config.get("required_attrs", {}).items()
            }
            or None,
            dataset_attrs=set(config.get("dataset_attrs", [])) or None,
        )

    def __post_init__(self) -> None:
        variables: dict[str, xv.DataArraySchema] = {}
        for name, dims in (self.data_vars or {}).items():
            variables[name] = xv.DataArraySchema(
                dtype=(self.dtypes or {}).get(name),
                attrs=self._attrs_for(name),
                checks=[partial(_check_dims, dims)],
            )

        coord_names = (
            set(self.units or {}) | set(self.units_compatible or {}) | set(self.required_attrs or {})
        ) - set(variables)
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
            attrs=_attrs_schema(required=self.dataset_attrs),
            checks=[self._check_remaining_constraints],
        )

    def _attrs_for(self, name: str) -> xv.AttrsSchema | None:
        compatible_units = self.units_compatible or {}
        units = self.units or {}
        required_attrs = (self.required_attrs or {}).get(name)
        if name in compatible_units:
            return _attrs_schema(required=required_attrs, compatible=compatible_units[name])
        if name in units:
            return _attrs_schema(required=required_attrs, unit=units[name], require_units=True)
        return _attrs_schema(required=required_attrs)

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
