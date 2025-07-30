import logging
import cf_xarray.units  # noqa: F401  # Needed to register units
import pint_xarray
import pint
import xarray as xr

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openghg.dataobjects import ObsData, ObsColumnData, FootprintData, FluxData
    from openghg.dataobjects._basedata import _BaseData

from openghg.util._file import load_internal_json

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def _openghg_unit_registry() -> pint.UnitRegistry:
    ureg = pint.UnitRegistry(force_ndarray_like=True)
    ureg.define("ppb = 1e-9 mol/mol = parts_per_billion")
    ureg.define("ppt = 1e-12 mol/mol= parts_per_trillion")
    ureg.define("permeg = 0.001 permille")
    ureg.define("m2 = m*m = metres_squared")
    ureg.define("hpa = hectopascal")
    ureg.define("degrees_north = degree")
    ureg.define("degrees_east = degree")
    ureg.define("degrees_west = degree")
    ureg.define("degrees_south = degree")
    ureg.define("Degrees_north = degree")
    ureg.define("Degrees_east = degree")
    ureg.define("Degrees_west = degree")
    ureg.define("Degrees_south = degree")
    return ureg


def _normalize_unit(unit_str: str) -> str:
    """
    Convert a unit string to a normalized or preferred format for display or comparison.

    This function maps specific unit strings (e.g., from a dataset or user input)
    to a standardized or more readable format using predefined substitutions.
    If the input unit is not found in the substitutions, it is returned unchanged.

    Args:
        unit_str: The input unit string (e.g., "m s-1", "kg m-2 s-1").

    Returns:
        str: The normalized or unchanged unit string.
    """
    substitutions = {
        "m s-1": "m/s",
        "m2 s-1": "m²/s",
        "kg m-2 s-1": "kg/(m²·s)",
        "μmol m-2 s-1": "umol/(m²·s)",  # if applicable
    }
    return substitutions.get(unit_str, unit_str)


def _read_attributes_json() -> dict:
    return load_internal_json("attributes.json")


def assign_units(
    data: "ObsData | ObsColumnData | FootprintData | FluxData | _BaseData",
    target_units: dict | None = None,
    is_dequantified: bool = True,
) -> xr.Dataset:
    """This function is used to assign units as well as convert the units of the dataset if target_units are supplied to the function. The final supplied values are dequantified to ensure the ModelScenario usecases are not broken.

    Args:
        data: xarray dataset
        target_units: Dictionary specifying the desired units for each variable in the dataset. Keys are variable names, and values are the units to which the data should be converted.
    Example:
        {
            "mf": "ppm",
            "mf_variability": "ppm"
        }
    Returns:
        xr.Dataset
    """
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    attrs = _read_attributes_json()
    unit_mapping = attrs["unit_pint"]
    non_standard = attrs["unit_non_standard_interpret"]

    # Invert the unit_mapping to go from canonical pint units back to original strings
    inverse_unit_mapping = {v: k for k, v in unit_mapping.items()}

    for key in data.data.data_vars:
        if "units" not in data.data[key].attrs:
            continue

        if key in ["lat", "latitude", "lon", "longitude"]:
            continue

        i_unit = _normalize_unit(data.data[key].attrs["units"].lower())

        try:
            # Resolve input unit → canonical Pint unit
            if i_unit in unit_mapping:
                pint_unit_str = unit_mapping[i_unit]
            elif i_unit in non_standard:
                pint_unit_str = non_standard[i_unit]
            else:
                pint_unit_str = i_unit

            # Quantify the data with the unit
            quantified_data = data.data[key].pint.quantify(ureg.parse_units(pint_unit_str))

            # Convert to target units if requested
            if target_units and key in target_units:
                quantified_data = quantified_data.pint.to(target_units[key])
                quantified_data.attrs["converted_pint_units"] = str(quantified_data.pint.units)

            data.data[key] = quantified_data

            pint_final_unit = str(quantified_data.pint.units)

            # Map back to original preferred unit string if available
            preferred_unit = inverse_unit_mapping.get(pint_final_unit, pint_final_unit)
            # Store this back in attrs (user-facing)
            if is_dequantified:
                data.data[key] = data.data[key].pint.dequantify()
            data.data[key].attrs["units"] = preferred_unit
        except pint.errors.UndefinedUnitError:
            logger.warning(f"The unit '{i_unit}' for key '{key}' is not recognised by mappings or Pint.")

    return data
