import logging
import cf_xarray.units
import pint
import pint_xarray
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
    substitutions = {
        "m s-1": "m/s",
        "m2 s-1": "m²/s",
        "kg m-2 s-1": "kg/(m²·s)",
        "μmol m-2 s-1": "umol/(m²·s)",  # if applicable
    }
    return substitutions.get(unit_str, unit_str)


def _read_attributes_json() -> dict:
    return load_internal_json("attributes.json")


def assign_units(data, target_units=None):
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    attrs = _read_attributes_json()
    unit_mapping = attrs["unit_pint"]
    non_standard = attrs["unit_non_standard_interpret"]

    for key in data.data.data_vars:
        if "units" in data.data[key].attrs:
            i_unit = data.data[key].attrs["units"].lower()
            i_unit = _normalize_unit(i_unit)
            try:
                if i_unit in unit_mapping:
                    unit = unit_mapping[i_unit]
                elif i_unit in non_standard:
                    unit = non_standard[i_unit]
                else:
                    unit = i_unit   

                quanitified_data = data.data[key].pint.quantify(ureg.parse_units(unit))
                
                
                if target_units and key in target_units:
                    quanitified_data = quanitified_data.pint.to(target_units[key])

                data.data[key] = quanitified_data
                # Added the original attrs unit value back to the data variable on a pint quantified dataset, as the entire workflow is not yet fetching units via data[key].pint.units. 
                cf_unit = f"{quanitified_data.pint.units:cf}"
                data.data[key].attrs["units"] = i_unit

            except pint.errors.UndefinedUnitError or pint.errors.is_valid_unit_name():
                logger.warning(f"The unit '{i_unit}' for key '{key}' is not recognised by mappings or Pint.")

    return data  