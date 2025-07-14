import logging
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
    ureg.define("hPa = hectopascal")
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


# def _assign_obs_units(data, target_units: dict | None = None):
#     ureg = _openghg_unit_registry()
#     pint_xarray.accessors.default_registry = ureg

#     attrs = _read_attributes_json()
#     unit_mapping = attrs["unit_pint"]
#     non_standard = attrs["unit_non_standard_interpret"]

#     for key in data.data.keys():
#         if "units" in data.data[key].attrs:
#             i_unit = data.data[key].attrs["units"]

#             # Determine appropriate pint unit from mappings
#             if i_unit in unit_mapping:
#                 unit = unit_mapping[i_unit]
#             elif i_unit == "mol/mol":
#                 unit = i_unit
#             elif i_unit in non_standard:
#                 unit = non_standard[i_unit]
#             else:
#                 raise ValueError(f"The {key} with unit {i_unit} is not recognised.")

#             # Quantify the data with inferred unit
#             data.data[key] = data.data[key].pint.quantify(ureg.parse_units(unit))

#             # Only convert if target unit provided
#             if target_units and key in target_units:
#                 data.data[key] = data.data[key].pint.to(target_units[key])

#     return data


# def _assign_footprint_units(data, target_units: dict | None = None):
#     ureg = _openghg_unit_registry()
#     pint_xarray.accessors.default_registry = ureg

#     for key in data.data.keys():
#         if "units" in data.data[key].attrs:
#             # Special handling for ambiguous units
#             if key == "air_temperature":
#                 i_unit = "degC"
#             elif key == "wind_speed":
#                 i_unit = "m/s"
#             else:
#                 i_unit = data.data[key].attrs["units"]

#             try:
#                 for coord in data.data[key].coords:
#                     data.data[key][coord].attrs.pop("units", None)
#                 data.data[key] = data.data[key].pint.quantify(ureg.parse_units(i_unit))

#                 # Convert to user-specified unit if provided
#                 if target_units and key in target_units:
#                     data.data[key] = data.data[key].pint.to(target_units[key])

#             except pint.errors.UndefinedUnitError:
#                 print(f"Skipping {key} as pint could not parse {i_unit}")

#     return data


# def _assign_flux_units(data, target_units: dict | None = None):
#     ureg = _openghg_unit_registry()
#     pint_xarray.accessors.default_registry = ureg

#     attrs = _read_attributes_json()
#     non_standard = attrs["unit_non_standard_interpret"]

#     flux_unit = data.data.flux.attrs.get("units", "mol/m^2/s")  # Use default if missing

#     if flux_unit in non_standard:
#         flux_unit = non_standard[flux_unit]

#     data.data["flux"] = data.data["flux"].pint.quantify(ureg.parse_units(flux_unit))

#     if target_units and "flux" in target_units:
#         data.data["flux"] = data.data["flux"].pint.to(target_units["flux"])

#     return data



# def _assign_bc_units(data):
#     ureg = _openghg_unit_registry()
#     pint_xarray.accessors.default_registry = ureg

#     for direction in ["n", "e", "s", "w"]:
#         var = f"vmr_{direction}"
#         try:
#             bc_unit = data.data[var].attrs["units"]
#         except KeyError:
#             print(f"No units provided for {var}. Assuming units of mol/mol")
#             bc_unit = "mol/mol"
#         data.data[var] = data.data[var].pint.quantify(ureg.parse_units(bc_unit))

#     return data


# def assign_units(
#     data, target_units
# ):
#     """
#     Assign pint units to OpenGHG data objects based on their type.
#     """
#     if type(data).__name__ == "ObsData" or type(data).__name__ == "ObsColumnData":
#         return _assign_obs_units(data=data, target_units=target_units)
#     elif type(data).__name__ == "FootprintData":
#         return _assign_footprint_units(data)
#     elif type(data).__name__ == "FluxData":
#         return _assign_flux_units(data)
#     elif type(data).__name__ == "BoundaryConditionsData":
#         return _assign_bc_units(data)
#     else:
#         raise TypeError("Unsupported data type for unit assignment.")



def assign_units(data, target_units=None):
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    attrs = _read_attributes_json()
    unit_mapping = attrs["unit_pint"]
    non_standard = attrs["unit_non_standard_interpret"]

    for key in data.data.keys():
        if "units" in data.data[key].attrs:
            i_unit = data.data[key].attrs["units"]
            i_unit = _normalize_unit(i_unit)
            try:
                if i_unit in unit_mapping:
                    unit = unit_mapping[i_unit]
                elif i_unit in non_standard:
                    unit = non_standard[i_unit]
                else:
                    unit = i_unit   

                data.data[key] = data.data[key].pint.quantify(ureg.parse_units(unit))

                if target_units and key in target_units:
                    data.data[key] = data.data[key].pint.to(target_units[key])

            except pint.errors.UndefinedUnitError:
                logger.warning(f"The unit '{i_unit}' for key '{key}' is not recognised by mappings or Pint.")

    return data  