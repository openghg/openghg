import pint
import pint_xarray

from openghg.dataobjects import (
    BoundaryConditionsData,
    FluxData,
    FootprintData,
    ObsData,
)

from openghg.util._file import load_internal_json


def _openghg_unit_registry() -> pint.UnitRegistry:
    ureg = pint.UnitRegistry(force_ndarray_like=True)
    ureg.define("ppb = 1e-9 mol/mol = parts_per_billion")
    ureg.define("ppt = 1e-12 mol/mol= parts_per_trillion")
    ureg.define("permeg = 0.001 permille")
    ureg.define("m2 = m*m = metres_squared")
    return ureg


def _read_attributes_json() -> dict:
    return load_internal_json("attributes.json")


def _assign_obs_units(data: ObsData) -> ObsData:
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    attrs = _read_attributes_json()
    unit_mapping = attrs["unit_pint"]
    non_standard = attrs["unit_non_standard_interpret"]

    for key in data.data.keys():
        if "units" in data.data[key].attrs:
            i_unit = data.data[key].attrs["units"]

            if i_unit in unit_mapping:
                unit = unit_mapping[i_unit]
                data.data[key] = data.data[key].pint.quantify(ureg.parse_units(unit)).pint.to("mol/mol")
            elif i_unit == "mol/mol":
                data.data[key] = data.data[key].pint.quantify(ureg.parse_units(i_unit))
            elif i_unit in non_standard:
                unit = non_standard[i_unit]
                data.data[key] = data.data[key].pint.quantify(ureg.parse_units(unit))
            else:
                raise ValueError(f"The unit {i_unit} is not recognised.")

    return data


def _assign_footprint_units(data: FootprintData) -> FootprintData:
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    for key in data.data.keys():
        if "units" in data.data[key].attrs:
            if key == "air_temperature":
                i_unit = "degC"
            elif key == "wind_speed":
                i_unit = "m/s"
            else:
                i_unit = data.data[key].attrs["units"]

            try:
                for coord in data.data[key].coords:
                    data.data[key][coord].attrs.pop("units", None)
                data.data[key] = data.data[key].pint.quantify(ureg.parse_units(i_unit))
            except pint.errors.UndefinedUnitError:
                print(f"Skipping {key} as pint could not parse {i_unit}")

    return data


def _assign_flux_units(data: FluxData) -> FluxData:
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    attrs = _read_attributes_json()
    non_standard = attrs["unit_non_standard_interpret"]

    flux_unit = data.data.flux.attrs["units"]
    if flux_unit in non_standard:
        flux_unit = non_standard[flux_unit]

    data.data["flux"] = data.data["flux"].pint.quantify(ureg.parse_units(flux_unit))

    return data


def _assign_bc_units(data: BoundaryConditionsData) -> BoundaryConditionsData:
    ureg = _openghg_unit_registry()
    pint_xarray.accessors.default_registry = ureg

    for direction in ["n", "e", "s", "w"]:
        var = f"vmr_{direction}"
        try:
            bc_unit = data.data[var].attrs["units"]
        except KeyError:
            print(f"No units provided for {var}. Assuming units of mol/mol")
            bc_unit = "mol/mol"
        data.data[var] = data.data[var].pint.quantify(ureg.parse_units(bc_unit))

    return data


def assign_units(
    data: ObsData | FootprintData | FluxData | BoundaryConditionsData,
) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
    """
    Assign pint units to OpenGHG data objects based on their type.
    """
    if isinstance(data, ObsData):
        return _assign_obs_units(data)
    elif isinstance(data, FootprintData):
        return _assign_footprint_units(data)
    elif isinstance(data, FluxData):
        return _assign_flux_units(data)
    elif isinstance(data, BoundaryConditionsData):
        return _assign_bc_units(data)
    else:
        raise TypeError("Unsupported data type for unit assignment.")
