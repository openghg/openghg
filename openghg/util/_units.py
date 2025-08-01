import logging
import cf_xarray.units  # noqa: F401  # Needed to register units
from cf_xarray.units import units as cf_ureg
import pint_xarray  # noqa: F401  # Needed to activate xarray pint accessor
import pint
import xarray as xr

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


# convert scientific notation to volume ratios
unit_mapping = {"1e-6": "ppm", "1e-9": "ppb", "1e-12": "ppt", "1e-15": "ppq"}
cf_ureg.preprocessors.append(lambda x: unit_mapping.get(x, x))

# remove spaces from some non-standard units ("per mil", "per meg", etc.)
cf_ureg.preprocessors.append(lambda x: x.replace("per m", "per_m"))

cf_ureg.define("ppb = 1e-9 mol/mol = parts_per_billion")
cf_ureg.define("ppt = 1e-12 mol/mol= parts_per_trillion")
cf_ureg.define("ppq = 1e-15 mol/mol= parts_per_quadrillion")
cf_ureg.define("@alias permille = permil = per_mil = per_mille")
cf_ureg.define("permeg = 0.001 permille = per_meg")
cf_ureg.define("hpa = 100.0 Pa = hectopascal = hPa")

# Degrees_north is not an accepted CF unit, but we encounter it
cf_ureg.define("@alias degrees_north = Degrees_north")
cf_ureg.define("@alias degrees_east = Degrees_east")

cf_ureg.define(
    "degrees_west = degree = degrees_west = Degrees_west = degrees_W = degreesW = degree_west = degree_W = degreeW"
)
cf_ureg.define(
    "degrees_south = degree = degrees_south = Degrees_south = degrees_S = degreesS = degree_south = degree_S = degreeS"
)


# Invert the unit_mapping to go from cf_xarray pint units back to original strings
# note that `getattr(cf_ureg, v)` will get the unit corresponding to the string v,
# and `f"{...:cf}"` will print in cf format.
#
# NOTE: an important side effect here is using the "cf" formatter; we need to use this
# before defining the "openghg" formatter, or might not be available through cf_ureg.formatter._formatters.
# This might be because of some lazy loading in pint.
inverse_unit_mapping = {f"{getattr(cf_ureg, v):cf}": k for k, v in unit_mapping.items()}


# create custom units registry that does cf formatting, then converts parts_per_billion to 1e-9, etc
@pint.register_unit_format("openghg")
def openghg_format(unit, registry):  # type: ignore
    cf_fmt = registry.formatter._formatters.get("cf")
    out = cf_fmt.format_unit(unit) if cf_fmt is not None else str(unit)
    return inverse_unit_mapping.get(out, out)


cf_ureg.formatter.default_format = "openghg"


def convert_units(ds: xr.Dataset, target_units: dict) -> None:
    for dv, target_unit in target_units.items():
        if dv in ds.data_vars:
            try:
                ds[dv] = ds[dv].pint.to(target_unit)
            except ValueError as e:
                raise ValueError(
                    "Cannot convert unquantified Dataset. Use `ds.pint.quantify()` first."
                ) from e
            ds[dv].attrs["units_definition"] = f"{ds[dv].pint.units:cf}"


def assign_units(
    data: xr.Dataset,
    target_units: dict | None = None,
    is_dequantified: bool = True,
) -> xr.Dataset:
    """This function is used to assign units as well as convert the units of the dataset if target_units are supplied to the function.

    The final supplied values are dequantified to ensure the ModelScenario usecases are not broken.

    Args:
        data: xarray dataset
        target_units: Dictionary specifying the desired units for each variable in the dataset.
            Keys are variable names, and values are the units to which the data should be converted.
            For example: {"mf": "ppm", "mf_variability": "ppm"}.
        is_dequantified: if True, "dequantify" resulting dataset, so units are stored in "units" attribute
            rather than in `.pint.units`

    Returns:
        xr.Dataset
    """
    data = data.pint.quantify()

    if target_units is not None:
        convert_units(data, target_units)

    if is_dequantified:
        data = data.pint.dequantify()

    return data
