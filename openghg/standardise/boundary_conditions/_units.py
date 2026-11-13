from xarray import Dataset

from openghg.store._boundary_conditions import BOUNDARY_CONDITION_COORD_UNITS, DEFAULT_VMR_UNIT


def normalise_boundary_condition_units(data: Dataset, vmr_units: str = DEFAULT_VMR_UNIT) -> None:
    """Fill the known units of the OpenGHG boundary-condition format."""
    for name, unit in BOUNDARY_CONDITION_COORD_UNITS.items():
        if name in data.coords:
            data[name].attrs["units"] = unit

    for name in ("vmr_n", "vmr_e", "vmr_s", "vmr_w"):
        if name in data and not data[name].attrs.get("units"):
            data[name].attrs["units"] = vmr_units
