from xarray import Dataset


def normalise_boundary_condition_units(data: Dataset, vmr_units: str = "mol/mol") -> None:
    """Fill the known units of the OpenGHG boundary-condition format."""
    for name, unit in {
        "lat": "degrees_north",
        "lon": "degrees_east",
        "level": "m",
        "height": "m",
    }.items():
        if name in data.coords:
            data[name].attrs["units"] = unit

    for name in ("vmr_n", "vmr_e", "vmr_s", "vmr_w"):
        if name in data and not data[name].attrs.get("units"):
            data[name].attrs["units"] = vmr_units
