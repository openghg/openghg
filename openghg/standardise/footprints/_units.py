from xarray import Dataset

_FOOTPRINT_SIGNALS = (
    "fp",
    "fp_low",
    "fp_high",
    "fp_HiTRes",
    "fp_time_resolved",
    "fp_residual",
)


def normalise_footprint_units(data: Dataset) -> None:
    """Apply known internal units without inventing a footprint signal unit."""
    for name, unit in {
        "lat": "degrees_north",
        "lon": "degrees_east",
        "lat_high": "degrees_north",
        "lon_high": "degrees_east",
        "height": "m",
        "H_back": "hour",
    }.items():
        if name in data.coords:
            data[name].attrs["units"] = unit

    signal_unit = next(
        (
            str(data[name].attrs["units"])
            for name in _FOOTPRINT_SIGNALS
            if name in data and data[name].attrs.get("units")
        ),
        None,
    )
    if signal_unit is not None:
        for name in _FOOTPRINT_SIGNALS:
            if name in data and not data[name].attrs.get("units"):
                data[name].attrs["units"] = signal_unit

    for data_var in data.data_vars:
        if isinstance(data_var, str) and data_var.startswith("particle_locations_"):
            data[data_var].attrs["units"] = "1"
        elif isinstance(data_var, str) and data_var.startswith("mean_age_particles_"):
            data[data_var].attrs["units"] = "hour"
