from typing import cast

import numpy as np
import xarray as xr

from openghg.util import check_lifetime_monthly, species_lifetime, time_offset
from ._utils import check_units


def baseline_sensitivities(
    bc: xr.Dataset, fp: xr.Dataset, species: str | None = None, output_units: float = 1.0
) -> xr.Dataset:
    """Compute contributions from NESW boundary curtains.

    Computes "mean_particle_age" * "vmr" for the NESW boundary curtains,
    including a loss term for short-lifetime species.

    Args:
        bc: boundary conditions dataset
        fp: footprints (or scenario) dataset
        species: optional species, used to check for short-lifetime
        output_units: units conversion to apply; e.g. `1e-9` will scale the
          outputs by `1e9`, converting mol/mol to ppb.

    Returns:
        Dataset with data variables `bc_n`, `bc_e`, `bc_s`, `bc_w` for baseline constributions.

    Raises:
        ValueError: if wrong footprints used for short-lifetime species.

    """
    bc = bc.reindex_like(fp, "ffill")

    lifetime_value = species_lifetime(species)
    check_monthly = check_lifetime_monthly(lifetime_value)

    if check_monthly:
        lifetime_monthly = cast(list[str] | None, lifetime_value)
        lifetime: str | None = None
    else:
        lifetime_monthly = None
        lifetime = cast(str | None, lifetime_value)

    if lifetime is not None:
        short_lifetime = True
        lt_time_delta = time_offset(period=lifetime)
        lifetime_hrs: float | np.ndarray = lt_time_delta.total_seconds() / 3600.0
    elif lifetime_monthly:
        short_lifetime = True
        lifetime_monthly_hrs = []
        for lt in lifetime_monthly:
            lt_time_delta = time_offset(period=lt)
            lt_hrs = lt_time_delta.total_seconds() / 3600.0
            lifetime_monthly_hrs.append(lt_hrs)

        # calculate the lifetime_hrs associated with each time point in fp data
        # this is because lifetime can be a list of monthly values
        time_month = fp["time"].dt.month
        lifetime_hrs = np.array([lifetime_monthly_hrs[item - 1] for item in time_month.values])
    else:
        short_lifetime = False

    # Include loss condition if lifetime of species is specified
    if short_lifetime:
        expected_vars = (
            "mean_age_particles_n",
            "mean_age_particles_e",
            "mean_age_particles_s",
            "mean_age_particles_w",
        )
        for var in expected_vars:
            if var not in fp.data_vars:
                raise ValueError(
                    f"Unable to calculate baseline for short-lived species {species} without species specific footprint."
                )

        # Ignoring type below -  - problem with xarray patching np.exp to return DataArray rather than ndarray
        loss_n: xr.DataArray | float = np.exp(-1 * fp["mean_age_particles_n"] / lifetime_hrs).rename(  # type: ignore
            "loss_n"
        )
        loss_e: xr.DataArray | float = np.exp(-1 * fp["mean_age_particles_e"] / lifetime_hrs).rename(  # type: ignore
            "loss_e"
        )
        loss_s: xr.DataArray | float = np.exp(-1 * fp["mean_age_particles_s"] / lifetime_hrs).rename(  # type: ignore
            "loss_s"
        )
        loss_w: xr.DataArray | float = np.exp(-1 * fp["mean_age_particles_w"] / lifetime_hrs).rename(  # type: ignore
            "loss_w"
        )

    else:
        loss_n = 1.0
        loss_e = 1.0
        loss_s = 1.0
        loss_w = 1.0

    # Check and extract units as float, if present.
    units_default = 1.0
    units_n = check_units(bc["vmr_n"], default=units_default)
    units_e = check_units(bc["vmr_e"], default=units_default)
    units_s = check_units(bc["vmr_s"], default=units_default)
    units_w = check_units(bc["vmr_w"], default=units_default)

    sensitivities = {
        "bc_n": fp["particle_locations_n"] * bc["vmr_n"] * loss_n * units_n,
        "bc_e": fp["particle_locations_e"] * bc["vmr_e"] * loss_e * units_e,
        "bc_s": fp["particle_locations_s"] * bc["vmr_s"] * loss_s * units_s,
        "bc_w": fp["particle_locations_w"] * bc["vmr_w"] * loss_w * units_w,
    }

    return xr.Dataset(sensitivities) / output_units
