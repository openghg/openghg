from typing import cast

import numpy as np
import xarray as xr

from openghg.analyse._alignment import reindex_time_to_target
from openghg.util import check_lifetime_monthly, species_lifetime, time_offset


def baseline_sensitivities(
    bc: xr.Dataset, fp: xr.Dataset, species: str | None = None, allow_climatology: bool = True
) -> xr.Dataset:
    """Compute contributions from NESW boundary curtains.

    Computes "mean_particle_age" * "vmr" for the NESW boundary curtains,
    including a loss term for short-lifetime species.

    Args:
        bc: boundary conditions dataset
        fp: footprints (or scenario) dataset
        species: optional species, used to check for short-lifetime
        allow_climatology: when aligning bc data to fp data, if one year is present and outside the target
            date range of the fp data, allow the bc data to be treated as a climatology and reindexed based on this.

    Returns:
        Dataset with data variables `bc_n`, `bc_e`, `bc_s`, `bc_w` for baseline contributions.

    Raises:
        ValueError: if wrong footprints used for short-lifetime species.

    """
    # explicitly copy units for coordinates if they're missing; implicitly we're assuming units are aligned
    for dim in bc.dims:
        if dim in fp.dims and "units" not in bc[dim].attrs:
            bc[dim].attrs["units"] = fp[dim].attrs.get("units")

    bc = reindex_time_to_target(bc, fp, allow_climatology=allow_climatology)
    fp = fp.pint.quantify()
    bc = bc.pint.quantify().pint.reindex_like(fp, "ffill")

    # align chunks for time after filling
    fp_time_chunks = fp.particle_locations_n.chunksizes.get("time")
    if fp_time_chunks is not None:
        fp_time_chunk = fp_time_chunks[0]
        bc = bc.chunk({"time": fp_time_chunk})

    # check if loss term is needed
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

    sensitivities = {
        "bc_n": fp["particle_locations_n"] * bc["vmr_n"] * loss_n,
        "bc_e": fp["particle_locations_e"] * bc["vmr_e"] * loss_e,
        "bc_s": fp["particle_locations_s"] * bc["vmr_s"] * loss_s,
        "bc_w": fp["particle_locations_w"] * bc["vmr_w"] * loss_w,
    }

    # convert units then dequantify so output has correct units, but is not quantified, which
    # might cause issues with dask in subsequent computations
    result = cast(xr.Dataset, xr.Dataset(sensitivities).pint.dequantify())

    # keep float32
    result = result.astype({f"bc_{d}": "float32" for d in "nesw"})

    return result


def baseline_reference_sensitivities(
    bc: xr.Dataset,
    fp: xr.Dataset,
    *,
    species: str,
    reference: float,
    reference_units: str,
    mass_tolerance: float,
) -> xr.Dataset:
    """Compute stable CO2/O2 boundary anomaly and mass operands.

    Each native curtain retains its dimensions. All four curtains are normalized
    together per observation, then ``anomaly = mass * (vmr - reference)``.
    Reconstruct absolute contributions as ``anomaly + bc_reference * mass``.
    Concentrations and the scalar reference use the north curtain's units.

    BC timestamps mark interval starts, with validity given by the dataset's
    ``time_period`` attribute (OpenGHG period syntax, e.g. ``1 month``). Forward
    filling is allowed only inside those intervals, including for the final BC
    timestamp. Shared spatial indexes must match exactly; no spatial filling is
    performed. Inputs and their metadata are borrowed without mutation.

    Arithmetic uses float64 but cannot recover precision absent in the inputs.
    Dask data validation runs when the operands are materialized; coordinates,
    units, species, reference and tolerance are validated immediately. No chunk
    alignment or full-array materialization is requested here.

    Args:
        bc: Dataset containing ``vmr_n/e/s/w`` with concentration units.
        fp: Dataset containing dimensionless ``particle_locations_n/e/s/w``.
        species: Stable pilot species, CO2 or O2 (case insensitive).
        reference: Finite arithmetic reference, before unit conversion.
        reference_units: Concentration units of the reference.
        mass_tolerance: Explicit absolute tolerance for raw mass versus unity;
            must be finite and satisfy ``0 <= mass_tolerance < 1``.

    Returns:
        Dataset with ``bc_anomaly_n/e/s/w``, dimensionless ``bc_mass_n/e/s/w``
        and scalar ``bc_reference``. All values are float64.

    Raises:
        ValueError: For unsupported species, invalid data, units, coordinates,
            coverage, reference or mass tolerance.
    """
    import pandas as pd
    from pint.errors import PintError

    from openghg.util import relative_time_offset
    from openghg.util._units import cf_ureg

    if species.lower() not in {"co2", "o2"}:
        raise ValueError("Centered boundary operands support stable CO2/O2 only.")
    if not np.isfinite(reference):
        raise ValueError("Boundary reference must be finite.")
    if not np.isfinite(mass_tolerance) or not 0 <= mass_tolerance < 1:
        raise ValueError("mass_tolerance must be finite and satisfy 0 <= tolerance < 1.")

    for dataset in (bc, fp):
        if "time" not in dataset.indexes:
            raise ValueError("Boundary and footprint time must be indexed coordinates.")
        index = dataset.indexes["time"]
        if (
            not isinstance(index, pd.DatetimeIndex)
            or not len(index)
            or not index.is_unique
            or not index.is_monotonic_increasing
            or index.hasnans
        ):
            raise ValueError("Boundary and footprint time must be nonempty, unique and increasing.")
    if "time_period" not in bc.attrs:
        raise ValueError("BC time_period must declare forward-fill coverage.")
    period = relative_time_offset(period=bc.attrs["time_period"])
    bc_time = bc.indexes["time"]
    fp_time = fp.indexes["time"]
    if bc_time[0] + period <= bc_time[0]:
        raise ValueError("BC time_period must be positive.")
    positions = bc_time.get_indexer(fp_time, method="ffill")
    if (positions < 0).any() or any(t >= bc_time[i] + period for t, i in zip(fp_time, positions)):
        raise ValueError("BC time_period coverage does not cover every footprint observation.")

    def conversion(source: str, target: str) -> float:
        try:
            source_unit = cf_ureg.Unit(source)
            target_unit = cf_ureg.Unit(target)
            if not source_unit.dimensionless or not target_unit.dimensionless:
                raise ValueError("Expected concentration or dimensionless units.")
            return float(cf_ureg.Quantity(1.0, source_unit).to(target_unit).magnitude)
        except (PintError, ValueError, TypeError) as exc:
            raise ValueError(f"Invalid boundary units: {source!r} -> {target!r}.") from exc

    units = bc["vmr_n"].attrs.get("units")
    if not units or not reference_units:
        raise ValueError("Boundary concentration and reference units must be explicit.")
    converted_reference = np.float64(reference) * conversion(reference_units, units)
    if not np.isfinite(converted_reference):
        raise ValueError("Converted boundary reference must be finite.")

    def checked_values(values: np.ndarray, *, nonnegative: bool = False) -> np.ndarray:
        if not np.isfinite(values).all() or (nonnegative and (values < 0).any()):
            raise ValueError("Boundary concentrations/weights must be finite; weights must be nonnegative.")
        return values

    def checked_total(values: np.ndarray) -> np.ndarray:
        if (
            not np.isfinite(values).all()
            or (values <= 0).any()
            or (np.abs(values - 1) > mass_tolerance).any()
        ):
            raise ValueError("Total boundary mass must be positive and within mass_tolerance of unity.")
        return values

    weights = {}
    concentrations = {}
    for direction in "nesw":
        weight = fp[f"particle_locations_{direction}"]
        concentration = bc[f"vmr_{direction}"]
        if "time" not in weight.dims or set(weight.dims) != set(concentration.dims):
            raise ValueError(
                "Boundary concentration and weight curtain dimensions must match, including time."
            )
        for dim in weight.dims:
            if dim == "time":
                continue
            if dim not in bc.indexes or dim not in fp.indexes or not bc.indexes[dim].equals(fp.indexes[dim]):
                raise ValueError(f"Boundary curtain coordinate {dim!r} must match exactly.")
            bc_unit, fp_unit = bc[dim].attrs.get("units"), fp[dim].attrs.get("units")
            if bc_unit and fp_unit and cf_ureg.Unit(bc_unit) != cf_ureg.Unit(fp_unit):
                raise ValueError(f"Boundary curtain coordinate units for {dim!r} must match exactly.")
        concentration_units = concentration.attrs.get("units")
        if not concentration_units:
            raise ValueError("Boundary concentrations must have explicit units.")
        scale = conversion(concentration_units, units)
        weight_units = weight.attrs.get("units")
        if not weight_units or conversion(weight_units, "1") != 1:
            raise ValueError("Boundary weights must have unscaled dimensionless units.")
        weights[direction] = xr.apply_ufunc(
            checked_values,
            weight.astype("float64", copy=False),
            kwargs={"nonnegative": True},
            dask="parallelized",
            output_dtypes=[np.float64],
        )
        selected = concentration.reindex(time=fp.time, method="ffill", copy=False).astype(
            "float64", copy=False
        )
        if scale != 1:
            selected = selected * scale
        concentrations[direction] = xr.apply_ufunc(
            checked_values,
            selected,
            dask="parallelized",
            output_dtypes=[np.float64],
        )

    total = sum(w.sum([dim for dim in w.dims if dim != "time"], skipna=False) for w in weights.values())
    total = xr.apply_ufunc(checked_total, total, dask="parallelized", output_dtypes=[np.float64])
    result = {}
    for direction in "nesw":
        mass = weights[direction] / total
        anomaly = mass * (concentrations[direction] - converted_reference)
        result[f"bc_mass_{direction}"] = mass.assign_attrs(units="1")
        result[f"bc_anomaly_{direction}"] = anomaly.assign_attrs(units=units)
    result["bc_reference"] = xr.DataArray(converted_reference, attrs={"units": units})
    return xr.Dataset(result)
