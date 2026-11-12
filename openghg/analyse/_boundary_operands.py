"""Accurate centered boundary operands for stable CO2/O2 surface inversions."""

import numpy as np
import xarray as xr

from openghg.util._units import cf_ureg


def _mole_fraction_unit_scale(raw_units: str) -> float:
    """Return the scale of a (possibly scaled) mole-fraction expression."""
    try:
        quantity = cf_ureg.parse_expression(raw_units)
        if hasattr(quantity, "to"):
            return float(quantity.to("mol/mol").magnitude)
        return float(quantity)
    except Exception as exc:
        raise ValueError(f"Invalid mole-fraction units: {raw_units!r}.") from exc


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
    Arithmetic uses the north curtain's scale. Output units retain its units,
    with any explicit numerical factor absorbed into the output values.

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

    from openghg.util import relative_time_offset

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
    if (positions < 0).any() or any(t >= bc_time[int(i)] + period for t, i in zip(fp_time, positions)):
        raise ValueError("BC time_period coverage does not cover every footprint observation.")

    def conversion(source: str, target: str) -> float:
        try:
            source_scale = _mole_fraction_unit_scale(source)
            target_scale = _mole_fraction_unit_scale(target)
            if not all(np.isfinite(scale) and scale > 0 for scale in (source_scale, target_scale)):
                raise ValueError("Boundary unit scales must be finite and positive.")
            return source_scale / target_scale
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid boundary units: {source!r} -> {target!r}.") from exc

    units = bc["vmr_n"].attrs.get("units")
    if not units or not reference_units:
        raise ValueError("Boundary concentration and reference units must be explicit.")
    converted_reference = np.float64(reference) * conversion(reference_units, units)
    if not np.isfinite(converted_reference):
        raise ValueError("Converted boundary reference must be finite.")
    # Pint array quantification needs a unit, rather than a scaled expression.
    expression = cf_ureg.parse_expression(units)
    output_scale = float(getattr(expression, "magnitude", expression))
    output_units = units if output_scale == 1 else str(getattr(expression, "units", "1"))

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

    weights: dict[str, xr.DataArray] = {}
    concentrations: dict[str, xr.DataArray] = {}
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
    result: dict[str, xr.DataArray] = {}
    for direction in "nesw":
        mass = weights[direction] / total
        anomaly = mass * (concentrations[direction] - converted_reference)
        if output_scale != 1:
            anomaly = anomaly * output_scale
        result[f"bc_mass_{direction}"] = mass.assign_attrs(units="1")
        result[f"bc_anomaly_{direction}"] = anomaly.assign_attrs(units=output_units)
    result["bc_reference"] = xr.DataArray(converted_reference * output_scale, attrs={"units": output_units})
    return xr.Dataset(result)
