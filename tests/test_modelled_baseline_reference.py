"""Independent numerical checks for the experimental stable boundary operands."""

import copy

import dask
import dask.array as da
from dask.callbacks import Callback
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse._modelled_baseline import baseline_reference_sensitivities


def inputs(constant=False, dtype="float64"):
    time = pd.date_range("2024-01-01", periods=2, freq="D")
    coords = {"time": time, "height": [100, 200], "lon": [1, 2, 3], "lat": [4, 5]}
    bc = xr.Dataset(coords=coords, attrs={"time_period": "1 day", "source": "borrowed"})
    fp = xr.Dataset(coords=coords, attrs={"source": "borrowed footprint"})
    # Deliberately different curtain shapes, values and observation mass errors.
    count = 2 * (2 * 3 + 2 * 2)
    for i, direction in enumerate("nesw"):
        axis = "lon" if direction in "ns" else "lat"
        shape = (2, 2, len(coords[axis]))
        dims = ("time", "height", axis)
        values = (
            np.full(shape, 209300.0)
            if constant
            else 209300 + np.arange(np.prod(shape)).reshape(shape) * 0.013 + i
        )
        bc[f"vmr_{direction}"] = xr.DataArray(values.astype(dtype), dims=dims, attrs={"units": "ppm"})
        weights = np.full(shape, 1 / count)
        weights[0] *= 1 + 2e-5
        weights[1] *= 1 - 2e-5
        fp[f"particle_locations_{direction}"] = xr.DataArray(
            weights.astype(dtype), dims=dims, attrs={"units": "1"}
        )
    for ds in (bc, fp):
        ds.height.attrs["units"] = "m"
        ds.lon.attrs["units"] = "degrees_east"
        ds.lat.attrs["units"] = "degrees_north"
    return bc, fp


def calculate(bc, fp, reference=209300.0, **kwargs):
    return baseline_reference_sensitivities(
        bc, fp, species="o2", reference=reference, reference_units="ppm", mass_tolerance=3e-5, **kwargs
    )


def oracle(bc, fp):
    """Plain NumPy per-observation normalization, independent of xarray kernel."""
    total = np.zeros(fp.sizes["time"], dtype=np.float64)
    absolute = np.zeros_like(total)
    for direction in "nesw":
        w = fp[f"particle_locations_{direction}"].values.astype(np.float64)
        c = bc[f"vmr_{direction}"].values.astype(np.float64)
        total += w.reshape(len(total), -1).sum(axis=1)
        absolute += (w * c).reshape(len(total), -1).sum(axis=1)
    return absolute / total


def reconstruct(result):
    return sum(
        (result[f"bc_anomaly_{d}"] + result.bc_reference * result[f"bc_mass_{d}"]).sum(
            [dim for dim in result[f"bc_mass_{d}"].dims if dim != "time"], skipna=False
        )
        for d in "nesw"
    )


@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_constant_curtains_correct_joint_mass(dtype):
    bc, fp = inputs(constant=True, dtype=dtype)
    result = calculate(bc, fp)
    np.testing.assert_allclose(reconstruct(result), 209300, rtol=0, atol=6e-11)
    for d in "nesw":
        assert result[f"bc_anomaly_{d}"].dims == fp[f"particle_locations_{d}"].dims
        assert not result[f"bc_anomaly_{d}"].values.any()
        assert result[f"bc_anomaly_{d}"].attrs["units"] == "ppm"
        assert result[f"bc_mass_{d}"].attrs["units"] == "1"
    assert all(value.dtype == np.float64 for value in result.data_vars.values())


@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_nonconstant_oracle_and_reference_invariance(dtype):
    bc, fp = inputs(dtype=dtype)
    first = calculate(bc, fp)
    second = calculate(bc, fp, reference=200000)
    for result in (first, second):
        np.testing.assert_allclose(reconstruct(result), oracle(bc, fp), rtol=0, atol=9e-11)
    for d in "nesw":
        np.testing.assert_allclose(
            second[f"bc_anomaly_{d}"] - first[f"bc_anomaly_{d}"],
            9300 * first[f"bc_mass_{d}"],
            rtol=2e-15,
            atol=2e-12,
        )


def test_reference_and_curtain_unit_conversion():
    bc, fp = inputs(constant=True)
    bc.vmr_e.data *= 1000
    bc.vmr_e.attrs["units"] = "ppb"
    result = baseline_reference_sensitivities(
        bc, fp, species="CO2", reference=0.2093, reference_units="mol/mol", mass_tolerance=3e-5
    )
    assert result.bc_reference.item() == pytest.approx(209300)
    np.testing.assert_allclose(reconstruct(result), 209300, rtol=0, atol=6e-11)


@pytest.mark.parametrize("bad", [np.nan, np.inf, -0.01, 0.0, 0.1])
def test_invalid_weights(bad):
    bc, fp = inputs()
    for d in "nesw":
        fp[f"particle_locations_{d}"].data[:] = bad
    with pytest.raises(ValueError, match="finite|nonnegative|mass"):
        calculate(bc, fp)


@pytest.mark.parametrize("bad", [np.nan, np.inf])
def test_invalid_concentrations_even_at_zero_weight(bad):
    bc, fp = inputs()
    bc.vmr_n.data[0, 0, 0] = bad
    fp.particle_locations_n.data[0, 0, 0] = 0
    with pytest.raises(ValueError, match="finite"):
        calculate(bc, fp)


@pytest.mark.parametrize("target,unit", [("vmr_n", None), ("vmr_e", "m"), ("vmr_s", "nonsense")])
def test_invalid_concentration_units(target, unit):
    bc, fp = inputs()
    bc[target].attrs["units"] = unit
    with pytest.raises(ValueError, match="units"):
        calculate(bc, fp)


@pytest.mark.parametrize("unit", [None, "m", "ppm"])
def test_invalid_weight_units(unit):
    bc, fp = inputs()
    fp.particle_locations_w.attrs["units"] = unit
    with pytest.raises(ValueError, match="units"):
        calculate(bc, fp)


@pytest.mark.parametrize("species", ["ch4", "rn", "apo", "o2/n2"])
def test_unsupported_species(species):
    bc, fp = inputs()
    with pytest.raises(ValueError, match="CO2/O2"):
        baseline_reference_sensitivities(
            bc, fp, species=species, reference=209300, reference_units="ppm", mass_tolerance=3e-5
        )


@pytest.mark.parametrize("tolerance", [np.nan, np.inf, -1, 1])
def test_invalid_tolerance(tolerance):
    bc, fp = inputs()
    with pytest.raises(ValueError, match="mass_tolerance"):
        baseline_reference_sensitivities(
            bc, fp, species="co2", reference=400, reference_units="ppm", mass_tolerance=tolerance
        )


def test_no_implicit_mass_tolerance():
    bc, fp = inputs()
    with pytest.raises(TypeError, match="mass_tolerance"):
        baseline_reference_sensitivities(bc, fp, species="co2", reference=400, reference_units="ppm")


def test_tolerance_is_absolute_and_selected():
    bc, fp = inputs()
    with pytest.raises(ValueError, match="mass_tolerance"):
        baseline_reference_sensitivities(
            bc, fp, species="co2", reference=400, reference_units="ppm", mass_tolerance=1e-5
        )


def test_ffill_only_within_declared_intervals():
    bc, fp = inputs(constant=True)
    fp = fp.assign_coords(time=fp.time + pd.Timedelta(hours=12))
    result = calculate(bc, fp)
    xr.testing.assert_equal(result.time, fp.time)
    np.testing.assert_allclose(reconstruct(result), 209300)
    for shift in [-13, 12]:
        with pytest.raises(ValueError, match="coverage"):
            calculate(bc, fp.assign_coords(time=fp.time + pd.Timedelta(hours=shift)))
    bc.attrs["time_period"] = "6 hours"
    with pytest.raises(ValueError, match="coverage"):
        calculate(bc, fp)
    del bc.attrs["time_period"]
    with pytest.raises(ValueError, match="time_period"):
        calculate(bc, fp)


@pytest.mark.parametrize("axis", ["lat", "lon", "height"])
def test_exact_spatial_coordinates(axis):
    bc, fp = inputs()
    with pytest.raises(ValueError, match="coordinate"):
        calculate(bc.assign_coords({axis: bc[axis] + 0.1}), fp)
    with pytest.raises(ValueError, match="coordinate"):
        calculate(bc.isel({axis: slice(None, None, -1)}), fp)


@pytest.mark.parametrize("which", ["bc", "fp"])
def test_invalid_time_indexes(which):
    bc, fp = inputs()
    ds = bc if which == "bc" else fp
    ds.coords["time"] = [ds.time.values[0], ds.time.values[0]]
    with pytest.raises(ValueError, match="time"):
        calculate(bc, fp)


def test_dask_is_lazy_preserves_borrowed_arrays_metadata_and_chunks():
    bc, fp = inputs()
    bc = bc.chunk({"time": 1, "height": 1})
    fp = fp.chunk({"time": 1, "height": 1})
    before_bc, before_fp = bc.copy(deep=True), fp.copy(deep=True)
    attrs_bc = copy.deepcopy(bc.to_dict(data=False))
    attrs_fp = copy.deepcopy(fp.to_dict(data=False))
    borrowed = {name: value.data for ds in (bc, fp) for name, value in ds.data_vars.items()}
    tasks = []
    with Callback(pretask=lambda key, *_: tasks.append(key)):
        result = calculate(bc, fp)
    assert not tasks
    for d in "nesw":
        assert isinstance(result[f"bc_mass_{d}"].data, da.Array)
        assert result[f"bc_mass_{d}"].chunks == fp[f"particle_locations_{d}"].chunks
        assert not any("rechunk" in name for name in result[f"bc_anomaly_{d}"].data.dask.layers)
    with dask.config.set(scheduler="synchronous"):
        np.testing.assert_allclose(
            reconstruct(result).compute(), oracle(before_bc, before_fp), rtol=0, atol=9e-11
        )
    for ds in (bc, fp):
        for name, value in ds.data_vars.items():
            assert value.data is borrowed[name]
    assert bc.to_dict(data=False) == attrs_bc
    assert fp.to_dict(data=False) == attrs_fp
    xr.testing.assert_identical(bc, before_bc)
    xr.testing.assert_identical(fp, before_fp)


@pytest.mark.parametrize("invalid", ["weight", "total", "concentration"])
def test_lazy_validation_fails_on_materialization(invalid):
    bc, fp = inputs()
    if invalid == "weight":
        fp.particle_locations_n.data[0, 0, 0] = np.nan
    elif invalid == "total":
        fp.particle_locations_n.data[:] *= 2
    else:
        bc.vmr_n.data[0, 0, 0] = np.nan
    result = calculate(bc.chunk({"time": 1}), fp.chunk({"time": 1}))
    with pytest.raises(ValueError, match="finite|mass"):
        result.compute(scheduler="synchronous")


def test_nonuniform_native_operands_match_per_cell_oracle():
    bc, fp = inputs()
    for i, d in enumerate("nesw"):
        w = fp[f"particle_locations_{d}"]
        w.data *= np.arange(w.size).reshape(w.shape) + i + 1
    raw_total = sum(fp[f"particle_locations_{d}"].values.reshape(2, -1).sum(axis=1) for d in "nesw")
    for d in "nesw":
        fp[f"particle_locations_{d}"].data /= raw_total[:, None, None]
    result = calculate(bc, fp)
    for d in "nesw":
        expected_mass = fp[f"particle_locations_{d}"].values
        expected_anomaly = expected_mass * (bc[f"vmr_{d}"].values - 209300)
        np.testing.assert_allclose(result[f"bc_mass_{d}"], expected_mass, rtol=2e-15)
        np.testing.assert_allclose(result[f"bc_anomaly_{d}"], expected_anomaly, rtol=2e-15)


def test_float64_preserves_input_contrast_but_cannot_restore_float32_contrast():
    bc, fp = inputs(constant=True)
    for d in "nesw":
        bc[f"vmr_{d}"].data += 0.001
    accurate = calculate(bc, fp)
    rounded = calculate(bc.astype("float32"), fp)
    assert sum(float(accurate[f"bc_anomaly_{d}"].sum()) for d in "nesw") == pytest.approx(0.002)
    assert all(not rounded[f"bc_anomaly_{d}"].values.any() for d in "nesw")


@pytest.mark.parametrize("reference,units", [(np.nan, "ppm"), (np.inf, "ppm"), (400, "m"), (400, "")])
def test_invalid_reference(reference, units):
    bc, fp = inputs()
    with pytest.raises(ValueError, match="reference|units"):
        baseline_reference_sensitivities(
            bc, fp, species="co2", reference=reference, reference_units=units, mass_tolerance=3e-5
        )


def test_spatial_validation_is_eager_for_lazy_data():
    bc, fp = inputs()
    tasks = []
    with Callback(pretask=lambda key, *_: tasks.append(key)):
        with pytest.raises(ValueError, match="coordinate"):
            calculate(bc.assign_coords(height=[101, 201]).chunk(), fp.chunk())
    assert not tasks


def test_calendar_month_coverage_including_leap_year():
    bc, fp = inputs()
    bc = bc.assign_coords(time=pd.to_datetime(["2024-01-01", "2024-02-01"]))
    bc.attrs["time_period"] = "1 month"
    fp = fp.assign_coords(time=pd.to_datetime(["2024-01-31", "2024-02-29"]))
    calculate(bc, fp)
    with pytest.raises(ValueError, match="coverage"):
        calculate(bc, fp.assign_coords(time=pd.to_datetime(["2024-01-31", "2024-03-01"])))
