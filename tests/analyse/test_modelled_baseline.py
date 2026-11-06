"""Independent oracles for native-release boundary contributions."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import _modelled_baseline, baseline_sensitivities


def _inputs(
    release_times=("2021-01-02 06:30",),
    source_times=("2021-01-02 00:00", "2021-01-02 03:00", "2021-01-02 06:00"),
    ages=(0.0, 0.0),
    weights=(0.25, 0.75),
    concentrations=None,
    age_units="hours",
):
    """Make two horizontal cells in each curtain, with native NAME dimension order."""
    releases = pd.to_datetime(list(release_times))
    sources = pd.to_datetime(list(source_times))
    ages = np.broadcast_to(ages, (len(releases), 2)).copy()
    weights = np.broadcast_to(weights, (len(releases), 2)).copy()
    if concentrations is None:
        concentrations = np.arange(len(sources))[:, None] * 20.0 + [10.0, 20.0]
    concentrations = np.broadcast_to(concentrations, (len(sources), 2)).copy()
    coords = {"height": [500.0], "lat": [1.0, 2.0], "lon": [10.0, 11.0]}
    fp = xr.Dataset(coords={**coords, "time": releases})
    bc = xr.Dataset(coords={**coords, "time": sources}, attrs={"time_period": "3h"})
    for direction in "nesw":
        horizontal = "lon" if direction in "ns" else "lat"
        dims = ("time", horizontal, "height")
        fp[f"particle_locations_{direction}"] = xr.DataArray(
            weights[..., None], dims=dims, attrs={"units": "1"}
        ).transpose("height", horizontal, "time")
        fp[f"mean_age_particles_{direction}"] = xr.DataArray(
            ages[..., None], dims=dims, attrs={"units": age_units}
        ).transpose("height", horizontal, "time")
        bc[f"vmr_{direction}"] = xr.DataArray(
            concentrations[..., None], dims=dims, attrs={"units": "ppm"}
        ).transpose("height", horizontal, "time")
    for dataset in (fp, bc):
        dataset.lat.attrs["units"] = "degrees_north"
        dataset.lon.attrs["units"] = "degrees_east"
        dataset.height.attrs["units"] = "m"
    return bc, fp


def _assert_curtains(result, expected, fp):
    assert set(result.data_vars) == {f"bc_{direction}" for direction in "nesw"}
    xr.testing.assert_identical(result.time, fp.time)
    for direction in "nesw":
        horizontal = "lon" if direction in "ns" else "lat"
        contribution = result[f"bc_{direction}"]
        assert contribution.dims == fp[f"particle_locations_{direction}"].dims
        assert contribution.dtype == np.float32
        in_ppm = contribution.pint.quantify().pint.to("ppm").pint.dequantify()
        np.testing.assert_allclose(
            in_ppm.transpose("time", horizontal, "height").values[..., 0], expected, rtol=1e-6
        )


def test_default_release_time_remains_available_without_ages():
    bc, fp = _inputs()
    fp = fp.drop_vars([f"mean_age_particles_{direction}" for direction in "nesw"])

    result = baseline_sensitivities(bc, fp)

    _assert_curtains(result, [[12.5, 45.0]], fp)
    xr.testing.assert_identical(result, baseline_sensitivities(bc, fp, bc_time_method="release_time"))


@pytest.mark.parametrize("ages", [(0.0, 0.0), (1.25, 4.5)])
def test_constant_boundary_field_is_unchanged_by_lag(ages):
    bc, fp = _inputs(ages=ages, concentrations=[7.0, 7.0])

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[1.75, 5.25]], fp)


def test_fractional_ages_select_a_different_native_interval_for_each_cell():
    bc, fp = _inputs(
        release_times=("2021-01-02 06:30", "2021-01-02 07:30"),
        ages=[[0.0, 0.75], [1.25, 4.5]],
        weights=[[0.25, 0.75], [0.4, 0.6]],
    )

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[12.5, 30.0], [20.0, 24.0]], fp)


def test_linear_field_at_exact_samples_matches_weighted_mean_age_oracle():
    # C(t) = 10 + 2*t hours; both requested crossing times are exact source samples.
    bc, fp = _inputs(
        release_times=("2021-01-02 06:00",),
        source_times=pd.date_range("2021-01-02", periods=7, freq="h"),
        ages=(1.0, 3.0),
        concentrations=np.repeat((10.0 + 2.0 * np.arange(7))[:, None], 2, axis=1),
    )

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period="1h")

    _assert_curtains(result, [[5.0, 12.0]], fp)
    assert float(result.bc_n.sum()) == pytest.approx(17.0)


@pytest.mark.parametrize(
    ("source_times", "release_time", "period"),
    [
        (("2021-01-02 00:00", "2021-01-02 03:00"), "2021-01-02 03:15", "3h"),
        (("2021-01-01", "2021-01-02"), "2021-01-02 00:15", "daily"),
        (("2020-12-31 21:00", "2021-01-01 00:00"), "2021-01-01 00:15", "3h"),
        (("2021-01-01", "2021-02-01"), "2021-02-01 00:15", "monthly"),
    ],
)
def test_native_intervals_at_off_hour_releases_and_calendar_seams(source_times, release_time, period):
    bc, fp = _inputs(release_times=(release_time,), source_times=source_times, ages=(0.0, 0.5))

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period=period)

    _assert_curtains(result, [[7.5, 15.0]], fp)


def test_exact_interval_start_is_included_without_rounding_nearby_age():
    bc, fp = _inputs(release_times=("2021-01-02 03:00",), ages=(0.0, 0.0001))

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[7.5, 15.0]], fp)


@pytest.mark.parametrize(
    ("source_times", "release_time", "ages"),
    [
        (("2021-01-02 00:00",), "2021-01-02 00:15", (1.0, 1.0)),
        (("2021-01-02 00:00", "2021-01-02 06:00"), "2021-01-02 04:00", (0.0, 0.0)),
        (("2021-01-02 00:00", "2021-01-02 06:00"), "2021-01-02 09:00", (0.0, 0.0)),
    ],
)
def test_insufficient_history_internal_gaps_and_expired_last_interval_raise(source_times, release_time, ages):
    bc, fp = _inputs(release_times=(release_time,), source_times=source_times, ages=ages)

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, allow_climatology=False, bc_time_method="mean_age").compute()


@pytest.mark.parametrize("source_times", [("2021-01-02 06:00",), ("2021-01-02 00:00", "2021-01-02 06:00")])
def test_undeclared_interval_support_is_rejected(source_times):
    bc, fp = _inputs(source_times=source_times)
    bc.attrs.clear()

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


@pytest.mark.parametrize("age", [0.0, 2.0])
def test_cf_bounds_take_precedence_over_period_and_preserve_gaps(age):
    bc, fp = _inputs(source_times=("2021-01-02 00:00", "2021-01-02 06:00"), ages=(age, age))
    bc["time_bounds"] = (
        ("time", "bounds"),
        pd.to_datetime(
            ["2021-01-02 00:00", "2021-01-02 03:00", "2021-01-02 06:00", "2021-01-02 09:00"]
        ).values.reshape(2, 2),
    )
    bc.time.attrs["bounds"] = "time_bounds"

    if age:
        with pytest.raises(ValueError):
            baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period="12h").compute()
    else:
        result = baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period="12h")
        _assert_curtains(result, [[7.5, 30.0]], fp)


def test_unsorted_source_storage_keeps_concentrations_attached_to_their_timestamps():
    bc, fp = _inputs(ages=(0.0, 0.75))

    result = baseline_sensitivities(bc.isel(time=[2, 0, 1]), fp, bc_time_method="mean_age")

    _assert_curtains(result, [[12.5, 30.0]], fp)


def test_climatology_maps_lagged_cell_times_across_the_reference_year_seam():
    bc, fp = _inputs(
        release_times=("2022-01-01 00:15",),
        source_times=("2016-01-01 00:00", "2016-12-31 21:00"),
        ages=(0.0, 0.5),
    )

    result = baseline_sensitivities(bc, fp, allow_climatology=True, bc_time_method="mean_age")

    _assert_curtains(result, [[2.5, 30.0]], fp)
    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, allow_climatology=False, bc_time_method="mean_age").compute()


def test_climatology_retains_existing_february_29_clamp():
    bc, fp = _inputs(release_times=("2020-02-29 12:00",), source_times=("2019-02-28",))

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period="daily")

    _assert_curtains(result, [[2.5, 15.0]], fp)


@pytest.mark.parametrize("age", [np.nan, np.inf, -0.25])
@pytest.mark.parametrize("chunked", [False, True])
def test_positive_weight_cells_require_finite_nonnegative_ages(age, chunked):
    bc, fp = _inputs(ages=(age, 0.0))
    if chunked:
        bc, fp = bc.chunk({"time": 1}), fp.chunk({"time": 1})

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


@pytest.mark.parametrize("units", [None, "m", "unknown_age_unit"])
def test_mean_age_requires_declared_duration_units(units):
    bc, fp = _inputs(age_units=units)

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


def test_mean_age_requires_ages_in_active_curtains():
    bc, fp = _inputs()
    fp = fp.drop_vars("mean_age_particles_n")

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


@pytest.mark.parametrize("weight", [np.nan, np.inf, -0.25])
def test_mean_age_rejects_nonfinite_or_negative_weights(weight):
    bc, fp = _inputs(weights=(weight, 0.75))

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


def test_unannotated_weights_are_dimensionless():
    bc, fp = _inputs(ages=(0.0, 0.75))
    for direction in "nesw":
        fp[f"particle_locations_{direction}"].attrs.clear()

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[12.5, 30.0]], fp)


def test_age_subtraction_cannot_wrap_into_future_boundary_history():
    # The true crossing is in 1500; unchecked nanosecond subtraction wraps into 2084.
    bc, fp = _inputs(
        release_times=("1700-01-01",),
        source_times=("2084-07-19 23:34:33.709551616",),
        ages=(200 * 365.25 * 24, 200 * 365.25 * 24),
    )

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, allow_climatology=False, bc_time_method="mean_age").compute()


def test_zero_weight_cells_ignore_invalid_ages_and_concentrations_but_active_zero_ages_remain():
    bc, fp = _inputs(ages=(0.0, np.nan), weights=(1.0, 0.0), concentrations=[50.0, np.nan])

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[50.0, 0.0]], fp)


@pytest.mark.parametrize("age_metadata", ["missing", "unitless", "incompatible"])
def test_inactive_curtain_needs_no_usable_age_array_or_concentration(age_metadata):
    bc, fp = _inputs()
    fp["particle_locations_n"] = xr.zeros_like(fp.particle_locations_n)
    bc["vmr_n"] = xr.full_like(bc.vmr_n, np.nan)
    if age_metadata == "missing":
        fp = fp.drop_vars("mean_age_particles_n")
    else:
        fp["mean_age_particles_n"] = xr.full_like(fp.mean_age_particles_n, np.nan)
        fp.mean_age_particles_n.attrs["units"] = None if age_metadata == "unitless" else "m"

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    np.testing.assert_array_equal(result.bc_n.values, 0.0)
    np.testing.assert_allclose(result.bc_e.values.ravel(), [12.5, 45.0])


@pytest.mark.parametrize("value", [np.nan, np.inf])
def test_positive_weight_cells_require_finite_selected_concentrations(value):
    bc, fp = _inputs(concentrations=[value, 50.0])

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age").compute()


def test_convertible_age_units_preserve_fractional_crossing_times():
    bc, hours = _inputs(ages=(0.0, 0.75), age_units="hrs")
    _, seconds = _inputs(ages=(0.0, 2700.0), age_units="seconds")

    in_hours = baseline_sensitivities(bc, hours, bc_time_method="mean_age")
    in_seconds = baseline_sensitivities(bc, seconds, bc_time_method="mean_age")

    xr.testing.assert_identical(in_hours, in_seconds)
    _assert_curtains(in_seconds, [[12.5, 30.0]], seconds)


@pytest.mark.parametrize("units", ["ppb", "1e-9"])
def test_concentration_unit_scale_is_preserved(units):
    bc, fp = _inputs(concentrations=[1000.0, 1000.0])
    for direction in "nesw":
        bc[f"vmr_{direction}"].attrs["units"] = units

    result = baseline_sensitivities(bc, fp, bc_time_method="mean_age")

    _assert_curtains(result, [[0.25, 0.75]], fp)


@pytest.mark.parametrize("monthly", [False, True])
@pytest.mark.parametrize("method", ["release_time", "mean_age"])
def test_lifetime_loss_uses_age_once_and_receptor_month_with_native_dimension_order(
    monkeypatch, monthly, method
):
    bc, fp = _inputs(
        release_times=("2021-01-31 23:30", "2021-02-01 00:30"),
        source_times=("2021-01-31 18:00", "2021-01-31 21:00", "2021-02-01 00:00"),
        ages=(1.0, 2.0),
        concentrations=[100.0, 100.0],
    )
    lifetime = ["1D", "2D"] + ["1D"] * 10 if monthly else "1D"
    monkeypatch.setattr(_modelled_baseline, "species_lifetime", lambda species: lifetime)
    expected = np.array([25.0, 75.0]) * np.exp(-np.array([1.0, 2.0]) / 24.0)
    expected = np.repeat(expected[None, :], 2, axis=0)
    if monthly:
        expected[1] = np.array([25.0, 75.0]) * np.exp(-np.array([1.0, 2.0]) / 48.0)

    result = baseline_sensitivities(bc, fp, species="test", bc_time_method=method)

    _assert_curtains(result, expected, fp)


@pytest.mark.parametrize("method", ["release_time", "mean_age"])
def test_helper_preserves_inputs_including_missing_coordinate_unit_attributes(method):
    bc, fp = _inputs(ages=(0.0, 0.75))
    bc.lon.attrs.clear()
    bc.height.attrs.clear()
    original_bc, original_fp = bc.copy(deep=True), fp.copy(deep=True)

    baseline_sensitivities(bc, fp, bc_time_method=method).compute()

    xr.testing.assert_identical(bc, original_bc)
    xr.testing.assert_identical(fp, original_fp)


@pytest.mark.parametrize("mismatched_chunks", [False, True])
def test_chunked_lookup_remains_lazy_and_agrees_with_eager_oracle(mismatched_chunks):
    import dask.array as da

    bc, fp = _inputs(
        release_times=("2021-01-02 06:30", "2021-01-02 07:30"),
        ages=[[0.0, 0.75], [1.25, 4.5]],
        weights=[[0.25, 0.75], [0.4, 0.6]],
    )
    eager = baseline_sensitivities(bc, fp, bc_time_method="mean_age")
    if mismatched_chunks:
        chunked_bc = bc.chunk({"time": 2, "lat": 2, "lon": 1})
        chunked_fp = fp.chunk({"time": 2, "lat": 1, "lon": 2})
        for direction in "nesw":
            horizontal = "lon" if direction in "ns" else "lat"
            chunked_fp[f"mean_age_particles_{direction}"] = fp[f"mean_age_particles_{direction}"].chunk(
                {"time": 1, horizontal: 1 if horizontal == "lon" else 2}
            )
    else:
        chunked_bc = bc.chunk({"time": 1, "lat": 1, "lon": 1})
        chunked_fp = fp.chunk({"time": 1, "lat": 1, "lon": 1})

    lazy = baseline_sensitivities(
        chunked_bc,
        chunked_fp,
        bc_time_method="mean_age",
    )

    for contribution in lazy.data_vars.values():
        assert isinstance(contribution.data, da.Array)
        assert contribution.ndim == 3
        assert len(contribution.data.__dask_graph__()) < 5000
    xr.testing.assert_identical(lazy.compute(), eager)
    _assert_curtains(lazy, [[12.5, 30.0], [20.0, 24.0]], fp)


@pytest.mark.parametrize("inactive_north", [False, True])
def test_chunked_lookup_reads_only_source_history_needed_by_active_cells(inactive_north):
    import dask.array as da
    from dask import delayed

    bc, fp = _inputs(
        release_times=("2021-01-02 12:30",),
        source_times=pd.date_range("2021-01-02", periods=9, freq="3h"),
        ages=(0.25, np.nan),
        weights=(1.0, 0.0),
    )
    concentrations = bc.vmr_n.values.copy()
    reads = []

    @delayed
    def load_source_chunk(chunk):
        if chunk != 1:
            raise AssertionError("Unrelated boundary history was read")
        reads.append(chunk)
        return concentrations[..., chunk * 3 : (chunk + 1) * 3]

    source = da.concatenate(
        [da.from_delayed(load_source_chunk(chunk), shape=(1, 2, 3), dtype=float) for chunk in range(3)],
        axis=2,
    )
    for direction in "nesw":
        bc[f"vmr_{direction}"].data = source
    if inactive_north:
        fp["particle_locations_n"] = xr.zeros_like(fp.particle_locations_n)

        @delayed
        def load_inactive_curtain():
            raise AssertionError("An inactive curtain's concentrations were read")

        bc["vmr_n"].data = da.from_delayed(load_inactive_curtain(), shape=(1, 2, 9), dtype=float)

    result = baseline_sensitivities(bc, fp.chunk({"time": 1}), bc_time_method="mean_age")

    assert reads == []
    result = result.compute(scheduler="synchronous")
    assert set(reads) == {1}
    if inactive_north:
        np.testing.assert_array_equal(result.bc_n.values, 0.0)
        np.testing.assert_allclose(result.bc_e.values.ravel(), [90.0, 0.0])
    else:
        _assert_curtains(result, [[90.0, 0.0]], fp)


@pytest.mark.parametrize("period", ["garbage", "0h", "-1h"])
def test_invalid_interval_period_raises_value_error(period):
    bc, fp = _inputs()

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="mean_age", bc_time_period=period).compute()


def test_unknown_time_method_is_rejected():
    bc, fp = _inputs()

    with pytest.raises(ValueError):
        baseline_sensitivities(bc, fp, bc_time_method="unsupported")
