"""Numerical ModelScenario integration checks for optional time-resolved maps."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import ModelScenario, _fp_x_flux
from openghg.dataobjects import FluxData, FootprintData


def _model(releases, legacy=False, inherited=False):
    releases = pd.DatetimeIndex(releases)
    coords = {"time": releases, "lat": [50.0, 51.0], "lon": [0.0], "H_back": [0, 3]}
    fp = xr.Dataset(
        {
            "fp_time_resolved": (("time", "lat", "lon", "H_back"), np.full((len(releases), 2, 1, 2), 2.0)),
            "fp_residual": (("time", "lat", "lon"), np.full((len(releases), 2, 1), 0.5)),
        },
        coords=coords,
    )
    for variable in fp.data_vars:
        fp[variable].attrs["units"] = "m2 s umol-1"
    if legacy:
        fp = xr.Dataset(
            {
                "fp_HiTRes": xr.concat(
                    [
                        fp.fp_time_resolved,
                        fp.fp_residual.expand_dims(H_back=[4]),
                    ],
                    dim="H_back",
                )
            }
        )
        fp["fp"] = fp.fp_HiTRes.sum("H_back")
        fp.fp.attrs["units"] = "m2 s umol-1"
        if inherited:
            fp.fp_HiTRes.attrs.pop("units", None)
    times = pd.date_range("2023-12-30 00:15", periods=100, freq="h")
    flux = xr.DataArray(
        (np.arange(100, dtype=float) + 1).reshape(-1, 1, 1) * np.ones((1, 2, 1)) * 1e-6,
        dims=("time", "lat", "lon"),
        coords={"time": times, "lat": fp.lat, "lon": fp.lon},
        attrs={"units": "mol m-2 s-1"},
    )
    model = ModelScenario()
    model.footprint = FootprintData(data=fp, metadata={"species": "co2"})
    model.fluxes = {
        "bio": FluxData(data=flux.to_dataset(name="flux"), metadata={"species": "co2"}),
        "fossil": FluxData(
            data=(flux * 3).assign_attrs(flux.attrs).to_dataset(name="flux"), metadata={"species": "co2"}
        ),
    }
    return model


def _calculate(model, maps, sectors=False):
    return model.calc_modelled_obs(
        resample_to=None,
        output_fp_x_flux=maps,
        split_by_sectors=sectors,
        output_units="ppm",
        recalculate=True,
    ).compute()


@pytest.mark.parametrize(
    "releases",
    [
        ["2024-01-01 00:37", "2024-01-01 01:44", "2024-01-01 03:51"],
        pd.date_range("2024-01-01 00:37", periods=3, freq="3h"),
    ],
)
@pytest.mark.parametrize("legacy,inherited", [(False, False), (True, False), (True, True)])
def test_map_flag_preserves_physics_units_and_sector_totals(releases, legacy, inherited):
    """Maps and sector sums represent the same ppm timeseries for both footprint forms."""
    pytest.importorskip("numba")
    model = _model(releases, legacy=legacy, inherited=inherited)
    expected = _calculate(model, maps=False, sectors=True)
    actual = _calculate(model, maps=True, sectors=True)
    xr.testing.assert_allclose(actual.mf_mod_high_res, expected.mf_mod_high_res)
    xr.testing.assert_allclose(actual.mf_mod_high_res_sectoral, expected.mf_mod_high_res_sectoral)
    xr.testing.assert_allclose(actual.fp_x_flux.sum(["lat", "lon"]), actual.mf_mod_high_res)
    xr.testing.assert_allclose(actual.fp_x_flux_sectoral.sum(["lat", "lon"]), actual.mf_mod_high_res_sectoral)
    xr.testing.assert_allclose(actual.fp_x_flux_sectoral.sum("source"), actual.fp_x_flux)
    assert actual.source.values.tolist() == ["bio", "fossil"]
    assert actual.fp_x_flux.dtype == np.float64
    assert actual.fp_x_flux.attrs["operator"] == _fp_x_flux.FP_X_FLUX_OPERATOR
    for variable in actual.data_vars:
        assert actual[variable].attrs["units"] == "1e-06"
    if inherited:
        assert "units" not in model.footprint.data.fp_HiTRes.attrs


def test_map_has_hand_calculated_ppm_values():
    """At 00:37 use native values 49 and 46, plus January's supplied mean 74.5."""
    model = _model(["2024-01-01 00:37"])
    actual = _calculate(model, maps=True)
    # Two cells, two sources summing to four times the bio flux; umol footprint cancels 1e-6 flux.
    expected_map_ppm = (2 * 49 + 2 * 46 + 0.5 * 74.5) * 4 * 1e6
    np.testing.assert_allclose(actual.fp_x_flux, expected_map_ppm)
    np.testing.assert_allclose(actual.mf_mod_high_res, 2 * expected_map_ppm)


def test_map_falls_back_when_numba_is_unavailable(monkeypatch):
    """Disable the actual optional dependency marker and still calculate maps."""
    model = _model(["2024-01-01 00:37", "2024-01-01 03:37"])
    expected = _calculate(model, maps=False)
    monkeypatch.setattr(_fp_x_flux, "njit", None)
    actual = _calculate(model, maps=True)
    assert "operator" not in actual.fp_x_flux.attrs
    xr.testing.assert_allclose(actual.mf_mod_high_res, expected.mf_mod_high_res)
    xr.testing.assert_allclose(actual.fp_x_flux.sum(["lat", "lon"]), expected.mf_mod_high_res)


@pytest.mark.parametrize("maps", [False, True])
def test_internal_averaging_applies_before_dispatch(maps):
    """Average release labels identically before xarray or Numba receives footprints."""
    model = _model(pd.date_range("2024-01-01 00:37", periods=4, freq="3h"))
    model.scenario = model.footprint.data
    actual = model._calc_modelled_obs_HiTRes(output_fpXflux=maps, averaging="2h")
    model.scenario = model.scenario.resample(time="2h").ffill()
    expected = model._calc_modelled_obs_HiTRes(output_fpXflux=maps)
    xr.testing.assert_allclose(actual.compute(), expected.compute())


def test_numba_unit_errors_do_not_fall_back():
    """An installed Numba path rejects incompatible declared units."""
    pytest.importorskip("numba")
    model = _model(["2024-01-01 00:37"])
    model.footprint.data.fp_time_resolved.attrs["units"] = "kg"
    with pytest.raises(ValueError, match="units"):
        _calculate(model, maps=True)


def test_unrelated_import_errors_do_not_fall_back(monkeypatch):
    """Only optional dependency absence selects the reference implementation."""
    pytest.importorskip("numba")
    model = _model(["2024-01-01 00:37"])

    def broken_operator(*args, **kwargs):
        raise ImportError("unrelated dependency")

    monkeypatch.setattr(_fp_x_flux, "fp_x_flux_time_resolved_numba", broken_operator)
    with pytest.raises(ImportError, match="unrelated dependency"):
        _calculate(model, maps=True)


@pytest.mark.parametrize("hours", [[9, 3], [9, 3, 9]])
def test_optional_fallback_preserves_release_order_and_duplicates(monkeypatch, hours):
    """Missing Numba retains the exact nonmonotonic release labels and values."""
    releases = pd.DatetimeIndex(
        [pd.Timestamp("2024-01-01") + pd.Timedelta(hours=hour, minutes=30) for hour in hours]
    )
    model = _model(releases.sort_values().unique())
    # FootprintData sorts on construction; supply the operator its requested release order.
    model.footprint.data = model.footprint.data.sel(time=releases)
    expected = _calculate(_model(releases.sort_values().unique()), maps=True)
    monkeypatch.setattr(_fp_x_flux, "njit", None)
    actual = _calculate(model, maps=True)
    np.testing.assert_array_equal(actual.time.values, releases.values)
    xr.testing.assert_allclose(actual.transpose("lat", "lon", "time"), expected.sel(time=releases))
    # For each cell: two lag weights of 2, plus residual 0.5 times January's mean 74.5.
    values_ppm = [(2 * (49 + hour) + 2 * (46 + hour) + 0.5 * 74.5) * 4 * 1e6 for hour in hours]
    np.testing.assert_allclose(
        actual.fp_x_flux.transpose("lat", "lon", "time").values,
        np.broadcast_to(values_ppm, (2, 1, len(hours))),
    )


def test_missing_footprint_units_scaled_flux_matches_optional_fallback(monkeypatch):
    """Canonical footprint defaults give the same ppm maps with and without Numba."""
    model = _model(["2024-01-01 00:37"])
    for variable in model.footprint.data.data_vars:
        del model.footprint.data[variable].attrs["units"]
    for flux in model.fluxes.values():
        flux.data.flux.attrs["units"] = "umol m-2 s-1"
    expected = _calculate(model, maps=True)
    monkeypatch.setattr(_fp_x_flux, "njit", None)
    actual = _calculate(model, maps=True)
    xr.testing.assert_allclose(actual.transpose("lat", "lon", "time"), expected)
    np.testing.assert_allclose(actual.fp_x_flux.values, 909e-6)
    assert all("units" not in model.footprint.data[name].attrs for name in model.footprint.data.data_vars)
