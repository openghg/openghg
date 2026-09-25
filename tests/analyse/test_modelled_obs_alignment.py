import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse import _modelled_obs


def _irregular_time_resolved_fp_and_flux():
    lat = [1.0]
    lon = [10.0]
    h_back = np.array([0, 1])
    fp_times = pd.to_datetime(
        [
            "2012-01-01 00:37:00",
            "2012-01-01 01:44:00",
            "2012-01-01 02:51:00",
            "2012-01-01 03:58:00",
        ]
    )
    fp = xr.Dataset(
        {
            "fp_time_resolved": (("time", "lat", "lon", "H_back"), np.ones((4, 1, 1, 2))),
            "fp_residual": (("time", "lat", "lon"), np.zeros((4, 1, 1))),
        },
        coords={"time": fp_times, "lat": lat, "lon": lon, "H_back": h_back},
    )
    fp.fp_time_resolved.attrs["units"] = "m2 s mol-1"
    fp.fp_residual.attrs["units"] = "m2 s mol-1"
    fp.lat.attrs["units"] = "degrees_north"
    fp.lon.attrs["units"] = "degrees_east"
    fp.H_back.attrs["units"] = "Hours"

    flux_times = pd.date_range("2011-12-31 20:00:00", "2012-01-01 05:00:00", freq="h")
    flux_values = np.arange(len(flux_times), dtype=float).reshape(-1, 1, 1)
    flux = xr.DataArray(
        flux_values,
        coords={"time": flux_times, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
        attrs={"units": "mol m-2 s-1"},
    )
    flux.lat.attrs["units"] = "degrees_north"
    flux.lon.attrs["units"] = "degrees_east"

    expected = xr.DataArray(
        np.array([7.0, 9.0, 11.0, 13.0]).reshape(4, 1, 1),
        coords={"time": fp.time, "lat": fp.lat, "lon": fp.lon},
        dims=("time", "lat", "lon"),
    )

    return fp, flux, expected


def test_fp_x_flux_time_resolved_uses_flux_intervals_for_irregular_footprint_times():
    """Irregular footprint times should use the containing left-labelled flux intervals."""
    fp, flux, expected = _irregular_time_resolved_fp_and_flux()

    result = _modelled_obs.fp_x_flux_time_resolved(fp, flux)

    xr.testing.assert_identical(result.time, fp.time)
    xr.testing.assert_allclose(result, expected)
