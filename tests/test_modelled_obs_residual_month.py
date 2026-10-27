"""Monthly residual flux must retain the month containing the history start."""

import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse._modelled_obs import _make_low_freq_flux


def test_midmonth_history_retains_monthly_residual_mean():
    time = pd.date_range("2024-01-01", "2024-03-01", freq="h", inclusive="left")
    flux = xr.DataArray(
        np.where(time.month == 1, 2.0, 6.0)[None, None, :],
        dims=("lat", "lon", "time"),
        coords={"lat": [50.0], "lon": [0.0], "time": time},
        attrs={"units": "mol m-2 s-1"},
    ).chunk({"time": 48})
    fp = xr.Dataset(
        coords={
            "lat": [50.0],
            "lon": [0.0],
            "H_back": [0, 1, 2],
            "time": pd.to_datetime(["2024-01-15T12:00", "2024-02-01T12:00"]),
        }
    )
    result = _make_low_freq_flux(flux, fp)
    assert hasattr(result.data, "__dask_graph__")
    np.testing.assert_allclose(result.compute(), [[[2.0, 6.0]]])
