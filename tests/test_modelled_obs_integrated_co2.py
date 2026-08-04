"""Tests for integrated CO2 modelled-observation behaviour."""

import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse import ModelScenario, make_integrated_low_freq_flux
from openghg.analyse._modelled_obs import fp_x_flux_integrated
from openghg.dataobjects import FluxData, FootprintData, ObsData


def test_modelled_obs_integrated_co2_uses_integrated_pipeline():
    """Integrated CO2 footprints use monthly-mean fluxes, not the HiTRes path."""
    obs = ObsData(
        data=xr.Dataset(
            {"mf": ("time", np.ones(2))},
            coords={"time": pd.to_datetime(["2012-01-01 00:00", "2012-01-01 01:00"])},
            attrs={"species": "co2", "site": "TEST_SITE", "inlet": "10m"},
        ),
        metadata={"species": "co2", "site": "TEST_SITE", "inlet": "10m"},
    )

    footprint = FootprintData(
        data=xr.Dataset(
            {"fp": (("time", "lat", "lon"), np.ones((2, 1, 1)))},
            coords={
                "time": pd.to_datetime(["2012-01-01 00:00", "2012-01-01 01:00"]),
                "lat": [1.0],
                "lon": [10.0],
            },
        ),
        metadata={"species": "co2", "site": "TEST_SITE", "inlet": "10m", "domain": "TESTDOMAIN"},
    )
    footprint.data.fp.attrs["units"] = "m2 s mol-1"
    footprint.data.lat.attrs["units"] = "degrees_north"
    footprint.data.lon.attrs["units"] = "degrees_east"

    flux = FluxData(
        data=xr.Dataset(
            {"flux": (("time", "lat", "lon"), np.array([[[1.0]], [[3.0]], [[5.0]]]))},
            coords={
                "time": pd.to_datetime(["2011-12-31 23:00", "2012-01-01 00:00", "2012-01-01 02:00"]),
                "lat": [1.0],
                "lon": [10.0],
            },
        ),
        metadata={"species": "co2", "source": "TESTSOURCE", "domain": "TESTDOMAIN"},
    )
    flux.data.flux.attrs["units"] = "mol m-2 s-1"
    flux.data.lat.attrs["units"] = "degrees_north"
    flux.data.lon.attrs["units"] = "degrees_east"

    scenario = ModelScenario(obs=obs, footprint=footprint, flux=flux)
    combined = scenario.footprints_data_merge()

    assert "mf_mod" in combined
    assert "mf_mod_high_res" not in combined

    flux_combined = scenario.combine_flux_sources()
    flux_monthly = make_integrated_low_freq_flux(flux_combined)
    expected_flux = flux_combined.flux.resample({"time": "1MS"}).mean().to_dataset(name="flux")
    xr.testing.assert_allclose(flux_monthly, expected_flux)

    expected = (
        fp_x_flux_integrated(combined, flux_monthly)
        .pint.quantify()
        .sum(["lat", "lon"])
        .pint.dequantify()
    )
    xr.testing.assert_allclose(combined.mf_mod, expected)
