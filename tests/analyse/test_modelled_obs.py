"""Test modelled obs calculations.

The focus is on CO2, because the calculations for time-resolved footprints
are more complicated.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import ModelScenario
from openghg.analyse._modelled_obs import time_resolved_and_residual_footprints, _max_h_back
from openghg.dataobjects import ObsData
from openghg.dataobjects._footprint_data import FootprintData


@pytest.fixture
def obs_co2_dummy():
    """
    Create example ObsData object with dummy data
     - Species is carbon dioxide (co2)
     - 30-min frequency for 2012-01-01 (48 time points)
     - "mf" values are from 1, 48
    """

    time = pd.date_range("2012-01-01T00:00:00", "2012-01-01T23:30:00", freq="30min")

    ntime = len(time)
    values = np.arange(0, ntime, 1)

    species = "co2"
    site = "TEST_SITE"
    inlet = "10m"
    sampling_period = "60.0"

    attributes = {"species": species, "site": site, "inlet": inlet, "sampling_period": sampling_period}

    data = xr.Dataset({"mf": ("time", values)}, coords={"time": time}, attrs=attributes)

    # Potential metadata:
    # - site, instrument, sampling_period, inlet, port, type, network, species, calibration_scale
    #   long_name, data_owner, data_owner_email, station_longitude, station_latitude, ...
    # - data_type
    metadata = attributes
    metadata["object_store"] = "/tmp/test-store-123"

    obsdata = ObsData(data=data, metadata=metadata)

    return obsdata


@pytest.fixture
def footprint_co2_dummy():
    """
    Create example FootprintData object with dummy data:
     - Carbon dioxide high time resolution footprint
     - Includes two sets of footprint data
     - Integrated footprint, fp
        - Daily frequency from 2011-12-31T23:00:00 to 2012-01-01T02:00:00 (inclusive) (4 points)
        - "fp" values are all 1 **May change**
     - High time resolution fp, fp_HiTRes
        - Same frequency as fp
        - Hourly back footprints (H_back) for 3 hours and residual integrated footprint (4 points)
        - "fp_HiTRes" values are different along time points and H_back (0.0, 1.0, ...)
     - Small lat, lon (TEST_DOMAIN)
    """
    from openghg.dataobjects import FootprintData

    time = pd.date_range("2011-12-31T23:00:00", "2012-01-01T02:00:00", freq="h")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]
    H_back = np.arange(0, 25, 1, dtype=int)

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    fp_values = np.ones(shape)

    # Create array with different (but predictable) values for fp_HiTRes
    # Set distinct values along H_back dimension
    # Based on range of 0.0-24.0 (inclusive) in increments of 1.0
    nh_back = len(H_back)
    h_back_values = np.arange(0.0, nh_back, 1.0)
    fp_HiTRes_values = np.expand_dims(h_back_values, axis=(0, 1, 2))

    # Repeat (initially) along other dimensions (time, lat, lon)
    expand_axes = (0, 1, 2)
    for i in expand_axes:
        fp_HiTRes_values = np.repeat(fp_HiTRes_values, shape[i], axis=i)

    # Build on this to set distinct values along time dimension as well
    # Add to each array - 0.0-24.0, 1.0-25.0, 2.0-26.0, 3.0-27.0, ... (all inclusive)
    add = 0
    for i in range(ntime):
        fp_HiTRes_values[i, ...] += add
        add += 1

    data = xr.Dataset(
        {
            "fp": (("time", "lat", "lon"), fp_values),
            "fp_HiTRes": (("time", "lat", "lon", "H_back"), fp_HiTRes_values),
        },
        coords={"lat": lat, "lon": lon, "time": time, "H_back": H_back},
    )

    # Potential metadata:
    # - site, inlet, domain, model, network, start_date, end_date, heights, ...
    # - species (if applicable)
    # - data_type="footprints"
    species = "co2"
    metadata = {
        "site": "TESTSITE",
        "inlet": "10m",
        "domain": "TESTDOMAIN",
        "data_type": "footprints",
        "species": species,
    }

    footprintdata = FootprintData(data=data, metadata=metadata)

    return footprintdata


@pytest.fixture
def flux_co2_dummy():
    """
    Create example FluxData object with dummy data
     - Species is carbon dioxide (co2)
     - Data is 2-hourly from 2011-12-31 - 2012-01-02 (inclusive)
     - Small lat, lon (TEST_DOMAIN)
     - "flux" values are in a range from 1, ntime+1, different along the time axis.
    """
    from openghg.dataobjects import FluxData

    time = pd.date_range("2011-12-31", "2012-01-02", freq="2h")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.arange(1.0, ntime + 1, 1.0)

    expand_axes = (1, 2)
    values = np.expand_dims(values, axis=expand_axes)
    for j in expand_axes:
        values = np.repeat(values, shape[j], axis=j)

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), values)}, coords={"lat": lat, "lon": lon, "time": time}
    )

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "co2"
    metadata = {"species": species, "source": "TESTSOURCE", "domain": "TESTDOMAIN"}

    fluxdata = FluxData(data=flux, metadata=metadata)

    return fluxdata


@pytest.fixture
def model_scenario_co2_dummy(obs_co2_dummy, footprint_co2_dummy, flux_co2_dummy):
    """Create ModelScenario with input dummy data for co2"""
    model_scenario = ModelScenario(obs=obs_co2_dummy, footprint=footprint_co2_dummy, flux=flux_co2_dummy)

    return model_scenario


def expected_modelled_mf_at_time(release_time, aligned_time0, flux, footprint, max_hours_back):
    """Calculate expected modelled mf at a given release time.

    Args:
        release_time: release time...
        aligned_time0: initial value of `aligned_time` array
        flux: flux dataset
        footprint: footprint dataset with `fp_time_resolved` and `fp_residual` data variables
        max_hours_back: max value of H_back
    """
    back_time = release_time - pd.Timedelta(max_hours_back, "hours")
    flux_hitres = flux["flux"].sel(time=slice(back_time, release_time))

    flux_integrated = (
        flux["flux"].resample({"time": "1MS"}).mean().sel(time=aligned_time0)
    )  # This may need to be updated

    # Update footprint data to use number of hours back to derive a time
    footprint_at_release = footprint.sel(time=release_time)
    h_back_time = np.array(
        [release_time - pd.Timedelta(hour, "hours") for hour in footprint["H_back"].values],
        dtype=np.datetime64,
    )
    footprint_at_release = footprint_at_release.assign_coords({"H_back_time": ("H_back", h_back_time)})
    footprint_at_release = footprint_at_release.swap_dims({"H_back": "H_back_time"})
    footprint_at_release = footprint_at_release.rename(
        {"time": "release_time"}
    )  # Rename original time parameter
    footprint_at_release = footprint_at_release.rename(
        {"H_back_time": "time"}
    )  # Set new time parameter based on H_back

    # Extract footprint data covering correct time period
    footprint_hitres = footprint_at_release.fp_time_resolved
    footprint_integrated = footprint_at_release.fp_residual

    # Align flux to footprint data (flux is 2 hourly, footprint H_back is 1 hourly)
    flux_hitres = flux_hitres.reindex_like(footprint_hitres, method="ffill")
    # Calculate high time resolution and residual components of modelled mole fraction
    modelled_mf_htres = (flux_hitres * footprint_hitres).sum().values
    modelled_mf_residual = (flux_integrated * footprint_integrated).sum().values

    # Combine to create expected modelled mole fraction
    expected_modelled_mf_hr = modelled_mf_htres + modelled_mf_residual

    return expected_modelled_mf_hr


def test_model_modelled_obs_co2(model_scenario_co2_dummy, footprint_co2_dummy, flux_co2_dummy):
    """Test expected modelled observations within footprints_dat_merge() method with known dummy data for co2"""
    combined_dataset = model_scenario_co2_dummy.footprints_data_merge()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == pd.Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == pd.Timestamp("2012-01-01T02:00:00")

    # Create expected value(s) for modelled mole fraction, "mf_mod_high_res"
    footprint = footprint_co2_dummy.data
    flux = flux_co2_dummy.data

    # Find maximum number of hours of the back run from footprint data
    max_hours_back = footprint["H_back"].values.max()
    footprint = time_resolved_and_residual_footprints(footprint.fp_HiTRes)

    # Loop over each time point so we can calculate expected value and compare
    for t in range(len(aligned_time)):
        print(t)
        # Extract flux data to match H_back and residual time period
        release_time = aligned_time[t].values
        expected_modelled_mf_hr = expected_modelled_mf_at_time(release_time, aligned_time[0], flux, footprint, max_hours_back)
        modelled_mf_hr = combined_dataset["mf_mod_high_res"].sel(time=release_time).values

        assert np.isclose(modelled_mf_hr, expected_modelled_mf_hr)


# Test with new PARIS-style CO2 footprint data
@pytest.fixture
def footprint_paris_co2_dummy(footprint_co2_dummy):
    fp = FootprintData(data=footprint_co2_dummy.data.copy(), metadata=footprint_co2_dummy.metadata)
    fp.data["fp_time_resolved"] = fp.data.fp_HiTRes.sel(H_back=slice(0, 23))
    fp.data["fp_residual"] = fp.data.fp_HiTRes.sel(H_back=24)
    del fp.data["fp_HiTRes"]

    fp.data = fp.data.sel(H_back=slice(0, 23), drop=True)  # remove unused H_back value

    return fp


def test_get_fp_time_resolved_and_residual(footprint_co2_dummy, footprint_paris_co2_dummy):
    """Check that the helper function `time_resolved_and_residual_footprints` works."""
    expected = footprint_paris_co2_dummy.data[["fp_time_resolved", "fp_residual"]]
    result = time_resolved_and_residual_footprints(footprint_co2_dummy.data.fp_HiTRes)

    xr.testing.assert_equal(expected, result)


@pytest.fixture
def model_scenario_paris_co2_dummy(obs_co2_dummy, footprint_paris_co2_dummy, flux_co2_dummy):
    """Create ModelScenario with input dummy data for co2, using PARIS style footprints."""
    model_scenario = ModelScenario(obs=obs_co2_dummy, footprint=footprint_paris_co2_dummy, flux=flux_co2_dummy)

    return model_scenario


def test_model_modelled_obs_paris_co2(model_scenario_paris_co2_dummy, footprint_paris_co2_dummy, flux_co2_dummy):
    """Test expected modelled observations within footprints_dat_merge() method with known dummy data for co2.

    This test uses the newer "PARIS" footprint format, with `fp_time_resolved` and `fp_residual` data variables.
    """
    combined_dataset = model_scenario_paris_co2_dummy.footprints_data_merge()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == pd.Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == pd.Timestamp("2012-01-01T02:00:00")

    # Create expected value(s) for modelled mole fraction, "mf_mod_high_res"
    footprint = footprint_paris_co2_dummy.data
    flux = flux_co2_dummy.data

    # Find maximum number of hours of the back run from footprint data
    max_hours_back = _max_h_back(footprint)  # use function that compensates for max H_back = 23 in PARIS format footprints

    # Loop over each time point so we can calculate expected value and compare
    for t in range(len(aligned_time)):
        print(t)
        # Extract flux data to match H_back and residual time period
        release_time = aligned_time[t].values
        expected_modelled_mf_hr = expected_modelled_mf_at_time(release_time, aligned_time[0], flux, footprint, max_hours_back)
        modelled_mf_hr = combined_dataset["mf_mod_high_res"].sel(time=release_time).values

        assert np.isclose(modelled_mf_hr, expected_modelled_mf_hr)


def test_modelled_obs_co2_consistency(model_scenario_co2_dummy, model_scenario_paris_co2_dummy):
    """Modelled obs should be the same for the old-style co2 dummy data and the PARIS style co2 dummy data."""
    combined = model_scenario_co2_dummy.footprints_data_merge()
    combined_paris = model_scenario_paris_co2_dummy.footprints_data_merge()

    xr.testing.assert_equal(combined.mf_mod_high_res, combined_paris.mf_mod_high_res)
