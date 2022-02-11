import pytest
import numpy as np
import xarray as xr
import pandas as pd
from pandas import Timestamp
from openghg.analyse import ModelScenario, calc_dim_resolution, stack_datasets
from openghg.analyse import calc_dim_resolution
from openghg.retrieve import get_obs_surface, get_footprint, get_flux

def test_scenario_direct_objects():
    '''
    Test ModelScenario class can be created with direct objects
    (ObsData, FootprintData, FluxData)
    '''
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain="EUROPE"
    species="ch4"
    network="DECC"
    inlet="100m"
    source="anthro"

    obs_surface = get_obs_surface(site=site, 
                                  species=species,
                                  start_date=start_date,
                                  end_date=end_date,
                                  inlet=inlet,
                                  network=network)

    footprint = get_footprint(site=site, domain=domain, height=inlet,
               start_date=start_date, end_date=end_date)

    flux = get_flux(species=species, domain=domain, source=source)

    model_scenario = ModelScenario(obs=obs_surface, footprint=footprint, flux=flux)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.flux is not None

    # TODO: Add more stringent tests here to check actual obs and fp values?


def test_scenario_infer_inputs():
    '''
    Test ModelScenario can find underlying data based on keyword inputs.
    '''
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    inlet = "100m"
    network = "DECC"
    source = "anthro"

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   inlet=inlet,
                                   network=network,
                                   domain=domain,
                                   sources=source,
                                   start_date=start_date,
                                   end_date=end_date)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.flux is not None

    # TODO: Add more stringent tests here to check actual obs and fp values?
    # To make sure this is grabbing the right data


def test_scenario_infer_inlet():
    '''
    Test ModelScenario can find underlying data for both observations and
    footprint when omitting the inlet label. This should be inferred from the
    obs data returned and used for the footprint data.
    '''
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"

    # TODO: Add extraction of flux data as well (get_flux) and add to ModelScenario call

    # Explicitly not including inlet to test this can be inferred.
    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   domain=domain,
                                   start_date=start_date,
                                   end_date=end_date)

    assert model_scenario.obs is not None    
    assert model_scenario.footprint is not None


def test_scenario_too_few_inputs():
    '''
    Test no output is included if data can't be found using keywords.
    '''

    site = "tac"

    # Explicitly not including inlet to test this can be inferred.
    model_scenario = ModelScenario(site=site)

    assert model_scenario.obs is None

    # TODO: get_footprint() is not currently returning None - check this
    # assert model_scenario.footprint is None


@pytest.fixture(scope="function")
def model_scenario_1():
    '''Create model scenario as fixture for data in object store'''

    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    inlet = "100m"
    network = "DECC"
    source = "anthro"

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   inlet=inlet,
                                   network=network,
                                   domain=domain,
                                   sources=source,
                                   start_date=start_date,
                                   end_date=end_date)

    return model_scenario


def test_combine_obs_footprint(model_scenario_1):
    '''Test the combine_obs_footprint method of the ModelScenario class'''

    combined_data = model_scenario_1.combine_obs_footprint(resample_to="coarsest")

    cached_data = model_scenario_1.scenario

    # Check returned value and cache associated with class
    data_to_check = [combined_data, cached_data]

    for data in data_to_check:
        # Check mole fraction and footprint values are within this combined dataset
        assert "mf" in data
        assert "fp" in data

        # Check times align with expected values based on resample_to input
        time = data.time
        assert time[0] == Timestamp("2012-08-01T00:00:00")
        assert time[-1] == Timestamp("2012-08-31T22:00:00")

        # Could add more checks here but may be better doing this with mocked data


def test_calc_modelled_obs(model_scenario_1):
    '''Test calc_modelled_obs method of ModelScenario class'''
    modelled_obs = model_scenario_1.calc_modelled_obs(resample_to="coarsest")
    cached_modelled_obs = model_scenario_1.modelled_obs

    # Check returned value and cache associated with class
    data_to_check = [modelled_obs, cached_modelled_obs]

    for data in data_to_check:
        # Check times align with expected values based on resample_to input
        time = data.time
        assert time[0] == Timestamp("2012-08-01T00:00:00")
        assert time[-1] == Timestamp("2012-08-31T22:00:00")

        # Could add more checks here but may be better doing this with mocked data


def test_calc_modelled_obs_period(model_scenario_1):
    '''Test calc_modelled_obs method of ModelScenario class can allow pandas resample inputs'''
    modelled_obs = model_scenario_1.calc_modelled_obs(resample_to="1D")
    cached_modelled_obs = model_scenario_1.modelled_obs

    # Check returned value and cache associated with class
    data_to_check = [modelled_obs, cached_modelled_obs]

    for data in data_to_check:
        # Check times align with expected values based on resample_to input
        time = data.time
        assert time[0] == Timestamp("2012-08-01T00:00:00")
        assert time[-1] == Timestamp("2012-08-31T00:00:00")
        assert len(time) == 31

        # Could add more checks here but may be better doing this with mocked data

# TODO: Add test for stacking flux datasets - only looked at one so far.

#%% Test more generic dataset functions

@pytest.fixture
def flux_daily():
    """Fixture of simple 3D dataset with daily frequency"""

    time = pd.date_range("2012-01-01", "2012-02-01", freq="D")
    lat = [1, 2]
    lon = [10, 20]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), np.ones(shape))},
                       coords={"lat":lat, "lon":lon, "time":time})

    return flux


@pytest.fixture
def flux_single_time():
    """Fixture of simple 3D dataset with unknown frequency (1 time point)"""

    time = pd.date_range("2012-01-01", "2012-01-31", freq="MS")
    lat = [1, 2]
    lon = [10, 20]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), np.ones(shape))},
                       coords={"lat":lat, "lon":lon, "time":time})

    return flux 


def test_calc_resolution(flux_daily):
    """Test frequency/resolution can be calculated (daily data)"""
    frequency = calc_dim_resolution(flux_daily, dim="time")

    frequency_d = frequency.astype("timedelta64[D]").astype(int)
    assert frequency_d == 1


def test_calc_resolution_one_time(flux_single_time):
    """Test NaT value can be calculated for unknown frequency (1 time point)"""
    frequency = calc_dim_resolution(flux_single_time, dim="time")

    assert isinstance(frequency, np.timedelta64)
    assert pd.isnull(frequency)


def test_stack_datasets(flux_daily, flux_single_time):
    """
    Test that datasets can be successfully resampled and added.
    Inputs:
     - daily frequency
     - unknown frequency (1 time value)
    Both contain one data variable, "flux", with all values equal to 1.
    Dimensions are (time, lat, lon) and values other than time are identical.
    """
    datasets = [flux_daily, flux_single_time]

    dataset_stacked = stack_datasets(datasets, dim="time")

    # Check time dimension
    expected_time = flux_daily.time  # Should match to data with highest resolution
    output_time = dataset_stacked.time
    xr.testing.assert_equal(output_time, expected_time)

    # Check summed flux values
    expected_flux = 2  # All values should be 2
    output_flux = dataset_stacked.flux.values
    np.testing.assert_allclose(output_flux, expected_flux)