import pytest
import numpy as np
import xarray as xr
import pandas as pd
from pandas import Timestamp
from openghg.analyse import ModelScenario
from openghg.analyse import match_dataset_dims, calc_dim_resolution, stack_datasets
from openghg.retrieve import get_obs_surface, get_footprint, get_flux

#%% Test ModelScenario initialisation options

def test_scenario_direct_objects():
    """
    Test ModelScenario class can be created with direct objects
    (ObsData, FootprintData, FluxData)
    """
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

    # Check values have been stored in ModelScenario object correctly
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None

    # Check values stored within model_scenario object match inputs
    xr.testing.assert_equal(model_scenario.obs.data, obs_surface.data)
    xr.testing.assert_equal(model_scenario.footprint.data, footprint.data)
    xr.testing.assert_equal(model_scenario.fluxes[source].data, flux.data)


def test_scenario_infer_inputs_ch4():
    """
    Test ModelScenario can find underlying data based on keyword inputs.
    """
    start_date = "2012-08-01"
    end_date = "2012-09-01"

    site = "tac"
    domain = "EUROPE"
    inlet = "100m"
    network = "DECC"

    species = "ch4"
    source = "anthro"

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   inlet=inlet,
                                   network=network,
                                   domain=domain,
                                   sources=source,
                                   start_date=start_date,
                                   end_date=end_date)

    # Check data is being found and stored
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None

    # Check attributes are being assigned correctly
    assert model_scenario.site == site
    assert model_scenario.species == species
    assert model_scenario.flux_sources == [source]

    # Check data stored is as expected
    # Obs data - time range
    obs_data = model_scenario.obs.data
    obs_time = obs_data["time"]
    assert obs_time[0] == Timestamp("2012-08-01T00:00:30")
    assert obs_time[-1] == Timestamp("2012-08-31T23:47:30")

    # Obs data - values
    obs_mf = obs_data["mf"]
    assert np.isclose(obs_mf[0], 1915.11)
    assert np.isclose(obs_mf[-1], 1942.41)

    # Footprint data - time range
    footprint_data = model_scenario.footprint.data
    footprint_time = footprint_data["time"]
    assert footprint_time[0] == Timestamp("2012-08-01T00:00:00")
    assert footprint_time[-1] == Timestamp("2012-08-31T22:00:00")

    # Flux data - stored as dictionary and contains expected time
    fluxes = model_scenario.fluxes
    assert source in fluxes
    assert len(fluxes.keys()) == 1

    flux_data = model_scenario.fluxes[source].data
    flux_time = flux_data["time"]
    assert flux_time[0] == Timestamp("2012-01-01T00:00:00")

    # Note: flux is allowed to be outside imposed time bounds as this is often
    # of lower frequency than obs and footprint but can be forward-filled


def test_scenario_infer_inputs_co2():
    """
    Test ModelScenario can find data for co2 including specific co2 footprint.
    """

    start_date = "2014-07-01"
    end_date = "2014-08-01"

    site = "tac"
    domain = "TEST"
    inlet = "100m"
    network = "DECC"

    species = "co2"
    source = "natural-rtot"

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   inlet=inlet,
                                   network=network,
                                   domain=domain,
                                   sources=source,
                                   start_date=start_date,
                                   end_date=end_date)

    # Check data is being found and stored
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None  # May need to be updated

    # Check attributes are being assigned correctly
    assert model_scenario.site == site
    assert model_scenario.species == species
    assert model_scenario.flux_sources == [source]

    # Check data stored is as expected
    # Obs data - time range
    obs_data = model_scenario.obs.data
    obs_time = obs_data["time"]
    assert obs_time[0] == Timestamp("2014-07-01T00:26:30")
    assert obs_time[-1] == Timestamp("2014-07-31T23:29:30")

    # Obs data - values
    obs_mf = obs_data["mf"]
    assert np.isclose(obs_mf[0], 396.99)
    assert np.isclose(obs_mf[-1], 388.51)
    assert obs_mf.attrs["units"] == "1e-6"

    # Footprint data - species
    assert model_scenario.footprint.metadata["species"] == "co2"

    # Footprint data - time range
    footprint_data = model_scenario.footprint.data
    footprint_time = footprint_data["time"]
    assert footprint_time[0] == Timestamp("2014-07-01T00:00:00")
    assert footprint_time[-1] == Timestamp("2014-07-04T00:00:00")  # Test file - reduced time axis

    # Flux data - stored as dictionary and contains expected time
    flux_data = model_scenario.fluxes[source].data
    flux_time = flux_data["time"]
    assert flux_time[0] == Timestamp("2014-06-29T18:00:00")  # Test file - reduced time axis


def test_scenario_infer_inlet():
    """
    Test ModelScenario can find underlying data for both observations and
    footprint when omitting the inlet label. This should be inferred from the
    obs data returned and used for the footprint data.
    """
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    source = "anthro"

    # Explicitly not including inlet to test this can be inferred from obs data.
    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   domain=domain,
                                   sources=source,
                                   start_date=start_date,
                                   end_date=end_date)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None


def test_scenario_mult_fluxes():
    """
    Extract multiple flux sources at once from available data
    """

    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    inlet = "100m"
    species = "ch4"
    sources = ["anthro", "waste"]

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   domain=domain,
                                   inlet=inlet,
                                   sources=sources,
                                   start_date=start_date,
                                   end_date=end_date)

    for source in sources:
        assert source in model_scenario.fluxes


def test_scenario_too_few_inputs():
    """
    Test no output is included if data can't be found using keywords.
    """

    site = "tac"

    # Explicitly not including inlet to test this can be inferred.
    model_scenario = ModelScenario(site=site)

    # TODO: This may be updated to be include empty ObsData() class
    # May need to include:
    # assert model_scenario.obs.data is None
    assert model_scenario.obs is None

    # TODO: get_footprint() is not currently returning None - check this
    # assert model_scenario.footprint.data is None


def test_add_data():
    """
    Test add_* functions can be used to add new data after initalisation step.
    """

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    inlet = "100m"
    source = "anthro"

    model_scenario = ModelScenario()

    model_scenario.add_obs(site=site, species=species, inlet=inlet)
    model_scenario.add_footprint(site=site, inlet=inlet, domain=domain, species=species)
    model_scenario.add_flux(species=species, domain=domain, sources=source)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None

#%% Test ModelScenario methods with real data

@pytest.fixture(scope="function")
def model_scenario_1():
    """Create model scenario as fixture for data in object store"""

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
    """Test the combine_obs_footprint method of the ModelScenario class"""

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
    """Test calc_modelled_obs method of ModelScenario class"""
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
    """Test calc_modelled_obs method of ModelScenario class can allow pandas resample inputs"""
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


def test_add_multiple_flux(model_scenario_1):
    """Test multiple flux sources can be added."""
    species = "ch4"
    source = "waste"
    domain = "EUROPE"

    model_scenario_1.add_flux(species=species, sources=source, domain=domain)

    expected_sources = ["anthro", source]

    for source in expected_sources:

        assert source in model_scenario_1.fluxes

        metadata = model_scenario_1.fluxes[source].metadata
        assert metadata["source"] == source


def test_combine_flux_sources(model_scenario_1):
    """Test fluxes can be combined to produce a stacked output"""
    species = "ch4"
    source = "waste"
    domain = "EUROPE"

    model_scenario_1.add_flux(species=species, sources=source, domain=domain)

    flux_stacked = model_scenario_1.combine_flux_sources()

    assert flux_stacked.attrs["sources"] == f"anthro, {source}"


def test_footprints_data_merge(model_scenario_1):
    """Test footprints_data_merge method can be run"""
    combined_dataset = model_scenario_1.footprints_data_merge(resample_to="coarsest")

    assert "fp" in combined_dataset
    assert "mf" in combined_dataset
    assert "mf_mod" in combined_dataset

    attributes = combined_dataset.attrs
    assert attributes["resample_to"] == "coarsest"

#%% Test method functionality with dummy data (CH4)

@pytest.fixture
def obs_dummy():
    """
    Create example ObsData object with dummy data
     - Hourly frequency for 2012-01-01 - 2012-01-02 (48 time points)
     - "mf" values are from 1, 48
    """
    from openghg.dataobjects import ObsData

    time = pd.date_range("2012-01-01T00:00:00", "2012-01-02T23:00:00", freq="H")

    ntime = len(time)
    values = np.arange(1, ntime+1, 1)

    data = xr.Dataset({"mf":("time", values)},
                       coords={"time":time},
                       attrs={"sampling_period": "60"}
                       )

    # Potential metadata:
    # - site, instrument, sampling_period, inlet, port, type, network, species, calibration_scale
    #   long_name, data_owner, data_owner_email, station_longitude, station_latitude, ...
    # - data_type
    metadata = {"site":"TEST_SITE", "species":"ch4", "inlet":"10m", "sampling_period":"60"}

    obsdata = ObsData(data=data, metadata=metadata)

    return obsdata


@pytest.fixture
def footprint_dummy():
    """
    Create example FootprintData object with dummy data:
     - Daily frequency from 2011-12-31 to 2012-01-03 (inclusive) (4 time points)
     - Small lat, lon (TEST_DOMAIN)
     - "fp" values are all 1 **May change**
    """
    from openghg.dataobjects import FootprintData

    time = pd.date_range("2011-12-31", "2012-01-03", freq="D")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.ones(shape)

    data = xr.Dataset({"fp":(("time", "lat", "lon"), values)},
                       coords={"lat":lat, "lon":lon, "time":time})

    # Potential metadata:
    # - site, height, domain, model, network, start_date, end_date, heights, ...
    # - data_type="footprints"
    metadata = {"site":"TESTSITE", "height":"10m", "domain":"TESTDOMAIN", "data_type":"footprints"}

    footprintdata = FootprintData(data=data,
                                 metadata=metadata,
                                 flux={},
                                 bc={},
                                 species="INERT",
                                 scales="",
                                 units="",
                                 )

    return footprintdata


@pytest.fixture
def flux_dummy():
    """
    Create example FluxData object with dummy data
     - Annual frequency (2011-01-01, 2012-01-01) (2 time points)
     - Small lat, lon (TEST_DOMAIN)
     - "flux" values are:
       - 2011-01-01 - all 2
       - 2012-01-01 - all 3
    """
    from openghg.dataobjects import FluxData

    time = pd.date_range("2011-01-01", "2012-12-31", freq="AS")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.ones(shape)
    values[0, ...] *= 2
    values[1, ...] *= 3

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), values)},
                       coords={"lat":lat, "lon":lon, "time":time})

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "ch4"
    metadata = {"species":species, "source":"TESTSOURCE", "domain":"TESTDOMAIN"}

    fluxdata = FluxData(data=flux,
                        metadata=metadata,
                        flux={},
                        bc={},
                        species=species,
                        scales="",
                        units="",
                        )

    return fluxdata


@pytest.fixture
def model_scenario_dummy(obs_dummy, footprint_dummy, flux_dummy):
    """Create ModelScenario with input dummy data"""
    model_scenario = ModelScenario(obs=obs_dummy,
                                   footprint=footprint_dummy,
                                   flux=flux_dummy)

    return model_scenario


def test_model_resample(model_scenario_dummy):
    """Test expected resample values for obs with known dummy data"""
    combined_dataset = model_scenario_dummy.combine_obs_footprint()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected values for resampled observations
    # In our case:
    # - observation data contains values from 1, 48 for each time point
    # - output should contain 2 time points (2012-01-01, 2012-01-02)
    # Expect this to be resampled to daily (24H) frequency based on footprint frequency
    obs_data = model_scenario_dummy.obs.data
    obs_mf_1 = obs_data["mf"].sel(time=slice("2012-01-01T00:00:00", "2012-01-01T23:00:00")).values.mean()
    obs_mf_2 = obs_data["mf"].sel(time=slice("2012-01-02T00:00:00", "2012-01-02T23:00:00")).values.mean()
    expected_obs_mf = [obs_mf_1, obs_mf_2]

    resampled_mf = combined_dataset["mf"].values
    assert np.allclose(resampled_mf, expected_obs_mf)


def test_model_modelled_obs(model_scenario_dummy):
    """Test expected modelled observations with known dummy data"""
    combined_dataset = model_scenario_dummy.footprints_data_merge()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected value(s) for modelled_mf
    # In our case:
    # - dummy footprint input contains 1.0 for all values
    # - dummy flux input contains 3.0 for all value *at the correct time*
    # Expect same modelled mf for both times since flux is annual
    nlat, nlon = len(combined_dataset.lat), len(combined_dataset.lon)
    input_flux_value = 3.0
    input_fp_value = 1.0
    expected_modelled_mf = nlat*nlon*input_flux_value*input_fp_value

    modelled_mf = combined_dataset["mf_mod"].values
    assert np.allclose(modelled_mf, expected_modelled_mf)


#%% Test method functionality with dummy data (CO2)

# TODO: Add relevant dummy tests with known inputs and outputs for
# high time resolution (co2) workflow
# Additional fixtures needed (minimum):
# - obs_co2_dummy - likely same as obs_dummy but for co2 species
# - fp_co2_dummy - footprint with "fp" and "fp_HiTRes" (time and H_back) for co2 species
# - flux_co2_dummy - 2-hourly flux for co2 species
# Tests:
# - footprint_data_merge method for co2 inputs

#%% Test generic dataset functions

@pytest.fixture
def flux_daily():
    """Fixture of simple 3D dataset with daily frequency"""

    time = pd.date_range("2012-01-01", "2012-02-01", freq="D")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), np.ones(shape))},
                       coords={"lat":lat, "lon":lon, "time":time})

    return flux


@pytest.fixture
def flux_single_time():
    """Fixture of simple 3D dataset with unknown frequency (1 time point)"""

    time = pd.date_range("2012-01-01", "2012-01-31", freq="MS")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), np.ones(shape))},
                       coords={"lat":lat, "lon":lon, "time":time})

    return flux


@pytest.fixture
def flux_daily_small_dim_diff():
    """Fixture of simple 3D dataset with small lat, lon difference and daily time frequency"""

    time = pd.date_range("2012-01-01", "2012-02-01", freq="D")
    small_mismatch = 1e-6
    lat = [1. + small_mismatch, 2. + small_mismatch]
    lon = [10. + small_mismatch, 20. + small_mismatch]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    flux_values = np.ones(shape) + 10.

    flux = xr.Dataset({"flux":(("time", "lat", "lon"), flux_values)},
                       coords={"lat":lat, "lon":lon, "time":time})

    return flux


def test_match_dataset_dims(flux_daily, flux_daily_small_dim_diff):
    """
    Test datasets can be matched:
     - lat, lon have small differences (1e-6, tolerance should be more than this)
     - time should be identical already
    """
    # Pass datasets with diff lat, lon values (< tolerance), same time values
    datasets = [flux_daily, flux_daily_small_dim_diff]
    datasets_matched = match_dataset_dims(datasets, dims="all")

    # Check lat, lon have now been aligned to first dataset
    compare_dims = ["lat", "lon"]
    ds0 = datasets_matched[0]
    for ds in datasets_matched[1:]:
        for dim in compare_dims:
            coords_compare = ds0[dim]
            coords = ds[dim]
            xr.testing.assert_equal(coords_compare, coords)

    # Check time dimension has not been changed (should already match)
    unchanged_dim = "time"
    for ds, ds_org in zip(datasets_matched, datasets):
        coords_compare = ds_org[unchanged_dim]
        coords = ds[unchanged_dim]
        xr.testing.assert_equal(coords_compare, coords)


def test_match_dataset_dims_diff_time(flux_daily, flux_single_time):
    """
    Test same datasets are returned if non-matching dimensions are ignored;
     - lat, lon are identical
     - time is different but is not passed as a dimension to match
    """

    # Pass datasets with the same lat, lon but different time values
    datasets = [flux_daily, flux_single_time]
    datasets_matched = match_dataset_dims(datasets, dims=["lat", "lon"])

    # Check no dimensions have been changed (lat, lon should already match)
    for ds, ds_org in zip(datasets_matched, datasets):
        xr.testing.assert_equal(ds, ds_org)


def test_calc_resolution(flux_daily):
    """Test frequency/resolution can be calculated (daily data)"""
    frequency = calc_dim_resolution(flux_daily, dim="time")

    frequency_d = frequency.astype("timedelta64[D]").astype(int)
    assert frequency_d == 1


def test_calc_resolution_single_time(flux_single_time):
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


def test_stack_datasets_with_alignment(flux_daily, flux_daily_small_dim_diff):
    """
    Test that datasets can be successfully added after being matched.
    Inputs:
     - flux_daily - daily frequency
     - flux_daily_small_dim_diff - daily frequency (small lat, lon mismatch - 1e-6)
    Both contain one data variable, "flux",
     - flux_daily - all values equal to 1.
     - flux_daily_small_dim_diff - all values equal to 11
    Dimensions are (time, lat, lon).
    """
    datasets = [flux_daily, flux_daily_small_dim_diff]

    datasets_matched = match_dataset_dims(datasets, dims=["lat", "lon"])
    dataset_stacked = stack_datasets(datasets_matched, dim="time")

    # Check time dimension
    expected_time = flux_daily.time  # Should match to data with highest resolution
    output_time = dataset_stacked.time
    xr.testing.assert_equal(output_time, expected_time)

    # Check summed flux values
    expected_flux = 1 + 11  # All values should be 12
    output_flux = dataset_stacked.flux.values
    np.testing.assert_allclose(output_flux, expected_flux)
