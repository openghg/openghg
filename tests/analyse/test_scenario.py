from typing import Optional

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from helpers import clear_test_stores
from openghg.analyse import ModelScenario, calc_dim_resolution, match_dataset_dims, stack_datasets
from openghg.retrieve import get_bc, get_flux, get_footprint, get_obs_surface
from pandas import Timedelta, Timestamp
from xarray import Dataset


def test_scenario_direct_objects():
    """
    Test ModelScenario class can be created with direct objects
    (ObsData, FootprintData, FluxData)
    """
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    network = "DECC"
    inlet = "100m"
    source = "anthro"
    bc_input = "MOZART"

    obs_surface = get_obs_surface(
        site=site, species=species, start_date=start_date, end_date=end_date, inlet=inlet, network=network
    )

    footprint = get_footprint(
        site=site, domain=domain, height=inlet, start_date=start_date, end_date=end_date
    )

    flux = get_flux(species=species, domain=domain, source=source)

    bc = get_bc(species=species, domain=domain, bc_input=bc_input)

    model_scenario = ModelScenario(obs=obs_surface, footprint=footprint, flux=flux, bc=bc)

    # Check values have been stored in ModelScenario object correctly
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None
    assert model_scenario.bc is not None

    # Check values stored within model_scenario object match inputs
    xr.testing.assert_equal(model_scenario.obs.data, obs_surface.data)
    xr.testing.assert_equal(model_scenario.footprint.data, footprint.data)
    xr.testing.assert_equal(model_scenario.fluxes[source].data, flux.data)
    xr.testing.assert_equal(model_scenario.bc.data, bc.data)


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

    bc_input = "MOZART"

    model_scenario = ModelScenario(
        site=site,
        species=species,
        inlet=inlet,
        network=network,
        domain=domain,
        sources=source,
        bc_input=bc_input,
        start_date=start_date,
        end_date=end_date,
    )

    # Check data is being found and stored
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None
    assert model_scenario.bc is not None

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

    # Boundary conditions data - time point
    bc_data = model_scenario.bc.data
    bc_time = bc_data["time"]
    assert bc_time[0] == Timestamp("2012-08-01T00:00:00")


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

    model_scenario = ModelScenario(
        site=site,
        species=species,
        inlet=inlet,
        network=network,
        domain=domain,
        sources=source,
        start_date=start_date,
        end_date=end_date,
    )

    # Check data is being found and stored
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None  # May need to be updated
    assert model_scenario.bc is not None

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

    # BC data
    assert model_scenario.bc.metadata["species"] == "co2"
    # TODO: Could add more checks here if needed.


def test_scenario_flux_extend_co2():
    """
    Check ModelScenario can extract full date range of flux values for a given
    source.
    Note: for co2, the start and end date range is not used when grabbing the
    flux data. This is to ensure all relevant flux data is grabbed for creating
    modelled observations as this may be outside the date range.
    """

    start_date = "2014-07-01"
    end_date = "2014-08-01"

    site = "tac"
    domain = "TEST"
    inlet = "100m"
    network = "DECC"

    species = "co2"
    source = "ocean"

    model_scenario = ModelScenario(
        site=site,
        species=species,
        inlet=inlet,
        network=network,
        domain=domain,
        sources=source,
        start_date=start_date,
        end_date=end_date,
    )

    # Check data is being found and stored
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None

    # Check two flux files for the same source have been returned
    flux_data = model_scenario.fluxes[source].data
    flux_time = flux_data["time"]
    assert flux_time[0] == Timestamp("2013-12-01T00:00:00")  # From test file 1 - reduced time axis
    assert flux_time[1] == Timestamp("2014-07-01T00:00:00")  # From test file 2 - reduced time axis


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
    model_scenario = ModelScenario(
        site=site, species=species, domain=domain, sources=source, start_date=start_date, end_date=end_date
    )

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None

    assert model_scenario.inlet == "100m"
    assert model_scenario.fp_inlet == "100m"


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

    model_scenario = ModelScenario(
        site=site,
        species=species,
        domain=domain,
        inlet=inlet,
        sources=sources,
        start_date=start_date,
        end_date=end_date,
    )

    for source in sources:
        assert source in model_scenario.fluxes


def test_scenario_too_few_inputs():
    """
    Test no output is included if data can't be found using keywords.
    """

    site = "tac"

    model_scenario = ModelScenario(site=site)

    assert model_scenario.obs is None
    assert model_scenario.footprint is None


def test_scenario_uses_fp_inlet():
    """
    Test ModelScenario is using fp_inlet in place of inlet if this is passed.
    In this case we expect the observation data to be found but the footprint
    data to be missing.
    """
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    inlet = "100m"  # Correct inlet
    fp_inlet = "999m"  # Incorrect inlet

    model_scenario = ModelScenario(
        site=site,
        species=species,
        inlet=inlet,
        domain=domain,
        fp_inlet=fp_inlet,
        start_date=start_date,
        end_date=end_date,
    )

    # Expect observation data to be found
    assert model_scenario.obs is not None

    # Expect footprint data to be missing in this case
    assert not hasattr(model_scenario, fp_inlet)
    assert model_scenario.footprint is None


def test_scenario_matches_fp_inlet():
    """
    Test ModelScenario is able to use "height_name" data from site_info file to
    map between different inlet values for observation data and footprints.

    In this case for "WAO" data we want to be able to use a dictionary to allow
    older footprints which were run at 20m to be used for 10m inlet e.g.

    "WAO": {
        "ICOS": {
            "height": ["10m"],
            "height_name": {"10m": ["10magl", "20magl"]},
            ...
    },
    """
    start_date = "2021-12-01"
    end_date = "2022-01-01"

    site = "wao"
    domain = "TEST"
    species = "rn"
    inlet = "10m"

    model_scenario = ModelScenario(
        site=site, species=species, inlet=inlet, domain=domain, start_date=start_date, end_date=end_date
    )

    expected_obs_inlet = inlet  # inlet for observation data
    expected_fp_inlet = "20m"  # inlet for footprint data

    # Check obs and footprint data is found and inlets are expected values
    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.obs.metadata["inlet"] == expected_obs_inlet
    assert model_scenario.footprint.metadata["inlet"] == expected_fp_inlet

    assert model_scenario.inlet == expected_obs_inlet
    assert model_scenario.fp_inlet == expected_fp_inlet


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
    model_scenario.add_bc(species=species, domain=domain)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None
    assert model_scenario.fluxes is not None
    assert model_scenario.bc is not None


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
    bc_input = "mozart"

    model_scenario = ModelScenario(
        site=site,
        species=species,
        inlet=inlet,
        network=network,
        domain=domain,
        sources=source,
        bc_input=bc_input,
        start_date=start_date,
        end_date=end_date,
    )

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


def test_calc_modelled_baseline(model_scenario_1):
    """Test modelled baselin can be calculated from ModelScenario"""
    modelled_baseline = model_scenario_1.calc_modelled_baseline(resample_to="coarsest")
    cached_modelled_baseline = model_scenario_1.modelled_baseline

    # Check returned value and cache associated with class
    data_to_check = [modelled_baseline, cached_modelled_baseline]

    for data in data_to_check:
        # Check times align with expected values based on resample_to input
        time = data.time
        assert time[0] == Timestamp("2012-08-01T00:00:00")
        assert time[-1] == Timestamp("2012-08-31T22:00:00")

        # Could add more checks here but may be better doing this with mocked data


def test_footprints_data_merge(model_scenario_1):
    """Test footprints_data_merge method can be run"""
    combined_dataset = model_scenario_1.footprints_data_merge(resample_to="coarsest")

    assert "fp" in combined_dataset
    assert "mf" in combined_dataset
    assert "mf_mod" in combined_dataset

    attributes = combined_dataset.attrs
    assert attributes["resample_to"] == "coarsest"


def test_combine_obs_sampling_period_infer():
    """
    If the sampling_period is "NOT_SET" then when combining obs and footprints
    this should infer the sampling period from the frequency of the data but this
    was raising a value error. Reported as part of Issue #620.

    Test to ensure this functionality is now working.
     - sampling_period attribute for WAO data file is "NOT_SET"
    """
    start_date = "2021-12-01"
    end_date = "2022-01-01"

    site = "wao"
    domain = "TEST"
    species = "rn"
    inlet = "10m"

    model_scenario = ModelScenario(
        site=site, species=species, inlet=inlet, domain=domain, start_date=start_date, end_date=end_date
    )

    obs_data_1 = model_scenario.obs.data
    assert obs_data_1.attrs["sampling_period"] == "NOT_SET"

    model_scenario.combine_obs_footprint()  # Check operation can be run

    obs_data_2 = model_scenario.obs.data
    assert obs_data_2.attrs["sampling_period_estimate"] == "3600.0"


# TODO: Dummy tests included below but may want to add checks which use real
# data for short-lived species (different footprint)
# - species with single lifetime (e.g. "Rn")
#    - e.g. WAO-20magl_UKV_rn_EUROPE_201801.nc
# - species with monthly lifetime (e.g. "HFO-1234zee")
#    - e.g. MHD-10magl_UKV_hfo-1234zee_EUROPE_201401.nc

# %% Test method functionality with dummy data (CH4)


@pytest.fixture
def obs_ch4_dummy():
    """
    Create example ObsData object with dummy data
     - Species is methane (ch4)
     - Hourly frequency for 2012-01-01 - 2012-01-02 (48 time points)
     - "mf" values are from 1, 48
    """
    from openghg.dataobjects import ObsData

    time = pd.date_range("2012-01-01T00:00:00", "2012-01-02T23:00:00", freq="H")

    ntime = len(time)
    values = np.arange(0, ntime, 1)

    species = "ch4"
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

    obsdata = ObsData(data=data, metadata=metadata)

    return obsdata


@pytest.fixture
def footprint_dummy():
    """
    Create example FootprintData object with dummy data:
     - Daily frequency from 2011-12-31 to 2012-01-03 (inclusive) (4 time points)
     - Small lat, lon (TEST_DOMAIN)
     - Small height
     - "fp" values are all 1 **May change**
     - "particle_locations_*" are 2, 3, 4, 5 for "n", "e", "s", "w"
     - "INERT" species
    """
    from openghg.dataobjects import FootprintData

    time = pd.date_range("2011-12-31", "2012-01-03", freq="D")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]
    height = [500, 1500]

    # Add dummy footprint values
    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.ones(shape)

    data_vars = {}
    data_vars["fp"] = (("time", "lat", "lon"), values)

    # Add dummy particle location values for NESW boundaries of domain
    nheight = len(height)
    shape_ns = (ntime, nlon, nheight)
    shape_ew = (ntime, nlat, nheight)
    values_n = np.ones(shape_ns) * 2
    values_e = np.ones(shape_ew) * 3
    values_s = np.ones(shape_ns) * 4
    values_w = np.ones(shape_ew) * 5

    data_vars["particle_locations_n"] = (("time", "lon", "height"), values_n)
    data_vars["particle_locations_e"] = (("time", "lat", "height"), values_e)
    data_vars["particle_locations_s"] = (("time", "lon", "height"), values_s)
    data_vars["particle_locations_w"] = (("time", "lat", "height"), values_w)

    coords = {"lat": lat, "lon": lon, "time": time, "height": height}

    data = xr.Dataset(data_vars, coords=coords)

    # Potential metadata:
    # - site, inlet, domain, model, network, start_date, end_date, heights, ...
    # - data_type="footprints"
    metadata = {"site": "TESTSITE", "inlet": "10m", "domain": "TESTDOMAIN", "data_type": "footprints"}

    footprintdata = FootprintData(data=data, metadata=metadata)

    return footprintdata


@pytest.fixture
def flux_ch4_dummy():
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

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), values)}, coords={"lat": lat, "lon": lon, "time": time}
    )

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "ch4"
    metadata = {"species": species, "source": "TESTSOURCE", "domain": "TESTDOMAIN"}

    fluxdata = FluxData(data=flux, metadata=metadata)

    return fluxdata


@pytest.fixture
def bc_ch4_dummy():
    """
    Create example BoundaryConditionsData object with dummy data
     - Annual frequency (2011-01-01, 2012-01-01) (2 time points)
     - Small lat, lon (TEST_DOMAIN); dummy height values
     - 'vmr_X' values for 'n', 'e', 's', 'w' are:
       - 2011-01-01 all points - 2, 3, 4, 5 (i.e. 'vmr_n' is 2)
       - 2012-01-01 all points - 3, 4, 5, 6 (i.e. 'vmr_w' is 6)
    """
    from openghg.dataobjects import BoundaryConditionsData

    time = pd.date_range("2011-01-01", "2012-12-31", freq="AS")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]
    height = [500, 1500]

    nheight, nlat, nlon, ntime = len(height), len(lat), len(lon), len(time)

    shape_ns = (ntime, nlon, nheight)
    shape_ew = (ntime, nlat, nheight)

    dims_ns = ("time", "lon", "height")
    dims_ew = ("time", "lat", "height")

    param = ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]
    data_vars = {}
    for i, dv in enumerate(param):
        direction = dv.split("_")[-1]
        if direction in ("n", "s"):
            shape = shape_ns
            dims = dims_ns
        elif direction in ("e", "w"):
            shape = shape_ew
            dims = dims_ew
        values = np.ones(shape)
        values[0, ...] *= 2 + i  # 2, 3, 4, 5
        values[1, ...] *= 3 + i  # 3, 4, 5, 6
        data_vars[dv] = (dims, values)

    coords = {"lat": lat, "lon": lon, "time": time, "height": height}

    bc = xr.Dataset(data_vars, coords=coords)

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "ch4"
    metadata = {"species": species, "bc_input": "TESTINPUT", "domain": "TESTDOMAIN"}

    bc_data = BoundaryConditionsData(data=bc, metadata=metadata)

    return bc_data


@pytest.fixture
def model_scenario_ch4_dummy(obs_ch4_dummy, footprint_dummy, flux_ch4_dummy, bc_ch4_dummy):
    """Create ModelScenario with input dummy data"""
    model_scenario = ModelScenario(
        obs=obs_ch4_dummy, footprint=footprint_dummy, flux=flux_ch4_dummy, bc=bc_ch4_dummy
    )

    return model_scenario


def test_model_resample_ch4(model_scenario_ch4_dummy):
    """Test expected resample values for obs with known dummy data"""
    combined_dataset = model_scenario_ch4_dummy.combine_obs_footprint()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected values for resampled observations
    # In our case:
    # - observation data contains values from 1, 48 for each time point
    # - output should contain 2 time points (2012-01-01, 2012-01-02)
    # Expect this to be resampled to daily (24H) frequency based on footprint frequency
    obs_data = model_scenario_ch4_dummy.obs.data
    obs_mf_1 = obs_data["mf"].sel(time=slice("2012-01-01T00:00:00", "2012-01-01T23:00:00")).values.mean()
    obs_mf_2 = obs_data["mf"].sel(time=slice("2012-01-02T00:00:00", "2012-01-02T23:00:00")).values.mean()
    expected_obs_mf = [obs_mf_1, obs_mf_2]

    resampled_mf = combined_dataset["mf"].values
    assert np.allclose(resampled_mf, expected_obs_mf)


def test_model_modelled_obs_ch4(model_scenario_ch4_dummy, footprint_dummy, flux_ch4_dummy):
    """Test expected modelled observations within footprints_data_merge method with known dummy data"""
    combined_dataset = model_scenario_ch4_dummy.footprints_data_merge()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected value(s) for modelled_mf
    # In our case:
    # - dummy footprint input contains 1.0 for all values
    # - dummy flux input contains 3.0 for all value *at the correct time*
    # Expect same modelled mf for both times 2012/01/01 and 2012/01/02 since flux is annual
    time_slice = slice(aligned_time[0], aligned_time[-1])
    footprint = footprint_dummy.data.sel(time=time_slice)
    flux = flux_ch4_dummy.data.sel(time=time_slice)

    input_flux_values = flux["flux"].reindex_like(footprint, method="ffill")
    input_fp_values = footprint["fp"]

    expected_modelled_mf = (input_fp_values * input_flux_values).sum(dim=("lat", "lon")).values

    modelled_mf = combined_dataset["mf_mod"].values
    assert np.allclose(modelled_mf, expected_modelled_mf)


def calc_expected_baseline(footprint: Dataset, bc: Dataset, lifetime_hrs: Optional[float] = None):
    fp_vars = ["particle_locations_n", "particle_locations_e", "particle_locations_s", "particle_locations_w"]
    bc_vars = ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]

    # Only relevant if lifetime_hrs input is set
    mean_age_vars = [
        "mean_age_particles_n",
        "mean_age_particles_e",
        "mean_age_particles_s",
        "mean_age_particles_w",
    ]

    dims = {"N": ["height", "lon"], "E": ["height", "lat"], "S": ["height", "lon"], "W": ["height", "lat"]}

    for i, bc_var, fp_var, age_var in zip(range(len(bc_vars)), bc_vars, fp_vars, mean_age_vars):
        input_bc_values = bc[bc_var].reindex_like(footprint, method="ffill")
        input_fp_values = footprint[fp_var]

        direction = bc_var.split("_")[-1].upper()

        if lifetime_hrs is not None:
            input_age_values = footprint[age_var]
            loss = np.exp(-1 * input_age_values / lifetime_hrs)
        else:
            loss = 1.0

        baseline_component = (input_fp_values * input_bc_values * loss).sum(dim=dims[direction]).values
        if i == 0:
            expected_modelled_baseline = baseline_component
        else:
            expected_modelled_baseline += baseline_component

    return expected_modelled_baseline


def test_modelled_baseline_ch4(model_scenario_ch4_dummy, footprint_dummy, bc_ch4_dummy):
    """Test expected modelled baseline with known dummy data"""
    modelled_baseline = model_scenario_ch4_dummy.calc_modelled_baseline(output_units=1)

    aligned_time = modelled_baseline["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected value(s) for modelled_mf
    # In our case:
    # - dummy particle_locations_* contains 2, 3, 4, 5 for n, e, s, w
    # - dummy boundary conditions, vmr_* contains 3, 4, 5, 6 at the correct time for n, e, s, w
    # Expect same modelled baseline for both times 2012/01/01 and 2012/01/02 since bc is annual
    time_slice = slice(aligned_time[0], aligned_time[-1])
    footprint = footprint_dummy.data.sel(time=time_slice)
    bc = bc_ch4_dummy.data.sel(time=time_slice)

    expected_modelled_baseline = calc_expected_baseline(footprint, bc, lifetime_hrs=None)

    assert np.allclose(modelled_baseline, expected_modelled_baseline)


# %% Test method functionality with dummy data (CO2)


@pytest.fixture
def obs_co2_dummy():
    """
    Create example ObsData object with dummy data
     - Species is carbon dioxide (co2)
     - 30-min frequency for 2012-01-01 (48 time points)
     - "mf" values are from 1, 48
    """
    from openghg.dataobjects import ObsData

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

    time = pd.date_range("2011-12-31T23:00:00", "2012-01-01T02:00:00", freq="H")
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

    time = pd.date_range("2011-12-31", "2012-01-02", freq="2H")
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


def test_model_modelled_obs_co2(model_scenario_co2_dummy, footprint_co2_dummy, flux_co2_dummy):
    """Test expected modelled observations within footprints_dat_merge() method with known dummy data for co2"""
    combined_dataset = model_scenario_co2_dummy.footprints_data_merge()

    aligned_time = combined_dataset["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-01T02:00:00")

    # Create expected value(s) for modelled mole fraction, "mf_mod_high_res"
    footprint = footprint_co2_dummy.data
    flux = flux_co2_dummy.data

    # Find maximum number of hours of the back run from footprint data
    max_hours_back = footprint["H_back"].values.max()

    # Loop over each time point so we can calculate expected value and compare
    for t in range(len(aligned_time)):
        # Extract flux data to match H_back and residual time period
        release_time = aligned_time[t].values
        back_time = release_time - Timedelta(max_hours_back, "hours")
        flux_hitres = flux["flux"].sel(time=slice(back_time, release_time))
        flux_integrated = (
            flux["flux"].resample({"time": "1MS"}).mean().sel(time=aligned_time[0])
        )  # This may need to be updated

        # Update footprint data to use number of hours back to derive a time
        footprint_at_release = footprint["fp_HiTRes"].sel(time=release_time)
        h_back_time = np.array(
            [release_time - Timedelta(hour, "hours") for hour in footprint["H_back"].values],
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
        back_time_exclude = back_time + Timedelta(1, "s")  # Time to go up to but not include back_time
        footprint_hitres = footprint_at_release.sel({"time": slice(release_time, back_time_exclude)})
        footprint_integrated = footprint_at_release.sel({"time": back_time})

        # Align flux to footprint data (flux is 2 hourly, footprint H_back is 1 hourly)
        flux_hitres = flux_hitres.reindex_like(footprint_hitres, method="ffill")

        # Calculate high time resolution and residual components of modelled mole fraction
        modelled_mf_htres = (flux_hitres * footprint_hitres).sum().values
        modelled_mf_residual = (flux_integrated * footprint_integrated).sum().values
        # Combine to create expected modelled mole fraction
        expected_modelled_mf_hr = modelled_mf_htres + modelled_mf_residual

        modelled_mf_hr = combined_dataset["mf_mod_high_res"].sel(time=release_time).values
        assert np.isclose(modelled_mf_hr, expected_modelled_mf_hr)


# %% Test baseline calculation for short-lived species
# Radon (Rn) - currently has one lifetime value defined
# HFO-1234zee - currently has monthly lifetimes defined
# - see openghg/data/acrg_species_info.json for details


def replace_species(data_object, species):
    """
    Create new dummy data based on the original but with a new species label
    """
    data = data_object  # Can we copy this?

    data.data.attrs["species"] = species
    data.metadata["species"] = species

    return data


@pytest.fixture
def obs_radon_dummy(obs_ch4_dummy):
    """
    Create example ObsData object with dummy data
     - Values match to obs_ch4_dummy
     - Species is Radon (rn)
    Same values as obs_ch4_dummy fixture but updated species to be "Rn"
    """
    species = "Rn"
    return replace_species(obs_ch4_dummy, species)


@pytest.fixture
def footprint_radon_dummy(footprint_dummy):
    """
    Create example footprint data for Rn (short-lived species)
     - Footprint values ('fp') match to footprint_dummy
     - Additional parameters for short-lived species
      - mean_age_particles_X for 'n','e','s','w'
        - time point 0 all points - 10, 11, 12, 13
        - time point 1 all points - 11, 12, 13, 14
    """
    from openghg.dataobjects import FootprintData

    footprint_ds = footprint_dummy.data  # Can we copy this?
    footprint_metadata = footprint_dummy.metadata  # Can we copy this?

    species = "Rn"
    footprint_metadata["species"] = species
    footprint_ds.attrs["species"] = species

    fp_dims = footprint_ds.dims
    nlat = fp_dims["lat"]
    nlon = fp_dims["lon"]
    nheight = fp_dims["height"]
    ntime = fp_dims["time"]

    shape_ns = (ntime, nlon, nheight)
    shape_ew = (ntime, nlat, nheight)

    dims_ns = ("time", "lon", "height")
    dims_ew = ("time", "lat", "height")

    param = ["mean_age_particles_n", "mean_age_particles_e", "mean_age_particles_s", "mean_age_particles_w"]
    data_vars = {}
    for i, dv in enumerate(param):
        direction = dv.split("_")[-1]
        if direction in ("n", "s"):
            shape = shape_ns
            dims = dims_ns
        elif direction in ("e", "w"):
            shape = shape_ew
            dims = dims_ew
        values = np.ones(shape)
        # Assuming dim 0 is time and has length 2...
        values[0, ...] *= 10.0 + i  # 10, 11, 12, 13
        values[1, ...] *= 11.0 + i  # 11, 12, 13, 14
        data_vars[dv] = (dims, values)

    footprint_ds = footprint_ds.assign(data_vars)

    footprintdata = FootprintData(data=footprint_ds, metadata=footprint_metadata)

    return footprintdata


@pytest.fixture
def bc_radon_dummy(bc_ch4_dummy):
    """
    Create example BoundaryConditionsData object with dummy data
     - Values match to bc_ch4_dummy
     - Species is Radon (rn)
    Same values as bc_ch4_dummy fixture but updated species to be "Rn"

    """
    species = "Rn"
    return replace_species(bc_ch4_dummy, species)


@pytest.fixture
def model_scenario_radon_dummy(obs_radon_dummy, footprint_radon_dummy, bc_radon_dummy):
    """Create ModelScenario with input dummy data for Radon (a short-lived species)"""
    model_scenario = ModelScenario(obs=obs_radon_dummy, footprint=footprint_radon_dummy, bc=bc_radon_dummy)

    return model_scenario


def test_modelled_baseline_radon(model_scenario_radon_dummy, footprint_radon_dummy, bc_radon_dummy):
    """Test expected modelled baseline for Rn (short-lived species) with known dummy data"""
    modelled_baseline = model_scenario_radon_dummy.calc_modelled_baseline(output_units=1)

    aligned_time = modelled_baseline["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected value(s) for modelled_baseline for short-lived species
    # This includes a loss component vased on mean_age_particles for each direction
    # In our case:
    # - dummy particle_locations_* contains 2, 3, 4, 5 for n, e, s, w
    # - dummy boundary conditions, vmr_* contains 3, 4, 5, 6 at the correct time for n, e, s, w
    # - dummy mean age, mean_age_particles_* contains 11, 12, 13, 14 at the correct time for n, e, s, w
    time_slice = slice(aligned_time[0], aligned_time[-1])
    footprint = footprint_radon_dummy.data.sel(time=time_slice)
    bc = bc_radon_dummy.data.sel(time=time_slice)

    lifetime_rn_days = 5.5157  # Should match value within acrg_species_info.json file
    lifetime_rn_hrs = lifetime_rn_days * 24.0

    expected_modelled_baseline = calc_expected_baseline(footprint, bc, lifetime_hrs=lifetime_rn_hrs)

    assert np.allclose(modelled_baseline, expected_modelled_baseline)


@pytest.fixture
def obs_short_life_dummy(obs_ch4_dummy):
    """
    Create example ObsData object with dummy data
     - Values match to obs_ch4_dummy
     - Species is HFO-1234zee
    Same values as obs_ch4_dummy fixture but updated species to be "HFO1234zee"
    """
    species = "HFO1234zee"
    return replace_species(obs_ch4_dummy, species)


@pytest.fixture
def footprint_short_life_dummy(footprint_radon_dummy):
    """
    Create example FootprintData object with dummy data
     - Values match to footprint_radon_dummy
     - Species is HFO1234zee
    Same values as footprint_radon_dummy fixture but updated species to be "HFO1234zee"
    """
    species = "HFO1234zee"
    return replace_species(footprint_radon_dummy, species)


@pytest.fixture
def bc_short_life_dummy(bc_ch4_dummy):
    """
    Create example BoundaryConditionsData object with dummy data
     - Values match to bc_ch4_dummy
     - Species is HFO1234zee
    Same values as bc_ch4_dummy fixture but updated species to be "HFO-1234zee"

    """
    species = "HFO1234zee"
    return replace_species(bc_ch4_dummy, species)


@pytest.fixture
def model_scenario_short_life_dummy(obs_short_life_dummy, footprint_short_life_dummy, bc_short_life_dummy):
    """Create ModelScenario with input dummy data for short-lived species 'HFO-1234zee'"""
    model_scenario = ModelScenario(
        obs=obs_short_life_dummy, footprint=footprint_short_life_dummy, bc=bc_short_life_dummy
    )

    return model_scenario


def test_modelled_baseline_short_life(
    model_scenario_short_life_dummy, footprint_short_life_dummy, bc_short_life_dummy
):
    """Test expected modelled baseline for short-lived species 'HFO-1234zee' with known dummy data"""
    modelled_baseline = model_scenario_short_life_dummy.calc_modelled_baseline(output_units=1)

    aligned_time = modelled_baseline["time"]
    assert aligned_time[0] == Timestamp("2012-01-01T00:00:00")
    assert aligned_time[-1] == Timestamp("2012-01-02T00:00:00")

    # Create expected value(s) for modelled_baseline for short-lived species
    # This includes a loss component vased on mean_age_particles for each direction
    # In our case:
    # - dummy particle_locations_* contains 2, 3, 4, 5 for n, e, s, w
    # - dummy boundary conditions, vmr_* contains 3, 4, 5, 6 at the correct time for n, e, s, w
    # - dummy mean age, mean_age_particles_* contains 11, 12, 13, 14 at the correct time for n, e, s, w
    time_slice = slice(aligned_time[0], aligned_time[-1])
    footprint = footprint_short_life_dummy.data.sel(time=time_slice)
    bc = bc_short_life_dummy.data.sel(time=time_slice)

    lifetime_days_HFO1234zee_jan = 56.3  # Should match value within acrg_species_info.json file
    lifetime_HFO1234zee_hrs = lifetime_days_HFO1234zee_jan * 24.0

    expected_modelled_baseline = calc_expected_baseline(footprint, bc, lifetime_hrs=lifetime_HFO1234zee_hrs)

    assert np.allclose(modelled_baseline, expected_modelled_baseline)


# %% Test generic dataset functions


@pytest.fixture
def flux_daily():
    """Fixture of simple 3D dataset with daily frequency"""

    time = pd.date_range("2012-01-01", "2012-02-01", freq="D")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), np.ones(shape))}, coords={"lat": lat, "lon": lon, "time": time}
    )

    return flux


@pytest.fixture
def flux_single_time():
    """Fixture of simple 3D dataset with unknown frequency (1 time point)"""

    time = pd.date_range("2012-01-01", "2012-01-31", freq="MS")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), np.ones(shape))}, coords={"lat": lat, "lon": lon, "time": time}
    )

    return flux


@pytest.fixture
def flux_daily_small_dim_diff():
    """Fixture of simple 3D dataset with small lat, lon difference and daily time frequency"""

    time = pd.date_range("2012-01-01", "2012-02-01", freq="D")
    small_mismatch = 1e-6
    lat = [1.0 + small_mismatch, 2.0 + small_mismatch]
    lon = [10.0 + small_mismatch, 20.0 + small_mismatch]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    flux_values = np.ones(shape) + 10.0

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), flux_values)}, coords={"lat": lat, "lon": lon, "time": time}
    )

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
    frequency_d = frequency.astype("timedelta64[D]")
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


def test_scenario_infer_flux_source_ch4():
    """
    Test ModelScenario can find the source of a flux
    if only a single flux matches the given metadata
    and source is in the flux metadata.
    """
    from openghg.objectstore import get_readable_buckets
    from openghg.retrieve import get_flux

    result = get_flux(species="ch4", domain="europe", source="waste")

    model_scenario = ModelScenario()
    model_scenario.add_flux(flux=result)

    # expect 'waste' to be found in flux metadata:
    assert "waste" in model_scenario.fluxes


def test_modelscenario_doesnt_error_empty_objectstore():
    clear_test_stores()

    site = "TAC"
    domain = "EUROPE"
    species = "co2"
    height = "185m"
    source_natural = "natural"
    start_date = "2017-07-01"
    end_date = "2017-07-07"

    scenario = ModelScenario(
        site=site,
        inlet=height,
        domain=domain,
        species=species,
        source=source_natural,
        start_date=start_date,
        end_date=end_date,
    )

    assert not scenario


# NOTE: the test store is modified by the last two tests
