import pytest
import numpy as np
from openghg.analyse import ModelScenario
from openghg.retrieve import get_obs_surface, get_footprint
# from openghg.retrieve import get_flux


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
    # source="anthro"

    obs_surface = get_obs_surface(site=site, 
                                  species=species,
                                  start_date=start_date,
                                  end_date=end_date,
                                  inlet=inlet,
                                  network=network)

    footprint = get_footprint(site=site, domain=domain, height=inlet,
               start_date=start_date, end_date=end_date)

    # TODO: Add extraction of flux data as well (get_flux) and add to ModelScenario call

    model_scenario = ModelScenario(obs=obs_surface, footprint=footprint)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None

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
    # source = "anthro"

    # TODO: Add extraction of flux data as well (get_flux) and add to ModelScenario call

    model_scenario = ModelScenario(site=site,
                                   species=species,
                                   inlet=inlet,
                                   network=network,
                                   domain=domain,
                                   start_date=start_date,
                                   end_date=end_date)

    assert model_scenario.obs is not None
    assert model_scenario.footprint is not None

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
