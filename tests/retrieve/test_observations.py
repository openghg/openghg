from openghg.retrieve._access import get_obs_surface
from openghg.dataobjects._obsdata import ObsData

def test_get_obs_surface_one_inlet():
    '''
    Test we can access site and species data without needing to specify the inlet
    when this is not needed.
    '''
    # Rely on data already loaded from conftest.py and in local object store
    data = get_obs_surface(site="tac", species="ch4")

    assert isinstance(data, ObsData)
