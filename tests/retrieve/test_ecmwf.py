import pytest
import os
from openghg.retrieve.met import (
    retrieve_site_met,
    _create_dummy_dataset
)

@pytest.mark.skip(reason="Currently tests are not connected to the ECMWF CDS API")
def test_create_dummy_dataset(tmpdir):
    """
    Downloads a pre-determined ECMWF dataset
    """
    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": ['u_component_of_wind', 'v_component_of_wind'],
        "pressure_level": ['975', '1000'],
        "year": ["2014"],
        "month": ["5"],
        "day": [str(x).zfill(2) for x in range(1, 32)],
        "time": [f"{str(x).zfill(2)}:00" for x in range(0, 24)],
        "area": [  41.25, -124.25,   41.  , -124.  ],
    }

    dataset_savepath = os.path.join(
        tmpdir, f'Met_{"THD"}_{"AGAGE"}_{"2014"}{"5"}_temp.nc',
    )
    dataset = _create_dummy_dataset(request, tmpdir)

    ## add here dataset checks
    assert dataset is not None
    assert 'u' in dataset.variables
    assert 'v' in dataset.variables
    assert 'pressure_level' in dataset.dims
    assert 'valid_time' in dataset.dims
