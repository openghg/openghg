import logging
import numpy as np
from pandas import Timestamp
import pytest

from openghg.standardise.surface import parse_openghg
from openghg.standardise.meta import metadata_default_keys
from helpers import get_datapath, parsed_surface_metachecker, check_cf_compliance

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file():
    filepath = get_datapath(filename="tac_co2_openghg.nc", data_type="OPENGHG")
    data = parse_openghg(filepath)

    assert "co2" in data

    output_co2 = data["co2"]
    data_co2 = output_co2["data"]

    time = data_co2["time"]
    assert time[0] == Timestamp("2012-07-30T17:03:08")
    assert time[-1] == Timestamp("2012-08-03T21:43:08")

    co2 = data_co2["co2"]
    assert np.isclose(co2[0], 385.25)
    assert np.isclose(co2[-1], 394.57)

    co2_variability = data_co2["co2_variability"]
    assert np.isclose(co2_variability[0], 0.843)
    assert np.isclose(co2_variability[-1], 0.682)
    
    attributes = data_co2.attrs

    metadata_keys = metadata_default_keys()
    expected_metadata = {param: value for param, value in attributes.items() if param in metadata_keys}

    metadata = output_co2["metadata"]
    assert metadata.items() >= expected_metadata.items()

# TODO: Add tests
# - Check this can read in a file if outlined keywords specified manually
#   - for this create file that doesn't contain any attributes
#   - should read from inputs and from site info etc. files
# - Check this can read in a file if *all* keywords specified manually
#   - for this create file for *new* site and with no attributes
# - Check ObsSurface.read_file() can successfully run this
#   - may need to add to a different test file
# - Check process_obs() can also successfully run this
#   - may need to add to a different test file


#%% Compliance checks for processed data for this standardisation method

@pytest.fixture(scope="session")
def openghg_data():
    filepath = get_datapath(filename="tac_co2_openghg.nc", data_type="OPENGHG")
    data = parse_openghg(data_filepath=filepath)
    return data


def test_data_metachecker(openghg_data):
    parsed_surface_metachecker(data=openghg_data)

@pytest.mark.cfchecks
def test_openghg_cf_compliance(openghg_data):
    co2_data = openghg_data["co2"]["data"]
    assert check_cf_compliance(dataset=co2_data)
