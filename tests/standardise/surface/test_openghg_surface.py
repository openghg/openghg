import logging

import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.meta import attributes_default_keys
from openghg.standardise.surface import parse_openghg


mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file_no_attr():
    """
    Test file in correct OpenGHG format but without attributes can be parsed
    as long as the missing and necessary attribute values are provided.
    Note: this will extract station details from values pre-defined within
    the 'site_info.json' file.
    """
    filepath = get_surface_datapath(filename="tac_co2_no_attr_openghg.nc", source_format="OPENGHG")

    param = {}

    # Needed variables to store the data
    param["site"] = "tac"
    param["species"] = "co2"
    param["network"] = "DECC"
    param["inlet"] = "54m"
    param["instrument"] = "crds"
    param["sampling_period"] = "60s"
    param["calibration_scale"] = "wmo2000"
    param["data_owner"] = "Simon O'Doherty"
    param["data_owner_email"] = "s.odoherty@bristol.ac.uk"

    # TODO: May need to update this if we decide to update this list.
    # Note: other necessary attributes inferred from pre-existing site info details

    data = parse_openghg(filepath, **param)

    output_co2 = data["co2"]
    data_co2 = output_co2["data"]
    attributes = data_co2.attrs

    assert attributes != {}
    assert attributes["site"] == param["site"]
    assert attributes["species"] == param["species"]

    attribute_keys = attributes_default_keys()
    expected_metadata = {param: value for param, value in attributes.items() if param in attribute_keys}

    metadata = output_co2["metadata"]
    assert metadata.items() >= expected_metadata.items()


# TODO: Add tests for new site (i.e. no current data stored)
# - when/if possible!
# - [] Check this can read in a file if *all* keywords specified manually
#   - for this create file for *new* site and with no attributes


# %% Compliance checks for processed data for this standardisation method


@pytest.fixture(scope="session")
def openghg_data():
    filepath = get_surface_datapath(filename="tac_co2_openghg.nc", source_format="OPENGHG")
    data = parse_openghg(filepath=filepath)
    return data


def test_data_metachecker(openghg_data):
    parsed_surface_metachecker(data=openghg_data)


@pytest.mark.xfail(reason="broken link to cf conventions")
@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_openghg_cf_compliance(openghg_data):
    co2_data = openghg_data["co2"]["data"]
    assert check_cf_compliance(dataset=co2_data)
