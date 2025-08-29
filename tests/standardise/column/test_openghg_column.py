import logging

import numpy as np
import pytest
from helpers import get_column_datapath  # , parsed_surface_metachecker, check_cf_compliance
from openghg.standardise.column import parse_openghg, parse_tccon
from openghg.standardise.meta import attributes_default_keys
from pandas import Timestamp

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_parse_openghg():
    """
    Test file in OpenGHG format (variables and attributes) can be
    correctly parsed.
    """
    filepath = get_column_datapath(filename="gosat-fts_gosat_20170318_ch4-column.nc")

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    data = parse_openghg(
        filepath,
        satellite=satellite,
        domain=domain,
        species=species,
    )

    assert "ch4" in data

    assert "exposure_id" not in data
    assert "id" not in data

    output_ch4 = data["ch4"]
    data_ch4 = output_ch4["data"]

    time = data_ch4["time"]
    assert time[0] == Timestamp("2017-03-18T15:32:54")
    assert time[-1] == Timestamp("2017-03-18T17:22:23")

    xch4 = data_ch4["xch4"]
    assert np.isclose(xch4[0], 1844.2019)
    assert np.isclose(xch4[-1], 1762.8855)

    expected_metadata = {
        "satellite": "GOSAT",
        "species": "ch4",
        "domain": domain,
        "selection": domain,
        "instrument": "TANSO-FTS",
        "data_owner": "University of Leicester, Rob Parker",
        "data_owner_email": "rjp23@leicester.ac.uk",
    }

    metadata = output_ch4["metadata"]
    assert metadata.items() >= expected_metadata.items()

    attributes = data_ch4.attrs
    assert attributes.items() >= expected_metadata.items()


def test_parse_tccon():
    """
    Test file in TCCON format (variables and attributes) can be
    correctly parsed.
    """
    filepath = get_column_datapath(filename="hw20230402_20230402.public.qc.nc")

    domain = "EUROPE"
    species = "ch4"
    pressure_weights_method = "pressure_weight"

    data = parse_tccon(
        filepath,
        pressure_weights_method=pressure_weights_method,
        domain=domain,
        species=species,
    )

    assert "ch4" in data

    assert "integration_operator" not in data
    assert "ak_altitude" not in data
    assert "prior_altitude" not in data

    output_ch4 = data["ch4"]
    data_ch4 = output_ch4["data"]

    time = data_ch4["time"]
    assert time[0] == Timestamp("2023-04-02T15:00:00")
    assert time[1] == Timestamp("2023-04-02T16:00:00")

    xch4 = data_ch4["xch4"].values
    assert np.isclose(xch4[0], 1888.025)
    assert np.isclose(xch4[-1], 1889.0175)

    expected_metadata = {
        "species": "ch4",
        "domain": domain,
        "inlet": "column",
        "site": "THW",
        "network": "TCCON",
        "platform": "site",
        "longitude": "-1.320",
        "latitude": "51.570",
        "data_owner": "Damien Weidmann",
        "data_owner_email": "<damien.weidmann@stfc.ac.uk>",
        "file_start_date": "2023-04-02",
        "file_end_date": "2023-04-02",
        "file_format_version": "2020.B",
        "data_revision": "R0",
        "description": "TCCON data standardised from hw20230402_20230402.public.qc.nc, with the pressure weights estimated via 'pressure_weight'.",
        "calibration_scale": "WMO CH4 X2004",
    }

    metadata = output_ch4["metadata"]
    assert metadata.items() >= expected_metadata.items()

    expected_attributes = expected_metadata
    expected_attributes.update({"longitude": "-1.320", "latitude": "51.570"})
    attributes = data_ch4.attrs
    assert attributes.items() >= expected_metadata.items()


def test_parse_openghg_column_multi_file():
    """
    Test the parser for column is able to accept multiple files and concatenate them.
    """

    data_path_1 = get_column_datapath(filename="gosat-fts_gosat_20160101_ch4-column.nc")
    data_path_2 = get_column_datapath(filename="gosat-fts_gosat_20170318_ch4-column.nc")

    filepath = [data_path_1, data_path_2]

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    data = parse_openghg(
        filepath,
        satellite=satellite,
        domain=domain,
        species=species,
    )

    assert "ch4" in data

    output_ch4 = data["ch4"]
    data_ch4 = output_ch4["data"]

    time = data_ch4["time"]
    assert time[0] == Timestamp("2016-01-01T14:59:12.5")
    assert time[-1] == Timestamp("2017-03-18T17:22:23")

    xch4 = data_ch4["xch4"]
    assert np.isclose(xch4[0], 1810.89001465)
    assert np.isclose(xch4[-1], 1762.8855)


# def test_read_file_no_attr():
#     """
#     Test file in correct OpenGHG format but without attributes can be parsed
#     as long as the missing and necessary attribute values are provided.
#     Note: this will extract station details from values pre-defined within
#     the 'site_info.json' file.
#     """
#     filepath = get_surface_datapath(filename="tac_co2_no_attr_openghg.nc", data_type="OPENGHG")

#     param = {}

#     # Needed variables to store the data
#     param["site"] = "tac"
#     param["species"] = "co2"
#     param["network"] = "DECC"
#     param["inlet"] = "54m"
#     param["instrument"] = "crds"
#     param["sampling_period"] = "60s"
#     param["calibration_scale"] = "wmo2000"
#     param["data_owner"] = "Simon O'Doherty"
#     param["data_owner_email"] = "s.odoherty@bristol.ac.uk"

#     # TODO: May need to update this if we decide to update this list.
#     # Note: other necessary attributes inferred from pre-existing site info details

#     data = parse_openghg(filepath, **param)

#     output_co2 = data["co2"]
#     data_co2 = output_co2["data"]
#     attributes = data_co2.attrs

#     assert attributes != {}
#     assert attributes["site"] == param["site"]
#     assert attributes["species"] == param["species"]

#     metadata_keys = attributes_default_keys()
#     expected_metadata = {param: value for param, value in attributes.items() if param in metadata_keys}

#     metadata = output_co2["metadata"]
#     assert metadata.items() >= expected_metadata.items()


# # TODO: Add tests for new site (i.e. no current data stored)
# # - when/if possible!
# # - [] Check this can read in a file if *all* keywords specified manually
# #   - for this create file for *new* site and with no attributes


# #%% Compliance checks for processed data for this standardisation method

# @pytest.fixture(scope="session")
# def openghg_data():
#     filepath = get_surface_datapath(filename="tac_co2_openghg.nc", data_type="OPENGHG")
#     data = parse_openghg(data_filepath=filepath)
#     return data


# def test_data_metachecker(openghg_data):
#     parsed_surface_metachecker(data=openghg_data)

# @pytest.mark.cfchecks
# def test_openghg_cf_compliance(openghg_data):
#     co2_data = openghg_data["co2"]["data"]
#     assert check_cf_compliance(dataset=co2_data)
