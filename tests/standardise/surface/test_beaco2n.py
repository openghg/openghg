import pytest
from helpers import get_surface_datapath
from numpy import isnan
from openghg.standardise.surface import parse_beaco2n
from pandas import Timestamp

# def test_read_file():
#     beacon = BEACO2N()
#     filepath = get_surface_datapath(filename="Charlton_Community_Center.csv", source_format="BEACO2N")

#     result = beacon.read_file(
#         filepath=filepath,
#         site="charlton community centre",
#         network="BEACO2N",
#         inlet="5m",
#         sampling_period=1200,
#     )

#     pm_data = result["pm"]["data"]
#     co2_data = result["co2"]["data"]

#     pm_metadata = result["pm"]["metadata"]
#     co2_metadata = result["co2"]["metadata"]

#     pm_metadata = result["pm"]["metadata"]
#     co2_metadata = result["co2"]["metadata"]

#     pm_attrs = result["pm"]["attributes"]
#     co2_attrs = result["co2"]["attributes"]
#     pm_data.time[0] == Timestamp("2015-04-18T04:00:00")
#     pm_data["pm"][0] == 20.3
#     pm_data["pm_qc"][0] == 2.0
#     co2_data.time[0] == Timestamp("2015-04-18T04:00:00")
#     co2_data["co2"][0] == 410.4
#     co2_data["co2_qc"][0] == 2

#     assert pm_metadata == {
#         "units": "ug/m3",
#         "site": "CHARLTONCOMMUNITYCENTER",
#         "species": "pm",
#         "inlet": "NA",
#         "network": "beaco2n",
#         "sampling_period": "1200",
#     }
#     assert co2_metadata == {
#         "units": "ppm",
#         "site": "CHARLTONCOMMUNITYCENTER",
#         "species": "co2",
#         "inlet": "NA",
#         "network": "beaco2n",
#         "sampling_period": "1200",
#     }

#     assert pm_attrs == {
#         "deployed": "2017-01-24",
#         "id": 75,
#         "latitude": 29.688,
#         "longitude": -95.276,
#         "magl": 6.477,
#         "masl": 9.9,
#         "node_folder_id": 886,
#         "comment": "Retrieved from http://beacon.berkeley.edu/",
#     }

#     assert co2_attrs == {
#         "deployed": "2017-01-24",
#         "id": 75,
#         "latitude": 29.688,
#         "longitude": -95.276,
#         "magl": 6.477,
#         "masl": 9.9,
#         "node_folder_id": 886,
#         "comment": "Retrieved from http://beacon.berkeley.edu/",
#     }


def test_read_glasgow_valid_data():
    filepath = get_surface_datapath(filename="175_BELLAHOUSTONACADEMY.csv", source_format="BEACO2N")

    result = parse_beaco2n(
        filepath=filepath,
        site="BELLAHOUSTONACADEMY",
        network="BEACO2N",
        inlet="99m",
    )

    co2_data = result["co2"]["data"]

    assert sorted(list(result.keys())) == sorted(["pm", "co", "co2"])

    assert co2_data.time[0] == Timestamp("2021-07-15T12:00:00")
    assert co2_data.co2[0] == 410.7
    assert isnan(co2_data.co2_qc[0])


def test_read_glasgow_no_valid_data():
    filepath = get_surface_datapath(filename="171_UNIVERSITYOFSTRATHCLYDE.csv", source_format="BEACO2N")

    result = parse_beaco2n(
        filepath=filepath,
        site="UNIVERSITYOFSTRATHCLYDE",
        network="BEACO2N",
        inlet="99m",
    )

    assert not result


def test_incorrect_file_read_raises():
    filepath = get_surface_datapath(filename="incorrect_format.csv", source_format="BEACO2N")

    with pytest.raises(ValueError):
        parse_beaco2n(filepath=filepath, site="test", network="test", inlet="test")


def test_incorrect_site_raises():
    filepath = get_surface_datapath(filename="Unknown_site.csv", source_format="BEACO2N")

    with pytest.raises(ValueError):
        parse_beaco2n(filepath=filepath, site="test", network="test", inlet="test")
