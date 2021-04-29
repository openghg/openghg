import logging
import os
from pathlib import Path

from openghg.modules import CRDS
from openghg.objectstore import get_local_bucket
from openghg.processing import assign_attributes
# import tempfile
# from cfchecker import CFChecker

# flake8: noqa

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")

def test_crds_attributes():
    get_local_bucket(empty=True)

    crds = CRDS()

    filepath = get_datapath(filename="tac.picarro.1minute.100m.test.dat", data_type="CRDS")

    combined = crds.read_data(data_filepath=filepath, site="tac", network="DECC")

    combined_attributes = assign_attributes(data=combined, site="tac")

    # for key in combined_attributes:
    #     ds = combined_attributes[key]["data"]
    #     ds.to_netcdf(f"/tmp/testfile_{key}.nc")

    ch4_data = combined_attributes["ch4"]["data"]
    co2_data = combined_attributes["co2"]["data"]

    ch4_attr = ch4_data.attrs
    co2_attr = co2_data.attrs

    ch4_attr_complete = ch4_attr.copy()
    co2_attr_complete = co2_attr.copy()

    del ch4_attr["File created"]
    del co2_attr["File created"]
    del ch4_attr["species"]
    del co2_attr["species"]
    del ch4_attr["Calibration_scale"]
    del co2_attr["Calibration_scale"]
    del ch4_attr["data_owner_email"]
    del co2_attr["data_owner_email"]
    del ch4_attr["data_owner"]
    del co2_attr["data_owner"]

    global_attributes = {
        "inlet_height_magl": "100m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        'Processed by': 'OpenGHG_Cloud',
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "Tacolneston Tower, UK",
        "station_height_masl": 50.0,
    }

    assert ch4_attr == global_attributes
    assert co2_attr == global_attributes

    assert ch4_attr_complete["species"] == "ch4"
    assert co2_attr_complete["species"] == "co2"

    # Check the individual variables attributes

    time_attributes = {
        "label": "left",
        "standard_name": "time",
        "comment": "Time stamp corresponds to beginning of sampling period. Time since midnight UTC of reference date. Note that sampling periods are approximate.",
        "sampling_period_seconds": 60,
    }

    assert ch4_data.time.attrs == time_attributes
    assert co2_data.time.attrs == time_attributes

    # Check individual variables
    assert ch4_data["ch4"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air",
        "units": "1e-9",
    }
    assert ch4_data["ch4_variability"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air_variability",
        "units": "1e-9",
    }
    assert ch4_data["ch4_number_of_observations"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air_number_of_observations"
    }

    assert co2_data["co2"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air",
        "units": "1e-6",
    }
    assert co2_data["co2_variability"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air_variability",
        "units": "1e-6",
    }
    assert co2_data["co2_number_of_observations"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air_number_of_observations"
    }

    # with tempfile.TemporaryDirectory() as tmpdir:
    #     files = []
    #     # Write files to tmpdir and call cfchecker
    #     for key in combined_attributes:
    #         dataset = combined_attributes[key]["data"]
    #         filepath = Path(tmpdir).joinpath(f"test_{key}.nc")
    #         dataset.to_netcdf(filepath)
    #         files.append(filepath)

    #     checker = CFChecker(version="1.6", silent=True)

    #     for f in files:
    #         checker.checker(str(f))

    #         results = checker.get_total_counts()

    #         assert results["FATAL"] == 0
    #         assert results["ERROR"] == 0
    #         assert results["WARN"] < 3


# 2020-03-30 15:03:50
# TODO - expand these tests

# def test_old_new_attrs():
#     # from openghg.processing import acrg_attributes
#     from acrg_obs.utils import attributes

#     _ = get_local_bucket(empty=True)

#     crds = CRDS.load()

#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filename = "tac.picarro.1minute.100m.test.dat"

#     filepath = os.path.join(dir_path, test_data, filename)

#     filepath = Path(filepath)

#     combined = crds.read_data(data_filepath=filepath, site="tac")

#     ch4_data_orig = combined["ch4"]["data"]


#     # # Get my attributes
#     site_attributes = combined["ch4"]["attributes"]

#     ch4_data = get_attributes(ds=ch4_data_orig, species="ch4", site="tac",
#   global_attributes=site_attributes, units="ppm", scale="test_scale")

#     ch4_data_acrg = attributes(ds=ch4_data_orig, species="ch4", global_attributes=site_attributes,
#        site="tac", units="ppm", scale="test_scale")

#     # assert False

#     del ch4_data.attrs["Processed by"]
#     del ch4_data_acrg.attrs["Processed by"]
#     del ch4_data.attrs["File created"]
#     del ch4_data_acrg.attrs["File created"]

#     print(ch4_data.attrs, "\n\n\n")

#     print(ch4_data_acrg.attrs)

#     assert ch4_data.attrs == ch4_data_acrg.attrs
